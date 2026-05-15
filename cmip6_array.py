# -*- coding: utf-8 -*-
"""
Download and process NEX-GDDP-CMIP6 daily climate data from the NASA S3 bucket.

Source : https://www.nasa.gov/nex-gddp-cmip6/
         s3://nex-gddp-cmip6/NEX-GDDP-CMIP6/  (public, anonymous access)

Designed to run as a SLURM job array (one model per task) via run_cmip6_array.sh.
Can also be run standalone:

    python cmip6_array.py 25 \\
        --models MIROC6 \\
        --hist-start 1980 --hist-end 2014 \\
        --ssp-start  2015 --ssp-end  2100 \\
        --lat-min 6.1 --lat-max 12.8 --lon-min 0.6 --lon-max 4.0 \\
        --output-dir /path/to/output \\
        --max-workers 8

Unit conversions applied on output:
  pr              : kg/m2/s  -> mm/day       (x 86400)
  tas/tasmax/tasmin : K      -> degC          (- 273.15)
  sfcWind         : m/s 10m  -> m/s 2m       (x 0.75, log-profile correction)
  rsds            : W/m2     -> MJ/m2/day    (x 0.0864)

Performance note:
  If s3fs is installed, datasets are opened lazily directly from S3 and only
  the spatial chunks covering the bounding box are transferred (~1-3 MB instead
  of 80-150 MB for a small bbox like Benin). Otherwise files are downloaded in
  full via HTTP with automatic retry.
  s3fs is initialised inside each worker process (after fork) to avoid
  asyncio fork-safety issues with ProcessPoolExecutor.

Dependencies: xarray, netCDF4, h5netcdf, s3fs, requests, numpy.
              See environment_cmip6.yml to recreate the environment.
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
from pathlib import Path
import xarray as xr
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Optional
import time
import sys

# Session HTTP par worker: reuse des connexions TCP + retry automatique
_retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
_session = requests.Session()
_session.mount('https://', HTTPAdapter(max_retries=_retry, pool_maxsize=4))

# s3fs disponible ? (verifie au niveau module, mais _fs cree APRES le fork)
try:
    import s3fs as _s3fs_mod
    _S3FS_AVAILABLE = True
except ImportError:
    _S3FS_AVAILABLE = False

_fs = None
_USE_S3FS = False

def _init_worker():
    """Initialise s3fs DANS chaque worker, apres le fork — evite le probleme fork-safety."""
    global _fs, _USE_S3FS
    if _S3FS_AVAILABLE:
        _fs = _s3fs_mod.S3FileSystem(anon=True)
        _USE_S3FS = True

# ==================== CONFIGURATION ====================

# Defauts — surchargés par les arguments CLI
_DEFAULT_EXPERIMENTS = "historical,ssp126,ssp245,ssp370,ssp585"
_DEFAULT_VARIABLES   = "tas,tasmax,tasmin,hurs,sfcWind,rsds,pr"

# ==================== VARIANTES COMMUNES ====================
# Variants les plus courants pour chaque modele
DEFAULT_VARIANTS = {
    "ACCESS-CM2":        "r1i1p1f1",
    "ACCESS-ESM1-5":     "r1i1p1f1",
    "BCC-CSM2-MR":       "r1i1p1f1",
    "CanESM5":           "r1i1p1f1",
    "CESM2":             "r4i1p1f1",
    "CESM2-WACCM":       "r3i1p1f1",
    "CMCC-CM2-SR5":      "r1i1p1f1",
    "CMCC-ESM2":         "r1i1p1f1",
    "CNRM-CM6-1":        "r1i1p1f2",
    "CNRM-ESM2-1":       "r1i1p1f2",
    "EC-Earth3":         "r1i1p1f1",
    "EC-Earth3-Veg-LR":  "r1i1p1f1",
    "FGOALS-g3":         "r3i1p1f1",
    "GFDL-CM4":          "r1i1p1f1",
    "GFDL-CM4_gr2":      "r1i1p1f1",
    "GFDL-ESM4":         "r1i1p1f1",
    "GISS-E2-1-G":       "r1i1p1f2",
    "HadGEM3-GC31-LL":   "r1i1p1f3",
    "HadGEM3-GC31-MM":   "r1i1p1f3",
    "IITM-ESM":          "r1i1p1f1",
    "INM-CM4-8":         "r1i1p1f1",
    "INM-CM5-0":         "r1i1p1f1",
    "IPSL-CM6A-LR":      "r1i1p1f1",
    "KACE-1-0-G":        "r1i1p1f1",
    "KIOST-ESM":         "r1i1p1f1",
    "MIROC6":            "r1i1p1f1",
    "MIROC-ES2L":        "r1i1p1f2",
    "MPI-ESM1-2-HR":     "r1i1p1f1",
    "MPI-ESM1-2-LR":     "r1i1p1f1",
    "MRI-ESM2-0":        "r1i1p1f1",
    "NESM3":             "r1i1p1f1",
    "NorESM2-LM":        "r1i1p1f1",
    "NorESM2-MM":        "r1i1p1f1",
    "TaiESM1":           "r1i1p1f1",
    "UKESM1-0-LL":       "r1i1p1f2",
}


# ==================== GRID LABELS PAR MODELE ====================
# Grid label exact tel que disponible dans le bucket S3 NEX-GDDP-CMIP6
MODEL_GRID_LABELS = {
    "ACCESS-CM2":        "gn",
    "ACCESS-ESM1-5":     "gn",
    "BCC-CSM2-MR":       "gn",
    "CanESM5":           "gn",
    "CESM2":             "gn",
    "CESM2-WACCM":       "gn",
    "CMCC-CM2-SR5":      "gn",
    "CMCC-ESM2":         "gn",
    "CNRM-CM6-1":        "gr",
    "CNRM-ESM2-1":       "gr",
    "EC-Earth3":         "gr",
    "EC-Earth3-Veg-LR":  "gr",
    "FGOALS-g3":         "gn",
    "GFDL-CM4":          "gr1",
    "GFDL-CM4_gr2":      "gr2",
    "GFDL-ESM4":         "gr1",
    "GISS-E2-1-G":       "gn",
    "HadGEM3-GC31-LL":   "gn",
    "HadGEM3-GC31-MM":   "gn",
    "IITM-ESM":          "gn",
    "INM-CM4-8":         "gr1",
    "INM-CM5-0":         "gr1",
    "IPSL-CM6A-LR":      "gr",
    "KACE-1-0-G":        "gr",
    "KIOST-ESM":         "gr1",
    "MIROC6":            "gn",
    "MIROC-ES2L":        "gn",
    "MPI-ESM1-2-HR":     "gn",
    "MPI-ESM1-2-LR":     "gn",
    "MRI-ESM2-0":        "gn",
    "NESM3":             "gn",
    "NorESM2-LM":        "gn",
    "NorESM2-MM":        "gn",
    "TaiESM1":           "gn",
    "UKESM1-0-LL":       "gn",
}

# Liste complete des 35 modeles disponibles (ordre de DEFAULT_VARIANTS)
_COMPLETE_MODELS = list(DEFAULT_VARIANTS.keys())

# ==================== FONCTIONS ====================

def get_variant_for_model(model: str, experiment: str) -> str:
    """Retourne le variant par defaut pour un modele donne"""
    return DEFAULT_VARIANTS.get(model, "r1i1p1f1")

def download_and_process_file(
    model: str,
    experiment: str,
    variant: str,
    variable: str,
    year: int,
    bbox: Optional[Dict],
    output_base_dir: Path
) -> tuple:
    """Download, crop, convert units and save one NetCDF file.

    Opens the file directly from S3 with lazy loading (s3fs) when available,
    so that only the chunks covering the bounding box are transferred.
    Falls back to a full HTTP download otherwise.
    Crop is applied before unit conversion to minimise data loaded into memory.

    Parameters
    ----------
    model : str
        CMIP6 model name (key in DEFAULT_VARIANTS).
    experiment : str
        Experiment id, e.g. 'historical', 'ssp245'.
    variant : str
        Variant label, e.g. 'r1i1p1f1'.
    variable : str
        Variable name, e.g. 'tas', 'pr'.
    year : int
        Year to process.
    bbox : dict or None
        {'lat_min', 'lat_max', 'lon_min', 'lon_max'} for spatial subset,
        or None for global download.
    output_base_dir : Path
        Root output directory; files saved under model/experiment/variable/.

    Returns
    -------
    tuple
        (status, year, model, experiment, variable, size_mb, error_msg)
        where status is 'success', 'exists', or 'error'.
    """

    output_dir = output_base_dir / model / experiment / variable
    output_dir.mkdir(parents=True, exist_ok=True)

    grid_label = MODEL_GRID_LABELS.get(model, 'gn')
    filename = f"{variable}_day_{model}_{experiment}_{variant}_{grid_label}_{year}_v2.0.nc"
    output_filename = (f"{variable}_day_{model}_{experiment}_{variant}_{grid_label}_{year}_cropped.nc"
                       if bbox else filename)
    output_path = output_dir / output_filename

    # Verifier si le fichier existe deja (et est valide — > 1 KB)
    pattern = (f"{variable}_day_{model}_{experiment}_{variant}_*_{year}_cropped.nc"
               if bbox else f"{variable}_day_{model}_{experiment}_{variant}_*_{year}_v2.0.nc")
    existing = [p for p in output_dir.glob(pattern) if p.stat().st_size > 1024]
    if existing:
        return ('exists', year, model, experiment, variable, existing[0].stat().st_size / (1024*1024), None)
    # Supprimer les stubs corrompus (<= 1 KB) laisses par un run precedent echoue
    for stub in output_dir.glob(pattern):
        if stub.stat().st_size <= 1024:
            stub.unlink(missing_ok=True)

    temp_path = None
    try:
        # Ouverture du dataset: s3fs (lecture partielle) ou requests (telechargement complet)
        if _USE_S3FS:
            s3_path = f"nex-gddp-cmip6/NEX-GDDP-CMIP6/{model}/{experiment}/{variant}/{variable}/{filename}"
            # Ouvrir dans un context manager et materialiser en memoire AVANT de fermer la connexion S3.
            # xarray ouvre les datasets de facon lazy : si on ne charge pas maintenant,
            # to_netcdf() essaie de lire depuis S3 apres que le contexte soit ferme ou corrompu.
            with _fs.open(s3_path, 'rb', cache_type='blockcache') as s3f:
                ds_raw = xr.open_dataset(s3f, engine='h5netcdf')
                if float(ds_raw.lon.max()) > 180:
                    ds_raw = ds_raw.assign_coords(lon=(((ds_raw.lon + 180) % 360) - 180)).sortby('lon')
                if bbox:
                    ds_raw = ds_raw.sel(
                        lat=slice(bbox['lat_min'], bbox['lat_max']),
                        lon=slice(bbox['lon_min'], bbox['lon_max'])
                    )
                ds_raw.load()  # materialise les chunks bbox depuis S3
                # Copie profonde : cree un dataset entierement en memoire,
                # sans aucune reference au backend h5netcdf/s3fs.
                # Sans ca, to_netcdf() repasse par le backend et declenche un HDF error.
                ds = ds_raw.copy(deep=True)
                ds_raw.close()
        else:
            base_url = (f"https://nex-gddp-cmip6.s3.us-west-2.amazonaws.com/"
                        f"NEX-GDDP-CMIP6/{model}/{experiment}/{variant}/{variable}/")
            with tempfile.NamedTemporaryFile(delete=False, suffix='.nc') as tmp:
                temp_path = tmp.name
                response = _session.get(base_url + filename, stream=True, timeout=120)
                response.raise_for_status()
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        tmp.write(chunk)
            ds = xr.open_dataset(temp_path, engine='h5netcdf')
            if float(ds.lon.max()) > 180:
                ds = ds.assign_coords(lon=(((ds.lon + 180) % 360) - 180)).sortby('lon')
            if bbox:
                ds = ds.sel(
                    lat=slice(bbox['lat_min'], bbox['lat_max']),
                    lon=slice(bbox['lon_min'], bbox['lon_max'])
                )

        # Conversions d'unites
        if variable == 'pr':
            ds[variable] = ds[variable] * 86400
            ds[variable].attrs['units'] = 'mm/day'
        elif variable in ['tas', 'tasmax', 'tasmin']:
            ds[variable] = ds[variable] - 273.15
            ds[variable].attrs['units'] = 'degree C'
        elif variable == 'sfcWind':
            ds[variable] = ds[variable] * 0.75
            ds[variable].attrs['height'] = '2m'
        elif variable == 'rsds':
            ds[variable] = ds[variable] * 0.0864
            ds[variable].attrs['units'] = 'MJ/m2/day'

        # h5netcdf engine pour eviter le conflit de libhdf5 entre h5py et netcdf4-python
        encoding = {var: {'compression': 'gzip', 'compression_opts': 4} for var in ds.data_vars}
        ds.to_netcdf(output_path, engine='h5netcdf', encoding=encoding)
        ds.close()
        del ds

        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)

        return ('success', year, model, experiment, variable, None, None)

    except requests.exceptions.HTTPError as e:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)
        return ('error', year, model, experiment, variable, None, f"HTTP Error ({grid_label}): {str(e)}")
    except Exception as e:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)
        return ('error', year, model, experiment, variable, None, f"Error ({grid_label}): {str(e)}")

# ==================== EXECUTION ====================

if __name__ == "__main__":

    import argparse
    parser = argparse.ArgumentParser(description="Telecharger NEX-GDDP-CMIP6 pour un modele")
    parser.add_argument("model_index",    type=int,
                        help="Index du modele dans la liste --models (= $SLURM_ARRAY_TASK_ID)")
    parser.add_argument("--models",       default="ALL",
                        help="'ALL' ou liste de noms separee par virgule (ex: MIROC6,CanESM5)")
    parser.add_argument("--hist-start",   type=int,   default=1980)
    parser.add_argument("--hist-end",     type=int,   default=2014)
    parser.add_argument("--ssp-start",    type=int,   default=2015)
    parser.add_argument("--ssp-end",      type=int,   default=2100)
    parser.add_argument("--experiments",  default=_DEFAULT_EXPERIMENTS,
                        help="Experiences separees par virgule")
    parser.add_argument("--variables",    default=_DEFAULT_VARIABLES,
                        help="Variables separees par virgule")
    parser.add_argument("--lat-min",      type=float, default=None)
    parser.add_argument("--lat-max",      type=float, default=None)
    parser.add_argument("--lon-min",      type=float, default=None)
    parser.add_argument("--lon-max",      type=float, default=None)
    parser.add_argument("--no-bbox",      action="store_true",
                        help="Desactiver le decoupage spatial (telechargement global)")
    parser.add_argument("--max-workers",  type=int,   default=8)
    parser.add_argument("--output-dir",   default="/outputdir/nex_gddp_cmip6_data",
                        help="Dossier de sortie racine")
    args = parser.parse_args()

    # Resoudre la liste de modeles
    if args.models.upper() == "ALL":
        resolved_models = _COMPLETE_MODELS
    else:
        resolved_models = [m.strip() for m in args.models.split(",")]
        unknown = [m for m in resolved_models if m not in DEFAULT_VARIANTS]
        if unknown:
            print(f"Erreur: modeles inconnus: {unknown}", flush=True)
            print(f"Modeles disponibles: {_COMPLETE_MODELS}", flush=True)
            sys.exit(1)

    if args.model_index < 0 or args.model_index >= len(resolved_models):
        print(f"Erreur: Index {args.model_index} hors limites (0-{len(resolved_models)-1})", flush=True)
        sys.exit(1)

    # Validation bbox
    if not args.no_bbox:
        if any(v is None for v in [args.lat_min, args.lat_max, args.lon_min, args.lon_max]):
            print("Erreur: --lat-min, --lat-max, --lon-min, --lon-max sont requis", flush=True)
            print("       (ou utiliser --no-bbox pour un telechargement global)", flush=True)
            sys.exit(1)
        bbox = {
            'lat_min': args.lat_min, 'lat_max': args.lat_max,
            'lon_min': args.lon_min, 'lon_max': args.lon_max
        }
    else:
        bbox = None

    model_index = args.model_index
    models      = [resolved_models[model_index]]
    hist_start  = args.hist_start
    hist_end    = args.hist_end
    ssp_start   = args.ssp_start
    ssp_end     = args.ssp_end
    experiments = args.experiments.split(",")
    variables   = args.variables.split(",")
    max_workers = args.max_workers

    print("="*80, flush=True)
    print("="*80, flush=True)
    print(f"Model index: {model_index}", flush=True)
    print(f"Modele: {models[0]}", flush=True)
    print(f"Experiences: {', '.join(experiments)}", flush=True)
    print(f"Variables: {', '.join(variables)}", flush=True)
    if bbox:
        print(f"Bounding Box: lat [{bbox['lat_min']}, {bbox['lat_max']}], lon [{bbox['lon_min']}, {bbox['lon_max']}]", flush=True)
    else:
        print("Bounding Box: desactive (telechargement global)", flush=True)
    print(f"Parallelisation: {max_workers} workers", flush=True)
    print(f"Annees historical: {hist_start}-{hist_end}", flush=True)
    print(f"Annees SSP: {ssp_start}-{ssp_end}", flush=True)
    print("="*80, flush=True)

    # Dossier de sortie
    output_base_dir = Path(args.output_dir)
    output_base_dir.mkdir(parents=True, exist_ok=True)

    # Pre-filtrer les fichiers deja existants avant soumission aux workers
    tasks = []
    n_exists = 0
    for model in models:
        for experiment in experiments:
            if experiment == "historical":
                start_year, end_year = hist_start, hist_end
            else:
                start_year, end_year = ssp_start, ssp_end
            variant = get_variant_for_model(model, experiment)
            for variable in variables:
                output_dir = output_base_dir / model / experiment / variable
                for year in range(start_year, end_year + 1):
                    pattern = (f"{variable}_day_{model}_{experiment}_{variant}_*_{year}_cropped.nc"
                               if bbox else
                               f"{variable}_day_{model}_{experiment}_{variant}_*_{year}_v2.0.nc")
                    if output_dir.exists() and list(output_dir.glob(pattern)):
                        n_exists += 1
                    else:
                        tasks.append((model, experiment, variant, variable, year, bbox, output_base_dir))

    print(f"\nFichiers deja existants (ignores): {n_exists}", flush=True)
    print(f"Taches a lancer: {len(tasks)}", flush=True)
    print(f"Demarrage du telechargement...\n", flush=True)

    # Statistiques
    stats = {
        'success': 0,
        'exists': 0,
        'error': 0,
        'errors_list': []
    }

    start_time = time.time()

    if _S3FS_AVAILABLE:
        print("s3fs disponible: lecture directe depuis S3 activee dans les workers", flush=True)

    with ProcessPoolExecutor(max_workers=max_workers, initializer=_init_worker) as executor:
        # Soumettre toutes les taches
        future_to_task = {
            executor.submit(download_and_process_file, *task): task 
            for task in tasks
        }
        
        for i, future in enumerate(as_completed(future_to_task), 1):
            task = future_to_task[future]
            result = future.result()
            
            status, year, model, experiment, variable, size, extra = result
            
            if status == 'success':
                stats['success'] += 1
                print(f"[{i}/{len(tasks)}] OK  {model}/{experiment}/{variable}/{year}", flush=True)
            
            elif status == 'exists':
                stats['exists'] += 1
                print(f"[{i}/{len(tasks)}]  {model}/{experiment}/{variable}/{year}", flush=True)
            
            elif status == 'error':
                stats['error'] += 1
                error_msg = extra
                stats['errors_list'].append((model, experiment, variable, year, error_msg))
                print(f"[{i}/{len(tasks)}]  {model}/{experiment}/{variable}/{year}: ERREUR", flush=True)
                print(f"    {error_msg[:100]}...", flush=True)

    elapsed_time = time.time() - start_time

    # ==================== RESUME ====================
    print("\n" + "="*80, flush=True)
    print(f"RESUME DU TELECHARGEMENT POUR {models[0]}", flush=True)
    print("="*80, flush=True)
    print(f"Temps total: {elapsed_time/60:.1f} minutes", flush=True)
    print(f"Fichiers telecharges avec succes: {stats['success']}", flush=True)
    print(f"Fichiers deja existants: {n_exists + stats['exists']}", flush=True)
    print(f"Erreurs: {stats['error']}", flush=True)
    print(f"Total traite: {stats['success'] + stats['exists']}/{len(tasks)}", flush=True)

    if stats['errors_list']:
        print(f"\nERREURS DETAILLEES ({len(stats['errors_list'])}):", flush=True)
        for model, exp, var, year, error in stats['errors_list'][:10]: 
            print(f"  - {model}/{exp}/{var}/{year}: {error[:80]}", flush=True)
        if len(stats['errors_list']) > 10:
            print(f"  ... et {len(stats['errors_list']) - 10} autres erreurs", flush=True)

    print(f"\nFichiers sauvegardes dans: {output_base_dir.absolute()}", flush=True)
    print("="*80, flush=True)
