# NEX-GDDP-CMIP6 Downloader

SLURM job-array pipeline to download and pre-process daily climate data from the
[NASA NEX-GDDP-CMIP6](https://www.nasa.gov/nex-gddp-cmip6/) dataset.

## Features

- **35 CMIP6 models**, 5 experiments (`historical`, `ssp126/245/370/585`), 7 variables
- **Spatial subsetting** — bounding box crop applied *before* any data loads into memory
- **Direct S3 partial reads** via `s3fs` — only bbox chunks transferred (~1–3 MB vs ~100 MB per file for a small region like Benin)
- **Parallel downloads** within each model task (`ProcessPoolExecutor`)
- **Idempotent** — skips already-downloaded files, safe to re-run after failure
- Automatic HTTP retry with exponential back-off
- Unit conversions applied on output (see table below)

## Repository structure

```
nex-gddp-cmip6-downloader/
├── cmip6_array.py        # Main download/processing script
├── run_cmip6_array.sh    # SLURM job-array launcher (single config file)
├── environment_cmip6.yml # Reproducible Python environment
├── tests/
│   └── test_cmip6.py     # Lightweight unit tests (no network required)
└── README.md
```

## Setup

```bash
git clone <repo-url>
cd nex-gddp-cmip6-downloader

# Option A — conda
conda env create -f environment_cmip6.yml
conda activate cmip6

# Option B — venv
python -m venv venv_cmip6
venv_cmip6/bin/pip install s3fs h5netcdf xarray netCDF4 requests
```

## Configuration

Open `run_cmip6_array.sh` and edit the **lines marked `[CONFIGURE]`**:

```bash
#SBATCH --partition cpu-dedicated     # [CONFIGURE] your cluster partition
#SBATCH --account dedicated-smp@cirad # [CONFIGURE] your SLURM account
...
OUTPUT_DIR=/path/to/your/output       # [CONFIGURE] where files will be saved
```

Everything else (models, variables, years, bounding box) is set in the same file
without touching the Python script. `WORK_DIR` is auto-detected from the script
location, so no path editing is needed after cloning.

## Usage

### SLURM (recommended)

```bash
# All 35 models
sbatch run_cmip6_array.sh

# Subset: edit MODELS and --array in the script, then:
# MODELS="MIROC6,CanESM5,MPI-ESM1-2-HR"
# #SBATCH --array=0-2
sbatch run_cmip6_array.sh
```

### Standalone (no SLURM)

```bash
python cmip6_array.py 25 \
    --models MIROC6 \
    --hist-start 1980 --hist-end 2014 \
    --ssp-start  2015 --ssp-end  2100 \
    --lat-min 6.1 --lat-max 12.8 --lon-min 0.6 --lon-max 4.0 \
    --output-dir /path/to/output \
    --max-workers 8

# Global download (no spatial crop)
python cmip6_array.py 25 --models MIROC6 --no-bbox --output-dir /path/to/output
```

Full argument reference:

```
positional:
  model_index           Index into --models list (= $SLURM_ARRAY_TASK_ID)

optional:
  --models              ALL or comma-separated list (e.g. MIROC6,CanESM5)
  --hist-start/end      Historical period  [default: 1980–2014]
  --ssp-start/end       SSP period         [default: 2015–2100]
  --experiments         Comma-separated    [default: all 5]
  --variables           Comma-separated    [default: all 7]
  --lat-min/max         Bounding box latitude
  --lon-min/max         Bounding box longitude
  --no-bbox             Download global (no crop)
  --max-workers         Parallel workers   [default: 8]
  --output-dir          Root output path
```

## Output structure

```
OUTPUT_DIR/
└── {model}/
    └── {experiment}/
        └── {variable}/
            └── {variable}_day_{model}_{experiment}_{variant}_{grid}_{year}_cropped.nc
```

## Unit conversions

| Variable | Input units | Output units | Factor |
|---|---|---|---|
| `pr` | kg/m²/s | mm/day | × 86 400 |
| `tas`, `tasmax`, `tasmin` | K | °C | − 273.15 |
| `sfcWind` | m/s (10 m) | m/s (2 m) | × 0.75 (log-profile) |
| `rsds` | W/m² | MJ/m²/day | × 0.0864 |

## Available models (35)

| Index | Model | Index | Model | Index | Model |
|---|---|---|---|---|---|
| 0 | ACCESS-CM2 | 12 | FGOALS-g3 | 24 | KIOST-ESM |
| 1 | ACCESS-ESM1-5 | 13 | GFDL-CM4 | 25 | MIROC6 |
| 2 | BCC-CSM2-MR | 14 | GFDL-CM4_gr2 | 26 | MIROC-ES2L |
| 3 | CanESM5 | 15 | GFDL-ESM4 | 27 | MPI-ESM1-2-HR |
| 4 | CESM2 | 16 | GISS-E2-1-G | 28 | MPI-ESM1-2-LR |
| 5 | CESM2-WACCM | 17 | HadGEM3-GC31-LL | 29 | MRI-ESM2-0 |
| 6 | CMCC-CM2-SR5 | 18 | HadGEM3-GC31-MM | 30 | NESM3 |
| 7 | CMCC-ESM2 | 19 | IITM-ESM | 31 | NorESM2-LM |
| 8 | CNRM-CM6-1 | 20 | INM-CM4-8 | 32 | NorESM2-MM |
| 9 | CNRM-ESM2-1 | 21 | INM-CM5-0 | 33 | TaiESM1 |
| 10 | EC-Earth3 | 22 | IPSL-CM6A-LR | 34 | UKESM1-0-LL |
| 11 | EC-Earth3-Veg-LR | 23 | KACE-1-0-G | | |

## Run tests

```bash
# With pytest
pytest tests/ -v

# Without pytest
python tests/test_cmip6.py
```

## Data source

Thrasher, B., et al. (2022). *NASA Global Daily Downscaled Projections, CMIP6.*
Scientific Data, 9, 262. https://doi.org/10.1038/s41597-022-01393-4

Dataset: https://registry.opendata.aws/nex-gddp-cmip6/
