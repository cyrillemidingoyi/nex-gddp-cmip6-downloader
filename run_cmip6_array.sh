#!/bin/bash
# =============================================================================
# run_cmip6_array.sh — SLURM job array: download NEX-GDDP-CMIP6 (one model/task)
#
# BEFORE FIRST USE — edit the two lines marked [CONFIGURE]:
#   1. #SBATCH --partition and #SBATCH --account  (SLURM directives below)
#   2. OUTPUT_DIR  (where NetCDF files will be saved, ~line 80)
#
# Environment: create the Python venv once with:
#   cd <this directory>
#   conda env create -f environment_cmip6.yml && conda activate cmip6
#   # or: python -m venv venv_cmip6 && venv_cmip6/bin/pip install s3fs h5netcdf xarray netCDF4 requests
# =============================================================================
#SBATCH --partition cpu-dedicated   # [CONFIGURE] your cluster partition
#SBATCH --account dedicated-smp@cirad   # [CONFIGURE] your SLURM account
#SBATCH --job-name=cmip6_download
#SBATCH --output=logs/cmip6_%A_%a.out
#SBATCH --error=logs/cmip6_%A_%a.err
#SBATCH --array=0-34
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=48:00:00

# Create logs directory if it doesn't exist
mkdir -p logs

# Print job information
echo "=========================================="
echo "Job ID: $SLURM_JOB_ID"
echo "Array Task ID: $SLURM_ARRAY_TASK_ID"
echo "Running on node: $HOSTNAME"
echo "Started at: $(date)"
echo "=========================================="

# Model indices (ALL_MODELS list in cmip6_array.py):
#  0: ACCESS-CM2        1: ACCESS-ESM1-5    2: BCC-CSM2-MR
#  3: CanESM5           4: CESM2            5: CESM2-WACCM
#  6: CMCC-CM2-SR5      7: CMCC-ESM2        8: CNRM-CM6-1
#  9: CNRM-ESM2-1      10: EC-Earth3       11: EC-Earth3-Veg-LR
# 12: FGOALS-g3        13: GFDL-CM4        14: GFDL-CM4_gr2
# 15: GFDL-ESM4        16: GISS-E2-1-G     17: HadGEM3-GC31-LL
# 18: HadGEM3-GC31-MM  19: IITM-ESM        20: INM-CM4-8
# 21: INM-CM5-0        22: IPSL-CM6A-LR    23: KACE-1-0-G
# 24: KIOST-ESM        25: MIROC6          26: MIROC-ES2L
# 27: MPI-ESM1-2-HR    28: MPI-ESM1-2-LR   29: MRI-ESM2-0
# 30: NESM3            31: NorESM2-LM      32: NorESM2-MM
# 33: TaiESM1          34: UKESM1-0-LL


# Tous les modèles (défaut)         
MODELS="ALL"
##SBATCH --array=0-34

# Sous-ensemble  # tu listes les modèles que tu veux dans la variable MODELS et tu ajustes l'array en conséquence
#MODELS="MIROC6,CanESM5,MPI-ESM1-2-HR"
#SBATCH --array=0-2   # pour les 3 modèles du sous-ensemble 0 1 2


# ---- Configuration ----  # par défaut. Adapter si besoin.
HIST_START=1980
HIST_END=2014
SSP_START=2015
SSP_END=2100

EXPERIMENTS="historical,ssp126,ssp245,ssp370,ssp585" # sélectionner si besoin d'un sous-ensemble d'expériences 
VARIABLES="tas,tasmax,tasmin,hurs,sfcWind,rsds,pr" # sélectionner si besoin d'un sous-ensemble de variables 

# Bounding box (BENIN region) — remplacer par les coordonnées de ta région d'intérêt ou mettre BBOX_ARGS="--no-bbox" pour tout télécharger
# Pour un telechargement global, remplacer les 5 lignes par: BBOX_ARGS="--no-bbox"
LAT_MIN=6.1
LAT_MAX=12.8
LON_MIN=0.6
LON_MAX=4
BBOX_ARGS="--lat-min $LAT_MIN --lat-max $LAT_MAX --lon-min $LON_MIN --lon-max $LON_MAX"

MAX_WORKERS=$SLURM_CPUS_PER_TASK

# Paths — WORK_DIR is auto-detected from the script location
WORK_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PYTHON=$WORK_DIR/venv_cmip6/bin/python
SCRIPT=$WORK_DIR/cmip6_array.py

# [CONFIGURE] Root directory where NetCDF files will be written
OUTPUT_DIR=/storage/replicated/cirad/projects/AIDA/LIMA/CMIP6/BENIN/nex_gddp_cmip6_data
# --------------------------------------------------

$PYTHON $SCRIPT \
    $SLURM_ARRAY_TASK_ID \
    --models      "$MODELS" \
    --hist-start  $HIST_START  --hist-end  $HIST_END \
    --ssp-start   $SSP_START   --ssp-end   $SSP_END \
    --experiments "$EXPERIMENTS" \
    --variables   "$VARIABLES" \
    $BBOX_ARGS \
    --max-workers $MAX_WORKERS \
    --output-dir  "$OUTPUT_DIR"

# Print completion information
echo "=========================================="
echo "Finished at: $(date)"
echo "=========================================="
