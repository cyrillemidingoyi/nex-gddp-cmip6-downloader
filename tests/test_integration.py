# -*- coding: utf-8 -*-
"""
Integration tests: real S3 downloads, 2 models × 2 experiments × 1 variable.

Requires internet access (~2–4 MB transferred per file via s3fs partial reads).
Safe to run in CI (GitHub Actions) — uses the public NEX-GDDP-CMIP6 S3 bucket.

Run with:  pytest tests/test_integration.py -v
       or: python tests/test_integration.py
"""

import sys
import tempfile
import shutil
import unittest
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))

import cmip6_array

# ── Integration test configuration ──────────────────────────────────────────
MODELS      = ["MIROC6", "CanESM5"]
EXPERIMENTS = {"historical": 2000, "ssp245": 2050}   # {experiment: year}
VARIABLE    = "tas"
BBOX        = {"lat_min": 8.0, "lat_max": 10.0, "lon_min": 1.0, "lon_max": 3.0}
# ────────────────────────────────────────────────────────────────────────────


def _download(model, experiment, year, output_dir):
    variant = cmip6_array.get_variant_for_model(model, experiment)
    return cmip6_array.download_and_process_file(
        model=model,
        experiment=experiment,
        variant=variant,
        variable=VARIABLE,
        year=year,
        bbox=BBOX,
        output_base_dir=Path(output_dir),
    )


class TestDownloadIntegration(unittest.TestCase):
    """Download 4 files (2 models × 2 experiments) and check outputs."""

    @classmethod
    def setUpClass(cls):
        # Initialise s3fs in this process (normally done by _init_worker after fork)
        cmip6_array._init_worker()
        cls.tmpdir = Path(tempfile.mkdtemp(prefix="cmip6_test_"))

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    # ── Download status ──────────────────────────────────────────────────────

    def test_miroc6_historical_download(self):
        status, *rest = _download("MIROC6", "historical", 2000, self.tmpdir)
        self.assertIn(status, ("success", "exists"), msg=f"download failed: {rest[-1]}")

    def test_miroc6_ssp245_download(self):
        status, *rest = _download("MIROC6", "ssp245", 2050, self.tmpdir)
        self.assertIn(status, ("success", "exists"), msg=f"download failed: {rest[-1]}")

    def test_canesm5_historical_download(self):
        status, *rest = _download("CanESM5", "historical", 2000, self.tmpdir)
        self.assertIn(status, ("success", "exists"), msg=f"download failed: {rest[-1]}")

    def test_canesm5_ssp245_download(self):
        status, *rest = _download("CanESM5", "ssp245", 2050, self.tmpdir)
        self.assertIn(status, ("success", "exists"), msg=f"download failed: {rest[-1]}")

    # ── Output file content ──────────────────────────────────────────────────

    def test_bbox_respected(self):
        """Spatial extent of output must lie within the requested bbox (+ 1 grid cell tolerance)."""
        import xarray as xr
        _download("MIROC6", "historical", 2000, self.tmpdir)
        files = list((self.tmpdir / "MIROC6" / "historical" / VARIABLE).glob("*.nc"))
        self.assertTrue(files, "No output file found")
        ds = xr.open_dataset(files[0], engine='h5netcdf')
        tol = 1.5  # grid-cell tolerance
        self.assertGreaterEqual(float(ds.lat.min()), BBOX["lat_min"] - tol)
        self.assertLessEqual(float(ds.lat.max()),    BBOX["lat_max"] + tol)
        self.assertGreaterEqual(float(ds.lon.min()), BBOX["lon_min"] - tol)
        self.assertLessEqual(float(ds.lon.max()),    BBOX["lon_max"] + tol)
        ds.close()

    def test_temperature_in_celsius(self):
        """tas values must be in °C after conversion (roughly −50 to 60)."""
        import xarray as xr
        _download("MIROC6", "historical", 2000, self.tmpdir)
        files = list((self.tmpdir / "MIROC6" / "historical" / VARIABLE).glob("*.nc"))
        self.assertTrue(files)
        ds = xr.open_dataset(files[0], engine='h5netcdf')
        vals = ds[VARIABLE].values
        self.assertGreater(float(np.nanmin(vals)), -60.0, "Temperature too low — still in Kelvin?")
        self.assertLess(float(np.nanmax(vals)),     60.0, "Temperature unexpectedly high")
        ds.close()

    def test_idempotent_second_call(self):
        """A second download of the same file must return status 'exists'."""
        _download("MIROC6", "historical", 2000, self.tmpdir)  # ensure file present
        status, *_ = _download("MIROC6", "historical", 2000, self.tmpdir)
        self.assertEqual(status, "exists")


if __name__ == "__main__":
    unittest.main(verbosity=2)
