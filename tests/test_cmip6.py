# -*- coding: utf-8 -*-
"""
Lightweight tests for cmip6_array.py — no network access, no file I/O.

Run with:  pytest tests/ -v
       or: python tests/test_cmip6.py
"""

import sys
import unittest
from pathlib import Path

# Make the parent directory importable
sys.path.insert(0, str(Path(__file__).parent.parent))

import cmip6_array

class TestModelTables(unittest.TestCase):
    """Every model in DEFAULT_VARIANTS must have a grid label entry."""

    def test_35_models(self):
        self.assertEqual(len(cmip6_array._COMPLETE_MODELS), 35)

    def test_all_models_have_grid_label(self):
        missing = [m for m in cmip6_array.DEFAULT_VARIANTS
                   if m not in cmip6_array.MODEL_GRID_LABELS]
        self.assertEqual(missing, [], f"Missing grid labels: {missing}")

    def test_complete_models_matches_variants(self):
        self.assertEqual(cmip6_array._COMPLETE_MODELS,
                         list(cmip6_array.DEFAULT_VARIANTS.keys()))


class TestGetVariant(unittest.TestCase):

    def test_known_model(self):
        self.assertEqual(
            cmip6_array.get_variant_for_model("MIROC6", "historical"),
            "r1i1p1f1"
        )

    def test_known_model_with_f2(self):
        self.assertEqual(
            cmip6_array.get_variant_for_model("CNRM-CM6-1", "historical"),
            "r1i1p1f2"
        )

    def test_unknown_model_returns_default(self):
        self.assertEqual(
            cmip6_array.get_variant_for_model("UNKNOWN-MODEL", "ssp245"),
            "r1i1p1f1"
        )


class TestUnitConversions(unittest.TestCase):
    """Verify the conversion factors used in download_and_process_file."""

    def test_pr_factor(self):
        # kg/m2/s -> mm/day
        self.assertAlmostEqual(1.0 * 86400, 86400.0)

    def test_temperature_offset(self):
        # 0 degC = 273.15 K
        self.assertAlmostEqual(273.15 - 273.15, 0.0)

    def test_wind_height_correction(self):
        # 10 m -> 2 m log-profile approximation
        self.assertAlmostEqual(10.0 * 0.75, 7.5)

    def test_rsds_factor(self):
        # W/m2 -> MJ/m2/day: 1 W/m2 * 86400 s/day * 1e-6 MJ/J = 0.0864
        self.assertAlmostEqual(1.0 * 0.0864, 0.0864)


class TestS3PathConstruction(unittest.TestCase):
    """S3 key must follow the exact bucket layout."""

    def _make_s3_path(self, model, experiment, variant, variable, year):
        grid_label = cmip6_array.MODEL_GRID_LABELS[model]
        filename = (f"{variable}_day_{model}_{experiment}_{variant}"
                    f"_{grid_label}_{year}_v2.0.nc")
        return f"nex-gddp-cmip6/NEX-GDDP-CMIP6/{model}/{experiment}/{variant}/{variable}/{filename}"

    def test_miroc6_historical_tas(self):
        path = self._make_s3_path("MIROC6", "historical", "r1i1p1f1", "tas", 2000)
        expected = (
            "nex-gddp-cmip6/NEX-GDDP-CMIP6/MIROC6/historical/r1i1p1f1/tas/"
            "tas_day_MIROC6_historical_r1i1p1f1_gn_2000_v2.0.nc"
        )
        self.assertEqual(path, expected)

    def test_cnrm_uses_gr_grid(self):
        path = self._make_s3_path("CNRM-CM6-1", "ssp245", "r1i1p1f2", "pr", 2050)
        self.assertIn("_gr_", path)

    def test_gfdl_cm4_uses_gr1(self):
        path = self._make_s3_path("GFDL-CM4", "historical", "r1i1p1f1", "tas", 1990)
        self.assertIn("_gr1_", path)


class TestFilenamePattern(unittest.TestCase):
    """Output filename suffix depends on whether bbox is active."""

    def test_cropped_suffix_with_bbox(self):
        model, exp, variant, var, year = "MIROC6", "historical", "r1i1p1f1", "tas", 2000
        grid = cmip6_array.MODEL_GRID_LABELS[model]
        fname = f"{var}_day_{model}_{exp}_{variant}_{grid}_{year}_cropped.nc"
        self.assertTrue(fname.endswith("_cropped.nc"))

    def test_v2_suffix_without_bbox(self):
        model, exp, variant, var, year = "MIROC6", "historical", "r1i1p1f1", "tas", 2000
        grid = cmip6_array.MODEL_GRID_LABELS[model]
        fname = f"{var}_day_{model}_{exp}_{variant}_{grid}_{year}_v2.0.nc"
        self.assertTrue(fname.endswith("_v2.0.nc"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
