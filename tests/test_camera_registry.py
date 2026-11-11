"""Unit tests for Camera Registry / traits / manager (synthetic mode)."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ingestion"))
sys.path.insert(0, str(ROOT / "spark"))

from camera_registry import CameraRegistry, apply_camera_traits  # noqa: E402
from camera_manager import CameraSimulatorManager, get_traffic_density  # noqa: E402
from camera_traits import apply_camera_traits as spark_apply_traits  # noqa: E402


class CameraRegistryTests(unittest.TestCase):
    def setUp(self):
        self.registry = CameraRegistry.from_json(ROOT / "config" / "cameras.json")

    def test_loads_at_least_eight_cameras(self):
        self.assertGreaterEqual(len(self.registry.all()), 8)

    def test_camera_ids_unique(self):
        ids = self.registry.ids()
        self.assertEqual(len(ids), len(set(ids)))

    def test_take_limit(self):
        subset = self.registry.take(3)
        self.assertEqual(len(subset), 3)

    def test_bridge_trait_no_pedestrian(self):
        cam = self.registry.get("CAM-HN-003")
        self.assertIsNotNone(cam)
        self.assertTrue(cam.has_trait("bridge") or cam.has_trait("no_pedestrian"))
        counts = {
            "Car": 10,
            "Motorcycle": 10,
            "Bus": 1,
            "Truck": 2,
            "Pedestrian": 8,
        }
        out = apply_camera_traits(counts, cam, cam.camera_id)
        self.assertEqual(out["Pedestrian"], 0)

    def test_spark_traits_match_ingestion(self):
        base = {
            "Car": 10,
            "Motorcycle": 20,
            "Bus": 1,
            "Truck": 2,
            "Pedestrian": 5,
        }
        cam = self.registry.get("CAM-HN-001")
        a = apply_camera_traits(dict(base), cam, "CAM-HN-001")
        b = spark_apply_traits(dict(base), "CAM-HN-001")
        self.assertEqual(a, b)

    def test_clickhouse_rows_shape(self):
        rows = self.registry.clickhouse_rows()
        self.assertGreaterEqual(len(rows), 8)
        self.assertEqual(len(rows[0]), 5)


class CameraManagerTests(unittest.TestCase):
    def test_synthetic_manager_builds_jpeg(self):
        mgr = CameraSimulatorManager(
            registry=CameraRegistry.from_json(ROOT / "config" / "cameras.json"),
            mode="synthetic",
            camera_limit=4,
        )
        cam = mgr.list_cameras()[0]
        data, filename, mime, ts = mgr.build_frame(cam, hour_offset=1)
        self.assertTrue(filename.startswith(cam.camera_id))
        self.assertEqual(mime, "image/jpeg")
        self.assertGreater(len(data), 100)
        # JPEG magic
        self.assertEqual(data[:2], b"\xff\xd8")
        self.assertTrue(ts)

    def test_density_peak_vs_night(self):
        cam = CameraRegistry.from_json(ROOT / "config" / "cameras.json").get("CAM-HN-002")
        peak = get_traffic_density(cam, "CAM-HN-002", 8)
        night = get_traffic_density(cam, "CAM-HN-002", 2)
        self.assertGreater(sum(peak.values()), sum(night.values()))

    def test_invalid_mode_raises(self):
        with self.assertRaises(ValueError):
            CameraSimulatorManager(mode="invalid")


if __name__ == "__main__":
    unittest.main()
