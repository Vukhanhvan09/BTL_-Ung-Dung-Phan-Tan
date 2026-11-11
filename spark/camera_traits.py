"""Camera trait helpers for Spark workers (loads cameras_registry.json)."""
from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_REGISTRY_CACHE = None


def _load_registry():
    global _REGISTRY_CACHE
    if _REGISTRY_CACHE is not None:
        return _REGISTRY_CACHE

    candidates = [
        Path("/opt/spark-jobs/cameras_registry.json"),
        Path("/opt/config/cameras.json"),
        Path(__file__).resolve().parent / "cameras_registry.json",
    ]
    for path in candidates:
        if path.is_file():
            data = json.loads(path.read_text(encoding="utf-8"))
            _REGISTRY_CACHE = {
                item["camera_id"]: tuple(item.get("traits") or ())
                for item in data.get("cameras", [])
            }
            logger.info("Loaded %d camera traits from %s", len(_REGISTRY_CACHE), path)
            return _REGISTRY_CACHE

    _REGISTRY_CACHE = {}
    logger.warning("cameras_registry.json not found; using empty trait map")
    return _REGISTRY_CACHE


def apply_camera_traits(counts: dict, camera_id: str) -> dict:
    traits = set(_load_registry().get(camera_id, ()))
    result = dict(counts)

    # Legacy fallbacks
    if not traits:
        if camera_id == "CAM-HN-003":
            traits = {"bridge", "no_pedestrian", "heavy_truck"}
        elif camera_id == "CAM-HN-001":
            traits = {"dense_moto", "urban"}

    if "no_pedestrian" in traits or "bridge" in traits:
        result["Pedestrian"] = 0
    if "heavy_truck" in traits:
        result["Truck"] = int(result.get("Truck", 0) * 1.5)
        result["Car"] = int(result.get("Car", 0) * 1.3)
    if "dense_moto" in traits:
        result["Motorcycle"] = int(result.get("Motorcycle", 0) * 1.3)
        result["Truck"] = max(0, result.get("Truck", 0) - 1)
    if "dense_pedestrian" in traits:
        result["Pedestrian"] = int(result.get("Pedestrian", 0) * 1.8)
        result["Motorcycle"] = max(0, int(result.get("Motorcycle", 0) * 0.8))
    if "dense_car" in traits or "highway" in traits:
        result["Car"] = int(result.get("Car", 0) * 1.4)
        result["Bus"] = max(result.get("Bus", 0), 1)
    if "tourist" in traits:
        result["Pedestrian"] = int(result.get("Pedestrian", 0) * 1.5)

    return result
