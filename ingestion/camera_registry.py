"""Camera Registry — quản lý metadata nhiều camera độc lập."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


DEFAULT_REGISTRY_PATHS = (
    Path(__file__).resolve().parents[1] / "config" / "cameras.json",
    Path("/opt/config/cameras.json"),
    Path("/opt/spark-jobs/cameras_registry.json"),
)


@dataclass(frozen=True)
class Camera:
    camera_id: str
    location: str
    latitude: float
    longitude: float
    traits: tuple[str, ...] = field(default_factory=tuple)

    def has_trait(self, trait: str) -> bool:
        return trait in self.traits


class CameraRegistry:
    def __init__(self, cameras: Iterable[Camera]):
        self._by_id = {c.camera_id: c for c in cameras}
        if not self._by_id:
            raise ValueError("Camera registry is empty")

    @classmethod
    def from_json(cls, path: str | Path | None = None) -> "CameraRegistry":
        candidates = [Path(path)] if path else list(DEFAULT_REGISTRY_PATHS)
        env_path = os.getenv("CAMERA_REGISTRY_PATH")
        if env_path:
            candidates.insert(0, Path(env_path))

        last_err = None
        for candidate in candidates:
            try:
                if not candidate.is_file():
                    continue
                data = json.loads(candidate.read_text(encoding="utf-8"))
                cameras = [
                    Camera(
                        camera_id=item["camera_id"],
                        location=item["location"],
                        latitude=float(item["latitude"]),
                        longitude=float(item["longitude"]),
                        traits=tuple(item.get("traits") or ()),
                    )
                    for item in data["cameras"]
                ]
                return cls(cameras)
            except Exception as exc:  # noqa: BLE001 — try next path
                last_err = exc
        raise FileNotFoundError(
            f"Cannot load camera registry from {candidates}: {last_err}"
        )

    def all(self) -> list[Camera]:
        return list(self._by_id.values())

    def ids(self) -> list[str]:
        return list(self._by_id.keys())

    def get(self, camera_id: str) -> Camera | None:
        return self._by_id.get(camera_id)

    def take(self, limit: int | None = None) -> list[Camera]:
        cams = self.all()
        if limit is None or limit <= 0 or limit >= len(cams):
            return cams
        return cams[:limit]

    def clickhouse_rows(self) -> list[tuple]:
        from datetime import datetime

        now = datetime.utcnow()
        return [
            (c.camera_id, c.location, c.latitude, c.longitude, now)
            for c in self.all()
        ]


def apply_camera_traits(counts: dict, camera: Camera | None, camera_id: str = "") -> dict:
    """Điều chỉnh mật độ xe theo trait camera (dùng chung client mock + spark mock)."""
    result = dict(counts)
    traits = set(camera.traits) if camera else set()

    # Backward-compatible hard IDs if registry missing on worker
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
