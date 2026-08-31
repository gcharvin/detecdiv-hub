from collections import namedtuple

from api.services.system_storage import collect_disk_usage, configured_disk_paths


DiskUsage = namedtuple("DiskUsage", "total used free")


def test_collect_disk_usage_warns_at_threshold() -> None:
    usages = {
        "/": DiskUsage(total=1000, used=760, free=240),
        "/data": DiskUsage(total=10_000, used=9500, free=500),
    }

    result = collect_disk_usage(
        ["/", "/data"],
        warning_threshold_percent=95,
        usage_reader=usages.__getitem__,
    )

    assert result[0]["status"] == "ok"
    assert result[0]["used_percent"] == 76.0
    assert result[1]["status"] == "warning"
    assert result[1]["used_percent"] == 95.0
    assert result[1]["free_bytes"] == 500


def test_collect_disk_usage_matches_df_rounding() -> None:
    result = collect_disk_usage(
        ["/data"],
        warning_threshold_percent=95,
        usage_reader=lambda _path: DiskUsage(total=10_000, used=9410, free=590),
    )

    assert result[0]["used_percent"] == 95
    assert result[0]["status"] == "warning"


def test_collect_disk_usage_skips_missing_optional_mount() -> None:
    def usage_reader(path: str):
        if path == "/data":
            raise FileNotFoundError(path)
        return DiskUsage(total=100, used=10, free=90)

    assert [item["path"] for item in collect_disk_usage(["/", "/data"], usage_reader=usage_reader)] == ["/"]


def test_configured_disk_paths_trims_empty_entries() -> None:
    assert configured_disk_paths(" /, /data, ,/archive ") == ["/", "/data", "/archive"]
