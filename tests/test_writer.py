import copy
import math
import tracemalloc
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from stac_geoparquet.arrow import stac_table_to_items

from pgstac_geoparquet_exporter.writer import _chunks, export_items_to_parquet


def make_item(
    i: int, assets: dict[str, Any] | None = None, **properties: Any
) -> dict[str, Any]:
    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": f"item-{i}",
        "collection": "test",
        "geometry": {"type": "Point", "coordinates": [i % 180, 0.0]},
        "bbox": [i % 180, 0.0, i % 180, 0.0],
        "properties": {"datetime": "2024-01-01T00:00:00Z", **properties},
        "assets": {} if assets is None else assets,
        "links": [],
    }


def export(
    items: list[dict[str, Any]], path: Path, chunk_size: int = 2, **kwargs: Any
) -> int:
    # pgstac_to_iter is called once per pass; hand out fresh copies each time
    with patch(
        "pgstac_geoparquet_exporter.writer.pgstac_to_iter",
        side_effect=lambda *a, **kw: (copy.deepcopy(i) for i in items),
    ):
        return export_items_to_parquet(
            "", str(path), "test", chunk_size=chunk_size, **kwargs
        )


def read(path: Path) -> pa.Table:
    return pq.read_table(path)  # type: ignore[no-untyped-call]


def test_empty_assets(tmp_path):
    out = tmp_path / "full.parquet"
    assert export([make_item(i) for i in range(5)], out) == 5

    table = read(out)
    assert table.schema.field("assets").type.num_fields == 1
    assert [item["assets"] for item in stac_table_to_items(table)] == [{}] * 5


def test_nested_empty_struct(tmp_path):
    out = tmp_path / "full.parquet"
    export([make_item(i, extra={}) for i in range(3)], out)
    assert read(out).num_rows == 3


def test_mixed_list_and_scalar(tmp_path, caplog):
    out = tmp_path / "full.parquet"
    # scalar-only first chunk, mixed second chunk, list-only third chunk
    tags = ["a", "b", "c", ["d", "e"], ["f"], ["g"]]
    export([make_item(i, tags=t) for i, t in enumerate(tags)], out)

    table = read(out)
    assert pa.types.is_list(table.schema.field("tags").type)
    assert table["tags"].to_pylist() == [["a"], ["b"], ["c"], ["d", "e"], ["f"], ["g"]]
    assert "properties.tags" in caplog.text


def test_schema_drift_between_chunks(tmp_path):
    out = tmp_path / "full.parquet"
    items = [make_item(i) for i in range(4)] + [make_item(4, late="x")]
    export(items, out)
    assert read(out)["late"].to_pylist() == [None] * 4 + ["x"]


def test_unresolvable_conflict_names_field(tmp_path):
    items = [make_item(0, bad={"a": 1}), make_item(1, bad=2)]
    with pytest.raises(ValueError, match="properties.bad"):
        export(items, tmp_path / "full.parquet")


def test_no_items_raises_stopiteration(tmp_path):
    with pytest.raises(StopIteration):
        export([], tmp_path / "full.parquet")


def test_partial_file_removed_on_failure(tmp_path):
    out = tmp_path / "full.parquet"
    items = [make_item(i) for i in range(6)]
    calls = 0

    def failing_iter(*a, **kw):
        nonlocal calls
        calls += 1
        for n, item in enumerate(items):
            if calls == 2 and n == 4:
                raise RuntimeError("connection lost")
            yield dict(item, properties=dict(item["properties"]))

    with patch(
        "pgstac_geoparquet_exporter.writer.pgstac_to_iter", side_effect=failing_iter
    ):
        with pytest.raises(RuntimeError):
            export_items_to_parquet("", str(out), "test", chunk_size=2)
    assert not out.exists()


def peak_memory(n_chunks: int, path: Path, chunk_size: int = 200) -> tuple[int, int]:
    items = [make_item(i, description="x" * 2000) for i in range(n_chunks * chunk_size)]
    return measure(items, path, chunk_size=chunk_size)


def measure(items: list[dict[str, Any]], path: Path, **kwargs: Any) -> tuple[int, int]:
    """Peak Python and Arrow memory of one export."""
    # A proxy pool scopes Arrow's high-water mark to this export
    original_pool = pa.default_memory_pool()
    arrow_pool = pa.proxy_memory_pool(original_pool)
    pa.set_memory_pool(arrow_pool)
    tracemalloc.start()
    try:
        export(items, path, **kwargs)
        _, peak_py = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
        pa.set_memory_pool(original_pool)
    return peak_py, arrow_pool.max_memory()


def test_memory_bounded_per_chunk(tmp_path):
    small_py, small_arrow = peak_memory(4, tmp_path / "small.parquet")
    large_py, large_arrow = peak_memory(40, tmp_path / "large.parquet")
    # 10x the data must not mean 10x the peak: memory is per chunk, not per collection
    assert large_py < 1.5 * small_py
    assert large_arrow < 1.5 * small_arrow


def test_mixed_list_and_scalar_in_assets(tmp_path, caplog):
    out = tmp_path / "full.parquet"
    expr = ["x", "x", ["y", "z"], ["w"]]
    items = [
        make_item(
            i,
            assets={
                "a": {"href": "a.tif", "s_expressions": expr[i]},
                "b": {"href": "b.tif", "s_expressions": expr[-1 - i]},
            },
        )
        for i in range(4)
    ]
    export(items, out)

    assets = [item["assets"] for item in stac_table_to_items(read(out))]
    assert [a["a"]["s_expressions"] for a in assets] == [
        ["x"],
        ["x"],
        ["y", "z"],
        ["w"],
    ]
    assert [a["b"]["s_expressions"] for a in assets] == [
        ["w"],
        ["y", "z"],
        ["x"],
        ["x"],
    ]
    assert "assets.*.s_expressions" in caplog.text


def test_chunks_bounded_by_count_and_size():
    small = {"id": "s"}
    big = {"id": "b", "data": "x" * 2 * 1024 * 1024}
    sizes = [len(c) for c in _chunks([small] * 5, chunk_size=2, max_chunk_mb=64)]
    assert sizes == [2, 2, 1]
    chunks = list(_chunks([small, big, small, small], chunk_size=100, max_chunk_mb=1))
    # An oversized item closes the chunk it lands in
    assert [[i["id"] for i in c] for c in chunks] == [["s", "b"], ["s", "s"]]


def huge_item(i: int, n_points: int) -> dict[str, Any]:
    item = make_item(i)
    ring = [
        [math.cos(2 * math.pi * k / n_points), math.sin(2 * math.pi * k / n_points)]
        for k in range(n_points)
    ]
    item["geometry"] = {"type": "Polygon", "coordinates": [ring + [ring[0]]]}
    item["bbox"] = [-1.0, -1.0, 1.0, 1.0]
    return item


def test_huge_geometries_bounded_by_size(tmp_path):
    # ~1 MB of GeoJSON per huge item
    items = [huge_item(i, 25_000) if i % 10 == 0 else make_item(i) for i in range(40)]
    unbounded_py, _ = measure(
        items, tmp_path / "a.parquet", chunk_size=8192, max_chunk_mb=10_000
    )
    bounded_py, _ = measure(
        items, tmp_path / "b.parquet", chunk_size=8192, max_chunk_mb=0.5
    )
    # Four huge items in one chunk vs one per chunk
    assert bounded_py < 0.5 * unbounded_py
    assert read(tmp_path / "b.parquet").num_rows == 40
