"""Stream pgSTAC items to GeoParquet with one schema for the whole collection.

Works around stac-geoparquet's FirstBatch writer, which fails on empty structs
(e.g. ``assets: {}``), on properties that are a list in some items and a scalar
in others, and on schema drift between chunks.
"""

import logging
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from typing import Any

import orjson
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from stac_geoparquet.arrow._api import stac_items_to_arrow
from stac_geoparquet.arrow._batch import StacJsonBatch
from stac_geoparquet.arrow._to_parquet import create_parquet_metadata
from stac_geoparquet.pgstac_reader import pgstac_to_iter

logger = logging.getLogger(__name__)

# Parquet cannot store a struct without children, so empty structs get this
# always-null child. It is itself a struct because stac-geoparquet's reader
# expects every asset to be one; null assets are dropped when reading back.
EMPTY_STRUCT_FIELD = pa.field("_empty", pa.struct([pa.field("_empty", pa.null())]))


# Objects whose fields may be a list in some items and a scalar in others.
# "*" matches every key, e.g. each asset.
COERCE_PREFIXES: list[tuple[str, ...]] = [("properties",), ("assets", "*")]

FieldPath = tuple[str, ...]


def _parents(obj: Any, prefix: Sequence[str]) -> Iterator[dict[str, Any]]:
    """Yield the dicts found at prefix, expanding "*" to every child."""
    if not isinstance(obj, dict):
        return
    if not prefix:
        yield obj
        return
    head, rest = prefix[0], prefix[1:]
    for child in obj.values() if head == "*" else [obj.get(head)]:
        yield from _parents(child, rest)


def _list_paths(items: Sequence[dict[str, Any]]) -> set[FieldPath]:
    return {
        prefix + (key,)
        for prefix in COERCE_PREFIXES
        for item in items
        for parent in _parents(item, prefix)
        for key, value in parent.items()
        if isinstance(value, list)
    }


def _coerce_scalars_to_lists(
    items: Sequence[dict[str, Any]], list_paths: set[FieldPath]
) -> set[FieldPath]:
    """Wrap scalars found at list_paths in a list; return the paths wrapped."""
    wrapped = set()
    for path in list_paths:
        for item in items:
            for parent in _parents(item, path[:-1]):
                value = parent.get(path[-1])
                if value is not None and not isinstance(value, list):
                    parent[path[-1]] = [value]
                    wrapped.add(path)
    return wrapped


def _promote_type(t: pa.DataType, path: FieldPath) -> pa.DataType:
    """Make the struct fields at path list-typed ("*" matches every field)."""
    if not pa.types.is_struct(t):
        return t
    fields = []
    for i in range(t.num_fields):
        f = t.field(i)
        if path[0] in ("*", f.name):
            if len(path) > 1:
                f = f.with_type(_promote_type(f.type, path[1:]))
            elif not (pa.types.is_list(f.type) or pa.types.is_null(f.type)):
                f = f.with_type(pa.list_(f.type))
        fields.append(f)
    return pa.struct(fields)


def _promote_to_list(schema: pa.Schema, list_paths: set[FieldPath]) -> pa.Schema:
    t = pa.struct(list(schema))
    for path in list_paths:
        t = _promote_type(t, path)
    return pa.schema([t.field(i) for i in range(t.num_fields)])


def _chunks(
    items: Iterable[dict[str, Any]], chunk_size: int, max_chunk_mb: float
) -> Iterator[list[dict[str, Any]]]:
    """Batch items by count and by approximate JSON size.

    Large geometries take many times their JSON size as Python objects, so a
    count alone does not bound memory. An oversized item gets its own chunk.
    """
    max_bytes = max_chunk_mb * 1024 * 1024
    chunk: list[dict[str, Any]] = []
    size = 0
    for item in items:
        chunk.append(item)
        size += len(orjson.dumps(item))
        if len(chunk) >= chunk_size or size >= max_bytes:
            yield chunk
            chunk, size = [], 0
    if chunk:
        yield chunk


def _fill_empty_structs(t: pa.DataType) -> pa.DataType:
    if pa.types.is_struct(t):
        if t.num_fields == 0:
            return pa.struct([EMPTY_STRUCT_FIELD])
        return pa.struct(
            [
                t.field(i).with_type(_fill_empty_structs(t.field(i).type))
                for i in range(t.num_fields)
            ]
        )
    if pa.types.is_list(t):
        return pa.list_(t.value_field.with_type(_fill_empty_structs(t.value_type)))
    return t


def _conflicting_fields(items: Sequence[dict[str, Any]]) -> list[str]:
    """Name the fields that pyarrow cannot convert to a single type."""
    top = {k for item in items for k in item} - {"geometry", "properties", "assets"}
    paths = [(key, [item.get(key) for item in items]) for key in top]
    for prefix in COERCE_PREFIXES:
        parents = [p for item in items for p in _parents(item, prefix)]
        for key in {k for p in parents for k in p}:
            name = ".".join(prefix + (key,))
            paths.append((name, [p.get(key) for p in parents]))
    bad = []
    for name, values in sorted(paths):
        try:
            pa.array(values)
        except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
            bad.append(f"{name} ({e})")
    return bad


def _infer_schema(items: Sequence[dict[str, Any]]) -> pa.Schema:
    try:
        return StacJsonBatch.from_dicts(items).inner.schema
    except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
        fields = ", ".join(_conflicting_fields(items)) or "unknown field"
        raise ValueError(f"Cannot infer schema for {fields}") from e


def export_items_to_parquet(
    conninfo: str,
    output_path: str,
    collection: str | None = None,
    start_datetime: datetime | None = None,
    end_datetime: datetime | None = None,
    chunk_size: int = 8192,
    max_chunk_mb: float = 64,
    row_func: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    filesystem: Any | None = None,
    schema_version: str = "1.1.0",
    statement_timeout: int | None = None,
) -> int:
    """Export pgSTAC items to one Parquet file and return the row count.

    Reads the items twice: once to infer a schema for the whole collection and
    once to write it chunk by chunk. Raises StopIteration if there are no items.
    Items changed between the two reads can make the write fail; the collection
    is then reported as failed and the partial file removed.
    """

    def chunks() -> Iterable[Sequence[dict[str, Any]]]:
        items = pgstac_to_iter(
            conninfo,
            collection,
            start_datetime,
            end_datetime,
            statement_timeout=statement_timeout,
            # psycopg buffers this many raw rows, geometries included
            cursor_itersize=min(chunk_size, 1000),
            row_func=row_func,
        )
        return _chunks(items, chunk_size, max_chunk_mb)

    # Pass 1: infer a schema covering every item
    schema = pa.schema([])
    list_paths: set[FieldPath] = set()
    for chunk in chunks():
        list_paths |= _list_paths(chunk)
        _coerce_scalars_to_lists(chunk, list_paths)
        chunk_schema = _promote_to_list(_infer_schema(chunk), list_paths)
        schema = pa.unify_schemas(
            [_promote_to_list(schema, list_paths), chunk_schema],
            promote_options="permissive",
        )
    if not schema.names:
        raise StopIteration
    schema = pa.schema([f.with_type(_fill_empty_structs(f.type)) for f in schema])

    if filesystem is None:
        filesystem, filepath = pafs.FileSystem.from_uri(output_path)  # type: ignore[attr-defined]
    else:
        filepath = str(output_path)
    filesystem.create_dir(str(Path(filepath).parent), recursive=True)

    # Pass 2: write chunk by chunk with the fixed schema
    writer = None
    rows = 0
    mixed_paths: set[FieldPath] = set()
    try:
        for chunk in chunks():
            mixed_paths |= _coerce_scalars_to_lists(chunk, list_paths)
            batch = stac_items_to_arrow(chunk, schema=schema)
            if writer is None:
                metadata = create_parquet_metadata(
                    batch.schema,
                    schema_version=schema_version,  # type: ignore[arg-type]
                )
                writer = pq.ParquetWriter(  # type: ignore[no-untyped-call]
                    filepath,
                    batch.schema.with_metadata(metadata),
                    filesystem=filesystem,
                )
            writer.write_batch(batch)  # type: ignore[no-untyped-call]
            rows += batch.num_rows
        if writer is not None:
            writer.close()  # type: ignore[no-untyped-call]
    except BaseException:
        if writer is not None:
            # Don't let a second close() error hide the original one
            with suppress(Exception):
                writer.close()  # type: ignore[no-untyped-call]
            try:
                filesystem.delete_file(filepath)
            except OSError as e:
                logger.warning(f"Could not remove partial file {filepath}: {e}")
        raise
    for path in sorted(mixed_paths):
        logger.warning(
            f"{collection}: {'.'.join(path)} has list and scalar values; "
            "wrote scalars as single-item lists"
        )
    return rows
