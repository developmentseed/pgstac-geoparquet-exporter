"""Stream pgSTAC items to GeoParquet with one schema for the whole collection.

Works around stac-geoparquet's FirstBatch writer, which fails on empty structs
(e.g. ``assets: {}``), on properties that are a list in some items and a scalar
in others, and on schema drift between chunks.
"""

import logging
from contextlib import suppress
from collections.abc import Callable, Iterable, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from stac_geoparquet.arrow._api import stac_items_to_arrow
from stac_geoparquet.arrow._batch import StacJsonBatch
from stac_geoparquet.arrow._to_parquet import create_parquet_metadata
from stac_geoparquet.arrow._util import batched_iter
from stac_geoparquet.pgstac_reader import pgstac_to_iter

logger = logging.getLogger(__name__)

# Parquet cannot store a struct without children, so empty structs get this
# always-null child. It is itself a struct because stac-geoparquet's reader
# expects every asset to be one; null assets are dropped when reading back.
EMPTY_STRUCT_FIELD = pa.field("_empty", pa.struct([pa.field("_empty", pa.null())]))


def _coerce_scalars_to_lists(
    items: Sequence[dict[str, Any]], list_keys: set[str]
) -> set[str]:
    """Wrap scalars of list_keys properties in a list; return the keys wrapped."""
    wrapped = set()
    for item in items:
        properties = item["properties"]
        for key in list_keys:
            value = properties.get(key)
            if value is not None and not isinstance(value, list):
                properties[key] = [value]
                wrapped.add(key)
    return wrapped


def _promote_to_list(schema: pa.Schema, list_keys: set[str]) -> pa.Schema:
    """Turn scalar `properties` fields named in list_keys into list fields."""
    if "properties" not in schema.names:
        return schema
    idx = schema.get_field_index("properties")
    props = schema.field(idx)
    fields = []
    for i in range(props.type.num_fields):
        f = props.type.field(i)
        if f.name in list_keys and not (
            pa.types.is_list(f.type) or pa.types.is_null(f.type)
        ):
            f = f.with_type(pa.list_(f.type))
        fields.append(f)
    return schema.set(idx, props.with_type(pa.struct(fields)))


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
    paths: list[tuple[str, list[Any]]] = []
    for key in {k for item in items for k in item} - {"geometry", "properties"}:
        paths.append((key, [item.get(key) for item in items]))
    for key in {k for item in items for k in item["properties"]}:
        paths.append(
            (f"properties.{key}", [item["properties"].get(key) for item in items])
        )
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
            cursor_itersize=chunk_size,
            row_func=row_func,
        )
        return batched_iter(items, chunk_size)

    # Pass 1: infer a schema covering every item
    schema = pa.schema([])
    list_keys: set[str] = set()
    for chunk in chunks():
        list_keys |= {
            key
            for item in chunk
            for key, value in item["properties"].items()
            if isinstance(value, list)
        }
        _coerce_scalars_to_lists(chunk, list_keys)
        chunk_schema = _promote_to_list(_infer_schema(chunk), list_keys)
        schema = pa.unify_schemas(
            [_promote_to_list(schema, list_keys), chunk_schema],
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
    mixed_keys: set[str] = set()
    try:
        for chunk in chunks():
            mixed_keys |= _coerce_scalars_to_lists(chunk, list_keys)
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
    for key in sorted(mixed_keys):
        logger.warning(
            f"{collection}: properties.{key} has list and scalar values; "
            "wrote scalars as single-item lists"
        )
    return rows
