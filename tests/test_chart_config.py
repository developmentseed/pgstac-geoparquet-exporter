"""The config rendered by the Helm chart is the config main() reads."""

import os
import shutil
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from pgstac_geoparquet_exporter.__main__ import main

CHART = Path(__file__).parent.parent / "charts" / "pgstac-geoparquet-exporter"

VALUES = """
exportConfig:
  exportAll: true
  settings:
    chunk_size: 8192
    max_chunk_mb: 64
    statement_timeout: 3600000
    rewrite: false
  overrides:
    emdat-events:
      chunk_size: 500
"""


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_rendered_config_reaches_export(tmp_path):
    values = tmp_path / "values.yaml"
    values.write_text(VALUES)
    rendered = subprocess.run(
        ["helm", "template", str(CHART), "-f", str(values)]
        + ["--show-only", "templates/configmap.yaml"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    config = tmp_path / "export-config.yaml"
    config.write_text(yaml.safe_load(rendered)["data"]["export-config.yaml"])

    env = {
        "PGHOST": "testhost",
        "PGDATABASE": "testdb",
        "PGUSER": "testuser",
        "PGPASSWORD": "testpass",
        "STAC_API_URL": "http://localhost:8000/stac/v1",
        "EXPORT_MODE": "complete",
        "CONFIG_PATH": str(config),
        "OUTPUT_PATH": str(tmp_path / "out"),
    }
    with (
        patch.dict(os.environ, env, clear=True),
        patch(
            "pgstac_geoparquet_exporter.__main__.get_all_collections",
            return_value=[{"name": "cems-events"}, {"name": "emdat-events"}],
        ),
        patch(
            "pgstac_geoparquet_exporter.__main__.export_items_to_parquet"
        ) as mock_export,
    ):
        assert main() == 0

    calls = {c.kwargs["collection"]: c.kwargs for c in mock_export.call_args_list}
    assert calls["cems-events"]["chunk_size"] == 8192
    assert calls["emdat-events"]["chunk_size"] == 500
    for kwargs in calls.values():
        assert kwargs["max_chunk_mb"] == 64
        assert kwargs["statement_timeout"] == 3600000
