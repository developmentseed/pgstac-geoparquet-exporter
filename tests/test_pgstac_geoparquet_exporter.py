from pgstac_geoparquet_exporter.__main__ import main


def test_main_returns_zero():
    assert main() == 0
