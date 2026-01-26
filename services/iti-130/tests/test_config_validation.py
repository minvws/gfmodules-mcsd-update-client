import pytest


def test_validate_config_rejects_non_http_fhir_base(publisher_module, base_cfg):
    cfg = base_cfg.__class__(**{**base_cfg.__dict__, "fhir_base": "ftp://example.test"})
    with pytest.raises(RuntimeError) as exc:
        publisher_module._validate_and_log_config(cfg)
    assert "absolute http(s) URL" in str(exc.value)


def test_validate_config_requires_sql_conn(publisher_module, base_cfg):
    cfg = base_cfg.__class__(**{**base_cfg.__dict__, "sql_conn": ""})
    with pytest.raises(RuntimeError) as exc:
        publisher_module._validate_and_log_config(cfg)
    assert "--sql-conn is required" in str(exc.value)


def test_sqlalchemy_url_from_sqlite_is_passthrough(publisher_module):
    url = "sqlite:///tmp/test.db"
    assert publisher_module._sqlalchemy_url_from_conn_str(url) == url
