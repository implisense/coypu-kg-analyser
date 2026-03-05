"""Tests für LiveQueryClient.

Unit-Tests mocken den HTTP-Request.
Integrations-Test (--integration) läuft gegen den echten Endpoint.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from coypu_kg_analyser.live_query import (
    COYPU_ENDPOINT,
    STANDARD_PREFIXES,
    LiveQueryClient,
    QueryResult,
)


# --- QueryResult Tests ---

def test_query_result_success() -> None:
    raw = {
        "head": {"vars": ["name", "value"]},
        "results": {
            "bindings": [
                {"name": {"value": "Foo"}, "value": {"value": "42"}},
                {"name": {"value": "Bar"}, "value": {"value": "7"}},
            ]
        },
    }
    result = QueryResult(query="SELECT ...", raw_response=raw)
    assert result.success
    assert result.row_count == 2
    assert result.variables == ["name", "value"]


def test_query_result_as_dicts() -> None:
    raw = {
        "head": {"vars": ["x"]},
        "results": {"bindings": [{"x": {"value": "hello"}}]},
    }
    result = QueryResult(query="SELECT ...", raw_response=raw)
    assert result.as_dicts() == [{"x": "hello"}]


def test_query_result_as_csv() -> None:
    raw = {
        "head": {"vars": ["a", "b"]},
        "results": {"bindings": [{"a": {"value": "1"}, "b": {"value": "2"}}]},
    }
    result = QueryResult(query="SELECT ...", raw_response=raw)
    csv_text = result.as_csv()
    assert "a,b" in csv_text
    assert "1,2" in csv_text


def test_query_result_error() -> None:
    result = QueryResult(query="SELECT ...", error="HTTP 500")
    assert not result.success
    assert result.row_count == 0
    assert result.bindings == []


def test_query_result_repr() -> None:
    result = QueryResult(query="Q", error="fail")
    assert "error" in repr(result)


# --- LiveQueryClient Unit-Tests (mit Mock) ---

def _make_mock_response(bindings: list[dict], vars_: list[str], status: int = 200) -> MagicMock:
    mock = MagicMock()
    mock.status_code = status
    mock.json.return_value = {
        "head": {"vars": vars_},
        "results": {"bindings": bindings},
    }
    mock.text = ""
    return mock


def test_client_sends_post_request() -> None:
    client = LiveQueryClient(add_standard_prefixes=False)
    mock_resp = _make_mock_response([], ["s"])

    with patch.object(client._session, "post", return_value=mock_resp) as mock_post:
        result = client.query("SELECT ?s WHERE { ?s ?p ?o } LIMIT 1")

    mock_post.assert_called_once()
    call_kwargs = mock_post.call_args
    assert call_kwargs[0][0] == COYPU_ENDPOINT
    assert "query" in call_kwargs[1]["data"]


def test_client_prepends_prefixes() -> None:
    client = LiveQueryClient(add_standard_prefixes=True)
    mock_resp = _make_mock_response([], ["s"])

    with patch.object(client._session, "post", return_value=mock_resp) as mock_post:
        client.query("SELECT ?s WHERE { ?s ?p ?o } LIMIT 1")

    sent_query = mock_post.call_args[1]["data"]["query"]
    assert "PREFIX coy:" in sent_query
    assert "PREFIX geo:" in sent_query


def test_client_no_prefixes_if_already_present() -> None:
    client = LiveQueryClient(add_standard_prefixes=True)
    mock_resp = _make_mock_response([], ["s"])
    query_with_prefix = "PREFIX coy: <https://schema.coypu.org/global#>\nSELECT ?s WHERE { ?s ?p ?o }"

    with patch.object(client._session, "post", return_value=mock_resp) as mock_post:
        client.query(query_with_prefix)

    sent_query = mock_post.call_args[1]["data"]["query"]
    # Kein doppelter PREFIX-Block
    assert sent_query.count("PREFIX coy:") == 1


def test_client_handles_http_error() -> None:
    client = LiveQueryClient()
    mock_resp = MagicMock()
    mock_resp.status_code = 500
    mock_resp.text = "Internal Server Error"

    with patch.object(client._session, "post", return_value=mock_resp):
        result = client.query("SELECT ?s WHERE { ?s ?p ?o }")

    assert not result.success
    assert "500" in result.error


def test_client_handles_timeout() -> None:
    import requests as req_lib
    client = LiveQueryClient(timeout=1)

    with patch.object(client._session, "post", side_effect=req_lib.Timeout):
        result = client.query("SELECT ?s WHERE { ?s ?p ?o }")

    assert not result.success
    assert "Timeout" in result.error


def test_client_handles_connection_error() -> None:
    import requests as req_lib
    client = LiveQueryClient()

    with patch.object(client._session, "post", side_effect=req_lib.ConnectionError("no route")):
        result = client.query("SELECT ?s WHERE { ?s ?p ?o }")

    assert not result.success
    assert "Verbindungsfehler" in result.error


def test_client_query_file(tmp_path: Path) -> None:
    sparql_file = tmp_path / "test.sparql"
    sparql_file.write_text("SELECT ?s WHERE { ?s ?p ?o } LIMIT 1", encoding="utf-8")

    client = LiveQueryClient(add_standard_prefixes=False)
    mock_resp = _make_mock_response([{"s": {"value": "http://example.org/x"}}], ["s"])

    with patch.object(client._session, "post", return_value=mock_resp):
        result = client.query_file(sparql_file)

    assert result.success
    assert result.row_count == 1


def test_client_run_library(tmp_path: Path) -> None:
    for name in ("a_bottleneck_connectivity.sparql", "b_scenario_enrichment.sparql"):
        (tmp_path / name).write_text("SELECT ?s WHERE { ?s ?p ?o } LIMIT 1", encoding="utf-8")

    client = LiveQueryClient(add_standard_prefixes=False)
    mock_resp = _make_mock_response([], ["s"])

    with patch.object(client._session, "post", return_value=mock_resp):
        pairs = client.run_library(tmp_path)

    assert len(pairs) == 2
    for path, result in pairs:
        assert result.success


def test_client_run_library_filter(tmp_path: Path) -> None:
    (tmp_path / "x_bottleneck_connectivity.sparql").write_text("SELECT ?s WHERE {?s ?p ?o}", encoding="utf-8")
    (tmp_path / "x_scenario_enrichment.sparql").write_text("SELECT ?s WHERE {?s ?p ?o}", encoding="utf-8")

    client = LiveQueryClient(add_standard_prefixes=False)
    mock_resp = _make_mock_response([], ["s"])

    with patch.object(client._session, "post", return_value=mock_resp):
        pairs = client.run_library(tmp_path, filter_type="bottleneck_connectivity")

    assert len(pairs) == 1
    assert "bottleneck_connectivity" in str(pairs[0][0])


def test_get_instance_count_returns_int() -> None:
    client = LiveQueryClient()
    mock_resp = _make_mock_response(
        [{"count": {"value": "42"}}], ["count"]
    )

    with patch.object(client._session, "post", return_value=mock_resp):
        count = client.get_instance_count("https://schema.coypu.org/global#Country")

    assert count == 42


def test_get_instance_count_on_error() -> None:
    client = LiveQueryClient()

    with patch.object(client._session, "post", side_effect=Exception("fail")):
        count = client.get_instance_count("https://schema.coypu.org/global#Country")

    assert count is None


def test_standard_prefixes_contain_coypu_namespaces() -> None:
    assert "schema.coypu.org/global#" in STANDARD_PREFIXES
    assert "schema.coypu.org/gta#" in STANDARD_PREFIXES
    assert "schema.coypu.org/em-dat#" in STANDARD_PREFIXES
    assert "opengis.net/ont/geosparql#" in STANDARD_PREFIXES


# --- Integrations-Test (nur manuell mit --integration-Flag) ---

@pytest.mark.skip(reason="Nur manuell ausführen: benötigt Netzwerkzugang zum CoyPu-Endpoint")
def test_live_endpoint_connectivity() -> None:
    """Prüft ob der echte CoyPu-Endpoint erreichbar ist."""
    client = LiveQueryClient()
    assert client.check_connectivity(), "CoyPu-Endpoint nicht erreichbar"


@pytest.mark.skip(reason="Nur manuell ausführen: benötigt Netzwerkzugang zum CoyPu-Endpoint")
def test_live_query_natural_disasters() -> None:
    """Fragt echte Katastrophen-Daten ab."""
    client = LiveQueryClient()
    result = client.query("""
SELECT ?name ?time WHERE {
    ?event a coy:Disaster ;
        rdfs:label ?name ;
        coy:hasPublicationTimestamp ?time .
} ORDER BY DESC(?time) LIMIT 5
""")
    assert result.success, f"Query fehlgeschlagen: {result.error}"
    assert result.row_count > 0
