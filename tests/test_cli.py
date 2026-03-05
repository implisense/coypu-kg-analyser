"""Tests für CLI-Befehle."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from coypu_kg_analyser.cli import main
from coypu_kg_analyser.live_query import LiveQueryClient, QueryResult


# --- Hilfsfunktionen (analog zu test_s1_parametrizer.py) ---

def _make_rows(n: int, var: str = "e") -> QueryResult:
    return QueryResult(
        query="SELECT ...",
        raw_response={
            "head": {"vars": [var]},
            "results": {
                "bindings": [{var: {"value": f"https://x.org/{i}"}} for i in range(n)]
            },
        },
    )


def _make_port(repair: str, size: str) -> QueryResult:
    base = "https://schema.coypu.org/world-port-index#"
    return QueryResult(
        query="SELECT ...",
        raw_response={
            "head": {"vars": ["repairClass", "portSize"]},
            "results": {
                "bindings": [
                    {
                        "repairClass": {"value": f"{base}RepairClass:{repair}"},
                        "portSize": {"value": f"{base}SizeClass:{size}"},
                    }
                ]
            },
        },
    )


def _make_empty() -> QueryResult:
    return QueryResult(
        query="SELECT ...",
        raw_response={"head": {"vars": []}, "results": {"bindings": []}},
    )


def _default_query_responses() -> list[QueryResult]:
    """20 Standardantworten für einen vollständigen S1-Lauf (alle Felder auf 1.0)."""
    return [
        _make_empty(), _make_empty(),  # BRA: emdat, gta
        _make_empty(), _make_empty(),  # ARG: emdat, gta
        _make_empty(), _make_empty(),  # USA: emdat, gta
        _make_empty(), _make_empty(),  # DEU: emdat, gta
        _make_port("A", "L"),          # Santos
        _make_port("A", "L"),          # Rosario
        _make_port("A", "L"),          # New Orleans
        _make_port("A", "L"),          # Hamburg
        _make_port("A", "L"),          # Paranaguá
        _make_port("A", "L"),          # Rotterdam
        _make_empty(),                 # fertilizer GTA (HS31)
        _make_empty(),                 # gas GTA (HS27)
        _make_empty(),                 # EXIOBASE eu_oil_mills
        _make_empty(),                 # EXIOBASE feed_mills
    ]


# --- parametrize-s1 Tests ---

def test_parametrize_s1_help() -> None:
    """--help gibt exit code 0 zurück und enthält erwartete Optionen."""
    runner = CliRunner()
    result = runner.invoke(main, ["parametrize-s1", "--help"])
    assert result.exit_code == 0
    assert "--output" in result.output
    assert "--lookback-years" in result.output
    assert "--reference-date" in result.output
    assert "--max-results" in result.output
    assert "--endpoint" in result.output


def test_parametrize_s1_stdout_json() -> None:
    """Ohne --output wird valides JSON auf stdout geschrieben."""
    runner = CliRunner(mix_stderr=False)

    mock_client = MagicMock(spec=LiveQueryClient)
    mock_client.query.side_effect = _default_query_responses()

    with patch("coypu_kg_analyser.live_query.LiveQueryClient", return_value=mock_client):
        result = runner.invoke(main, ["parametrize-s1"])

    assert result.exit_code == 0, f"exit={result.exit_code}, out={result.output}"
    data = json.loads(result.output)
    assert data["scenario"] == "soy_feed_disruption"
    assert "shocks" in data
    assert "metadata" in data
    assert "summary" in data


def test_parametrize_s1_invalid_reference_date() -> None:
    """Ungültiges --reference-date führt zu exit code != 0."""
    runner = CliRunner(mix_stderr=False)
    with patch("coypu_kg_analyser.live_query.LiveQueryClient"):
        result = runner.invoke(
            main,
            ["parametrize-s1", "--reference-date", "kein-datum"],
        )
    assert result.exit_code != 0


def test_parametrize_s1_output_file(tmp_path: Path) -> None:
    """--output schreibt JSON in Datei statt stdout."""
    runner = CliRunner(mix_stderr=False)
    out_file = tmp_path / "shocks.json"

    mock_client = MagicMock(spec=LiveQueryClient)
    mock_client.query.side_effect = _default_query_responses()

    with patch("coypu_kg_analyser.live_query.LiveQueryClient", return_value=mock_client):
        result = runner.invoke(main, ["parametrize-s1", "--output", str(out_file)])

    assert result.exit_code == 0, f"exit={result.exit_code}, out={result.output}"
    assert out_file.exists()
    data = json.loads(out_file.read_text(encoding="utf-8"))
    assert data["scenario"] == "soy_feed_disruption"
    assert len(data["shocks"]) == 14


def test_parametrize_s1_shock_targets_and_values() -> None:
    """Korrekte target_ids, magnitudes und metadata.lookback_years im Output."""
    runner = CliRunner(mix_stderr=False)

    mock_client = MagicMock(spec=LiveQueryClient)
    mock_client.query.side_effect = [
        _make_rows(8),  _make_rows(3),  # BRA: 8 EM-DAT → 0.70; 3 GTA → 0.75; min=0.70
        _make_rows(2),  _make_empty(),  # ARG: 2 EM-DAT → 0.85; 0 GTA → 1.0; min=0.85
        _make_empty(),  _make_empty(),  # USA: 1.0
        _make_empty(),  _make_empty(),  # DEU: 1.0
        _make_port("A", "L"),           # Santos → 1.0
        _make_port("B", "M"),           # Rosario → 0.85 (via _wpi_to_capacity)
        _make_port("A", "L"),           # New Orleans → 1.0
        _make_port("A", "L"),           # Hamburg → 1.0
        _make_port("A", "L"),           # Paranaguá → 1.0
        _make_port("A", "L"),           # Rotterdam → 1.0
        _make_rows(4, "intervention"),  # fertilizer HS31: 4 → 1.30
        _make_empty(),                  # gas HS27: 0 → 1.0
        _make_empty(),                  # EXIOBASE eu_oil_mills
        _make_empty(),                  # EXIOBASE feed_mills
    ]

    with patch("coypu_kg_analyser.live_query.LiveQueryClient", return_value=mock_client):
        result = runner.invoke(main, ["parametrize-s1", "--lookback-years", "5"])

    assert result.exit_code == 0, f"exit={result.exit_code}, out={result.output}"
    data = json.loads(result.output)

    target_ids = {s["target_id"] for s in data["shocks"]}
    expected_ids = {
        "bra_soy_farm", "arg_soy_farm", "usa_soy_farm", "deu_soy_farm",
        "fertilizer_input", "energy_input", "santos_port", "rosario_port",
        "paranagua_port", "rotterdam_port", "hamburg_port", "us_gulf_ports",
        "eu_oil_mills", "feed_mills",
    }
    assert target_ids == expected_ids

    def shock(tid: str) -> dict:
        return next(s for s in data["shocks"] if s["target_id"] == tid)

    assert shock("bra_soy_farm")["magnitude"] == 0.70
    assert shock("arg_soy_farm")["magnitude"] == 0.85
    assert shock("fertilizer_input")["magnitude"] == 1.30
    assert shock("energy_input")["magnitude"] == 1.0

    assert data["metadata"]["lookback_years"] == 5


def test_parametrize_s1_reference_date() -> None:
    """--reference-date wird akzeptiert und der Befehl läuft erfolgreich."""
    runner = CliRunner(mix_stderr=False)

    mock_client = MagicMock(spec=LiveQueryClient)
    mock_client.query.side_effect = _default_query_responses()

    with patch("coypu_kg_analyser.live_query.LiveQueryClient", return_value=mock_client):
        result = runner.invoke(
            main,
            ["parametrize-s1", "--reference-date", "2024-06-15"],
        )

    assert result.exit_code == 0, f"exit={result.exit_code}, out={result.output}"
    data = json.loads(result.output)
    assert data["scenario"] == "soy_feed_disruption"


