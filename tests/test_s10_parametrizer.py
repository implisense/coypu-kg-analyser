"""Tests für S10Parametrizer (S10-Iran-Angriff KG → Simulation-Schocks)."""
from __future__ import annotations
from unittest.mock import MagicMock
from coypu_kg_analyser.parametrizer.s10_iran import (
    S10Parametrizer, S10ParametrizerResult,
    _acled_to_capacity, _gta_sanctions_to_capacity,
)
from coypu_kg_analyser.live_query import LiveQueryClient, QueryResult


def test_acled_to_capacity_zero():
    assert _acled_to_capacity(0) == 1.0

def test_acled_to_capacity_low():
    assert _acled_to_capacity(1) == 0.7
    assert _acled_to_capacity(10) == 0.7

def test_acled_to_capacity_medium():
    assert _acled_to_capacity(11) == 0.5
    assert _acled_to_capacity(50) == 0.5

def test_acled_to_capacity_high():
    assert _acled_to_capacity(51) == 0.35
    assert _acled_to_capacity(200) == 0.35

def test_acled_to_capacity_severe():
    assert _acled_to_capacity(201) == 0.2

def test_gta_sanctions_to_capacity_zero():
    assert _gta_sanctions_to_capacity(0) == 1.0

def test_gta_sanctions_to_capacity_low():
    assert _gta_sanctions_to_capacity(1) == 0.75
    assert _gta_sanctions_to_capacity(2) == 0.75

def test_gta_sanctions_to_capacity_medium():
    assert _gta_sanctions_to_capacity(3) == 0.55
    assert _gta_sanctions_to_capacity(5) == 0.55

def test_gta_sanctions_to_capacity_high():
    assert _gta_sanctions_to_capacity(6) == 0.35
    assert _gta_sanctions_to_capacity(20) == 0.35


def test_s10_result_to_output_dict():
    result = S10ParametrizerResult(
        shocks=[
            {"target_id": "strait_of_hormuz", "shock_type": "capacity", "magnitude": 0.7},
            {"target_id": "global_oil_market", "shock_type": "price", "magnitude": 1.3},
        ],
        acled_gulf_count=8,
        acled_red_sea_count=3,
        gta_oil_sanctions_count=4,
        gta_copper_sanctions_count=1,
        gta_oil_price_count=6,
        port_capacities={"IRBND": 1.0, "NLRTM": 0.85, "AEJEA": 1.0},
        summary="Test-Summary",
        generated_at="2026-03-03T00:00:00",
    )
    out = result.to_output_dict(lookback_days=180)
    assert out["scenario"] == "iran_attack_scenario"
    assert out["generated_at"] == "2026-03-03T00:00:00"
    assert out["metadata"]["acled_gulf_count"] == 8
    assert out["metadata"]["acled_red_sea_count"] == 3
    assert out["metadata"]["gta_oil_sanctions_count"] == 4
    assert out["metadata"]["gta_copper_sanctions_count"] == 1
    assert out["metadata"]["gta_oil_price_count"] == 6
    assert out["metadata"]["lookback_days"] == 180
    assert out["metadata"]["port_capacities"]["NLRTM"] == 0.85
    assert len(out["shocks"]) == 2
    assert out["summary"] == "Test-Summary"


def _make_rows(n: int, var: str = "event") -> QueryResult:
    return QueryResult(
        query="SELECT ...",
        raw_response={
            "head": {"vars": [var]},
            "results": {"bindings": [{var: {"value": f"https://x.org/{i}"}} for i in range(n)]},
        },
    )

def _make_port(repair: str, size: str) -> QueryResult:
    base = "https://schema.coypu.org/world-port-index#"
    return QueryResult(
        query="SELECT ...",
        raw_response={
            "head": {"vars": ["repairClass", "portSize"]},
            "results": {"bindings": [{"repairClass": {"value": f"{base}RepairClass:{repair}"}, "portSize": {"value": f"{base}SizeClass:{size}"}}]},
        },
    )

def _make_empty() -> QueryResult:
    return QueryResult(
        query="SELECT ...",
        raw_response={"head": {"vars": []}, "results": {"bindings": []}},
    )


def test_build_shocks_has_all_targets():
    """build_shocks() muss genau 15 Shock-Objekte mit korrekten target_ids zurückgeben."""
    client = MagicMock(spec=LiveQueryClient)
    client.query.side_effect = [
        _make_rows(0),                   # ACLED Golf
        _make_rows(0),                   # ACLED Rotes Meer
        _make_rows(0, "intervention"),   # GTA HS27 USA
        _make_rows(0, "intervention"),   # GTA HS27 DEU
        _make_rows(0, "intervention"),   # GTA Kupfer USA
        _make_rows(0, "intervention"),   # GTA Kupfer DEU
        _make_rows(0, "intervention"),   # GTA HS27 global
        _make_port("A", "L"),            # WPI IRBND
        _make_port("A", "L"),            # WPI NLRTM
        _make_port("A", "L"),            # WPI AEJEA
        _make_empty(),                   # EXIOBASE Auto
        _make_empty(),                   # EXIOBASE Chemie
        _make_empty(),                   # EXIOBASE Metall
        _make_empty(),                   # EXIOBASE Luftfahrt
        _make_empty(),                   # EXIOBASE Gas/LNG
    ]
    p = S10Parametrizer(client)
    result = p.build_shocks()
    target_ids = {s["target_id"] for s in result.shocks}
    assert target_ids == {
        "strait_of_hormuz", "persian_gulf_region",
        "red_sea_route",
        "nioc", "iran_oil_fields",
        "global_oil_market",
        "iran_copper_mines",
        "kharg_island",
        "eu_refineries",
        "dubai_airport_dxb",
        "german_auto_industry",
        "german_chemical_industry",
        "eu_metal_industry",
        "european_airlines_rerouting",
        "eu_lng_terminals",
    }
    for shock in result.shocks:
        assert "shock_type" in shock
        assert "magnitude" in shock
        assert isinstance(shock["magnitude"], float)


def test_build_shocks_magnitudes():
    """Stichprobe: Korrekte Magnitude-Berechnung bei bekannten Inputs."""
    client = MagicMock(spec=LiveQueryClient)
    client.query.side_effect = [
        _make_rows(15),                  # ACLED Golf: 15 → 0.5
        _make_rows(5),                   # ACLED Rotes Meer: 5 → 0.7
        _make_rows(3, "intervention"),   # GTA HS27 USA: 3 (kumuliert mit DEU)
        _make_rows(1, "intervention"),   # GTA HS27 DEU: 1 → total 4 → 0.55
        _make_rows(0, "intervention"),   # GTA Kupfer USA
        _make_rows(0, "intervention"),   # GTA Kupfer DEU → total 0 → 1.0
        _make_rows(6, "intervention"),   # GTA HS27 global: 6 → 1.50
        _make_port("A", "L"),           # WPI IRBND → 1.0
        _make_port("B", "M"),           # WPI NLRTM → 0.85
        _make_port("A", "L"),           # WPI AEJEA → 1.0
        _make_empty(),                  # EXIOBASE Auto
        _make_empty(),                  # EXIOBASE Chemie
        _make_empty(),                  # EXIOBASE Metall
        _make_empty(),                  # EXIOBASE Luftfahrt
        _make_empty(),                  # EXIOBASE Gas/LNG
    ]
    p = S10Parametrizer(client)
    result = p.build_shocks()

    def shock(tid): return next(s for s in result.shocks if s["target_id"] == tid)

    assert shock("strait_of_hormuz")["magnitude"] == 0.5
    assert shock("persian_gulf_region")["magnitude"] == 0.5
    assert shock("red_sea_route")["magnitude"] == 0.7
    assert shock("nioc")["magnitude"] == 0.55
    assert shock("iran_oil_fields")["magnitude"] == 0.55
    assert shock("iran_copper_mines")["magnitude"] == 1.0
    assert shock("global_oil_market")["magnitude"] == 1.50
    assert shock("eu_refineries")["magnitude"] == 0.85
    assert shock("kharg_island")["magnitude"] == 1.0
    assert shock("dubai_airport_dxb")["magnitude"] == 1.0

    assert result.acled_gulf_count == 15
    assert result.acled_red_sea_count == 5
    assert result.gta_oil_sanctions_count == 4
    assert result.gta_copper_sanctions_count == 0
    assert result.gta_oil_price_count == 6
    assert result.port_capacities == {"IRBND": 1.0, "NLRTM": 0.85, "AEJEA": 1.0}
    assert "0.85" in result.summary or "85%" in result.summary


def test_build_shocks_failed_queries_default_to_safe_values():
    """Fehlgeschlagene Queries dürfen den Build nicht abbrechen — Fallback auf 1.0."""
    client = MagicMock(spec=LiveQueryClient)
    client.query.side_effect = [
        _make_empty(),  # ACLED Golf → 0 → 1.0
        _make_empty(),  # ACLED Rotes Meer
        _make_empty(),  # GTA HS27 USA
        _make_empty(),  # GTA HS27 DEU
        _make_empty(),  # GTA Kupfer USA
        _make_empty(),  # GTA Kupfer DEU
        _make_empty(),  # GTA HS27 global
        _make_empty(),  # WPI IRBND → 1.0
        _make_empty(),  # WPI NLRTM
        _make_empty(),  # WPI AEJEA
        _make_empty(),  # EXIOBASE Auto → 1.0
        _make_empty(),  # EXIOBASE Chemie → 1.0
        _make_empty(),  # EXIOBASE Metall → 1.0
        _make_empty(),  # EXIOBASE Luftfahrt → 1.0
        _make_empty(),  # EXIOBASE Gas/LNG → 1.0
    ]
    p = S10Parametrizer(client)
    result = p.build_shocks()
    for shock in result.shocks:
        assert shock["magnitude"] == 1.0


def test_build_shocks_to_output_dict_integration():
    """to_output_dict() nach echtem build_shocks() liefert korrektes Schema."""
    client = MagicMock(spec=LiveQueryClient)
    client.query.side_effect = [
        _make_rows(0), _make_rows(0),
        _make_rows(0, "intervention"), _make_rows(0, "intervention"),
        _make_rows(0, "intervention"), _make_rows(0, "intervention"),
        _make_rows(0, "intervention"),
        _make_port("A", "L"), _make_port("A", "L"), _make_port("A", "L"),
        _make_empty(), _make_empty(), _make_empty(), _make_empty(), _make_empty(),
    ]
    p = S10Parametrizer(client, lookback_days=90, reference_date="2026-03-01")
    result = p.build_shocks()
    out = result.to_output_dict(lookback_days=90)

    assert out["scenario"] == "iran_attack_scenario"
    assert out["metadata"]["lookback_days"] == 90
    assert isinstance(out["generated_at"], str)
    assert len(out["shocks"]) == 15
    assert "summary" in out
    target_ids = {s["target_id"] for s in out["shocks"]}
    assert "strait_of_hormuz" in target_ids
    assert "global_oil_market" in target_ids
    assert "iran_copper_mines" in target_ids
    assert "eu_refineries" in target_ids


# Hilfsfunktionen für EXIOBASE-Tests
def _make_empty_qr():
    from coypu_kg_analyser.live_query import QueryResult
    return QueryResult(
        query="SELECT ...",
        raw_response={"head": {"vars": []}, "results": {"bindings": []}},
    )


def _make_port_qr(repair: str, size: str):
    from coypu_kg_analyser.live_query import QueryResult
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


def _make_coeff_qr(quantity: float):
    from coypu_kg_analyser.live_query import QueryResult
    base = "https://data.coypu.org/industry/exiobase/regional/"
    return QueryResult(
        query="SELECT ...",
        raw_response={
            "head": {"vars": ["supplyIndustry", "quantity"]},
            "results": {
                "bindings": [
                    {
                        "supplyIndustry": {"value": f"{base}Petroleum_refineries_DE"},
                        "quantity": {"value": str(quantity)},
                    }
                ]
            },
        },
    )


def _make_rows_qr(n: int, var: str = "event"):
    from coypu_kg_analyser.live_query import QueryResult
    return QueryResult(
        query="SELECT ...",
        raw_response={
            "head": {"vars": [var]},
            "results": {"bindings": [{var: {"value": f"https://x/{i}"}} for i in range(n)]},
        },
    )


def _build_full_mock_responses(
    acled_gulf=0, acled_red_sea=0,
    gta_oil_usa=0, gta_oil_deu=0,
    gta_copper_usa=0, gta_copper_deu=0,
    gta_price=0,
    wpi_irbnd=("A", "L"), wpi_nlrtm=("A", "L"), wpi_aejea=("A", "L"),
    exio_auto=0.0, exio_chem=0.0, exio_metal=0.0, exio_air=0.0, exio_gas=0.0,
):
    """15 Queries: 2 ACLED + 4 GTA (2xUSA+DEU je Typ) + 1 GTA-Preis + 3 WPI + 5 EXIOBASE."""
    return [
        _make_rows_qr(acled_gulf),    # ACLED Persischer Golf
        _make_rows_qr(acled_red_sea), # ACLED Rotes Meer
        _make_rows_qr(gta_oil_usa),   # GTA Öl USA
        _make_rows_qr(gta_oil_deu),   # GTA Öl DEU
        _make_rows_qr(gta_copper_usa), # GTA Kupfer USA
        _make_rows_qr(gta_copper_deu), # GTA Kupfer DEU
        _make_rows_qr(gta_price),     # GTA Ölpreis (global, keine Länderschleife)
        _make_port_qr(*wpi_irbnd),    # WPI Bandar Abbas
        _make_port_qr(*wpi_nlrtm),    # WPI Rotterdam
        _make_port_qr(*wpi_aejea),    # WPI Jebel Ali
        _make_coeff_qr(exio_auto),    # EXIOBASE Auto
        _make_coeff_qr(exio_chem),    # EXIOBASE Chemie
        _make_coeff_qr(exio_metal),   # EXIOBASE Metall
        _make_coeff_qr(exio_air),     # EXIOBASE Luftfahrt
        _make_coeff_qr(exio_gas),     # EXIOBASE Gas/LNG
    ]


class TestS10BuildShocksWithExiobase:
    def test_exiobase_target_ids_in_output(self) -> None:
        """Alle 5 EXIOBASE-target_ids sind im Output."""
        mock_client = MagicMock(spec=LiveQueryClient)
        mock_client.query.side_effect = _build_full_mock_responses()

        p = S10Parametrizer(mock_client)
        result = p.build_shocks()

        target_ids = {s["target_id"] for s in result.shocks}
        assert "german_auto_industry" in target_ids
        assert "german_chemical_industry" in target_ids
        assert "eu_metal_industry" in target_ids
        assert "european_airlines_rerouting" in target_ids
        assert "eu_lng_terminals" in target_ids

    def test_total_shock_count_is_15(self) -> None:
        """15 Schocks total (10 bestehende + 5 EXIOBASE)."""
        mock_client = MagicMock(spec=LiveQueryClient)
        mock_client.query.side_effect = _build_full_mock_responses()

        p = S10Parametrizer(mock_client)
        result = p.build_shocks()

        assert len(result.shocks) == 15

    def test_zero_oil_coeff_gives_full_capacity(self) -> None:
        """Bei 0.0 Öl-Koeffizient ist magnitude == 1.0."""
        mock_client = MagicMock(spec=LiveQueryClient)
        mock_client.query.side_effect = _build_full_mock_responses(
            gta_price=6,  # Ölpreis +50% (Faktor 1.50)
            exio_auto=0.0,
        )

        p = S10Parametrizer(mock_client)
        result = p.build_shocks()

        auto = next(s for s in result.shocks if s["target_id"] == "german_auto_industry")
        assert auto["magnitude"] == 1.0

    def test_high_oil_coeff_reduces_capacity(self) -> None:
        """Hoher Öl-Koeffizient + hoher Ölpreis reduziert Kapazität."""
        mock_client = MagicMock(spec=LiveQueryClient)
        mock_client.query.side_effect = _build_full_mock_responses(
            gta_price=6,    # Ölpreis +50% → Faktor 1.50
            exio_air=0.15,  # Airlines: hoher Koeffizient
        )

        p = S10Parametrizer(mock_client)
        result = p.build_shocks()

        air = next(s for s in result.shocks if s["target_id"] == "european_airlines_rerouting")
        # 0.15 * (1.50 - 1.0) = 0.15 * 0.50 = 0.075 > 0.05 → 0.82
        assert air["magnitude"] == 0.82

    def test_exiobase_capacities_in_result(self) -> None:
        """exiobase_industry_capacities enthält alle 5 Einträge."""
        mock_client = MagicMock(spec=LiveQueryClient)
        mock_client.query.side_effect = _build_full_mock_responses(
            exio_auto=0.003, exio_chem=0.01,
        )

        p = S10Parametrizer(mock_client)
        result = p.build_shocks()

        assert "german_auto_industry" in result.exiobase_industry_capacities
        assert "german_chemical_industry" in result.exiobase_industry_capacities
        assert "eu_metal_industry" in result.exiobase_industry_capacities
        assert "european_airlines_rerouting" in result.exiobase_industry_capacities
        assert "eu_lng_terminals" in result.exiobase_industry_capacities
