"""Tests für S1Parametrizer (S1-Soja KG → Simulation-Schocks)."""
from __future__ import annotations
from unittest.mock import MagicMock
from coypu_kg_analyser.parametrizer.s1_soja import (
    S1Parametrizer, S1ParametrizerResult,
    _emdat_to_capacity, _gta_export_to_capacity,
)
from coypu_kg_analyser.live_query import LiveQueryClient, QueryResult


def _make_rows(n: int, var: str = "e") -> QueryResult:
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
    return QueryResult(query="SELECT ...", raw_response={"head": {"vars": []}, "results": {"bindings": []}})


# --- Mapping-Tests ---

def test_emdat_to_capacity_zero():
    assert _emdat_to_capacity(0) == 1.0

def test_emdat_to_capacity_low():
    assert _emdat_to_capacity(1) == 0.85
    assert _emdat_to_capacity(3) == 0.85

def test_emdat_to_capacity_medium():
    assert _emdat_to_capacity(4) == 0.70
    assert _emdat_to_capacity(10) == 0.70

def test_emdat_to_capacity_high():
    assert _emdat_to_capacity(11) == 0.55
    assert _emdat_to_capacity(50) == 0.55

def test_gta_export_to_capacity_zero():
    assert _gta_export_to_capacity(0) == 1.0

def test_gta_export_to_capacity_low():
    assert _gta_export_to_capacity(1) == 0.90
    assert _gta_export_to_capacity(2) == 0.90

def test_gta_export_to_capacity_medium():
    assert _gta_export_to_capacity(3) == 0.75
    assert _gta_export_to_capacity(5) == 0.75

def test_gta_export_to_capacity_high():
    assert _gta_export_to_capacity(6) == 0.60


# --- farm_risk kombiniert EM-DAT + GTA ---

def test_farm_risk_takes_minimum():
    """Schlechtester Wert aus EM-DAT und GTA dominiert."""
    client = MagicMock(spec=LiveQueryClient)
    # EM-DAT: 5 Ereignisse → 0.70; GTA: 1 Beschränkung → 0.90 → min = 0.70
    client.query.side_effect = [_make_rows(5), _make_rows(1)]
    p = S1Parametrizer(client)
    count_emdat, count_gta, factor = p.get_farm_risk("BRA")
    assert factor == 0.70
    assert count_emdat == 5
    assert count_gta == 1

def test_farm_risk_defaults_on_empty():
    client = MagicMock(spec=LiveQueryClient)
    client.query.return_value = _make_empty()
    p = S1Parametrizer(client)
    count_emdat, count_gta, factor = p.get_farm_risk("ARG")
    assert factor == 1.0


# --- Struktur: build_shocks ---

def test_build_shocks_has_all_targets():
    client = MagicMock(spec=LiveQueryClient)
    # Reihenfolge: BRA emdat, BRA gta, ARG emdat, ARG gta,
    #              USA emdat, USA gta, DEU emdat, DEU gta,
    #              Santos (BRSFE), Rosario (ARROS), New Orleans (USNOL), Hamburg (DEHAM),
    #              Paranaguá (BRPNG), Rotterdam (NLRTM),
    #              fertilizer GTA, gas GTA,
    #              EXIOBASE eu_oil_mills, EXIOBASE feed_mills
    client.query.side_effect = [
        _make_rows(0), _make_rows(0),   # BRA
        _make_rows(0), _make_rows(0),   # ARG
        _make_rows(0), _make_rows(0),   # USA
        _make_rows(0), _make_rows(0),   # DEU
        _make_port("A", "L"),           # Santos
        _make_port("A", "L"),           # Rosario
        _make_port("A", "L"),           # New Orleans
        _make_port("A", "L"),           # Hamburg
        _make_port("A", "L"),           # Paranaguá
        _make_port("A", "L"),           # Rotterdam
        _make_rows(0, "intervention"),  # fertilizer GTA
        _make_rows(0, "intervention"),  # gas GTA
        _make_empty(),                  # EXIOBASE eu_oil_mills
        _make_empty(),                  # EXIOBASE feed_mills
    ]
    p = S1Parametrizer(client)
    result = p.build_shocks()
    target_ids = {s["target_id"] for s in result.shocks}
    assert target_ids == {
        "bra_soy_farm", "arg_soy_farm", "usa_soy_farm", "deu_soy_farm",
        "fertilizer_input", "energy_input",
        "santos_port", "rosario_port",
        "paranagua_port", "rotterdam_port", "hamburg_port", "us_gulf_ports",
        "eu_oil_mills", "feed_mills",
    }
    for shock in result.shocks:
        assert "shock_type" in shock
        assert "magnitude" in shock
        assert isinstance(shock["magnitude"], float)


# --- Vollständiger Lauf ---

def test_build_shocks_mocked_full():
    client = MagicMock(spec=LiveQueryClient)
    client.query.side_effect = [
        _make_rows(8),  _make_rows(3),  # BRA: 8 EM-DAT → 0.70; 3 GTA → 0.75; min=0.70
        _make_rows(2),  _make_rows(0),  # ARG: 2 EM-DAT → 0.85; 0 GTA → 1.0; min=0.85
        _make_rows(0),  _make_rows(0),  # USA: 0 EM-DAT → 1.0; 0 GTA → 1.0; min=1.0
        _make_rows(0),  _make_rows(0),  # DEU: 0 EM-DAT → 1.0; 0 GTA → 1.0; min=1.0
        _make_port("A", "L"),           # Santos → 1.0
        _make_port("B", "M"),           # Rosario → 0.85
        _make_port("A", "L"),           # New Orleans → 1.0
        _make_port("A", "L"),           # Hamburg → 1.0
        _make_port("A", "L"),           # Paranaguá → 1.0
        _make_port("A", "L"),           # Rotterdam → 1.0
        _make_rows(4, "intervention"),  # fertilizer HS31: 4 → 1.30
        _make_rows(6, "intervention"),  # gas HS27: 6 → 1.50
        _make_empty(),                  # EXIOBASE eu_oil_mills
        _make_empty(),                  # EXIOBASE feed_mills
    ]
    p = S1Parametrizer(client, lookback_years=3)
    result = p.build_shocks()

    def shock(tid): return next(s for s in result.shocks if s["target_id"] == tid)

    assert shock("bra_soy_farm")["magnitude"] == 0.70
    assert shock("arg_soy_farm")["magnitude"] == 0.85
    assert shock("santos_port")["magnitude"] == 1.0
    assert shock("rosario_port")["magnitude"] == 0.85
    assert shock("fertilizer_input")["magnitude"] == 1.30
    assert shock("energy_input")["magnitude"] == 1.50

    assert result.farm_risks["BRA"] == (8, 3, 0.70)
    assert result.farm_risks["ARG"] == (2, 0, 0.85)

    out = result.to_output_dict()
    assert out["scenario"] == "soy_feed_disruption"
    assert len(out["shocks"]) == 14
    assert "summary" in out


# --- to_output_dict direkt ---

def test_s1_result_to_output_dict():
    result = S1ParametrizerResult(
        shocks=[
            {"target_id": "bra_soy_farm", "shock_type": "capacity", "magnitude": 0.85},
            {"target_id": "arg_soy_farm", "shock_type": "capacity", "magnitude": 1.0},
        ],
        farm_risks={
            "BRA": (2, 1, 0.85),
            "ARG": (0, 0, 1.0),
        },
        port_capacities={
            "BRSFE": 1.0,
            "ARROS": 0.9,
            "DEHAM": 1.0,
        },
        fertilizer_price=1.15,
        energy_price=1.30,
        summary="Test-Summary",
        generated_at="2026-03-01T00:00:00",
    )

    out = result.to_output_dict(lookback_years=3)

    assert out["scenario"] == "soy_feed_disruption"
    assert out["generated_at"] == "2026-03-01T00:00:00"
    assert out["metadata"]["lookback_years"] == 3
    assert out["metadata"]["fertilizer_price"] == 1.15
    assert out["metadata"]["energy_price"] == 1.30
    assert out["metadata"]["farm_risks"]["BRA"]["emdat_count"] == 2
    assert out["metadata"]["farm_risks"]["BRA"]["gta_count"] == 1
    assert out["metadata"]["farm_risks"]["BRA"]["capacity"] == 0.85
    assert out["metadata"]["port_capacities"]["BRSFE"] == 1.0
    assert len(out["shocks"]) == 2
    assert out["summary"] == "Test-Summary"


# Hilfsfunktionen für erweiterte S1-Tests
def _make_empty_s1():
    from coypu_kg_analyser.live_query import QueryResult
    return QueryResult(
        query="SELECT ...",
        raw_response={"head": {"vars": []}, "results": {"bindings": []}},
    )


def _make_port_s1(repair: str, size: str):
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


def _make_oilseed_s1(quantity: float):
    from coypu_kg_analyser.live_query import QueryResult
    base = "https://data.coypu.org/industry/exiobase/regional/"
    return QueryResult(
        query="SELECT ...",
        raw_response={
            "head": {"vars": ["supplyIndustry", "quantity"]},
            "results": {
                "bindings": [
                    {
                        "supplyIndustry": {"value": f"{base}Cultivation_of_oil_seeds_DE"},
                        "quantity": {"value": str(quantity)},
                    }
                ]
            },
        },
    )


def _build_s1_responses(
    bra_emdat=0, bra_gta=0,
    arg_emdat=0, arg_gta=0,
    usa_emdat=0, usa_gta=0,
    deu_emdat=0, deu_gta=0,
    wpi_santos=("A", "L"),
    wpi_rosario=("A", "L"),
    wpi_neworleans=("A", "L"),
    wpi_hamburg=("A", "L"),
    wpi_paranagua=("A", "L"),
    wpi_rotterdam=("A", "L"),
    gta_fert=0,
    gta_gas=0,
    exio_oilmill=0.0,
    exio_feedmill=0.0,
):
    """20 Queries: 8xEM-DAT+GTA (je Land 2) + 6xWPI + 2xGTA-Preis + 2xEXIOBASE."""
    def rows(n):
        from coypu_kg_analyser.live_query import QueryResult
        return QueryResult(
            query="SELECT ...",
            raw_response={
                "head": {"vars": ["e"]},
                "results": {"bindings": [{"e": {"value": f"x/{i}"}} for i in range(n)]},
            },
        )

    return [
        rows(bra_emdat), rows(bra_gta),       # BRA
        rows(arg_emdat), rows(arg_gta),       # ARG
        rows(usa_emdat), rows(usa_gta),       # USA
        rows(deu_emdat), rows(deu_gta),       # DEU
        _make_port_s1(*wpi_santos),           # Santos BRSFE
        _make_port_s1(*wpi_rosario),          # Rosario ARROS
        _make_port_s1(*wpi_neworleans),       # New Orleans USNOL
        _make_port_s1(*wpi_hamburg),          # Hamburg DEHAM
        _make_port_s1(*wpi_paranagua),        # Paranaguá BRPNG  ← NEU
        _make_port_s1(*wpi_rotterdam),        # Rotterdam NLRTM  ← NEU
        rows(gta_fert),                       # GTA HS31 Dünger
        rows(gta_gas),                        # GTA HS27 Gas
        _make_oilseed_s1(exio_oilmill),       # EXIOBASE eu_oil_mills
        _make_oilseed_s1(exio_feedmill),      # EXIOBASE feed_mills
    ]


class TestS1BuildShocksExtended:
    def test_new_port_targets_in_output(self) -> None:
        """paranagua_port und rotterdam_port sind im Output."""
        mock_client = MagicMock(spec=LiveQueryClient)
        mock_client.query.side_effect = _build_s1_responses()

        p = S1Parametrizer(mock_client)
        result = p.build_shocks()

        target_ids = {s["target_id"] for s in result.shocks}
        assert "paranagua_port" in target_ids
        assert "rotterdam_port" in target_ids
        assert "hamburg_port" in target_ids
        assert "us_gulf_ports" in target_ids

    def test_exiobase_industry_targets_in_output(self) -> None:
        """eu_oil_mills und feed_mills sind im Output."""
        mock_client = MagicMock(spec=LiveQueryClient)
        mock_client.query.side_effect = _build_s1_responses()

        p = S1Parametrizer(mock_client)
        result = p.build_shocks()

        target_ids = {s["target_id"] for s in result.shocks}
        assert "eu_oil_mills" in target_ids
        assert "feed_mills" in target_ids

    def test_total_shock_count_is_14(self) -> None:
        """14 Schocks total (8 bestehende + 6 neue)."""
        mock_client = MagicMock(spec=LiveQueryClient)
        mock_client.query.side_effect = _build_s1_responses()

        p = S1Parametrizer(mock_client)
        result = p.build_shocks()

        assert len(result.shocks) == 14

    def test_paranagua_wpi_b_s_gives_085(self) -> None:
        """WPI Repair=B, Size=S → capacity=0.85."""
        mock_client = MagicMock(spec=LiveQueryClient)
        mock_client.query.side_effect = _build_s1_responses(
            wpi_paranagua=("B", "S"),
        )

        p = S1Parametrizer(mock_client)
        result = p.build_shocks()

        paranagua = next(s for s in result.shocks if s["target_id"] == "paranagua_port")
        assert paranagua["magnitude"] == 0.85

    def test_eu_oil_mills_reduced_when_farms_disrupted(self) -> None:
        """Bei BRA-Ausfall (bra_emdat=11) → eu_oil_mills Kapazität reduziert."""
        mock_client = MagicMock(spec=LiveQueryClient)
        mock_client.query.side_effect = _build_s1_responses(
            bra_emdat=11,               # BRA cap = 0.55 → disruption = 0.45
            exio_oilmill=0.26,          # oilseed_coeff 0.26
        )

        p = S1Parametrizer(mock_client)
        result = p.build_shocks()

        oilmill = next(s for s in result.shocks if s["target_id"] == "eu_oil_mills")
        # 0.26 * 0.45 = 0.117 > 0.10 und < 0.20 → 0.80
        assert oilmill["magnitude"] == 0.80

    def test_exiobase_capacities_field_present(self) -> None:
        """S1ParametrizerResult hat exiobase_industry_capacities dict."""
        mock_client = MagicMock(spec=LiveQueryClient)
        mock_client.query.side_effect = _build_s1_responses(exio_oilmill=0.26, exio_feedmill=0.046)

        p = S1Parametrizer(mock_client)
        result = p.build_shocks()

        assert hasattr(result, "exiobase_industry_capacities")
        assert "eu_oil_mills" in result.exiobase_industry_capacities
        assert "feed_mills" in result.exiobase_industry_capacities
