"""Tests für parametrizer/_common.py — geteilte Hilfsfunktionen."""
from __future__ import annotations
import pytest
from unittest.mock import MagicMock
from coypu_kg_analyser.parametrizer._common import (
    _extract_uri_suffix, _gta_to_price, _wpi_to_capacity, query_wpi_port,
    _exiobase_io_to_capacity,
)
from coypu_kg_analyser.live_query import LiveQueryClient, QueryResult


def test_extract_uri_suffix_hash():
    assert _extract_uri_suffix("http://example.org#Foo") == "Foo"

def test_extract_uri_suffix_colon():
    assert _extract_uri_suffix("wpi:RepairClass:A") == "A"

def test_extract_uri_suffix_slash():
    assert _extract_uri_suffix("http://example.org/path/Bar") == "Bar"

def test_extract_uri_suffix_plain():
    assert _extract_uri_suffix("plain") == "plain"

def test_wpi_to_capacity_a_large():
    assert _wpi_to_capacity("A", "L") == 1.0

def test_wpi_to_capacity_a_vl():
    assert _wpi_to_capacity("A", "VL") == 1.0

def test_wpi_to_capacity_empty():
    assert _wpi_to_capacity("", "") == 1.0

def test_wpi_to_capacity_b():
    assert _wpi_to_capacity("B", "M") == 0.85

def test_wpi_to_capacity_c():
    assert _wpi_to_capacity("C", "S") == 0.7

def test_query_wpi_port_success():
    client = MagicMock(spec=LiveQueryClient)
    base = "https://schema.coypu.org/world-port-index#"
    client.query.return_value = QueryResult(
        query="SELECT ...",
        raw_response={
            "head": {"vars": ["repairClass", "portSize"]},
            "results": {"bindings": [{
                "repairClass": {"value": f"{base}RepairClass:A"},
                "portSize": {"value": f"{base}SizeClass:L"},
            }]},
        },
    )
    assert query_wpi_port(client, "DEHAM") == 1.0

def test_query_wpi_port_empty():
    client = MagicMock(spec=LiveQueryClient)
    client.query.return_value = QueryResult(
        query="SELECT ...",
        raw_response={"head": {"vars": []}, "results": {"bindings": []}},
    )
    assert query_wpi_port(client, "UNKNOWN") == 1.0

def test_extract_uri_suffix_empty():
    assert _extract_uri_suffix("") == ""

def test_query_wpi_port_repair_b():
    client = MagicMock(spec=LiveQueryClient)
    base = "https://schema.coypu.org/world-port-index#"
    client.query.return_value = QueryResult(
        query="SELECT ...",
        raw_response={
            "head": {"vars": ["repairClass", "portSize"]},
            "results": {"bindings": [{
                "repairClass": {"value": f"{base}RepairClass:B"},
                "portSize": {"value": f"{base}SizeClass:M"},
            }]},
        },
    )
    assert query_wpi_port(client, "BRSSZ") == 0.85


# --- _gta_to_price ---

def test_gta_to_price_zero():
    assert _gta_to_price(0) == 1.0

def test_gta_to_price_low():
    assert _gta_to_price(1) == 1.15
    assert _gta_to_price(2) == 1.15

def test_gta_to_price_medium():
    assert _gta_to_price(3) == 1.30
    assert _gta_to_price(5) == 1.30

def test_gta_to_price_high():
    assert _gta_to_price(6) == 1.50
    assert _gta_to_price(100) == 1.50


class TestExiobaseIoToCapacity:
    def test_zero_coeff_returns_full_capacity(self) -> None:
        assert _exiobase_io_to_capacity(0.0, 1.5) == 1.0

    def test_negligible_impact_returns_full_capacity(self) -> None:
        # 0.0005 * 0.5 = 0.00025 < 0.001 → 1.0
        assert _exiobase_io_to_capacity(0.0005, 1.5) == 1.0

    def test_low_impact_returns_097(self) -> None:
        # 0.004 * 0.5 = 0.002 → 0.97
        assert _exiobase_io_to_capacity(0.004, 1.5) == 0.97

    def test_medium_impact_returns_093(self) -> None:
        # 0.012 * 0.5 = 0.006 → 0.93
        assert _exiobase_io_to_capacity(0.012, 1.5) == 0.93

    def test_high_impact_returns_088(self) -> None:
        # 0.04 * 0.5 = 0.02 → 0.88
        assert _exiobase_io_to_capacity(0.04, 1.5) == 0.88

    def test_very_high_impact_returns_082(self) -> None:
        # 0.15 * 0.5 = 0.075 → 0.82
        assert _exiobase_io_to_capacity(0.15, 1.5) == 0.82

    def test_oil_price_factor_1_means_no_shock(self) -> None:
        # Ölpreis-Faktor 1.0 = kein Anstieg → kein Einfluss
        assert _exiobase_io_to_capacity(0.5, 1.0) == 1.0


class TestQueryExiobaseOilSensitivity:
    _BASE = "https://data.coypu.org/industry/exiobase/regional/"
    _AUTO_URI = f"{_BASE}Manufacture_of_motor_vehicles_trailers_and_semitrailers_DE"
    _OIL_URI = f"{_BASE}Petroleum_refineries_DE"

    def _make_coeff_result(self, quantity: float) -> QueryResult:
        return QueryResult(
            query="SELECT ...",
            raw_response={
                "head": {"vars": ["supplyIndustry", "quantity"]},
                "results": {
                    "bindings": [
                        {
                            "supplyIndustry": {"value": self._OIL_URI},
                            "quantity": {"value": str(quantity)},
                        }
                    ]
                },
            },
        )

    def test_returns_sum_of_oil_coefficients(self) -> None:
        from coypu_kg_analyser.parametrizer._common import query_exiobase_oil_sensitivity
        mock_client = MagicMock(spec=LiveQueryClient)
        mock_client.query.return_value = self._make_coeff_result(0.002740)
        result = query_exiobase_oil_sensitivity(mock_client, self._AUTO_URI)
        assert result == pytest.approx(0.002740, rel=1e-4)

    def test_empty_result_returns_zero(self) -> None:
        from coypu_kg_analyser.parametrizer._common import query_exiobase_oil_sensitivity
        mock_client = MagicMock(spec=LiveQueryClient)
        mock_client.query.return_value = QueryResult(
            query="SELECT ...",
            raw_response={"head": {"vars": []}, "results": {"bindings": []}},
        )
        result = query_exiobase_oil_sensitivity(mock_client, self._AUTO_URI)
        assert result == 0.0

    def test_query_error_returns_zero(self) -> None:
        from coypu_kg_analyser.parametrizer._common import query_exiobase_oil_sensitivity
        mock_client = MagicMock(spec=LiveQueryClient)
        mock_client.query.side_effect = Exception("network error")
        result = query_exiobase_oil_sensitivity(mock_client, self._AUTO_URI)
        assert result == 0.0

    def test_multiple_suppliers_summed(self) -> None:
        from coypu_kg_analyser.parametrizer._common import query_exiobase_oil_sensitivity
        mock_client = MagicMock(spec=LiveQueryClient)
        mock_client.query.return_value = QueryResult(
            query="SELECT ...",
            raw_response={
                "head": {"vars": ["supplyIndustry", "quantity"]},
                "results": {
                    "bindings": [
                        {"supplyIndustry": {"value": f"{self._BASE}Petroleum_refineries_DE"},
                         "quantity": {"value": "0.003"}},
                        {"supplyIndustry": {"value": f"{self._BASE}Extraction_of_crude_petroleum_DE"},
                         "quantity": {"value": "0.001"}},
                    ]
                },
            },
        )
        result = query_exiobase_oil_sensitivity(mock_client, self._AUTO_URI)
        assert result == pytest.approx(0.004, rel=1e-4)


from coypu_kg_analyser.parametrizer._common import _exiobase_oilseed_to_capacity, query_exiobase_oilseed_sensitivity


class TestExiobaseOilseedToCapacity:
    def test_zero_disruption_returns_full_capacity(self) -> None:
        # kein Anbauausfall → kein Einfluss auf Ölmühlen
        assert _exiobase_oilseed_to_capacity(0.26, 0.0) == 1.0

    def test_zero_coeff_returns_full_capacity(self) -> None:
        assert _exiobase_oilseed_to_capacity(0.0, 0.5) == 1.0

    def test_negligible_impact_returns_full_capacity(self) -> None:
        # 0.046 * 0.20 = 0.0092 < 0.01 → 1.0
        assert _exiobase_oilseed_to_capacity(0.046, 0.20) == 1.0

    def test_low_impact_returns_095(self) -> None:
        # 0.046 * 0.50 = 0.023 → 0.95
        assert _exiobase_oilseed_to_capacity(0.046, 0.50) == 0.95

    def test_medium_impact_returns_090(self) -> None:
        # 0.26 * 0.30 = 0.078 → 0.90
        assert _exiobase_oilseed_to_capacity(0.26, 0.30) == 0.90

    def test_high_impact_returns_080(self) -> None:
        # 0.26 * 0.60 = 0.156 → 0.80
        assert _exiobase_oilseed_to_capacity(0.26, 0.60) == 0.80

    def test_very_high_impact_returns_070(self) -> None:
        # 0.26 * 1.0 = 0.26 → 0.70
        assert _exiobase_oilseed_to_capacity(0.26, 1.0) == 0.70


class TestQueryExiobaseOilseedSensitivity:
    _BASE = "https://data.coypu.org/industry/exiobase/regional/"
    _OILMILL_URI = f"{_BASE}Processing_vegetable_oils_and_fats_DE"
    _OILSEED_URI = f"{_BASE}Cultivation_of_oil_seeds_DE"
    _VEGOIL_URI = f"{_BASE}Processing_vegetable_oils_and_fats_NL"

    def _make_oilseed_result(self, uri: str, quantity: float):
        return QueryResult(
            query="SELECT ...",
            raw_response={
                "head": {"vars": ["supplyIndustry", "quantity"]},
                "results": {
                    "bindings": [
                        {"supplyIndustry": {"value": uri}, "quantity": {"value": str(quantity)}}
                    ]
                },
            },
        )

    def test_returns_sum_of_oilseed_coefficients(self) -> None:
        mock_client = MagicMock(spec=LiveQueryClient)
        mock_client.query.return_value = self._make_oilseed_result(self._OILSEED_URI, 0.134588)
        result = query_exiobase_oilseed_sensitivity(mock_client, self._OILMILL_URI)
        assert result == pytest.approx(0.134588, rel=1e-4)

    def test_vegetable_oil_supplier_also_counted(self) -> None:
        mock_client = MagicMock(spec=LiveQueryClient)
        mock_client.query.return_value = self._make_oilseed_result(self._VEGOIL_URI, 0.006347)
        result = query_exiobase_oilseed_sensitivity(mock_client, self._OILMILL_URI)
        assert result == pytest.approx(0.006347, rel=1e-4)

    def test_empty_result_returns_zero(self) -> None:
        mock_client = MagicMock(spec=LiveQueryClient)
        mock_client.query.return_value = QueryResult(
            query="SELECT ...",
            raw_response={"head": {"vars": []}, "results": {"bindings": []}},
        )
        result = query_exiobase_oilseed_sensitivity(mock_client, self._OILMILL_URI)
        assert result == 0.0

    def test_query_error_returns_zero(self) -> None:
        mock_client = MagicMock(spec=LiveQueryClient)
        mock_client.query.side_effect = Exception("timeout")
        result = query_exiobase_oilseed_sensitivity(mock_client, self._OILMILL_URI)
        assert result == 0.0
