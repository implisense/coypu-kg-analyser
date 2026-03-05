# coypu_kg_analyser/parametrizer/_common.py
"""Geteilte Hilfsfunktionen für alle Parametrizer-Module."""
from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from coypu_kg_analyser.live_query import LiveQueryClient

_WPI_PORT_QUERY = """\
SELECT ?repairClass ?portSize WHERE {{
    <https://data.coypu.org/infrastructure/port/{locode}>
        wpi:hasRepairs ?repairClass ;
        wpi:hasPortSize ?portSize .
}}
LIMIT 1
"""


def _extract_uri_suffix(uri: str) -> str:
    """Extrahiert den letzten Teil einer URI (nach #, / oder :)."""
    for sep in ("#", "/", ":"):
        if sep in uri:
            suffix = uri.rsplit(sep, 1)[-1]
            # Restliche Separatoren auf dem Suffix weiter anwenden
            for inner_sep in (":", "/"):
                if inner_sep in suffix:
                    suffix = suffix.rsplit(inner_sep, 1)[-1]
            return suffix
    return uri


def _wpi_to_capacity(repair_class: str, port_size: str) -> float:
    """Port-Kapazitätsfaktor basierend auf WPI-Reparaturklasse und Portgröße."""
    if repair_class in ("A", "") and port_size in ("L", "VL", ""):
        return 1.0
    elif repair_class == "B":
        return 0.85
    elif repair_class == "C":
        return 0.7
    return 1.0


def _gta_to_price(red_intervention_count: int) -> float:
    """Preisfaktor basierend auf GTA Red-Interventionen."""
    if red_intervention_count == 0:
        return 1.0
    elif red_intervention_count <= 2:
        return 1.15
    elif red_intervention_count <= 5:
        return 1.30
    else:
        return 1.50


def _exiobase_io_to_capacity(total_oil_coeff: float, oil_price_factor: float) -> float:
    """IO-Koeffizient + Ölpreis-Faktor → Kapazitätsfaktor einer Industrie.

    cost_impact = total_oil_coeff * (oil_price_factor - 1.0)
    """
    cost_impact = total_oil_coeff * (oil_price_factor - 1.0)
    if cost_impact < 0.001:
        return 1.0
    elif cost_impact < 0.003:
        return 0.97
    elif cost_impact < 0.01:
        return 0.93
    elif cost_impact < 0.05:
        return 0.88
    else:
        return 0.82


def query_wpi_port(client: LiveQueryClient, locode: str) -> float:
    """Fragt WPI-Kapazität für einen Port-LOCODE ab. Gibt 1.0 bei Fehler zurück."""
    result = client.query(_WPI_PORT_QUERY.format(locode=locode))
    if not result.success or not result.bindings:
        return 1.0
    row = result.as_dicts()[0]
    repair_class = _extract_uri_suffix(row.get("repairClass", ""))
    port_size = _extract_uri_suffix(row.get("portSize", ""))
    return _wpi_to_capacity(repair_class, port_size)


_EXIOBASE_OIL_INPUT_QUERY = """\
SELECT ?supplyIndustry ?quantity WHERE {{
    ?c coy:isRequiredBy <{industry_uri}> ;
       coy:hasRequirement ?supplyIndustry ;
       coy:hasRequiredQuantity ?quantity .
    FILTER (
        CONTAINS(LCASE(STR(?supplyIndustry)), "petroleum") ||
        CONTAINS(LCASE(STR(?supplyIndustry)), "crude") ||
        CONTAINS(LCASE(STR(?supplyIndustry)), "oil") ||
        CONTAINS(LCASE(STR(?supplyIndustry)), "gas")
    )
}}
"""


def query_exiobase_oil_sensitivity(
    client: "LiveQueryClient", industry_uri: str
) -> float:
    """Summiert IO-Koeffizienten aller Öl-/Gas-Lieferanten einer Industrie.

    Gibt 0.0 zurück bei Fehler oder fehlenden Daten.
    """
    try:
        result = client.query(
            _EXIOBASE_OIL_INPUT_QUERY.format(industry_uri=industry_uri)
        )
    except Exception:
        return 0.0
    if not result.success or not result.bindings:
        return 0.0
    total = 0.0
    for row in result.as_dicts():
        try:
            total += float(row.get("quantity", "0"))
        except (ValueError, TypeError):
            pass
    return total


_EXIOBASE_OILSEED_INPUT_QUERY = """\
SELECT ?supplyIndustry ?quantity WHERE {{
    ?c coy:isRequiredBy <{industry_uri}> ;
       coy:hasRequirement ?supplyIndustry ;
       coy:hasRequiredQuantity ?quantity .
    FILTER (
        CONTAINS(LCASE(STR(?supplyIndustry)), "oil_seed") ||
        CONTAINS(LCASE(STR(?supplyIndustry)), "vegetable_oil")
    )
}}
"""


def _exiobase_oilseed_to_capacity(
    total_oilseed_coeff: float, supply_disruption: float
) -> float:
    """Oilseed IO-Koeffizient + Lieferstörung → Kapazitätsfaktor verarbeitender Betriebe.

    supply_disruption = 1.0 - min_farm_capacity (0.0=keine Störung, 1.0=Totalausfall)
    impact = total_oilseed_coeff * supply_disruption
    """
    impact = total_oilseed_coeff * supply_disruption
    if impact < 0.01:
        return 1.0
    elif impact < 0.05:
        return 0.95
    elif impact < 0.10:
        return 0.90
    elif impact < 0.20:
        return 0.80
    else:
        return 0.70


def query_exiobase_oilseed_sensitivity(
    client: "LiveQueryClient", industry_uri: str
) -> float:
    """Summiert IO-Koeffizienten aller Oilseed/Pflanzenöl-Lieferanten einer Industrie.

    Gibt 0.0 zurück bei Fehler oder fehlenden Daten.
    """
    try:
        result = client.query(
            _EXIOBASE_OILSEED_INPUT_QUERY.format(industry_uri=industry_uri)
        )
    except Exception:
        return 0.0
    if not result.success or not result.bindings:
        return 0.0
    total = 0.0
    for row in result.as_dicts():
        try:
            total += float(row.get("quantity", "0"))
        except (ValueError, TypeError):
            pass
    return total
