"""
S10 Iran-Angriff-Parametrizer: CoyPu KG-Daten → Simulations-Schocks.

Kombiniert ACLED-Konfliktereignisse (Persischer Golf, Rotes Meer),
GTA-Sanktionen (HS27 Öl, HS26/74 Kupfer) und WPI-Hafen-Kapazitäten
für das S10-Szenario (iran_attack_scenario).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from coypu_kg_analyser.parametrizer._common import (
    _exiobase_io_to_capacity,
    _gta_to_price,
    query_exiobase_oil_sensitivity,
    query_wpi_port,
)

if TYPE_CHECKING:
    from coypu_kg_analyser.live_query import LiveQueryClient

COUNTRY_URIS = {
    "USA": "https://data.coypu.org/country/USA",
    "DEU": "https://data.coypu.org/country/DEU",
}

_EXIOBASE_BASE = "https://data.coypu.org/industry/exiobase/regional/"

EXIOBASE_INDUSTRY_URIS: "dict[str, str]" = {
    "german_auto_industry": (
        f"{_EXIOBASE_BASE}Manufacture_of_motor_vehicles_trailers_and_semitrailers_DE"
    ),
    "german_chemical_industry": f"{_EXIOBASE_BASE}Chemicals_nec_DE",
    "eu_metal_industry": (
        f"{_EXIOBASE_BASE}"
        "Manufacture_of_basic_iron_and_steel_and_of_ferroalloys_and_first_products_thereof_DE"
    ),
    "european_airlines_rerouting": f"{_EXIOBASE_BASE}Air_transport_DE",
    "eu_lng_terminals": (
        f"{_EXIOBASE_BASE}"
        "Manufacture_of_gas_distribution_of_gaseous_fuels_through_mains_DE"
    ),
}


def _acled_to_capacity(conflict_count: int) -> float:
    """ACLED-Konfliktanzahl → Kapazitätsfaktor (Persischer Golf / Rotes Meer)."""
    if conflict_count == 0:
        return 1.0
    elif conflict_count <= 10:
        return 0.7
    elif conflict_count <= 50:
        return 0.5
    elif conflict_count <= 200:
        return 0.35
    else:
        return 0.2


def _gta_sanctions_to_capacity(sanction_count: int) -> float:
    """GTA Red-Sanktionen gegen Iran → Kapazitätsfaktor (NIOC, Ölfelder, Kupfer)."""
    if sanction_count == 0:
        return 1.0
    elif sanction_count <= 2:
        return 0.75
    elif sanction_count <= 5:
        return 0.55
    else:
        return 0.35


_ACLED_PERSIAN_GULF_QUERY = """\
SELECT ?event WHERE {{
    ?event a coy:Event ;
           coy:hasLatitude ?lat ;
           coy:hasLongitude ?lon ;
           coy:hasTimestamp ?ts .
    FILTER (?lat >= 24 && ?lat <= 32)
    FILTER (?lon >= 48 && ?lon <= 60)
    FILTER (?ts >= "{since}"^^xsd:date)
}}
LIMIT {limit}
"""

_ACLED_RED_SEA_QUERY = """\
SELECT ?event WHERE {{
    ?event a coy:Event ;
           coy:hasLatitude ?lat ;
           coy:hasLongitude ?lon ;
           coy:hasTimestamp ?ts .
    FILTER (?lat >= 10 && ?lat <= 28)
    FILTER (?lon >= 32 && ?lon <= 55)
    FILTER (?ts >= "{since}"^^xsd:date)
}}
LIMIT {limit}
"""

_GTA_IRAN_OIL_SANCTIONS_QUERY = """\
SELECT ?intervention WHERE {{
    ?intervention a gta:Intervention ;
                  gta:hasGTAEvaluation gta:Red ;
                  gta:hasAffectedProduct ?hs ;
                  gta:hasImplementingJurisdiction <{country_uri}> ;
                  gta:hasImplementationDate ?implDate .
    FILTER (CONTAINS(STR(?hs), "/27"))
    FILTER (?implDate <= "{today}"^^xsd:date)
    OPTIONAL {{ ?intervention gta:hasRemovalDate ?remDate }}
    FILTER (!BOUND(?remDate) || ?remDate >= "{today}"^^xsd:date)
}}
LIMIT {limit}
"""

_GTA_COPPER_SANCTIONS_QUERY = """\
SELECT ?intervention WHERE {{
    ?intervention a gta:Intervention ;
                  gta:hasGTAEvaluation gta:Red ;
                  gta:hasAffectedProduct ?hs ;
                  gta:hasImplementingJurisdiction <{country_uri}> ;
                  gta:hasImplementationDate ?implDate .
    FILTER (CONTAINS(STR(?hs), "/26") || CONTAINS(STR(?hs), "/74"))
    FILTER (?implDate <= "{today}"^^xsd:date)
    OPTIONAL {{ ?intervention gta:hasRemovalDate ?remDate }}
    FILTER (!BOUND(?remDate) || ?remDate >= "{today}"^^xsd:date)
}}
LIMIT {limit}
"""

_GTA_OIL_PRICE_QUERY = """\
SELECT ?intervention WHERE {{
    ?intervention a gta:Intervention ;
                  gta:hasGTAEvaluation gta:Red ;
                  gta:hasAffectedProduct ?hs ;
                  gta:hasImplementationDate ?implDate .
    FILTER (CONTAINS(STR(?hs), "/27"))
    FILTER (?implDate <= "{today}"^^xsd:date)
    OPTIONAL {{ ?intervention gta:hasRemovalDate ?remDate }}
    FILTER (!BOUND(?remDate) || ?remDate >= "{today}"^^xsd:date)
}}
LIMIT {limit}
"""


@dataclass
class S10ParametrizerResult:
    """Ergebnis der S10-Parametrisierung."""
    shocks: list[dict]
    acled_gulf_count: int
    acled_red_sea_count: int
    gta_oil_sanctions_count: int
    gta_copper_sanctions_count: int
    gta_oil_price_count: int
    port_capacities: dict[str, float]
    summary: str
    exiobase_industry_capacities: "dict[str, float]" = field(default_factory=dict)
    generated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_output_dict(self, lookback_days: int = 180) -> dict:
        return {
            "scenario": "iran_attack_scenario",
            "generated_at": self.generated_at,
            "metadata": {
                "acled_gulf_count": self.acled_gulf_count,
                "acled_red_sea_count": self.acled_red_sea_count,
                "gta_oil_sanctions_count": self.gta_oil_sanctions_count,
                "gta_copper_sanctions_count": self.gta_copper_sanctions_count,
                "gta_oil_price_count": self.gta_oil_price_count,
                "port_capacities": self.port_capacities,
                "lookback_days": lookback_days,
            },
            "shocks": self.shocks,
            "summary": self.summary,
        }


class S10Parametrizer:
    """Übersetzt CoyPu KG Live-Daten in Schocks für das S10-Iran-Angriff-Szenario."""

    def __init__(
        self,
        client: "LiveQueryClient",
        lookback_days: int = 180,
        reference_date: "str | None" = None,
        max_results: int = 2000,
    ) -> None:
        self.client = client
        self.lookback_days = lookback_days
        self.max_results = max_results
        self._reference = (
            datetime.fromisoformat(reference_date)
            if reference_date
            else datetime.utcnow()
        )

    def _since_date(self) -> str:
        dt = self._reference - timedelta(days=self.lookback_days)
        return dt.strftime("%Y-%m-%d")

    def _today(self) -> str:
        return self._reference.strftime("%Y-%m-%d")

    def _query_count(self, query: str) -> int:
        """Führt Query aus und gibt Zeilenanzahl zurück (0 bei Fehler)."""
        result = self.client.query(query)
        return result.row_count if result.success else 0

    def get_persian_gulf_risk(self) -> "tuple[int, float]":
        """ACLED Persischer Golf → (Konfliktanzahl, Kapazitätsfaktor)."""
        query = _ACLED_PERSIAN_GULF_QUERY.format(
            since=self._since_date(), limit=self.max_results
        )
        count = self._query_count(query)
        return count, _acled_to_capacity(count)

    def get_red_sea_risk(self) -> "tuple[int, float]":
        """ACLED Rotes Meer → (Konfliktanzahl, Kapazitätsfaktor)."""
        query = _ACLED_RED_SEA_QUERY.format(
            since=self._since_date(), limit=self.max_results
        )
        count = self._query_count(query)
        return count, _acled_to_capacity(count)

    def _get_sanctions_count(self, query_template: str) -> int:
        """Addiert Red-Interventionen für USA + DEU als Implementierer."""
        today = self._today()
        total = 0
        for country_uri in COUNTRY_URIS.values():
            query = query_template.format(
                country_uri=country_uri, today=today, limit=self.max_results
            )
            total += self._query_count(query)
        return total

    def get_iran_oil_sanctions(self) -> "tuple[int, float]":
        """GTA HS27 Sanktionen (USA+DEU) → (Anzahl, Kapazitätsfaktor)."""
        count = self._get_sanctions_count(_GTA_IRAN_OIL_SANCTIONS_QUERY)
        return count, _gta_sanctions_to_capacity(count)

    def get_copper_sanctions(self) -> "tuple[int, float]":
        """GTA HS26+74 Sanktionen (USA+DEU) → (Anzahl, Kapazitätsfaktor)."""
        count = self._get_sanctions_count(_GTA_COPPER_SANCTIONS_QUERY)
        return count, _gta_sanctions_to_capacity(count)

    def get_oil_price_shock(self) -> "tuple[int, float]":
        """GTA HS27 Red global → (Anzahl aktiver Interventionen, Preisfaktor)."""
        query = _GTA_OIL_PRICE_QUERY.format(
            today=self._today(), limit=self.max_results
        )
        count = self._query_count(query)
        return count, _gta_to_price(count)

    def get_port_capacities(self) -> "dict[str, float]":
        """WPI Bandar Abbas + Rotterdam + Jebel Ali → {locode: Kapazitätsfaktor}."""
        return {
            "IRBND": query_wpi_port(self.client, "IRBND"),
            "NLRTM": query_wpi_port(self.client, "NLRTM"),
            "AEJEA": query_wpi_port(self.client, "AEJEA"),
        }

    def get_industry_sensitivities(self, oil_price_factor: float) -> "dict[str, float]":
        """EXIOBASE IO-Koeffizienten → {target_id: Kapazitätsfaktor}."""
        result = {}
        for target_id, industry_uri in EXIOBASE_INDUSTRY_URIS.items():
            oil_coeff = query_exiobase_oil_sensitivity(self.client, industry_uri)
            result[target_id] = _exiobase_io_to_capacity(oil_coeff, oil_price_factor)
        return result

    def build_shocks(self) -> S10ParametrizerResult:
        """Führt alle KG-Abfragen durch und baut die Shock-Liste."""
        gulf_count, gulf_cap = self.get_persian_gulf_risk()
        red_sea_count, red_sea_cap = self.get_red_sea_risk()
        oil_sanctions_count, oil_sanctions_cap = self.get_iran_oil_sanctions()
        copper_count, copper_cap = self.get_copper_sanctions()
        oil_price_count, oil_price = self.get_oil_price_shock()
        port_caps = self.get_port_capacities()
        industry_caps = self.get_industry_sensitivities(oil_price)

        shocks = [
            {"target_id": "strait_of_hormuz",   "shock_type": "capacity", "magnitude": gulf_cap},
            {"target_id": "persian_gulf_region", "shock_type": "capacity", "magnitude": gulf_cap},
            {"target_id": "red_sea_route",       "shock_type": "capacity", "magnitude": red_sea_cap},
            {"target_id": "nioc",                "shock_type": "capacity", "magnitude": oil_sanctions_cap},
            {"target_id": "iran_oil_fields",     "shock_type": "capacity", "magnitude": oil_sanctions_cap},
            {"target_id": "global_oil_market",   "shock_type": "price",    "magnitude": oil_price},
            {"target_id": "iran_copper_mines",   "shock_type": "capacity", "magnitude": copper_cap},
            {"target_id": "kharg_island",        "shock_type": "capacity", "magnitude": port_caps["IRBND"]},
            {"target_id": "eu_refineries",       "shock_type": "capacity", "magnitude": port_caps["NLRTM"]},
            {"target_id": "dubai_airport_dxb",   "shock_type": "capacity", "magnitude": port_caps["AEJEA"]},
            {"target_id": "german_auto_industry",        "shock_type": "capacity", "magnitude": industry_caps["german_auto_industry"]},
            {"target_id": "german_chemical_industry",    "shock_type": "capacity", "magnitude": industry_caps["german_chemical_industry"]},
            {"target_id": "eu_metal_industry",           "shock_type": "capacity", "magnitude": industry_caps["eu_metal_industry"]},
            {"target_id": "european_airlines_rerouting", "shock_type": "capacity", "magnitude": industry_caps["european_airlines_rerouting"]},
            {"target_id": "eu_lng_terminals",            "shock_type": "capacity", "magnitude": industry_caps["eu_lng_terminals"]},
        ]

        def pct(f: float) -> str:
            return f"{int(f * 100)}%"

        def ppct(f: float) -> str:
            return f"+{int((f - 1) * 100)}%"

        summary = (
            f"Golf: {pct(gulf_cap)} ({gulf_count} Konflikte), "
            f"Rotes Meer: {pct(red_sea_cap)} ({red_sea_count} Konflikte), "
            f"Öl-Sanktionen: {pct(oil_sanctions_cap)} ({oil_sanctions_count} GTA-Red), "
            f"Kupfer-Sanktionen: {pct(copper_cap)} ({copper_count} GTA-Red), "
            f"Ölpreis: {ppct(oil_price)} ({oil_price_count} GTA-Red), "
            f"Bandar Abbas: {pct(port_caps['IRBND'])}, "
            f"Rotterdam: {pct(port_caps['NLRTM'])}, "
            f"Jebel Ali: {pct(port_caps['AEJEA'])}"
        )

        return S10ParametrizerResult(
            shocks=shocks,
            acled_gulf_count=gulf_count,
            acled_red_sea_count=red_sea_count,
            gta_oil_sanctions_count=oil_sanctions_count,
            gta_copper_sanctions_count=copper_count,
            gta_oil_price_count=oil_price_count,
            port_capacities=port_caps,
            summary=summary,
            exiobase_industry_capacities=industry_caps,
        )
