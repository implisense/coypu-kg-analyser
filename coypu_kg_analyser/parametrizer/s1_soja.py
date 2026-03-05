"""
S1 Soja-Parametrizer: CoyPu KG-Daten → Simulations-Schocks.

Kombiniert EM-DAT Klimaereignisse und GTA-Exportbeschränkungen
für das S1-Szenario (soy_feed_disruption).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from coypu_kg_analyser.parametrizer._common import (
    _exiobase_oilseed_to_capacity,
    _gta_to_price,
    query_exiobase_oilseed_sensitivity,
    query_wpi_port,
)

if TYPE_CHECKING:
    from coypu_kg_analyser.live_query import LiveQueryClient

COUNTRY_URIS = {
    "BRA": "https://data.coypu.org/country/BRA",
    "ARG": "https://data.coypu.org/country/ARG",
    "USA": "https://data.coypu.org/country/USA",
    "DEU": "https://data.coypu.org/country/DEU",
}

_EXIOBASE_BASE = "https://data.coypu.org/industry/exiobase/regional/"

EXIOBASE_S1_INDUSTRY_URIS: "dict[str, str]" = {
    "eu_oil_mills": f"{_EXIOBASE_BASE}Processing_vegetable_oils_and_fats_DE",
    "feed_mills": f"{_EXIOBASE_BASE}Processing_of_Food_products_nec_DE",
}


def _emdat_to_capacity(disaster_count: int) -> float:
    """EM-DAT Katastrophenanzahl → Anbau-Kapazitätsfaktor."""
    if disaster_count == 0:
        return 1.0
    elif disaster_count <= 3:
        return 0.85
    elif disaster_count <= 10:
        return 0.70
    else:
        return 0.55


def _gta_export_to_capacity(red_count: int) -> float:
    """GTA-Exportbeschränkungsanzahl → Angebots-Kapazitätsfaktor."""
    if red_count == 0:
        return 1.0
    elif red_count <= 2:
        return 0.90
    elif red_count <= 5:
        return 0.75
    else:
        return 0.60


_EMDAT_FARM_QUERY = """\
SELECT ?e WHERE {{
    ?e a ?type ;
       coy:hasCountryLocation <{country_uri}> ;
       coy:hasYear ?year .
    FILTER(?type IN (emdat:Drought, emdat:Flood, emdat:RiverineFlood, emdat:FlashFlood))
    FILTER(?year >= {min_year})
}}
LIMIT {limit}
"""

_GTA_EXPORT_QUERY = """\
SELECT ?intervention WHERE {{
    ?intervention a gta:Intervention ;
                  gta:hasGTAEvaluation gta:Red ;
                  gta:hasAffectedProduct ?hs ;
                  gta:hasImplementingJurisdiction <{country_uri}> ;
                  gta:hasImplementationDate ?implDate .
    FILTER(
        CONTAINS(STR(?hs), "/12") ||
        CONTAINS(STR(?hs), "/15") ||
        CONTAINS(STR(?hs), "/23")
    )
    FILTER(?implDate <= "{today}"^^xsd:date)
    OPTIONAL {{ ?intervention gta:hasRemovalDate ?remDate }}
    FILTER(!BOUND(?remDate) || ?remDate >= "{today}"^^xsd:date)
}}
LIMIT {limit}
"""

_GTA_PRICE_QUERY = """\
SELECT ?intervention WHERE {{
    ?intervention a gta:Intervention ;
                  gta:hasGTAEvaluation gta:Red ;
                  gta:hasAffectedProduct ?hs ;
                  gta:hasImplementationDate ?implDate .
    FILTER(CONTAINS(STR(?hs), "/{hs_chapter}"))
    FILTER(?implDate <= "{today}"^^xsd:date)
    OPTIONAL {{ ?intervention gta:hasRemovalDate ?remDate }}
    FILTER(!BOUND(?remDate) || ?remDate >= "{today}"^^xsd:date)
}}
LIMIT {limit}
"""


@dataclass
class S1ParametrizerResult:
    """Ergebnis der S1-Parametrisierung."""
    shocks: list[dict]
    farm_risks: dict[str, tuple[int, int, float]]  # country_code → (emdat_count, gta_count, capacity)
    port_capacities: dict[str, float]               # locode → capacity
    fertilizer_price: float
    energy_price: float
    summary: str
    exiobase_industry_capacities: "dict[str, float]" = field(default_factory=dict)
    generated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_output_dict(self, lookback_years: int = 3) -> dict:
        return {
            "scenario": "soy_feed_disruption",
            "generated_at": self.generated_at,
            "metadata": {
                "farm_risks": {
                    cc: {"emdat_count": ec, "gta_count": gc, "capacity": cap}
                    for cc, (ec, gc, cap) in self.farm_risks.items()
                },
                "port_capacities": self.port_capacities,
                "fertilizer_price": self.fertilizer_price,
                "energy_price": self.energy_price,
                "lookback_years": lookback_years,
            },
            "shocks": self.shocks,
            "summary": self.summary,
        }


class S1Parametrizer:
    """Übersetzt CoyPu KG Live-Daten in Schocks für das S1-Soja-Szenario."""

    def __init__(
        self,
        client: LiveQueryClient,
        lookback_years: int = 3,
        reference_date: str | None = None,
        max_results: int = 2000,
    ) -> None:
        self.client = client
        self.lookback_years = lookback_years
        self.max_results = max_results
        self._reference = (
            datetime.fromisoformat(reference_date)
            if reference_date
            else datetime.utcnow()
        )

    def _today(self) -> str:
        return self._reference.strftime("%Y-%m-%d")

    def _min_year(self) -> int:
        reference_year = self._reference.year
        return reference_year - self.lookback_years

    def get_farm_risk(self, country_code: str) -> tuple[int, int, float]:
        """EM-DAT + GTA HS12 → (emdat_count, gta_count, capacity_factor).

        capacity_factor = min(emdat_factor, gta_factor).
        """
        country_uri = COUNTRY_URIS[country_code]
        today = self._today()
        min_year = self._min_year()

        emdat_r = self.client.query(
            _EMDAT_FARM_QUERY.format(
                country_uri=country_uri, min_year=min_year, limit=self.max_results,
            )
        )
        emdat_count = emdat_r.row_count if emdat_r.success else 0

        gta_r = self.client.query(
            _GTA_EXPORT_QUERY.format(
                country_uri=country_uri, today=today, limit=self.max_results,
            )
        )
        gta_count = gta_r.row_count if gta_r.success else 0

        factor = min(_emdat_to_capacity(emdat_count), _gta_export_to_capacity(gta_count))
        return emdat_count, gta_count, factor

    def get_port_capacity(self, locode: str) -> float:
        return query_wpi_port(self.client, locode)

    def get_price_shock(self, hs_chapter: str) -> tuple[int, float]:
        """GTA Red aktiv für ein HS-Kapitel → (count, price_factor)."""
        result = self.client.query(
            _GTA_PRICE_QUERY.format(
                hs_chapter=hs_chapter, today=self._today(), limit=self.max_results,
            )
        )
        count = result.row_count if result.success else 0
        return count, _gta_to_price(count)

    def get_industry_sensitivities(self, supply_disruption: float) -> "dict[str, float]":
        """EXIOBASE Oilseed IO-Koeffizienten → {target_id: Kapazitätsfaktor}.

        supply_disruption = 1.0 - min_farm_capacity
        """
        result = {}
        for target_id, industry_uri in EXIOBASE_S1_INDUSTRY_URIS.items():
            oilseed_coeff = query_exiobase_oilseed_sensitivity(self.client, industry_uri)
            result[target_id] = _exiobase_oilseed_to_capacity(oilseed_coeff, supply_disruption)
        return result

    def build_shocks(self) -> S1ParametrizerResult:
        bra_emdat, bra_gta, bra_cap = self.get_farm_risk("BRA")
        arg_emdat, arg_gta, arg_cap = self.get_farm_risk("ARG")
        usa_emdat, usa_gta, usa_cap = self.get_farm_risk("USA")
        deu_emdat, deu_gta, deu_cap = self.get_farm_risk("DEU")
        santos_cap    = self.get_port_capacity("BRSFE")
        rosario_cap   = self.get_port_capacity("ARROS")
        neworleans_cap = self.get_port_capacity("USNOL")
        hamburg_cap   = self.get_port_capacity("DEHAM")
        paranagua_cap = self.get_port_capacity("BRPNG")
        rotterdam_cap = self.get_port_capacity("NLRTM")
        fert_count, fert_price = self.get_price_shock("31")
        gas_count,  gas_price  = self.get_price_shock("27")
        supply_disruption = 1.0 - min(bra_cap, arg_cap, usa_cap)
        industry_caps = self.get_industry_sensitivities(supply_disruption)

        shocks = [
            {"target_id": "bra_soy_farm",     "shock_type": "capacity", "magnitude": bra_cap},
            {"target_id": "arg_soy_farm",     "shock_type": "capacity", "magnitude": arg_cap},
            {"target_id": "usa_soy_farm",     "shock_type": "capacity", "magnitude": usa_cap},
            {"target_id": "deu_soy_farm",     "shock_type": "capacity", "magnitude": deu_cap},
            {"target_id": "fertilizer_input", "shock_type": "price",    "magnitude": fert_price},
            {"target_id": "energy_input",     "shock_type": "price",    "magnitude": gas_price},
            {"target_id": "santos_port",      "shock_type": "capacity", "magnitude": santos_cap},
            {"target_id": "rosario_port",     "shock_type": "capacity", "magnitude": rosario_cap},
            {"target_id": "paranagua_port",   "shock_type": "capacity", "magnitude": paranagua_cap},
            {"target_id": "rotterdam_port",   "shock_type": "capacity", "magnitude": rotterdam_cap},
            {"target_id": "hamburg_port",     "shock_type": "capacity", "magnitude": hamburg_cap},
            {"target_id": "us_gulf_ports",    "shock_type": "capacity", "magnitude": neworleans_cap},
            {"target_id": "eu_oil_mills",     "shock_type": "capacity", "magnitude": industry_caps["eu_oil_mills"]},
            {"target_id": "feed_mills",       "shock_type": "capacity", "magnitude": industry_caps["feed_mills"]},
        ]

        farm_risks = {
            "BRA": (bra_emdat, bra_gta, bra_cap),
            "ARG": (arg_emdat, arg_gta, arg_cap),
            "USA": (usa_emdat, usa_gta, usa_cap),
            "DEU": (deu_emdat, deu_gta, deu_cap),
        }
        port_capacities = {
            "BRSFE": santos_cap,
            "ARROS": rosario_cap,
            "BRPNG": paranagua_cap,
            "NLRTM": rotterdam_cap,
            "USNOL": neworleans_cap,
            "DEHAM": hamburg_cap,
        }

        def pct(f: float) -> int: return int(f * 100)
        def ppct(f: float) -> str: return f"+{int((f - 1) * 100)}%"

        summary = (
            f"BRA-Farmen: {pct(bra_cap)}% ({bra_emdat} EM-DAT, {bra_gta} GTA-HS12), "
            f"ARG-Farmen: {pct(arg_cap)}% ({arg_emdat} EM-DAT, {arg_gta} GTA-HS12), "
            f"USA-Farmen: {pct(usa_cap)}% ({usa_emdat} EM-DAT, {usa_gta} GTA-HS12), "
            f"DEU-Farmen: {pct(deu_cap)}% ({deu_emdat} EM-DAT, {deu_gta} GTA-HS12), "
            f"Santos: {pct(santos_cap)}%, Rosario: {pct(rosario_cap)}%, "
            f"Dünger: {ppct(fert_price)} ({fert_count} GTA-HS31), "
            f"Gas: {ppct(gas_price)} ({gas_count} GTA-HS27)"
        )

        return S1ParametrizerResult(
            shocks=shocks,
            farm_risks=farm_risks,
            port_capacities=port_capacities,
            fertilizer_price=fert_price,
            energy_price=gas_price,
            summary=summary,
            exiobase_industry_capacities=industry_caps,
        )
