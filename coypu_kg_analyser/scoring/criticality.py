"""
CriticalityScorer: Aggregiert die drei Metrik-Dimensionen zu einem Gesamtscore.

Score-Formel:
  criticality = 0.4 * betweenness + 0.3 * concentration + 0.3 * cascade_norm
  + 0.2 Bonus für Artikulationspunkte (cap bei 1.0)

Stufen: CRITICAL (>=0.7) | HIGH (0.5-0.7) | MEDIUM (0.3-0.5) | LOW (<0.3)
"""
from __future__ import annotations

from dataclasses import dataclass, field

import networkx as nx

from coypu_kg_analyser.metrics.bottleneck import BottleneckResult
from coypu_kg_analyser.metrics.concentration import ConcentrationResult
from coypu_kg_analyser.metrics.cascade import CascadeResult

# PDL-Szenarien-Mapping: relevante URIs → Szenarien
_SCENARIO_HINTS: dict[str, list[str]] = {
    "Country": ["s1-soja", "s2-halbleiter", "s3-pharma"],
    "Disaster": ["s1-soja", "s3-pharma", "s6-rechenzentren"],
    "Infrastructure": ["s5-wasser", "s6-rechenzentren", "s9-unterwasserkabel"],
    "Intervention": ["s2-halbleiter", "s4-duengemittel", "s7-seltene-erden"],
    "TradeAgreement": ["s2-halbleiter", "s7-seltene-erden"],
    "Supplier": ["s1-soja", "s2-halbleiter", "s3-pharma", "s4-duengemittel"],
    "Product": ["s1-soja", "s2-halbleiter", "s3-pharma"],
    "NaturalDisaster": ["s1-soja", "s5-wasser"],
    "Earthquake": ["s9-unterwasserkabel"],
    "Flood": ["s5-wasser", "s1-soja"],
    "SeaRoute": ["s9-unterwasserkabel"],
    "Commodity": ["s1-soja", "s4-duengemittel", "s7-seltene-erden"],
}


@dataclass
class CriticalityResult:
    """Vollständiges Analyseergebnis für ein ontologisches Konzept."""
    uri: str
    local_name: str
    namespace: str
    node_type: str
    label: str
    ontology: str

    # Metrik-Rohwerte
    betweenness_centrality: float = 0.0
    degree_centrality: float = 0.0
    individual_concentration: float = 0.0
    individual_count: int = 0
    taxonomy_width: int = 0
    monopoly_score: float = 0.0
    cross_ontology_count: int = 0
    referencing_namespaces: list[str] = field(default_factory=list)
    outgoing_cross_ontology: int = 0
    is_articulation_point: bool = False

    # Aggregierter Score
    criticality_score: float = 0.0
    criticality_level: str = "LOW"

    # Kontextinformationen
    relevant_pdl_scenarios: list[str] = field(default_factory=list)
    suggested_sparql_queries: list[str] = field(default_factory=list)


def _level(score: float) -> str:
    if score >= 0.7:
        return "CRITICAL"
    elif score >= 0.5:
        return "HIGH"
    elif score >= 0.3:
        return "MEDIUM"
    return "LOW"


def _find_scenarios(local_name: str) -> list[str]:
    """Findet relevante PDL-Szenarien für ein Konzept."""
    scenarios: set[str] = set()
    for keyword, scen_list in _SCENARIO_HINTS.items():
        if keyword.lower() in local_name.lower():
            scenarios.update(scen_list)
    return sorted(scenarios)


class CriticalityScorer:
    """Aggregiert Metriken zu Kritikalitäts-Scores."""

    def __init__(
        self,
        G: nx.DiGraph,
        bottleneck_results: dict[str, BottleneckResult],
        concentration_results: dict[str, ConcentrationResult],
        cascade_results: dict[str, CascadeResult],
    ) -> None:
        self.G = G
        self.bottleneck = bottleneck_results
        self.concentration = concentration_results
        self.cascade = cascade_results
        self._max_cross = max(
            (r.cross_ontology_count for r in cascade_results.values()),
            default=1,
        )

    def score_all(self) -> list[CriticalityResult]:
        """Berechnet Scores für alle Knoten, sortiert nach Score absteigend."""
        results = [self._score_node(uri) for uri in self.G.nodes()]
        results.sort(key=lambda r: r.criticality_score, reverse=True)
        return results

    def _score_node(self, uri: str) -> CriticalityResult:
        node_data = self.G.nodes[uri]
        b = self.bottleneck.get(uri, BottleneckResult(uri=uri))
        c = self.concentration.get(uri, ConcentrationResult(uri=uri))
        cs = self.cascade.get(uri, CascadeResult(uri=uri))

        # Cascade-Score normalisieren auf [0, 1]
        max_cross = max(self._max_cross, 1)
        cascade_norm = min(cs.cross_ontology_count / max_cross, 1.0)

        # Konzentrations-Score: Monopoly-Score als Basis, aber cap bei 1.0
        concentration_score = min(c.monopoly_score * 10.0, 1.0)

        # Gewichtete Aggregation
        raw = (
            0.4 * b.betweenness
            + 0.3 * concentration_score
            + 0.3 * cascade_norm
        )

        # Artikulationspunkt-Bonus
        if b.is_articulation_point:
            raw += 0.2

        score = min(raw, 1.0)
        local_name = node_data.get("local_name", uri.split("#")[-1])

        return CriticalityResult(
            uri=uri,
            local_name=local_name,
            namespace=node_data.get("namespace", "unknown"),
            node_type=node_data.get("node_type", "unknown"),
            label=node_data.get("label", local_name),
            ontology=node_data.get("ontology", "unknown"),
            betweenness_centrality=round(b.betweenness, 6),
            degree_centrality=round(b.degree_centrality, 6),
            individual_concentration=round(c.individual_concentration, 4),
            individual_count=c.individual_count,
            taxonomy_width=c.taxonomy_width,
            monopoly_score=round(c.monopoly_score, 4),
            cross_ontology_count=cs.cross_ontology_count,
            referencing_namespaces=cs.referencing_namespaces,
            outgoing_cross_ontology=cs.outgoing_cross_ontology,
            is_articulation_point=b.is_articulation_point,
            criticality_score=round(score, 6),
            criticality_level=_level(score),
            relevant_pdl_scenarios=_find_scenarios(local_name),
            suggested_sparql_queries=[],
        )
