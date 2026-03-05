"""
BottleneckAnalyser: Berechnet Netzwerk-Engpass-Metriken.

Dimension 1: Betweenness Centrality + Artikulationspunkte + Degree Centrality.
Konzepte mit hoher Betweenness sind Brücken, deren Wegfall das Schema zersplittert.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import networkx as nx


@dataclass
class BottleneckResult:
    uri: str
    betweenness: float = 0.0
    degree_centrality: float = 0.0
    is_articulation_point: bool = False
    in_degree: int = 0
    out_degree: int = 0


class BottleneckAnalyser:
    """Berechnet Betweenness-Centrality und Artikulationspunkte."""

    def __init__(self, G: nx.DiGraph) -> None:
        self.G = G
        self._undirected: nx.Graph | None = None

    def _get_undirected(self) -> nx.Graph:
        if self._undirected is None:
            self._undirected = self.G.to_undirected()
        return self._undirected

    def analyse(self) -> dict[str, BottleneckResult]:
        """Gibt ein Dict uri → BottleneckResult zurück."""
        U = self._get_undirected()

        betweenness = nx.betweenness_centrality(U, normalized=True, weight="weight")
        degree_cent = nx.degree_centrality(U)

        # Artikulationspunkte nur für größte Komponente
        art_points: set[str] = set()
        if nx.is_connected(U):
            art_points = set(nx.articulation_points(U))
        else:
            largest_cc = max(nx.connected_components(U), key=len)
            U_sub = U.subgraph(largest_cc)
            art_points = set(nx.articulation_points(U_sub))

        results: dict[str, BottleneckResult] = {}
        for node in self.G.nodes():
            results[node] = BottleneckResult(
                uri=node,
                betweenness=betweenness.get(node, 0.0),
                degree_centrality=degree_cent.get(node, 0.0),
                is_articulation_point=node in art_points,
                in_degree=self.G.in_degree(node),
                out_degree=self.G.out_degree(node),
            )

        return results

    def top_n(self, results: dict[str, BottleneckResult], n: int = 10) -> list[BottleneckResult]:
        """Gibt die Top-N nach Betweenness sortiert zurück."""
        return sorted(results.values(), key=lambda r: r.betweenness, reverse=True)[:n]
