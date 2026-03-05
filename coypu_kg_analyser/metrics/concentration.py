"""
ConcentrationAnalyser: Berechnet Konzentrationsrisiko-Metriken.

Dimension 2: Named-Individual-Ratio + Taxonomiebreite.
Monopol-Muster: wenige Alternativen im Klassensystem → hohes Risiko.
"""
from __future__ import annotations

from dataclasses import dataclass

import networkx as nx

from coypu_kg_analyser.loader.ontology_loader import OntologyLoader


@dataclass
class ConcentrationResult:
    uri: str
    individual_count: int = 0
    individual_concentration: float = 0.0  # 1 - (count / max_in_taxonomy)
    taxonomy_width: int = 0                  # Anzahl Blattknoten im Subtree
    sibling_count: int = 0                   # Anzahl Geschwisterklassen
    monopoly_score: float = 0.0              # (1/max(count,1)) * (1/max(siblings,1))


class ConcentrationAnalyser:
    """Berechnet Konzentrationsrisiken im Klassensystem."""

    def __init__(self, G: nx.DiGraph, loader: OntologyLoader) -> None:
        self.G = G
        self.loader = loader

    def analyse(self) -> dict[str, ConcentrationResult]:
        """Gibt ein Dict uri → ConcentrationResult zurück."""
        results: dict[str, ConcentrationResult] = {}

        # Maximale Individuen-Anzahl über alle Klassen bestimmen
        max_individuals = max(
            (self.G.nodes[n].get("individual_count", 0) for n in self.G.nodes()),
            default=1,
        )
        max_individuals = max(max_individuals, 1)

        # Sibling-Counts: für jede Klasse Anzahl ihrer Geschwister berechnen
        sibling_counts = self._compute_sibling_counts()

        for node, data in self.G.nodes(data=True):
            ind_count = data.get("individual_count", 0)

            # Individuen-Konzentration: hoch wenn wenige Individuen relativ zum Maximum
            if ind_count == 0:
                ind_concentration = 0.0
            else:
                ind_concentration = 1.0 - (ind_count / max_individuals)

            siblings = sibling_counts.get(node, 0)
            tax_width = self._taxonomy_width(node)

            # Monopoly-Score: hoch wenn wenige Individuen UND wenige Geschwister
            monopoly = (1.0 / max(ind_count, 1)) * (1.0 / max(siblings + 1, 1))
            # Normalisieren auf [0, 1]
            monopoly = min(monopoly, 1.0)

            results[node] = ConcentrationResult(
                uri=node,
                individual_count=ind_count,
                individual_concentration=round(ind_concentration, 4),
                taxonomy_width=tax_width,
                sibling_count=siblings,
                monopoly_score=round(monopoly, 4),
            )

        return results

    def _compute_sibling_counts(self) -> dict[str, int]:
        """Berechnet für jeden Knoten die Anzahl seiner Geschwister (gleicher Parent)."""
        siblings: dict[str, int] = {}
        for node in self.G.nodes():
            # Elternknoten via subClassOf-Kanten (node → parent)
            parents = [
                tgt for src, tgt, d in self.G.out_edges(node, data=True)
                if d.get("edge_type") == "subClassOf"
            ]
            if not parents:
                siblings[node] = 0
                continue
            # Geschwister = alle Knoten mit gleichem Parent - self
            sibling_set: set[str] = set()
            for parent in parents:
                for src, _, d in self.G.in_edges(parent, data=True):
                    if d.get("edge_type") == "subClassOf" and src != node:
                        sibling_set.add(src)
            siblings[node] = len(sibling_set)
        return siblings

    def _taxonomy_width(self, node: str, max_depth: int = 6) -> int:
        """Zählt Blattknoten im Subtree (Kinder via subClassOf) mit DFS."""
        visited = set()
        leaf_count = 0

        def dfs(current: str, depth: int) -> None:
            nonlocal leaf_count
            if current in visited or depth > max_depth:
                return
            visited.add(current)
            # Kinder: alle Knoten, die via subClassOf auf current zeigen
            children = [
                src for src, tgt, d in self.G.in_edges(current, data=True)
                if d.get("edge_type") == "subClassOf"
            ]
            if not children:
                leaf_count += 1
            else:
                for child in children:
                    dfs(child, depth + 1)

        dfs(node, 0)
        return leaf_count

    def top_n_monopoly(self, results: dict[str, ConcentrationResult], n: int = 10) -> list[ConcentrationResult]:
        """Gibt Top-N nach Monopoly-Score sortiert zurück."""
        return sorted(results.values(), key=lambda r: r.monopoly_score, reverse=True)[:n]
