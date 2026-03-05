"""
CascadeAnalyser: Erkennt Cross-Ontology-Kaskaden-Hubs.

Dimension 3: Konzepte, die mehrere Sub-Ontologien verbinden.
Ein Hub mit hohem cross_ontology_count ist ein Kaskadenrisiko.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import networkx as nx


@dataclass
class CascadeResult:
    uri: str
    cross_ontology_count: int = 0          # Anzahl verschiedener eingehender Namespaces
    referencing_namespaces: list[str] = field(default_factory=list)
    outgoing_cross_ontology: int = 0        # Ausgehende Cross-Ontology-Kanten
    cascade_paths: list[list[str]] = field(default_factory=list)  # DFS-Pfade durch Hubs


class CascadeAnalyser:
    """Erkennt Kaskaden-Hubs über Namespace-Grenzen hinweg."""

    def __init__(self, G: nx.DiGraph) -> None:
        self.G = G

    def analyse(self) -> dict[str, CascadeResult]:
        """Gibt ein Dict uri → CascadeResult zurück."""
        results: dict[str, CascadeResult] = {}

        for node in self.G.nodes():
            node_ns = self.G.nodes[node].get("namespace", "unknown")

            # Eingehende Namespaces sammeln
            inbound_namespaces: set[str] = set()
            for src, _, data in self.G.in_edges(node, data=True):
                src_ns = self.G.nodes[src].get("namespace", "unknown")
                if src_ns != node_ns and src_ns != "unknown":
                    inbound_namespaces.add(src_ns)

            # Ausgehende Cross-Ontology-Kanten
            outgoing_cross = sum(
                1 for _, tgt, data in self.G.out_edges(node, data=True)
                if data.get("cross_ontology", False)
            )

            results[node] = CascadeResult(
                uri=node,
                cross_ontology_count=len(inbound_namespaces),
                referencing_namespaces=sorted(inbound_namespaces),
                outgoing_cross_ontology=outgoing_cross,
            )

        # Kaskadenpfade für Top-Hubs berechnen
        top_hubs = sorted(results.values(), key=lambda r: r.cross_ontology_count, reverse=True)[:20]
        for hub in top_hubs:
            if hub.cross_ontology_count > 1:
                hub.cascade_paths = self._find_cascade_paths(hub.uri, max_depth=4)

        return results

    def _find_cascade_paths(self, start: str, max_depth: int = 4) -> list[list[str]]:
        """DFS durch Cross-Ontology-Hubs, gibt Pfade aus lokalen Namen zurück."""
        paths: list[list[str]] = []
        start_ns = self.G.nodes[start].get("namespace", "unknown")

        def local(uri: str) -> str:
            return uri.split("#")[-1] if "#" in uri else uri.split("/")[-1]

        def dfs(node: str, path: list[str], visited: set[str], depth: int) -> None:
            if depth > max_depth or node in visited:
                return
            visited = visited | {node}
            path = path + [local(node)]

            for _, tgt, data in self.G.out_edges(node, data=True):
                if data.get("cross_ontology", False):
                    tgt_ns = self.G.nodes[tgt].get("namespace", "unknown")
                    new_path = path + [f"[{tgt_ns}]{local(tgt)}"]
                    paths.append(new_path)
                    dfs(tgt, path, visited, depth + 1)

        dfs(start, [], set(), 0)
        return paths[:10]  # max 10 Pfade pro Hub

    def top_n_hubs(self, results: dict[str, CascadeResult], n: int = 10) -> list[CascadeResult]:
        """Gibt Top-N Kaskaden-Hubs zurück."""
        return sorted(
            results.values(),
            key=lambda r: (r.cross_ontology_count, r.outgoing_cross_ontology),
            reverse=True,
        )[:n]
