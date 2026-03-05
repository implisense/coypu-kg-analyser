"""
OntologyLoader: Lädt alle 6 CoyPu-TTL-Ontologie-Dateien in einen RDFLib ConjunctiveGraph.

Kein owl:imports-Auto-Resolve (verhindert HTTP-Requests).
Jede Datei erhält einen eigenen Named-Graph-Kontext für Provenance-Tracking.
"""
from __future__ import annotations

from pathlib import Path

from rdflib import ConjunctiveGraph, Graph, Namespace, URIRef
from rdflib.namespace import OWL, RDF, RDFS

# Bekannte Ontologie-Namespaces
KNOWN_NAMESPACES: dict[str, str] = {
    "coy": "https://schema.coypu.org/global#",
    "gta": "https://schema.coypu.org/gta#",
    "ta": "https://schema.coypu.org/ta#",
    "vtf": "https://schema.coypu.org/vtf#",
    "wpi": "https://schema.coypu.org/world-port-index#",
    "emdat": "https://schema.coypu.org/em-dat#",
}

# Dateiname → Namespace-Kürzel
FILE_TO_PREFIX: dict[str, str] = {
    "coypu-global-2.3.ttl": "coy",
    "gta-1.0.ttl": "gta",
    "ta-1.0.ttl": "ta",
    "vtf-1.4.ttl": "vtf",
    "world-port-index-1.0.ttl": "wpi",
    "em-dat-1.0.ttl": "emdat",
}


class OntologyLoader:
    """Lädt und verwaltet die CoyPu-Ontologie-Dateien."""

    def __init__(self, ontology_dir: Path) -> None:
        self.ontology_dir = Path(ontology_dir)
        self.graph = ConjunctiveGraph()
        self._named_graphs: dict[str, Graph] = {}
        self._uri_to_prefix: dict[str, str] = {}
        self._loaded_files: list[Path] = []

    def load_all(self) -> None:
        """Lädt alle bekannten TTL-Dateien aus dem Verzeichnis."""
        for filename, prefix in FILE_TO_PREFIX.items():
            ttl_path = self.ontology_dir / filename
            if ttl_path.exists():
                self._load_file(ttl_path, prefix)
            else:
                import warnings
                warnings.warn(f"Ontologie-Datei nicht gefunden: {ttl_path}", stacklevel=2)

        # URI-Prefix-Mapping aufbauen
        for prefix, ns_uri in KNOWN_NAMESPACES.items():
            self._uri_to_prefix[ns_uri] = prefix
            # Auch ohne # am Ende
            self._uri_to_prefix[ns_uri.rstrip("#")] = prefix

        # Namespace-Präfixe im Graph registrieren
        for prefix, ns_uri in KNOWN_NAMESPACES.items():
            self.graph.bind(prefix, Namespace(ns_uri))

    def _load_file(self, path: Path, prefix: str) -> None:
        """Lädt eine einzelne TTL-Datei in einen Named Graph."""
        ns_uri = KNOWN_NAMESPACES[prefix]
        context_uri = URIRef(f"urn:coypu:ontology:{prefix}")
        named_graph = self.graph.get_context(context_uri)

        # owl:imports-Triples herausfiltern, damit kein HTTP-Request ausgelöst wird
        tmp = Graph()
        tmp.parse(str(path), format="turtle")

        for s, p, o in tmp:
            if p == OWL.imports:
                continue
            named_graph.add((s, p, o))

        self._named_graphs[prefix] = named_graph
        self._loaded_files.append(path)

    def get_named_graph_uri(self, class_uri: URIRef) -> str:
        """Bestimmt den Namespace-Prefix einer URI.

        Gibt den Präfix (z.B. 'coy', 'gta') zurück, oder 'unknown'.
        """
        uri_str = str(class_uri)
        # Direkt-Match über bekannte Namespace-URIs
        for ns_uri, prefix in self._uri_to_prefix.items():
            if uri_str.startswith(ns_uri):
                return prefix
        return "unknown"

    def get_prefix_for_uri(self, uri: str) -> str:
        """Gibt den Namespace-Prefix für eine URI zurück."""
        for ns_uri, prefix in self._uri_to_prefix.items():
            if uri.startswith(ns_uri):
                return prefix
        return "unknown"

    @property
    def loaded_files(self) -> list[Path]:
        return self._loaded_files

    @property
    def named_graphs(self) -> dict[str, Graph]:
        return self._named_graphs

    def triple_count(self) -> int:
        return len(self.graph)
