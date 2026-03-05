"""
OWLGraphBuilder: Transformiert OWL-Ontologie-Konstrukte in einen NetworkX DiGraph.

Verwendet direkte RDF-API-Iteration (kein SPARQL) für Kompatibilität.

Knoten: owl:Class, owl:ObjectProperty/DatatypeProperty, owl:NamedIndividual
Kanten: subClassOf, property_domain, property_range, instanceOf, equivalent
Cross-Ontology-Kanten erhalten weight=2.0 und cross_ontology=True.
"""
from __future__ import annotations

from typing import Any

import networkx as nx
from rdflib import OWL, RDF, RDFS, URIRef
from rdflib.namespace import XSD

from coypu_kg_analyser.loader.ontology_loader import OntologyLoader

OWL_CLASS = OWL.Class
OWL_OBJECT_PROPERTY = OWL.ObjectProperty
OWL_DATATYPE_PROPERTY = OWL.DatatypeProperty
OWL_NAMED_INDIVIDUAL = OWL.NamedIndividual
OWL_EQUIVALENT_CLASS = OWL.equivalentClass
RDFS_SUBCLASS_OF = RDFS.subClassOf
RDFS_DOMAIN = RDFS.domain
RDFS_RANGE = RDFS.range
RDFS_LABEL = RDFS.label


def _local_name(uri: str) -> str:
    """Extrahiert den lokalen Namen aus einer URI."""
    if "#" in uri:
        return uri.split("#")[-1]
    if "/" in uri:
        return uri.split("/")[-1]
    return uri


def _is_owl_builtin(uri: str) -> bool:
    """Filtert OWL/RDF/RDFS/XSD built-in URIs heraus."""
    return any(
        uri.startswith(prefix)
        for prefix in (
            "http://www.w3.org/2002/07/owl#",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "http://www.w3.org/2000/01/rdf-schema#",
            "http://www.w3.org/2001/XMLSchema#",
            "http://www.w3.org/XML/1998/namespace",
        )
    )


class OWLGraphBuilder:
    """Baut einen NetworkX DiGraph aus den OWL-Ontologien."""

    def __init__(self, loader: OntologyLoader) -> None:
        self.loader = loader
        self.G: nx.DiGraph = nx.DiGraph()

    def build(self) -> nx.DiGraph:
        """Führt die vollständige Graph-Konstruktion durch."""
        g = self.loader.graph

        self._add_classes(g)
        self._add_properties(g)
        self._add_individuals(g)
        self._add_subclass_edges(g)
        self._add_domain_edges(g)
        self._add_range_edges(g)
        self._add_equivalent_edges(g)
        self._mark_cross_ontology_edges()

        return self.G

    def _node_attrs(self, uri: str, node_type: str, label: str | None = None) -> dict[str, Any]:
        prefix = self.loader.get_prefix_for_uri(uri)
        local = _local_name(uri)
        return {
            "uri": uri,
            "local_name": local,
            "namespace": prefix,
            "node_type": node_type,
            "label": label or local,
            "ontology": prefix,
            "individual_count": 0,
        }

    def _get_label(self, g: Any, uri: URIRef) -> str | None:
        for label in g.objects(uri, RDFS_LABEL):
            return str(label)
        return None

    def _add_classes(self, g: Any) -> None:
        for s in g.subjects(RDF.type, OWL_CLASS):
            if not isinstance(s, URIRef):
                continue
            uri = str(s)
            if _is_owl_builtin(uri):
                continue
            if uri not in self.G:
                label = self._get_label(g, s)
                self.G.add_node(uri, **self._node_attrs(uri, "class", label))

    def _add_properties(self, g: Any) -> None:
        for prop_type in (OWL_OBJECT_PROPERTY, OWL_DATATYPE_PROPERTY):
            for s in g.subjects(RDF.type, prop_type):
                if not isinstance(s, URIRef):
                    continue
                uri = str(s)
                if _is_owl_builtin(uri):
                    continue
                if uri not in self.G:
                    label = self._get_label(g, s)
                    self.G.add_node(uri, **self._node_attrs(uri, "property", label))

    def _add_individuals(self, g: Any) -> None:
        ind_class_count: dict[str, int] = {}

        for ind in g.subjects(RDF.type, OWL_NAMED_INDIVIDUAL):
            if not isinstance(ind, URIRef):
                continue
            ind_uri = str(ind)
            if _is_owl_builtin(ind_uri):
                continue

            if ind_uri not in self.G:
                label = self._get_label(g, ind)
                self.G.add_node(ind_uri, **self._node_attrs(ind_uri, "individual", label))

            # Kanten: individual → class (instanceOf)
            for cls in g.objects(ind, RDF.type):
                if not isinstance(cls, URIRef):
                    continue
                cls_uri = str(cls)
                if cls_uri == str(OWL_NAMED_INDIVIDUAL) or _is_owl_builtin(cls_uri):
                    continue
                self._ensure_node(cls_uri, "class")
                self.G.add_edge(ind_uri, cls_uri, edge_type="instanceOf", weight=1.0, cross_ontology=False)
                ind_class_count[cls_uri] = ind_class_count.get(cls_uri, 0) + 1

        for cls_uri, count in ind_class_count.items():
            if cls_uri in self.G:
                self.G.nodes[cls_uri]["individual_count"] = count

    def _add_subclass_edges(self, g: Any) -> None:
        for sub, sup in g.subject_objects(RDFS_SUBCLASS_OF):
            if not isinstance(sub, URIRef) or not isinstance(sup, URIRef):
                continue
            sub_uri, sup_uri = str(sub), str(sup)
            if _is_owl_builtin(sub_uri) or _is_owl_builtin(sup_uri):
                continue
            self._ensure_node(sub_uri, "class")
            self._ensure_node(sup_uri, "class")
            self.G.add_edge(sub_uri, sup_uri, edge_type="subClassOf", weight=1.0, cross_ontology=False)

    def _add_domain_edges(self, g: Any) -> None:
        for prop, domain in g.subject_objects(RDFS_DOMAIN):
            if not isinstance(prop, URIRef) or not isinstance(domain, URIRef):
                continue
            prop_uri, domain_uri = str(prop), str(domain)
            if _is_owl_builtin(prop_uri) or _is_owl_builtin(domain_uri):
                continue
            self._ensure_node(prop_uri, "property")
            self._ensure_node(domain_uri, "class")
            self.G.add_edge(prop_uri, domain_uri, edge_type="property_domain", weight=1.0, cross_ontology=False)

    def _add_range_edges(self, g: Any) -> None:
        for prop, rng in g.subject_objects(RDFS_RANGE):
            if not isinstance(prop, URIRef) or not isinstance(rng, URIRef):
                continue
            prop_uri, rng_uri = str(prop), str(rng)
            if _is_owl_builtin(prop_uri) or _is_owl_builtin(rng_uri):
                continue
            self._ensure_node(prop_uri, "property")
            self._ensure_node(rng_uri, "class")
            self.G.add_edge(prop_uri, rng_uri, edge_type="property_range", weight=1.0, cross_ontology=False)

    def _add_equivalent_edges(self, g: Any) -> None:
        for cls1, cls2 in g.subject_objects(OWL_EQUIVALENT_CLASS):
            if not isinstance(cls1, URIRef) or not isinstance(cls2, URIRef):
                continue
            uri1, uri2 = str(cls1), str(cls2)
            if _is_owl_builtin(uri1) or _is_owl_builtin(uri2):
                continue
            self._ensure_node(uri1, "class")
            self._ensure_node(uri2, "class")
            self.G.add_edge(uri1, uri2, edge_type="equivalent", weight=1.0, cross_ontology=False)

    def _ensure_node(self, uri: str, node_type: str) -> None:
        if uri not in self.G:
            self.G.add_node(uri, **self._node_attrs(uri, node_type))

    def _mark_cross_ontology_edges(self) -> None:
        """Markiert Kanten zwischen verschiedenen Namespaces mit weight=2.0."""
        for src, tgt, data in self.G.edges(data=True):
            src_ns = self.G.nodes[src].get("namespace", "unknown")
            tgt_ns = self.G.nodes[tgt].get("namespace", "unknown")
            if src_ns != tgt_ns and src_ns != "unknown" and tgt_ns != "unknown":
                data["cross_ontology"] = True
                data["weight"] = 2.0
