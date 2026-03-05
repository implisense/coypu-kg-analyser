"""Tests für OWLGraphBuilder."""
from __future__ import annotations

from pathlib import Path

import pytest

from coypu_kg_analyser.loader.ontology_loader import OntologyLoader
from coypu_kg_analyser.graph.owl_graph_builder import OWLGraphBuilder

ONTOLOGY_DIR = Path(__file__).parent.parent.parent / "ontology"

COY_COUNTRY = "https://schema.coypu.org/global#Country"
COY_DISASTER = "https://schema.coypu.org/global#Disaster"
EMDAT_NATURAL_DISASTER = "https://schema.coypu.org/em-dat#NaturalDisaster"
GTA_INTERVENTION = "https://schema.coypu.org/gta#Intervention"


@pytest.fixture(scope="module")
def graph():
    loader = OntologyLoader(ONTOLOGY_DIR)
    loader.load_all()
    builder = OWLGraphBuilder(loader)
    return builder.build()


def test_graph_has_nodes(graph) -> None:
    assert graph.number_of_nodes() > 200


def test_graph_has_edges(graph) -> None:
    assert graph.number_of_edges() > 300


def test_coy_country_exists(graph) -> None:
    assert COY_COUNTRY in graph
    node = graph.nodes[COY_COUNTRY]
    assert node["node_type"] == "class"
    assert node["namespace"] == "coy"
    assert node["local_name"] == "Country"


def test_coy_disaster_exists(graph) -> None:
    assert COY_DISASTER in graph


def test_gta_intervention_exists(graph) -> None:
    assert GTA_INTERVENTION in graph


def test_cross_ontology_edges_exist(graph) -> None:
    cross_edges = [
        (s, t, d) for s, t, d in graph.edges(data=True)
        if d.get("cross_ontology", False)
    ]
    assert len(cross_edges) > 0


def test_cross_ontology_edge_emdat_to_coy(graph) -> None:
    """emdat:NaturalDisaster → coy:Disaster via subClassOf (Cross-Ontology)."""
    # Die Kante existiert via subClassOf
    has_cross_path = False
    for src, tgt, data in graph.edges(data=True):
        src_ns = graph.nodes[src].get("namespace", "")
        tgt_ns = graph.nodes[tgt].get("namespace", "")
        if src_ns == "emdat" and tgt_ns == "coy" and data.get("cross_ontology", False):
            has_cross_path = True
            break
    assert has_cross_path, "Keine Cross-Ontology-Kante von emdat nach coy gefunden"


def test_cross_ontology_edges_have_weight_2(graph) -> None:
    for _, _, data in graph.edges(data=True):
        if data.get("cross_ontology", False):
            assert data["weight"] == 2.0


def test_node_types_present(graph) -> None:
    types = set(d["node_type"] for _, d in graph.nodes(data=True))
    assert "class" in types
    assert "property" in types
    # Individuen können vorhanden sein
    assert len(types) >= 2


def test_subclass_edge_type(graph) -> None:
    subclass_edges = [
        d for _, _, d in graph.edges(data=True)
        if d.get("edge_type") == "subClassOf"
    ]
    assert len(subclass_edges) > 10
