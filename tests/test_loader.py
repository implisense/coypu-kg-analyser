"""Tests für OntologyLoader."""
from __future__ import annotations

from pathlib import Path

import pytest

from coypu_kg_analyser.loader.ontology_loader import OntologyLoader

ONTOLOGY_DIR = Path(__file__).parent.parent.parent / "ontology"


@pytest.fixture(scope="module")
def loader() -> OntologyLoader:
    ldr = OntologyLoader(ONTOLOGY_DIR)
    ldr.load_all()
    return ldr


def test_loader_loads_all_six_files(loader: OntologyLoader) -> None:
    assert len(loader.loaded_files) == 6


def test_loader_triple_count(loader: OntologyLoader) -> None:
    """Mindestens 2000 Triples erwartet (ohne owl:imports)."""
    assert loader.triple_count() > 2000


def test_loader_named_graphs(loader: OntologyLoader) -> None:
    expected_prefixes = {"coy", "gta", "ta", "vtf", "wpi", "emdat"}
    assert set(loader.named_graphs.keys()) == expected_prefixes


def test_get_namespace_coy(loader: OntologyLoader) -> None:
    from rdflib import URIRef
    uri = URIRef("https://schema.coypu.org/global#Country")
    prefix = loader.get_named_graph_uri(uri)
    assert prefix == "coy"


def test_get_namespace_gta(loader: OntologyLoader) -> None:
    from rdflib import URIRef
    uri = URIRef("https://schema.coypu.org/gta#Intervention")
    prefix = loader.get_named_graph_uri(uri)
    assert prefix == "gta"


def test_get_namespace_emdat(loader: OntologyLoader) -> None:
    from rdflib import URIRef
    uri = URIRef("https://schema.coypu.org/em-dat#NaturalDisaster")
    prefix = loader.get_named_graph_uri(uri)
    assert prefix == "emdat"


def test_no_owl_imports_in_graph(loader: OntologyLoader) -> None:
    """owl:imports-Triples müssen herausgefiltert sein."""
    from rdflib.namespace import OWL, RDF
    import_triples = list(loader.graph.triples((None, OWL.imports, None)))
    assert len(import_triples) == 0, "owl:imports-Triples dürfen nicht im Graph sein"
