"""Tests für Metriken und Scoring."""
from __future__ import annotations

from pathlib import Path

import pytest

from coypu_kg_analyser.loader.ontology_loader import OntologyLoader
from coypu_kg_analyser.graph.owl_graph_builder import OWLGraphBuilder
from coypu_kg_analyser.metrics.bottleneck import BottleneckAnalyser
from coypu_kg_analyser.metrics.concentration import ConcentrationAnalyser
from coypu_kg_analyser.metrics.cascade import CascadeAnalyser
from coypu_kg_analyser.scoring.criticality import CriticalityScorer

ONTOLOGY_DIR = Path(__file__).parent.parent.parent / "ontology"

COY_COUNTRY = "https://schema.coypu.org/global#Country"
COY_DISASTER = "https://schema.coypu.org/global#Disaster"
GTA_INTERVENTION = "https://schema.coypu.org/gta#Intervention"


@pytest.fixture(scope="module")
def full_analysis():
    loader = OntologyLoader(ONTOLOGY_DIR)
    loader.load_all()
    builder = OWLGraphBuilder(loader)
    G = builder.build()

    b = BottleneckAnalyser(G)
    b_res = b.analyse()
    c = ConcentrationAnalyser(G, loader)
    c_res = c.analyse()
    cs = CascadeAnalyser(G)
    cs_res = cs.analyse()
    scorer = CriticalityScorer(G, b_res, c_res, cs_res)
    results = scorer.score_all()

    return {
        "G": G,
        "bottleneck": b_res,
        "concentration": c_res,
        "cascade": cs_res,
        "results": results,
    }


# --- Bottleneck Tests ---

def test_bottleneck_all_nodes_covered(full_analysis) -> None:
    G = full_analysis["G"]
    b_res = full_analysis["bottleneck"]
    assert len(b_res) == G.number_of_nodes()


def test_bottleneck_scores_in_range(full_analysis) -> None:
    for result in full_analysis["bottleneck"].values():
        assert 0.0 <= result.betweenness <= 1.0


def test_coy_disaster_high_betweenness(full_analysis) -> None:
    """coy:Disaster sollte hohe Betweenness haben (Cross-Ontology-Bridge)."""
    if COY_DISASTER in full_analysis["bottleneck"]:
        b = full_analysis["bottleneck"][COY_DISASTER]
        assert b.betweenness > 0.05, f"Erwartet: betweenness > 0.05, bekommen: {b.betweenness}"


def test_articulation_points_exist(full_analysis) -> None:
    art_points = [r for r in full_analysis["bottleneck"].values() if r.is_articulation_point]
    assert len(art_points) > 0


# --- Concentration Tests ---

def test_concentration_all_nodes_covered(full_analysis) -> None:
    G = full_analysis["G"]
    c_res = full_analysis["concentration"]
    assert len(c_res) == G.number_of_nodes()


def test_monopoly_scores_in_range(full_analysis) -> None:
    for result in full_analysis["concentration"].values():
        assert 0.0 <= result.monopoly_score <= 1.0


# --- Cascade Tests ---

def test_cascade_hubs_detected(full_analysis) -> None:
    cross_hubs = [
        r for r in full_analysis["cascade"].values()
        if r.cross_ontology_count > 0
    ]
    assert len(cross_hubs) > 0


def test_coy_disaster_is_cross_hub(full_analysis) -> None:
    """coy:Disaster soll als Cross-Ontology-Hub erkannt werden (>= 1 NS)."""
    if COY_DISASTER in full_analysis["cascade"]:
        cs = full_analysis["cascade"][COY_DISASTER]
        assert cs.cross_ontology_count >= 1


# --- Scoring Tests ---

def test_scoring_returns_all_nodes(full_analysis) -> None:
    G = full_analysis["G"]
    results = full_analysis["results"]
    assert len(results) == G.number_of_nodes()


def test_scoring_sorted_descending(full_analysis) -> None:
    results = full_analysis["results"]
    scores = [r.criticality_score for r in results]
    assert scores == sorted(scores, reverse=True)


def test_criticality_levels_valid(full_analysis) -> None:
    valid_levels = {"CRITICAL", "HIGH", "MEDIUM", "LOW"}
    for r in full_analysis["results"]:
        assert r.criticality_level in valid_levels


def test_at_least_one_critical(full_analysis) -> None:
    critical = [r for r in full_analysis["results"] if r.criticality_level == "CRITICAL"]
    assert len(critical) >= 1, "Mindestens ein CRITICAL-Konzept erwartet"


def test_scores_in_range(full_analysis) -> None:
    for r in full_analysis["results"]:
        assert 0.0 <= r.criticality_score <= 1.0


def test_top5_contains_known_hubs(full_analysis) -> None:
    """coy:Event, coy:Feature oder coy:Disaster sollten in Top-10 sein."""
    top10_uris = {r.uri for r in full_analysis["results"][:10]}
    known_hubs = {
        "https://schema.coypu.org/global#Event",
        "https://schema.coypu.org/global#Feature",
        "https://schema.coypu.org/global#Disaster",
    }
    assert len(top10_uris & known_hubs) > 0, f"Keiner der bekannten Hubs in Top-10: {top10_uris}"
