"""Tests für Output-Module."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from coypu_kg_analyser.loader.ontology_loader import OntologyLoader
from coypu_kg_analyser.graph.owl_graph_builder import OWLGraphBuilder
from coypu_kg_analyser.metrics.bottleneck import BottleneckAnalyser
from coypu_kg_analyser.metrics.concentration import ConcentrationAnalyser
from coypu_kg_analyser.metrics.cascade import CascadeAnalyser
from coypu_kg_analyser.scoring.criticality import CriticalityScorer
from coypu_kg_analyser.sparql.template_generator import SPARQLTemplateGenerator
from coypu_kg_analyser.output.json_exporter import JSONExporter
from coypu_kg_analyser.output.markdown_reporter import MarkdownReporter

ONTOLOGY_DIR = Path(__file__).parent.parent.parent / "ontology"


@pytest.fixture(scope="module")
def scored_results():
    loader = OntologyLoader(ONTOLOGY_DIR)
    loader.load_all()
    G = OWLGraphBuilder(loader).build()
    b_res = BottleneckAnalyser(G).analyse()
    c_res = ConcentrationAnalyser(G, loader).analyse()
    cs_res = CascadeAnalyser(G).analyse()
    results = CriticalityScorer(G, b_res, c_res, cs_res).score_all()
    gen = SPARQLTemplateGenerator()
    for r in results:
        r.suggested_sparql_queries = gen.generate_for(r)
    return results


def test_json_export_valid(scored_results, tmp_path) -> None:
    exporter = JSONExporter()
    out = tmp_path / "results.json"
    exporter.export_json(scored_results, out)

    assert out.exists()
    assert out.stat().st_size > 0

    with out.open() as f:
        data = json.load(f)

    assert data["schema_version"] == "1.0"
    assert "findings" in data
    assert len(data["findings"]) == len(scored_results)
    assert "summary" in data
    assert data["summary"]["total_findings"] == len(scored_results)


def test_json_export_finding_structure(scored_results, tmp_path) -> None:
    exporter = JSONExporter()
    out = tmp_path / "results.json"
    exporter.export_json(scored_results, out)
    with out.open() as f:
        data = json.load(f)

    finding = data["findings"][0]
    required_keys = {"uri", "local_name", "namespace", "node_type", "criticality_score", "criticality_level", "metrics"}
    assert required_keys.issubset(finding.keys())


def test_yaml_export_valid(scored_results, tmp_path) -> None:
    exporter = JSONExporter()
    out = tmp_path / "results.yaml"
    exporter.export_yaml(scored_results, out)

    assert out.exists()
    assert out.stat().st_size > 0

    with out.open() as f:
        data = yaml.safe_load(f)

    assert "agent_monitoring_priorities" in data
    assert "suggested_scenarios" in data
    assert "scenario_enrichments" in data
    assert len(data["agent_monitoring_priorities"]) > 0


def test_markdown_report_valid(scored_results, tmp_path) -> None:
    reporter = MarkdownReporter()
    out = tmp_path / "report.md"
    reporter.write(scored_results, out)

    assert out.exists()
    assert out.stat().st_size > 0

    content = out.read_text(encoding="utf-8")
    assert "# CoyPu Knowledge Graph" in content
    assert "Dimension 1" in content
    assert "Dimension 2" in content
    assert "Dimension 3" in content
    assert "SPARQL-Query-Index" in content


def test_sparql_template_generation(scored_results) -> None:
    gen = SPARQLTemplateGenerator()
    critical = [r for r in scored_results if r.criticality_level == "CRITICAL"]
    assert len(critical) > 0

    for r in critical[:3]:
        queries = gen.generate_for(r)
        assert len(queries) > 0
        # Jedes Query-Template muss renderbar sein
        for q_type in queries:
            rendered = gen.render(q_type, r)
            assert len(rendered) > 50
            assert "SELECT" in rendered or "PREFIX" in rendered


def test_sparql_library_export(scored_results, tmp_path) -> None:
    gen = SPARQLTemplateGenerator()
    for r in scored_results:
        r.suggested_sparql_queries = gen.generate_for(r)

    sparql_dir = tmp_path / "sparql_queries"
    gen.export_library(scored_results, sparql_dir)

    assert sparql_dir.exists()
    index_file = sparql_dir / "index.json"
    assert index_file.exists()

    with index_file.open() as f:
        index = json.load(f)
    assert len(index) > 0
    assert "concept" in index[0]
    assert "query_type" in index[0]
