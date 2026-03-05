# coypu-kg-analyser

[![Tests](https://github.com/implisense/coypu-kg-analyser/actions/workflows/pytest.yml/badge.svg)](https://github.com/implisense/coypu-kg-analyser/actions/workflows/pytest.yml)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Criticality analysis for [CoyPu](https://coypu.org/) Knowledge Graph ontologies. Identifies structurally critical concepts across six OWL/RDF ontologies and generates simulation shock parameters from live KG data.

## What it does

Given a set of CoyPu ontology files (TTL), the analyser computes a criticality score for each concept along three dimensions:

| Dimension | Metric | Weight |
|-----------|--------|--------|
| Network bottleneck | Betweenness centrality + articulation points | 40% |
| Concentration risk | Individual-ratio, taxonomy breadth | 30% |
| Cross-ontology cascade | Incoming namespace diversity | 30% |

Score formula: `0.4 × betweenness + 0.3 × concentration + 0.3 × cascade + 0.2 bonus (articulation points)`

Levels: **CRITICAL** (≥ 0.7) | **HIGH** (0.5–0.7) | **MEDIUM** (0.3–0.5) | **LOW** (< 0.3)

## Supported ontologies

The tool works with the six CoyPu sub-ontologies:

- `coypu-global` — core entities (Event, Feature, Country, …)
- `gta` — Global Trade Alert interventions
- `ta` — Trade agreements
- `vtf` — Value chain / supply network
- `world-port-index` — Port infrastructure (WPI)
- `em-dat` — Natural disaster data

## Installation

```bash
pip install -e ".[dev]"
```

**Requirements:** Python 3.9+, ontology TTL files from the CoyPu project.

## Usage

### Analyse ontologies

```bash
coypu-kg-analyser analyse \
  --ontology-dir /path/to/ontology/ \
  --output-dir ./output/ \
  --format all \
  --min-criticality medium
```

Output formats (`--format`): `json`, `yaml`, `markdown`, `sparql`, `all`

### Enrich a scenario

Match criticality results against a PDL scenario file:

```bash
coypu-kg-analyser enrich-scenario \
  --scenario s1-soja.pdl.yaml \
  --results output/results.json \
  --output s1-soja-enriched.pdl.yaml
```

### Live KG query

Query the CoyPu SPARQL endpoint directly:

```bash
coypu-kg-analyser query-live \
  --endpoint https://copper.coypu.org/coypu/ \
  --concept coy:Event
```

### Generate simulation shocks — S1 Soja

Translate live KG data (ACLED conflict events, GTA trade interventions, crop production statistics) into simulation shock parameters for the soybean supply chain scenario:

```bash
coypu-kg-analyser parametrize-s1 \
  --output shocks.json \
  --lookback-days 180
```

The output `shocks.json` contains ready-to-use shock parameters for agent-based supply chain simulation.

## Output files

| File | Description |
|------|-------------|
| `results.json` | Full results (schema v1) |
| `results.yaml` | PDL-optimised (scenario recommendations, monitoring priorities) |
| `report.md` | Markdown report, Top 10 per dimension |
| `sparql_queries/` | Generated SPARQL queries + `index.json` |

## Development

```bash
# Run tests
pytest tests/ -v

# Run with coverage
pytest --cov=coypu_kg_analyser tests/
```

## License

MIT © 2026
