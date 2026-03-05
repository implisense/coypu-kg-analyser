"""
SPARQLTemplateGenerator: Erzeugt SPARQL-Queries via Jinja2-Templates.

Generiert kontextspezifische Queries basierend auf CriticalityResult.
"""
from __future__ import annotations

import json
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

_TEMPLATES_DIR = Path(__file__).parent / "templates"


class SPARQLTemplateGenerator:
    """Generiert SPARQL-Queries aus Jinja2-Templates."""

    def __init__(self) -> None:
        self.env = Environment(
            loader=FileSystemLoader(str(_TEMPLATES_DIR)),
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def generate_for(self, result: object) -> list[str]:
        """Generiert passende SPARQL-Query-Namen für ein CriticalityResult.

        Gibt eine Liste von Template-Namen zurück (ohne .j2-Suffix).
        """
        queries: list[str] = []

        # Direkten Zugriff auf Attribute (duck-typing)
        betweenness = getattr(result, "betweenness_centrality", 0.0)
        is_art_point = getattr(result, "is_articulation_point", False)
        ind_concentration = getattr(result, "individual_concentration", 0.0)
        cross_count = getattr(result, "cross_ontology_count", 0)
        level = getattr(result, "criticality_level", "LOW")

        if betweenness > 0.3 or is_art_point:
            queries.append("bottleneck_connectivity")
        if ind_concentration > 0.5:
            queries.append("concentration_instance_count")
        if cross_count > 1:
            queries.append("cascade_bridge")
        if level in ("CRITICAL", "HIGH"):
            queries.append("scenario_enrichment")

        return queries

    def render(self, template_name: str, result: object) -> str:
        """Rendert ein Template mit den Werten aus CriticalityResult."""
        template_file = f"{template_name}.sparql.j2"
        tpl = self.env.get_template(template_file)

        # Attribute als Dict extrahieren
        from dataclasses import asdict
        try:
            ctx = asdict(result)
        except TypeError:
            ctx = {k: getattr(result, k) for k in dir(result) if not k.startswith("_")}

        return tpl.render(**ctx)

    def export_library(self, results: list[object], output_dir: Path) -> None:
        """Exportiert alle generierten SPARQL-Queries als .sparql-Dateien + index.json."""
        output_dir.mkdir(parents=True, exist_ok=True)
        index: list[dict] = []

        for result in results:
            level = getattr(result, "criticality_level", "LOW")
            if level not in ("CRITICAL", "HIGH", "MEDIUM"):
                continue

            local_name = getattr(result, "local_name", "unknown")
            ns = getattr(result, "namespace", "unknown")
            queries = getattr(result, "suggested_sparql_queries", [])

            for query_type in queries:
                try:
                    sparql_text = self.render(query_type, result)
                except Exception:
                    continue

                filename = f"{ns}_{local_name}_{query_type}.sparql"
                filepath = output_dir / filename
                filepath.write_text(sparql_text, encoding="utf-8")

                index.append({
                    "file": filename,
                    "concept": f"{ns}:{local_name}",
                    "uri": getattr(result, "uri", ""),
                    "query_type": query_type,
                    "criticality_level": level,
                    "criticality_score": getattr(result, "criticality_score", 0.0),
                })

        index_path = output_dir / "index.json"
        index_path.write_text(json.dumps(index, indent=2, ensure_ascii=False), encoding="utf-8")
