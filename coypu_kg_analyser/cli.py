"""CLI-Einstiegspunkt für coypu-kg-analyser."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

console = Console()


@click.group()
@click.version_option()
def main() -> None:
    """coypu-kg-analyser: Neuralgische-Punkte-Analyse der CoyPu-Wissensgraphen-Ontologien."""


@main.command()
@click.option(
    "--ontology-dir",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Verzeichnis mit den TTL-Ontologie-Dateien.",
)
@click.option(
    "--output-dir",
    default="./output",
    type=click.Path(file_okay=False, path_type=Path),
    help="Ausgabeverzeichnis für Ergebnisse.",
)
@click.option(
    "--format",
    "output_format",
    default="all",
    type=click.Choice(["json", "yaml", "markdown", "sparql", "all"]),
    help="Ausgabeformat.",
)
@click.option(
    "--min-criticality",
    default="low",
    type=click.Choice(["low", "medium", "high", "critical"]),
    help="Minimales Kritikalitätslevel für Ausgabe.",
)
def analyse(
    ontology_dir: Path,
    output_dir: Path,
    output_format: str,
    min_criticality: str,
) -> None:
    """Führt die vollständige Kritikalitätsanalyse der Ontologien durch."""
    from coypu_kg_analyser.loader.ontology_loader import OntologyLoader
    from coypu_kg_analyser.graph.owl_graph_builder import OWLGraphBuilder
    from coypu_kg_analyser.metrics.bottleneck import BottleneckAnalyser
    from coypu_kg_analyser.metrics.concentration import ConcentrationAnalyser
    from coypu_kg_analyser.metrics.cascade import CascadeAnalyser
    from coypu_kg_analyser.scoring.criticality import CriticalityScorer
    from coypu_kg_analyser.sparql.template_generator import SPARQLTemplateGenerator
    from coypu_kg_analyser.output.json_exporter import JSONExporter
    from coypu_kg_analyser.output.markdown_reporter import MarkdownReporter

    output_dir.mkdir(parents=True, exist_ok=True)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Lade Ontologien...", total=None)

        loader = OntologyLoader(ontology_dir)
        loader.load_all()
        progress.update(task, description=f"Geladen: {len(loader.graph)} Triples")

        progress.update(task, description="Baue OWL-Graph...")
        builder = OWLGraphBuilder(loader)
        G = builder.build()
        progress.update(
            task,
            description=f"Graph: {G.number_of_nodes()} Knoten, {G.number_of_edges()} Kanten",
        )

        progress.update(task, description="Berechne Metriken (Betweenness)...")
        bottleneck = BottleneckAnalyser(G)
        bottleneck_results = bottleneck.analyse()

        progress.update(task, description="Berechne Metriken (Konzentration)...")
        concentration = ConcentrationAnalyser(G, loader)
        concentration_results = concentration.analyse()

        progress.update(task, description="Berechne Metriken (Kaskade)...")
        cascade = CascadeAnalyser(G)
        cascade_results = cascade.analyse()

        progress.update(task, description="Aggregiere Kritikalitäts-Scores...")
        scorer = CriticalityScorer(G, bottleneck_results, concentration_results, cascade_results)
        results = scorer.score_all()

        min_level_map = {"low": 0, "medium": 1, "high": 2, "critical": 3}
        level_order = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}
        min_level_val = min_level_map[min_criticality.lower()]
        results = [r for r in results if level_order.get(r.criticality_level, 0) >= min_level_val]

        progress.update(task, description="Generiere SPARQL-Queries...")
        sparql_gen = SPARQLTemplateGenerator()
        for result in results:
            result.suggested_sparql_queries = sparql_gen.generate_for(result)

        progress.update(task, description="Schreibe Ausgaben...")

        if output_format in ("json", "all"):
            exporter = JSONExporter()
            exporter.export_json(results, output_dir / "results.json")
        if output_format in ("yaml", "all"):
            exporter = JSONExporter()
            exporter.export_yaml(results, output_dir / "results.yaml")
        if output_format in ("markdown", "all"):
            reporter = MarkdownReporter()
            reporter.write(results, output_dir / "report.md")
        if output_format in ("sparql", "all"):
            sparql_dir = output_dir / "sparql_queries"
            sparql_dir.mkdir(exist_ok=True)
            sparql_gen.export_library(results, sparql_dir)

        progress.update(task, description="Fertig.")

    console.print(f"\n[bold green]Analyse abgeschlossen.[/] {len(results)} Konzepte analysiert.")
    console.print(f"Ausgabe: [cyan]{output_dir}[/]")

    critical = [r for r in results if r.criticality_level == "CRITICAL"]
    high = [r for r in results if r.criticality_level == "HIGH"]
    console.print(f"  CRITICAL: {len(critical)}, HIGH: {len(high)}")


@main.command("enrich-scenario")
@click.option(
    "--scenario",
    required=True,
    type=click.Path(exists=True, path_type=Path),
    help="PDL-YAML-Szenario-Datei.",
)
@click.option(
    "--results",
    required=True,
    type=click.Path(exists=True, path_type=Path),
    help="Analyse-Ergebnisse (results.json).",
)
@click.option(
    "--output",
    required=True,
    type=click.Path(path_type=Path),
    help="Ausgabe-Pfad für angereichertes Szenario.",
)
def enrich_scenario(scenario: Path, results: Path, output: Path) -> None:
    """Reichert ein PDL-Szenario um kritische Entitäten aus der Analyse an."""
    import yaml

    with results.open() as f:
        analysis = json.load(f)

    with scenario.open() as f:
        pdl = yaml.safe_load(f)

    critical_concepts = [
        finding
        for finding in analysis.get("findings", [])
        if finding.get("criticality_level") in ("CRITICAL", "HIGH")
    ]

    if "metadata" not in pdl:
        pdl["metadata"] = {}
    pdl["metadata"]["enriched_by"] = "coypu-kg-analyser"
    pdl["metadata"]["critical_concepts"] = [
        {"uri": c["uri"], "local_name": c["local_name"], "score": c["criticality_score"]}
        for c in critical_concepts[:10]
    ]

    with output.open("w") as f:
        yaml.dump(pdl, f, allow_unicode=True, default_flow_style=False)

    console.print(f"[bold green]Szenario angereichert:[/] {output}")
    console.print(f"  {len(critical_concepts)} kritische Konzepte hinzugefügt.")


@main.command("query-live")
@click.option(
    "--query",
    "sparql_query",
    default=None,
    help="SPARQL-Query-String (alternativ zu --file oder --sparql-dir).",
)
@click.option(
    "--file",
    "query_file",
    default=None,
    type=click.Path(exists=True, path_type=Path),
    help=".sparql-Datei, die ausgeführt werden soll.",
)
@click.option(
    "--sparql-dir",
    default=None,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Verzeichnis mit .sparql-Dateien (aus analyse --format sparql).",
)
@click.option(
    "--filter-type",
    default=None,
    type=click.Choice(
        ["bottleneck_connectivity", "concentration_instance_count", "cascade_bridge", "scenario_enrichment"]
    ),
    help="Nur Queries eines bestimmten Typs aus --sparql-dir ausführen.",
)
@click.option(
    "--max-queries",
    default=10,
    show_default=True,
    help="Maximale Anzahl Queries bei --sparql-dir.",
)
@click.option(
    "--output-format",
    default="table",
    type=click.Choice(["table", "json", "csv"]),
    help="Ausgabeformat der Ergebnisse.",
)
@click.option(
    "--output-file",
    default=None,
    type=click.Path(path_type=Path),
    help="Ergebnisse in Datei schreiben statt stdout.",
)
@click.option(
    "--endpoint",
    default="https://copper.coypu.org/coypu/",
    show_default=True,
    help="SPARQL-Endpoint URL.",
)
@click.option(
    "--check",
    is_flag=True,
    default=False,
    help="Nur Konnektivität zum Endpoint prüfen.",
)
@click.option(
    "--enrich-results",
    "enrich_results_file",
    default=None,
    type=click.Path(exists=True, path_type=Path),
    help="results.json anreichern: Live-Instanz-Zahlen für Top-Konzepte abfragen.",
)
def query_live(
    sparql_query: str | None,
    query_file: Path | None,
    sparql_dir: Path | None,
    filter_type: str | None,
    max_queries: int,
    output_format: str,
    output_file: Path | None,
    endpoint: str,
    check: bool,
    enrich_results_file: Path | None,
) -> None:
    """Sendet SPARQL-Queries an den CoyPu Live Knowledge Graph Endpoint."""
    from coypu_kg_analyser.live_query import LiveQueryClient

    client = LiveQueryClient(endpoint=endpoint)

    # --- Konnektivitätscheck ---
    if check:
        console.print(f"Prüfe Verbindung zu [cyan]{endpoint}[/]...")
        ok = client.check_connectivity()
        if ok:
            console.print("[bold green]Endpoint erreichbar.[/]")
        else:
            console.print("[bold red]Endpoint nicht erreichbar.[/]")
            raise SystemExit(1)
        return

    # --- results.json anreichern ---
    if enrich_results_file:
        _enrich_with_live_counts(client, enrich_results_file, output_file)
        return

    # --- Einzelne Query oder Datei ---
    if sparql_query:
        result = client.query(sparql_query)
        _print_single_result(result, output_format, output_file)
        return

    if query_file:
        result = client.query_file(query_file)
        console.print(f"Query: [cyan]{query_file.name}[/]")
        _print_single_result(result, output_format, output_file)
        return

    # --- Verzeichnis mit mehreren Queries ---
    if sparql_dir:
        pairs = client.run_library(sparql_dir, filter_type=filter_type, max_queries=max_queries)
        _print_library_results(pairs, output_format, output_file)
        return

    console.print("[red]Fehler:[/] Bitte --query, --file, --sparql-dir, --check oder --enrich-results angeben.")
    raise SystemExit(1)


@main.command("parametrize-s1")
@click.option(
    "--output",
    default=None,
    type=click.Path(path_type=Path),
    help="Ausgabe-Pfad für shocks.json (stdout wenn nicht angegeben).",
)
@click.option(
    "--reference-date",
    default=None,
    help="Referenzdatum (YYYY-MM-DD), Standard: heute. Ermöglicht historische Szenarien.",
)
@click.option(
    "--lookback-years",
    default=3,
    show_default=True,
    help="Zeitraum in Jahren für EM-DAT- und GTA-Abfragen.",
)
@click.option(
    "--max-results",
    default=2000,
    show_default=True,
    help="Maximale Anzahl Ergebnisse pro Query (höher = genauere Zählung, langsamer).",
)
@click.option(
    "--endpoint",
    default="https://copper.coypu.org/coypu/",
    show_default=True,
    help="SPARQL-Endpoint URL.",
)
def parametrize_s1(
    output: Path | None,
    reference_date: str | None,
    lookback_years: int,
    max_results: int,
    endpoint: str,
) -> None:
    """Übersetzt CoyPu KG-Daten (EM-DAT, GTA, WPI) in S1-Soja-Schocks."""
    from coypu_kg_analyser.live_query import LiveQueryClient
    from coypu_kg_analyser.parametrizer import S1Parametrizer

    client = LiveQueryClient(endpoint=endpoint)
    parametrizer = S1Parametrizer(
        client,
        lookback_years=lookback_years,
        reference_date=reference_date,
        max_results=max_results,
    )

    err_console = Console(stderr=True)
    err_console.print("Frage KG-Daten ab (EM-DAT, GTA, WPI)...")
    result = parametrizer.build_shocks()
    output_dict = result.to_output_dict(lookback_years=lookback_years)

    text = json.dumps(output_dict, indent=2, ensure_ascii=False)

    if output:
        output.write_text(text, encoding="utf-8")
        err_console.print(f"[bold green]Gespeichert:[/] {output}")
    else:
        sys.stdout.write(text + "\n")

    err_console.print(f"\n[cyan]{result.summary}[/]")



@main.command("parametrize-s10")
@click.option(
    "--output",
    default=None,
    type=click.Path(path_type=Path),
    help="Ausgabe-Pfad für shocks.json (stdout wenn nicht angegeben).",
)
@click.option(
    "--lookback-days",
    default=180,
    show_default=True,
    help="Zeitraum in Tagen für ACLED- und GTA-Abfragen.",
)
@click.option(
    "--max-results",
    default=2000,
    show_default=True,
    help="Maximale Anzahl Ergebnisse pro Query.",
)
@click.option(
    "--reference-date",
    default=None,
    help="Referenzdatum (YYYY-MM-DD), Standard: heute.",
)
@click.option(
    "--endpoint",
    default="https://copper.coypu.org/coypu/",
    show_default=True,
    help="SPARQL-Endpoint URL.",
)
def parametrize_s10(
    output: Path | None,
    lookback_days: int,
    max_results: int,
    reference_date: str | None,
    endpoint: str,
) -> None:
    """Übersetzt CoyPu KG-Daten (ACLED, GTA, WPI) in S10-Iran-Angriff-Schocks."""
    from coypu_kg_analyser.live_query import LiveQueryClient
    from coypu_kg_analyser.parametrizer.s10_iran import S10Parametrizer

    client = LiveQueryClient(endpoint=endpoint)
    parametrizer = S10Parametrizer(
        client, lookback_days=lookback_days, reference_date=reference_date, max_results=max_results
    )

    err_console = Console(stderr=True)
    err_console.print("Frage KG-Daten ab (ACLED, GTA, WPI)...")
    result = parametrizer.build_shocks()
    output_dict = result.to_output_dict(lookback_days=lookback_days)

    text = json.dumps(output_dict, indent=2, ensure_ascii=False)

    if output:
        output.write_text(text, encoding="utf-8")
        err_console.print(f"[bold green]Gespeichert:[/] {output}")
    else:
        sys.stdout.write(text + "\n")

    err_console.print(f"\n[cyan]{result.summary}[/]")


def _print_single_result(result: object, output_format: str, output_file: Path | None) -> None:
    from coypu_kg_analyser.live_query import QueryResult
    assert isinstance(result, QueryResult)

    if not result.success:
        console.print(f"[bold red]Fehler:[/] {result.error}")
        raise SystemExit(1)

    console.print(
        f"[green]OK[/] — {result.row_count} Ergebnisse, {result.elapsed_ms:.0f}ms"
    )

    text = _format_result(result, output_format)

    if output_file:
        output_file.write_text(text, encoding="utf-8")
        console.print(f"Gespeichert: [cyan]{output_file}[/]")
    elif output_format == "table":
        _render_table(result)
    else:
        console.print(text)


def _print_library_results(
    pairs: list,
    output_format: str,
    output_file: Path | None,
) -> None:
    all_ok = 0
    all_err = 0
    combined: list[dict] = []

    for sparql_file, result in pairs:
        if result.success:
            all_ok += 1
            console.print(
                f"[green]OK[/] {sparql_file.name} — {result.row_count} Zeilen, {result.elapsed_ms:.0f}ms"
            )
            combined.append({
                "file": sparql_file.name,
                "rows": result.row_count,
                "data": result.as_dicts(),
            })
        else:
            all_err += 1
            console.print(f"[red]ERR[/] {sparql_file.name} — {result.error}")

    console.print(f"\n{all_ok} OK, {all_err} Fehler")

    if output_file and combined:
        text = json.dumps(combined, indent=2, ensure_ascii=False)
        output_file.write_text(text, encoding="utf-8")
        console.print(f"Gespeichert: [cyan]{output_file}[/]")


def _enrich_with_live_counts(
    client: object,
    results_file: Path,
    output_file: Path | None,
) -> None:
    from coypu_kg_analyser.live_query import LiveQueryClient
    assert isinstance(client, LiveQueryClient)

    with results_file.open() as f:
        analysis = json.load(f)

    findings = analysis.get("findings", [])
    top = [
        type("R", (), f)()
        for f in findings[:20]
        if f.get("criticality_level") in ("CRITICAL", "HIGH")
    ]

    # Objekte mit uri/local_name/namespace etc. bauen
    class _R:
        def __init__(self, d: dict) -> None:
            self.uri = d["uri"]
            self.local_name = d["local_name"]
            self.namespace = d["namespace"]
            self.criticality_level = d["criticality_level"]
            self.criticality_score = d["criticality_score"]
            self.individual_count = d["metrics"]["individual_count"]

    real_top = [_R(f) for f in findings[:20] if f.get("criticality_level") in ("CRITICAL", "HIGH")]

    console.print(f"Frage Live-Instanz-Zahlen für {len(real_top)} Konzepte ab...")
    enriched = client.enrich_criticality_results(real_top)

    table = Table(title="Live-Instanz-Zahlen im CoyPu KG")
    table.add_column("Konzept", style="cyan")
    table.add_column("Level")
    table.add_column("Schema-Ind.", justify="right")
    table.add_column("Live-Instanzen", justify="right")
    for row in enriched:
        live = str(row["live_instance_count"]) if row["live_instance_count"] is not None else "—"
        table.add_row(
            f"{row['namespace']}:{row['local_name']}",
            row["criticality_level"],
            str(row["schema_individual_count"]),
            live,
        )
    console.print(table)

    if output_file:
        output_file.write_text(
            json.dumps(enriched, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        console.print(f"Gespeichert: [cyan]{output_file}[/]")


def _format_result(result: object, output_format: str) -> str:
    from coypu_kg_analyser.live_query import QueryResult
    assert isinstance(result, QueryResult)
    if output_format == "json":
        return result.as_json()
    if output_format == "csv":
        return result.as_csv()
    return ""


def _render_table(result: object) -> None:
    from coypu_kg_analyser.live_query import QueryResult
    assert isinstance(result, QueryResult)

    table = Table()
    for var in result.variables:
        table.add_column(var, style="cyan")
    for row in result.as_dicts():
        table.add_row(*[row.get(v, "") for v in result.variables])
    console.print(table)
