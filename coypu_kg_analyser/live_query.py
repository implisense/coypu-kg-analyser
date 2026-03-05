"""
LiveQueryClient: Sendet SPARQL-Queries an den CoyPu Knowledge Graph Endpoint.

Endpoint: https://copper.coypu.org/coypul/
Kein Authentifizierung erforderlich (öffentlicher Zugriff).
Unterstützte Rückgabeformate: json, csv
"""
from __future__ import annotations

import csv
import io
import json
import time
from pathlib import Path
from typing import Any

import requests

COYPU_ENDPOINT = "https://copper.coypu.org/coypu/"

# Standard-Timeout für Anfragen in Sekunden
DEFAULT_TIMEOUT = 30

# Standard-Namespaces, die allen Queries vorangestellt werden können
STANDARD_PREFIXES = """\
PREFIX coy:      <https://schema.coypu.org/global#>
PREFIX rdfs:     <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl:      <http://www.w3.org/2002/07/owl#>
PREFIX geo:      <http://www.opengis.net/ont/geosparql#>
PREFIX spatial:  <http://jena.apache.org/spatial#>
PREFIX spatialF: <http://jena.apache.org/function/spatial#>
PREFIX units:    <http://www.opengis.net/def/uom/OGC/1.0/>
PREFIX geof:     <http://www.opengis.net/def/function/geosparql/>
PREFIX skos:     <http://www.w3.org/2004/02/skos/core#>
PREFIX xsd:      <http://www.w3.org/2001/XMLSchema#>
PREFIX dct:      <http://purl.org/dc/terms/>
PREFIX dcat:     <http://www.w3.org/ns/dcat#>
PREFIX meta:     <https://schema.coypu.org/metadata-template#>
PREFIX gta:      <https://schema.coypu.org/gta#>
PREFIX ta:       <https://schema.coypu.org/ta#>
PREFIX vtf:      <https://schema.coypu.org/vtf#>
PREFIX wpi:      <https://schema.coypu.org/world-port-index#>
PREFIX emdat:    <https://schema.coypu.org/em-dat#>
"""


class QueryResult:
    """Ergebnis einer SPARQL-Anfrage."""

    def __init__(
        self,
        query: str,
        raw_response: dict[str, Any] | None = None,
        error: str | None = None,
        elapsed_ms: float = 0.0,
    ) -> None:
        self.query = query
        self.raw_response = raw_response or {}
        self.error = error
        self.elapsed_ms = elapsed_ms

    @property
    def success(self) -> bool:
        return self.error is None

    @property
    def bindings(self) -> list[dict[str, Any]]:
        """Gibt die SPARQL-Ergebnis-Bindings zurück."""
        return self.raw_response.get("results", {}).get("bindings", [])

    @property
    def variables(self) -> list[str]:
        """Gibt die Variablennamen zurück."""
        return self.raw_response.get("head", {}).get("vars", [])

    @property
    def row_count(self) -> int:
        return len(self.bindings)

    def as_dicts(self) -> list[dict[str, str]]:
        """Gibt die Ergebnisse als flache Dicts zurück (variable → Wert)."""
        result = []
        for binding in self.bindings:
            row = {}
            for var in self.variables:
                cell = binding.get(var, {})
                row[var] = cell.get("value", "")
            result.append(row)
        return result

    def as_json(self, indent: int = 2) -> str:
        return json.dumps(self.raw_response, indent=indent, ensure_ascii=False)

    def as_csv(self) -> str:
        """Gibt die Ergebnisse als CSV-String zurück."""
        buf = io.StringIO()
        if not self.variables:
            return ""
        writer = csv.DictWriter(buf, fieldnames=self.variables)
        writer.writeheader()
        writer.writerows(self.as_dicts())
        return buf.getvalue()

    def __repr__(self) -> str:
        if self.error:
            return f"QueryResult(error={self.error!r})"
        return f"QueryResult(rows={self.row_count}, vars={self.variables}, elapsed={self.elapsed_ms:.0f}ms)"


class LiveQueryClient:
    """Sendet SPARQL-Queries an den CoyPu Live-Endpoint."""

    def __init__(
        self,
        endpoint: str = COYPU_ENDPOINT,
        timeout: int = DEFAULT_TIMEOUT,
        add_standard_prefixes: bool = True,
    ) -> None:
        self.endpoint = endpoint
        self.timeout = timeout
        self.add_standard_prefixes = add_standard_prefixes
        self._session = requests.Session()
        self._session.headers.update({
            "Accept": "application/sparql-results+json",
            "User-Agent": "coypu-kg-analyser/0.1.0",
        })

    def query(self, sparql: str, add_prefixes: bool | None = None) -> QueryResult:
        """Führt eine SPARQL-Query gegen den Live-Endpoint aus.

        Args:
            sparql: SPARQL-Query-Text (SELECT).
            add_prefixes: Ob Standard-Prefixes vorangestellt werden sollen.
                          None = Instanz-Default verwenden.

        Returns:
            QueryResult mit Ergebnis oder Fehlermeldung.
        """
        use_prefixes = add_prefixes if add_prefixes is not None else self.add_standard_prefixes
        if use_prefixes and not sparql.strip().upper().startswith("PREFIX"):
            sparql = STANDARD_PREFIXES + "\n" + sparql

        start = time.monotonic()
        try:
            response = self._session.post(
                self.endpoint,
                data={"query": sparql},
                timeout=self.timeout,
            )
            elapsed = (time.monotonic() - start) * 1000

            if response.status_code != 200:
                return QueryResult(
                    query=sparql,
                    error=f"HTTP {response.status_code}: {response.text[:500]}",
                    elapsed_ms=elapsed,
                )

            return QueryResult(
                query=sparql,
                raw_response=response.json(),
                elapsed_ms=elapsed,
            )

        except requests.Timeout:
            return QueryResult(
                query=sparql,
                error=f"Timeout nach {self.timeout}s",
                elapsed_ms=(time.monotonic() - start) * 1000,
            )
        except requests.ConnectionError as e:
            return QueryResult(
                query=sparql,
                error=f"Verbindungsfehler: {e}",
                elapsed_ms=(time.monotonic() - start) * 1000,
            )
        except Exception as e:
            return QueryResult(
                query=sparql,
                error=f"Unbekannter Fehler: {e}",
                elapsed_ms=(time.monotonic() - start) * 1000,
            )

    def query_file(self, sparql_path: Path) -> QueryResult:
        """Lädt eine .sparql-Datei und führt sie aus."""
        sparql = sparql_path.read_text(encoding="utf-8")
        # Kommentarzeilen (# ...) aus Templates erhalten — der Endpoint ignoriert sie
        return self.query(sparql, add_prefixes=False)

    def run_library(
        self,
        sparql_dir: Path,
        filter_type: str | None = None,
        max_queries: int | None = None,
    ) -> list[tuple[Path, QueryResult]]:
        """Führt alle .sparql-Dateien aus einem Verzeichnis aus.

        Args:
            sparql_dir: Verzeichnis mit .sparql-Dateien (aus sparql_queries/).
            filter_type: Optional — nur Queries eines bestimmten Typs ausführen
                         (z.B. 'bottleneck_connectivity', 'scenario_enrichment').
            max_queries: Maximale Anzahl Queries (None = alle).

        Returns:
            Liste von (Dateipfad, QueryResult) Tupeln.
        """
        sparql_files = sorted(sparql_dir.glob("*.sparql"))

        if filter_type:
            sparql_files = [f for f in sparql_files if filter_type in f.name]

        if max_queries is not None:
            sparql_files = sparql_files[:max_queries]

        results = []
        for sparql_file in sparql_files:
            result = self.query_file(sparql_file)
            results.append((sparql_file, result))

        return results

    def check_connectivity(self) -> bool:
        """Prüft ob der Endpoint erreichbar ist."""
        result = self.query("SELECT ?s WHERE { ?s ?p ?o } LIMIT 1", add_prefixes=False)
        return result.success

    def get_instance_count(self, class_uri: str) -> int | None:
        """Zählt Instanzen einer Klasse im Live-KG.

        Args:
            class_uri: Vollständige URI der Klasse.

        Returns:
            Anzahl der Instanzen oder None bei Fehler.
        """
        sparql = f"""
SELECT (COUNT(?instance) AS ?count) WHERE {{
    ?instance a <{class_uri}> .
}}
"""
        result = self.query(sparql)
        if not result.success or not result.bindings:
            return None
        try:
            return int(result.bindings[0]["count"]["value"])
        except (KeyError, ValueError):
            return None

    def enrich_criticality_results(
        self,
        results: list[Any],
        max_concepts: int = 20,
    ) -> list[dict[str, Any]]:
        """Reichert CriticalityResults mit Live-Instanz-Zahlen an.

        Fragt für jeden kritischen Knoten die tatsächliche Instanz-Anzahl
        im Live-KG ab und gibt erweiterte Dicts zurück.

        Args:
            results: Liste von CriticalityResult-Objekten.
            max_concepts: Maximale Anzahl abzufragender Konzepte.

        Returns:
            Liste von Dicts mit zusätzlichem 'live_instance_count'.
        """
        enriched = []
        for r in results[:max_concepts]:
            live_count = self.get_instance_count(r.uri)
            enriched.append({
                "uri": r.uri,
                "local_name": r.local_name,
                "namespace": r.namespace,
                "criticality_level": r.criticality_level,
                "criticality_score": r.criticality_score,
                "schema_individual_count": r.individual_count,
                "live_instance_count": live_count,
            })
        return enriched
