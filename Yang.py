from abc import ABC, abstractmethod
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON


# ============================================================
# BASE CLASS
# ============================================================

class QueryHandler(ABC):
    """
    Abstract base class for all query handlers.
    Each handler must implement getById() and expose a database path/URL.
    """

    def __init__(self):
        self.dbPathOrUrl = ""

    def getDbPathOrUrl(self) -> str:
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, url: str):
        if not isinstance(url, str):
            raise ValueError("Database path/URL must be a string.")
        self.dbPathOrUrl = url.strip()
        return True

    @abstractmethod
    def getById(self, entity_id: str) -> pd.DataFrame:
        pass


# ============================================================
# JOURNAL QUERY HANDLER (GRAPH DB)
# ============================================================

class JournalQueryHandler(QueryHandler):
    """
    Handles all SPARQL queries to the graph database (Blazegraph).
    Returns pandas DataFrames in a format compatible with BasicQueryEngine.
    """

    # ---- Internal helper: safe SPARQL execution ----
    def _run_sparql(self, query: str) -> list[dict]:
        """
        Executes a SPARQL query safely.
        Returns a list of bindings or an empty list on error.
        """
        try:
            sparql = SPARQLWrapper(self.getDbPathOrUrl())
            sparql.setMethod("GET")                     # REQUIRED for Blazegraph
            sparql.setReturnFormat(JSON)
            sparql.setQuery(query)
            results = sparql.query().convert()
            return results.get("results", {}).get("bindings", [])
        except Exception as e:
            print("SPARQL ERROR:", e)
            return []

    # ---- Get by ID ----
    def getById(self, journal_id: str) -> pd.DataFrame:
        jid = (journal_id or "").strip()
        if not jid:
            return pd.DataFrame()

        query = f"""
        PREFIX : <https://brigata.github.org/>
        SELECT ?journal ?title ?publisher ?license ?apc
               (GROUP_CONCAT(DISTINCT STR(?lang); separator=", ") AS ?languages)
               ?seal
        WHERE {{
            ?journal :id ?jid .
            FILTER(LCASE(STR(?jid)) = LCASE("{jid}"))

            OPTIONAL {{ ?journal :title ?title }}
            OPTIONAL {{ ?journal :publisher ?publisher }}
            OPTIONAL {{ ?journal :license ?license }}
            OPTIONAL {{ ?journal :apc ?apc }}
            OPTIONAL {{ ?journal :languages ?lang }}
            OPTIONAL {{ ?journal :seal ?seal }}
        }}
        GROUP BY ?journal ?title ?publisher ?license ?apc ?seal
        LIMIT 1
        """

        bindings = self._run_sparql(query)
        if not bindings:
            return pd.DataFrame()

        b = bindings[0]
        def v(var): return b.get(var, {}).get("value")

        return pd.DataFrame([{
            "id": jid,
            "title": v("title"),
            "publisher": v("publisher"),
            "license": v("license"),
            "apc": v("apc"),
            "languages": v("languages"),
            "seal": v("seal"),
        }])

    # ---- Get all journals ----
    def getAllJournals(self) -> pd.DataFrame:
        query = """
        PREFIX : <https://brigata.github.org/>
        SELECT ?journal ?id ?title ?publisher ?apc ?seal ?license
               (GROUP_CONCAT(DISTINCT STR(?lang); separator=", ") AS ?languages)
        WHERE {
            ?journal :id ?id .
            OPTIONAL { ?journal :title ?title }
            OPTIONAL { ?journal :publisher ?publisher }
            OPTIONAL { ?journal :apc ?apc }
            OPTIONAL { ?journal :seal ?seal }
            OPTIONAL { ?journal :license ?license }
            OPTIONAL { ?journal :languages ?lang }
        }
        GROUP BY ?journal ?id ?title ?publisher ?apc ?seal ?license
        """

        bindings = self._run_sparql(query)
        if not bindings:
            return pd.DataFrame(columns=["id", "title", "publisher", "apc", "seal", "license", "languages"])

        def v(b, var): return b.get(var, {}).get("value")

        data = [{
            "id": v(b, "id"),
            "title": v(b, "title"),
            "publisher": v(b, "publisher"),
            "apc": v(b, "apc"),
            "seal": v(b, "seal"),
            "license": v(b, "license"),
            "languages": v(b, "languages"),
        } for b in bindings]

        return pd.DataFrame(data)

    # ---- Journals with title ----
    def getJournalsWithTitle(self, partial_title: str) -> pd.DataFrame:
        pt = (partial_title or "").strip().replace('"', '\\"')

        query = f"""
        PREFIX : <https://brigata.github.org/>
        SELECT ?journal ?id ?title ?publisher ?apc ?seal ?license
               (GROUP_CONCAT(DISTINCT STR(?lang); separator=", ") AS ?languages)
        WHERE {{
            ?journal :id ?id .
            ?journal :title ?title .
            FILTER(CONTAINS(LCASE(STR(?title)), LCASE("{pt}")))

            OPTIONAL {{ ?journal :publisher ?publisher }}
            OPTIONAL {{ ?journal :apc ?apc }}
            OPTIONAL {{ ?journal :seal ?seal }}
            OPTIONAL {{ ?journal :license ?license }}
            OPTIONAL {{ ?journal :languages ?lang }}
        }}
        GROUP BY ?journal ?id ?title ?publisher ?apc ?seal ?license
        """

        bindings = self._run_sparql(query)
        if not bindings:
            return pd.DataFrame(columns=["id", "title", "publisher", "apc", "seal", "license", "languages"])

        def v(b, var): return b.get(var, {}).get("value")

        data = [{
            "id": v(b, "id"),
            "title": v(b, "title"),
            "publisher": v(b, "publisher"),
            "apc": v(b, "apc"),
            "seal": v(b, "seal"),
            "license": v(b, "license"),
            "languages": v(b, "languages"),
        } for b in bindings]

        return pd.DataFrame(data).drop_duplicates(subset=["id"])

    # ---- Journals by publisher ----
    def getJournalsPublishedBy(self, partial_name: str) -> pd.DataFrame:
        pn = (partial_name or "").strip().replace('"', '\\"')

        query = f"""
        PREFIX : <https://brigata.github.org/>
        SELECT ?journal ?id ?title ?publisher ?apc ?seal ?license
               (GROUP_CONCAT(DISTINCT STR(?lang); separator=", ") AS ?languages)
        WHERE {{
            ?journal :id ?id .
            ?journal :publisher ?publisher .
            FILTER(CONTAINS(LCASE(STR(?publisher)), LCASE("{pn}")))

            OPTIONAL {{ ?journal :title ?title }}
            OPTIONAL {{ ?journal :apc ?apc }}
            OPTIONAL {{ ?journal :seal ?seal }}
            OPTIONAL {{ ?journal :license ?license }}
            OPTIONAL {{ ?journal :languages ?lang }}
        }}
        GROUP BY ?journal ?id ?title ?publisher ?apc ?seal ?license
        """

        bindings = self._run_sparql(query)
        if not bindings:
            return pd.DataFrame(columns=["id", "title", "publisher", "apc", "seal", "license", "languages"])

        def v(b, var): return b.get(var, {}).get("value")

        data = [{
            "id": v(b, "id"),
            "title": v(b, "title"),
            "publisher": v(b, "publisher"),
            "apc": v(b, "apc"),
            "seal": v(b, "seal"),
            "license": v(b, "license"),
            "languages": v(b, "languages"),
        } for b in bindings]

        return pd.DataFrame(data).drop_duplicates(subset=["id"])

    # ---- Journals by license ----
    def getJournalsWithLicense(self, license_str: str) -> pd.DataFrame:
        lic = (license_str or "").strip().replace('"', '\\"')

        query = f"""
        PREFIX : <https://brigata.github.org/>
        SELECT ?journal ?id ?title ?publisher ?apc ?seal ?license
               (GROUP_CONCAT(DISTINCT STR(?lang); separator=", ") AS ?languages)
        WHERE {{
            ?journal :id ?id .
            ?journal :license ?license .
            FILTER(CONTAINS(LCASE(STR(?license)), LCASE("{lic}")))

            OPTIONAL {{ ?journal :title ?title }}
            OPTIONAL {{ ?journal :publisher ?publisher }}
            OPTIONAL {{ ?journal :apc ?apc }}
            OPTIONAL {{ ?journal :seal ?seal }}
            OPTIONAL {{ ?journal :languages ?lang }}
        }}
        GROUP BY ?journal ?id ?title ?publisher ?apc ?seal ?license
        """

        bindings = self._run_sparql(query)
        if not bindings:
            return pd.DataFrame(columns=["id", "title", "publisher", "apc", "seal", "license", "languages"])

        def v(b, var): return b.get(var, {}).get("value")

        data = [{
            "id": v(b, "id"),
            "title": v(b, "title"),
            "publisher": v(b, "publisher"),
            "apc": v(b, "apc"),
            "seal": v(b, "seal"),
            "license": v(b, "license"),
            "languages": v(b, "languages"),
        } for b in bindings]

        return pd.DataFrame(data).drop_duplicates(subset=["id", "license"])

    # ---- Journals with APC ----
    def getJournalsWithAPC(self, apc_str: str = "true") -> pd.DataFrame:
        apc_norm = (apc_str or "").strip().lower()

        if apc_norm not in {"true", "false"}:
            return pd.DataFrame(columns=["id", "title", "publisher", "apc", "seal", "license", "languages"])

        query = f"""
        PREFIX : <https://brigata.github.org/>
        SELECT ?journal ?id ?title ?publisher ?apc ?seal ?license
               (GROUP_CONCAT(DISTINCT STR(?lang); separator=", ") AS ?languages)
        WHERE {{
            ?journal :id ?id .
            OPTIONAL {{ ?journal :title ?title }}
            OPTIONAL {{ ?journal :publisher ?publisher }}
            OPTIONAL {{ ?journal :license ?license }}
            OPTIONAL {{ ?journal :seal ?seal }}
            OPTIONAL {{ ?journal :languages ?lang }}
            OPTIONAL {{ ?journal :apc ?apc }}

            FILTER(BOUND(?apc) && LCASE(STR(?apc)) = "{apc_norm}")
        }}
        GROUP BY ?journal ?id ?title ?publisher ?apc ?seal ?license
        """

        bindings = self._run_sparql(query)
        if not bindings:
            return pd.DataFrame(columns=["id", "title", "publisher", "apc", "seal", "license", "languages"])

        def v(b, var): return b.get(var, {}).get("value")

        data = [{
            "id": v(b, "id"),
            "title": v(b, "title"),
            "publisher": v(b, "publisher"),
            "apc": v(b, "apc"),
            "seal": v(b, "seal"),
            "license": v(b, "license"),
            "languages": v(b, "languages"),
        } for b in bindings]

        return pd.DataFrame(data).drop_duplicates(subset=["id"])

    # ---- Journals with DOAJ seal ----
    def getJournalsWithDOAJSeal(self, seal_str: str = "true") -> pd.DataFrame:
        seal_norm = (seal_str or "").strip().lower()

        if seal_norm not in {"true", "false"}:
            return pd.DataFrame(columns=["id", "title", "publisher", "apc", "seal", "license", "languages"])

        query = f"""
        PREFIX : <https://brigata.github.org/>
        SELECT ?journal ?id ?title ?publisher ?apc ?seal ?license
               (GROUP_CONCAT(DISTINCT STR(?lang); separator=", ") AS ?languages)
        WHERE {{
            ?journal :id ?id .
            OPTIONAL {{ ?journal :title ?title }}
            OPTIONAL {{ ?journal :publisher ?publisher }}
            OPTIONAL {{ ?journal :apc ?apc }}
            OPTIONAL {{ ?journal :license ?license }}
            OPTIONAL {{ ?journal :languages ?lang }}
            OPTIONAL {{ ?journal :seal ?seal }}

            FILTER(BOUND(?seal) && LCASE(STR(?seal)) = "{seal_norm}")
        }}
        GROUP BY ?journal ?id ?title ?publisher ?apc ?seal ?license
        """

        bindings = self._run_sparql(query)
        if not bindings:
            return pd.DataFrame(columns=["id", "title", "publisher", "apc", "seal", "license", "languages"])

        def v(b, var): return b.get(var, {}).get("value")

        data = [{
            "id": v(b, "id"),
            "title": v(b, "title"),
            "publisher": v(b, "publisher"),
            "apc": v(b, "apc"),
            "seal": v(b, "seal"),
            "license": v(b, "license"),
            "languages": v(b, "languages"),
        } for b in bindings]

        return pd.DataFrame(data).drop_duplicates(subset=["id"])
