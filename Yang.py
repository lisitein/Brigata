from abc import ABC, abstractmethod
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
from sqlalchemy import create_engine

class QueryHandler(ABC):
    def __init__(self):
        self.dbPathOrUrl = ''

    def getDbPathOrUrl(self) -> str:
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, url: str):
        if not isinstance(url, str):
            raise ValueError("The path/URL of the database must be a string")
        self.dbPathOrUrl = url
        return True

    @abstractmethod
    def getById(self, entity_id: str) -> pd.DataFrame:
        pass

class JournalQueryHandler(QueryHandler):

    def getById(self, journal_id: str) -> pd.DataFrame | None:
        jid = (journal_id or "").strip()
        if not jid:
            return None

        sparql = SPARQLWrapper(self.getDbPathOrUrl())

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

        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)

        results = sparql.query().convert()
        bindings = results.get("results", {}).get("bindings", [])

        if not bindings:
            return None

        b = bindings[0]

        def v(var: str):
            return b.get(var, {}).get("value")

        return pd.DataFrame([{
            "id": jid,
            "title": v("title"),
            "publisher": v("publisher"),
            "license": v("license"),
            "apc": v("apc"),
            "languages": v("languages"),
            "seal": v("seal"),
        }])                                         # updated 10/02/26

    def getAllJournals(self) -> pd.DataFrame:
        sparql = SPARQLWrapper(self.getDbPathOrUrl())
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
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        bindings = results.get("results", {}).get("bindings", [])

        if not bindings:
            return pd.DataFrame(columns=["id", "title", "publisher", "apc", "seal", "license", "languages"])

        def v(b, var):
            return b.get(var, {}).get("value")

        data = []
        for b in bindings:
            data.append({
                "id": v(b, "id"),
                "title": v(b, "title"),
                "publisher": v(b, "publisher"),
                "apc": v(b, "apc"),
                "seal": v(b, "seal"),
                "license": v(b, "license"),
                "languages": v(b, "languages"),
            })

        return pd.DataFrame(data)           # updated 10/02/26

    def getJournalsWithTitle(self, partial_title: str) -> pd.DataFrame:
        sparql = SPARQLWrapper(self.getDbPathOrUrl())
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

        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        bindings = results.get("results", {}).get("bindings", [])

        if not bindings:
            return pd.DataFrame(columns=[
                "journal", "id", "title", "publisher", "apc", "seal", "license", "languages"
            ])

        def v(b, var):
            return b.get(var, {}).get("value")

        data = [{
            "journal": v(b, "journal"),
            "id": v(b, "id"),
            "title": v(b, "title"),
            "publisher": v(b, "publisher"),
            "apc": v(b, "apc"),
            "seal": v(b, "seal"),
            "license": v(b, "license"),
            "languages": v(b, "languages"),
        } for b in bindings]

        return pd.DataFrame(data).drop_duplicates(subset=["id"])        # updated 10/02/26

    def getJournalsPublishedBy(self, partial_name: str) -> pd.DataFrame:
        sparql = SPARQLWrapper(self.getDbPathOrUrl())
        pn = (partial_name or "").strip().replace('"', '\\"')

        query = f"""
        PREFIX : <https://brigata.github.org/>
        SELECT ?journal ?id ?title ?publisher ?apc ?seal ?license
            (GROUP_CONCAT(DISTINCT STR(?lang); separator=", ") AS ?languages)
        WHERE {{
        ?journal :id ?id .
        ?journal :title ?title .
        ?journal :publisher ?publisher .
        FILTER(CONTAINS(LCASE(STR(?publisher)), LCASE("{pn}")))

        OPTIONAL {{ ?journal :apc ?apc }}
        OPTIONAL {{ ?journal :seal ?seal }}
        OPTIONAL {{ ?journal :license ?license }}
        OPTIONAL {{ ?journal :languages ?lang }}
        }}
        GROUP BY ?journal ?id ?title ?publisher ?apc ?seal ?license
        """

        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        bindings = results.get("results", {}).get("bindings", [])

        if not bindings:
            return pd.DataFrame(columns=[
                "journal", "id", "title", "publisher", "apc", "seal", "license", "languages"
            ])

        def v(b, var):
            return b.get(var, {}).get("value")

        data = [{
            "journal": v(b, "journal"),
            "id": v(b, "id"),
            "title": v(b, "title"),
            "publisher": v(b, "publisher"),
            "apc": v(b, "apc"),
            "seal": v(b, "seal"),
            "license": v(b, "license"),
            "languages": v(b, "languages"),
        } for b in bindings]

        return pd.DataFrame(data).drop_duplicates(subset=["id"])        # updated 10/02/26

    def getJournalsWithLicense(self, license_str: str) -> pd.DataFrame:
        sparql = SPARQLWrapper(self.getDbPathOrUrl())
        lic = (license_str or "").strip().replace('"', '\\"')

        query = f"""
        PREFIX : <https://brigata.github.org/>
        SELECT ?journal ?id ?title ?publisher ?apc ?seal ?license
            (GROUP_CONCAT(DISTINCT STR(?lang); separator=", ") AS ?languages)
        WHERE {{
        ?journal :id ?id .
        ?journal :title ?title .
        ?journal :license ?license .

        FILTER(CONTAINS(LCASE(STR(?license)), LCASE("{lic}")))

        OPTIONAL {{ ?journal :publisher ?publisher }}
        OPTIONAL {{ ?journal :apc ?apc }}
        OPTIONAL {{ ?journal :seal ?seal }}
        OPTIONAL {{ ?journal :languages ?lang }}
        }}
        GROUP BY ?journal ?id ?title ?publisher ?apc ?seal ?license
        """

        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        bindings = results.get("results", {}).get("bindings", [])

        if not bindings:
            return pd.DataFrame(columns=[
                "journal", "id", "title", "publisher", "apc", "seal", "license", "languages"
            ])

        def v(b, var):
            return b.get(var, {}).get("value")

        data = [{
            "journal": v(b, "journal"),
            "id": v(b, "id"),
            "title": v(b, "title"),
            "publisher": v(b, "publisher"),
            "apc": v(b, "apc"),
            "seal": v(b, "seal"),
            "license": v(b, "license"),
            "languages": v(b, "languages"),
        } for b in bindings]

        return pd.DataFrame(data).drop_duplicates(subset=["id", "license"])     # updated 22/02/26

    def getJournalsWithAPC(self, apc_str: str = "true") -> pd.DataFrame:
        sparql = SPARQLWrapper(self.getDbPathOrUrl())
        apc_norm = (apc_str or "").strip().lower().replace('"', '\\"')

        if apc_norm not in {"true", "false"}:
            return pd.DataFrame(columns=[
                "journal", "id", "title", "publisher",
                "apc", "seal", "license", "languages"
            ])

        query = f"""
        PREFIX : <https://brigata.github.org/>
        SELECT ?journal ?id ?title ?publisher ?apc ?seal ?license
            (GROUP_CONCAT(DISTINCT STR(?lang); separator=", ") AS ?languages)
        WHERE {{
        ?journal :id ?id .
        ?journal :title ?title .

        OPTIONAL {{ ?journal :publisher ?publisher }}
        OPTIONAL {{ ?journal :license ?license }}
        OPTIONAL {{ ?journal :seal ?seal }}
        OPTIONAL {{ ?journal :languages ?lang }}
        OPTIONAL {{ ?journal :apc ?apc }}

        FILTER(BOUND(?apc) && LCASE(STR(?apc)) = "{apc_norm}")
        }}
        GROUP BY ?journal ?id ?title ?publisher ?apc ?seal ?license
        """

        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        bindings = results.get("results", {}).get("bindings", [])

        if not bindings:
            return pd.DataFrame(columns=[
                "journal", "id", "title", "publisher",
                "apc", "seal", "license", "languages"
            ])

        def v(b, var):
            return b.get(var, {}).get("value")

        data = [{
            "journal": v(b, "journal"),
            "id": v(b, "id"),
            "title": v(b, "title"),
            "publisher": v(b, "publisher"),
            "apc": v(b, "apc"),
            "seal": v(b, "seal"),
            "license": v(b, "license"),
            "languages": v(b, "languages"),
        } for b in bindings]

        return pd.DataFrame(data).drop_duplicates(subset=["id"])        # updated 10/02/26

    def getJournalsWithDOAJSeal(self, seal_str: str = "true") -> pd.DataFrame:
        sparql = SPARQLWrapper(self.getDbPathOrUrl())
        seal_norm = (seal_str or "").strip().lower().replace('"', '\\"')

        if seal_norm not in {"true", "false"}:
            return pd.DataFrame(columns=[
                "journal", "id", "title", "publisher",
                "apc", "seal", "license", "languages"
            ])

        query = f"""
        PREFIX : <https://brigata.github.org/>
        SELECT ?journal ?id ?title ?publisher ?apc ?seal ?license
            (GROUP_CONCAT(DISTINCT STR(?lang); separator=", ") AS ?languages)
        WHERE {{
        ?journal :id ?id .
        ?journal :title ?title .

        OPTIONAL {{ ?journal :publisher ?publisher }}
        OPTIONAL {{ ?journal :apc ?apc }}
        OPTIONAL {{ ?journal :license ?license }}
        OPTIONAL {{ ?journal :languages ?lang }}
        OPTIONAL {{ ?journal :seal ?seal }}

        FILTER(BOUND(?seal) && LCASE(STR(?seal)) = "{seal_norm}")
        }}
        GROUP BY ?journal ?id ?title ?publisher ?apc ?seal ?license
        """

        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        bindings = results.get("results", {}).get("bindings", [])

        if not bindings:
            return pd.DataFrame(columns=[
                "journal", "id", "title", "publisher",
                "apc", "seal", "license", "languages"
            ])

        def v(b, var):
            return b.get(var, {}).get("value")

        data = [{
            "journal": v(b, "journal"),
            "id": v(b, "id"),
            "title": v(b, "title"),
            "publisher": v(b, "publisher"),
            "apc": v(b, "apc"),
            "seal": v(b, "seal"),
            "license": v(b, "license"),
            "languages": v(b, "languages"),
        } for b in bindings]

        return pd.DataFrame(data).drop_duplicates(subset=["id"])        # updated 10/02/26


class CategoryQueryHandler(QueryHandler):
    
    def getById(self, entity_id: str) -> pd.DataFrame:
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        eid = (entity_id or "").strip()

        if not eid:
            return pd.DataFrame()

        # 1) identify journal / category / area
        kind_row = pd.read_sql(
            """
            SELECT internalId
            FROM IdentifiableEntity
            WHERE id = :eid COLLATE NOCASE
            AND (
                internalId LIKE 'journal-%'
                OR internalId LIKE 'category-%'
                OR internalId LIKE 'area-%'
            )
            ORDER BY
            CASE
                WHEN internalId LIKE 'journal-%' THEN 1
                WHEN internalId LIKE 'category-%' THEN 2
                WHEN internalId LIKE 'area-%' THEN 3
                ELSE 99
            END
            LIMIT 1
            """,
            engine,
            params={"eid": eid},
        )

        if kind_row.empty:
            return pd.DataFrame()

        internal_id = str(kind_row.loc[0, "internalId"])

        # 2) journal_id (ISSN) -> categories + areas + quartile
        if internal_id.startswith("journal-"):
            df = pd.read_sql(
                """
                SELECT
                j.id        AS journal_id,
                c.id        AS category_id,
                c.quartile  AS category_quartile,
                a.id        AS area_id
                FROM IdentifiableEntity j
                LEFT JOIN HasCategory hc
                ON hc.journalId = j.internalId
                LEFT JOIN IdentifiableEntity c
                ON c.internalId = hc.categoryId
                LEFT JOIN HasArea ha
                ON ha.journalId = j.internalId
                LEFT JOIN IdentifiableEntity a
                ON a.internalId = ha.areaId
                WHERE j.id = :eid COLLATE NOCASE
                AND j.internalId LIKE 'journal-%'
                ORDER BY category_id, area_id
                """,
                engine,
                params={"eid": eid},
            )
            return df if not df.empty else pd.DataFrame(
                columns=["journal_id", "category_id", "category_quartile", "area_id"]
            )

        # 3) category name -> quartile + all journals
        if internal_id.startswith("category-"):
            df = pd.read_sql(
                """
                SELECT
                c.id        AS category_id,
                c.quartile  AS category_quartile,
                j.id        AS journal_id
                FROM IdentifiableEntity c
                LEFT JOIN HasCategory hc
                ON hc.categoryId = c.internalId
                LEFT JOIN IdentifiableEntity j
                ON j.internalId = hc.journalId
                WHERE c.id = :eid COLLATE NOCASE
                AND c.internalId LIKE 'category-%'
                ORDER BY journal_id
                """,
                engine,
                params={"eid": eid},
            )
            return df if not df.empty else pd.DataFrame(
                columns=["category_id", "category_quartile", "journal_id"]
            )

        # 4) area name -> all journals
        if internal_id.startswith("area-"):
            df = pd.read_sql(
                """
                SELECT
                a.id  AS area_id,
                j.id  AS journal_id
                FROM IdentifiableEntity a
                LEFT JOIN HasArea ha
                ON ha.areaId = a.internalId
                LEFT JOIN IdentifiableEntity j
                ON j.internalId = ha.journalId
                WHERE a.id = :eid COLLATE NOCASE
                AND a.internalId LIKE 'area-%'
                ORDER BY journal_id
                """,
                engine,
                params={"eid": eid},
            )
            return df if not df.empty else pd.DataFrame(columns=["area_id", "journal_id"])

        return pd.DataFrame() # updated 09/02/26

    def getAllCategories(self) -> pd.DataFrame:
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        query = """
        SELECT DISTINCT i.id AS category_id
        FROM IdentifiableEntity i
        WHERE i.internalId LIKE 'category-%'
        ORDER BY category_id
        """
        df = pd.read_sql(query, engine)
        if "category_id" not in df.columns and "id" in df.columns:
            df = df.rename(columns={"id": "category_id"})
        return df if not df.empty else pd.DataFrame(columns=["category_id", "quartile"]) # updated 09/02/26

    def getAllAreas(self) -> pd.DataFrame:
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        query = """
        SELECT DISTINCT i.id AS id
        FROM IdentifiableEntity i
        WHERE i.internalId LIKE 'area-%'
        ORDER BY id
        """
        return pd.read_sql(query, engine)

    def getCategoriesWithQuartile(self, quartile: str) -> pd.DataFrame:
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")

        query = """
        SELECT DISTINCT
            id       AS category_id,
            quartile AS quartile
        FROM IdentifiableEntity
        WHERE internalId LIKE 'category-%'
        AND quartile = :quartile
        ORDER BY category_id
        """
        return pd.read_sql(query, engine, params={"quartile": quartile})

    def getCategoriesAssignedToAreas(self, area_id: str) -> pd.DataFrame:
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")

        query = """
        SELECT DISTINCT
            c.id AS category_id
        FROM HasArea ha
        JOIN HasCategory hc
            ON hc.journalId = ha.journalId
        JOIN IdentifiableEntity a
            ON a.internalId = ha.areaId
        JOIN IdentifiableEntity c
            ON c.internalId = hc.categoryId
        WHERE a.id = :area_id
        ORDER BY category_id
        """

        return pd.read_sql(query, engine, params={"area_id": area_id})

    def getAreasAssignedToCategories(self, category_id: str) -> pd.DataFrame:
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")

        query = """
        SELECT DISTINCT
            a.id AS area_id
        FROM HasCategory hc
        JOIN HasArea ha
            ON ha.journalId = hc.journalId
        JOIN IdentifiableEntity c
            ON c.internalId = hc.categoryId
        JOIN IdentifiableEntity a
            ON a.internalId = ha.areaId
        WHERE c.id = :category_id
        ORDER BY area_id
        """

        return pd.read_sql(query, engine, params={"category_id": category_id})

    def getAllCategoryAssignments(self) -> pd.DataFrame:
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        query = """
        SELECT
        c.id AS category,
        c.quartile AS category_quartile,
        GROUP_CONCAT(DISTINCT j.id) AS identifiers
        FROM HasCategory hc
        JOIN IdentifiableEntity c ON c.internalId = hc.categoryId
        JOIN IdentifiableEntity j ON j.internalId = hc.journalId
        GROUP BY c.id, c.quartile
        """
        df = pd.read_sql(query, engine)
        return df if not df.empty else pd.DataFrame(columns=["category","category_quartile","identifiers"])

    def getAllAreaAssignments(self) -> pd.DataFrame:
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        query = """
        SELECT
        a.id AS area,
        GROUP_CONCAT(DISTINCT j.id) AS identifiers
        FROM HasArea ha
        JOIN IdentifiableEntity a ON a.internalId = ha.areaId
        JOIN IdentifiableEntity j ON j.internalId = ha.journalId
        GROUP BY a.id
        """
        df = pd.read_sql(query, engine)
        return df if not df.empty else pd.DataFrame(columns=["area","identifiers"])
