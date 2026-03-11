from abc import ABC, abstractmethod
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
from sqlalchemy import create_engine
import json

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
        SELECT ?journal ?title ?publisher ?license ?apc ?seal
            (GROUP_CONCAT(DISTINCT STR(?allid); separator=", ") AS ?ids)
            (GROUP_CONCAT(DISTINCT STR(?lang); separator=", ") AS ?languages)
        WHERE {{
            ?journal :id ?matchedId .
            FILTER(LCASE(STR(?matchedId)) = LCASE("{jid}"))

            ?journal :id ?allid .

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
            return pd.DataFrame(columns=[
                "journal", "id", "title", "publisher", "apc", "seal", "license", "languages"
            ])

        b = bindings[0]

        def v(var: str):
            return b.get(var, {}).get("value")

        return pd.DataFrame([{
            "id": v("ids"),
            "title": v("title"),
            "publisher": v("publisher"),
            "license": v("license"),
            "apc": v("apc"),
            "languages": v("languages"),
            "seal": v("seal"),
        }])                                    # updated 10/02/26

    def getAllJournals(self) -> pd.DataFrame:

        sparql = SPARQLWrapper(self.getDbPathOrUrl())
        query = """
        PREFIX : <https://brigata.github.org/>
        SELECT ?journal
            (SAMPLE(?allid) AS ?id)
            (GROUP_CONCAT(DISTINCT STR(?allid); separator=", ") AS ?all_ids)
            ?title ?publisher ?apc ?seal ?license
            (GROUP_CONCAT(DISTINCT STR(?lang); separator=", ") AS ?languages)
        WHERE {
        ?journal :id ?allid .
        OPTIONAL { ?journal :title ?title }
        OPTIONAL { ?journal :publisher ?publisher }
        OPTIONAL { ?journal :apc ?apc }
        OPTIONAL { ?journal :seal ?seal }
        OPTIONAL { ?journal :license ?license }
        OPTIONAL { ?journal :languages ?lang }
        }
        GROUP BY ?journal ?title ?publisher ?apc ?seal ?license
        """

        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)

        results = sparql.query().convert()

        print("DEBUG type:", type(results))

        if isinstance(results, bytes):
            text = results.decode("utf-8", errors="replace")
            print("DEBUG raw response first 500 chars:")
            print(text[:500])
            return pd.DataFrame()

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
            "id": v(b, "all_ids"),
            "title": v(b, "title"),
            "publisher": v(b, "publisher"),
            "apc": v(b, "apc"),
            "seal": v(b, "seal"),
            "license": v(b, "license"),
            "languages": v(b, "languages"),
        } for b in bindings]

        return pd.DataFrame(data).drop_duplicates(subset=["id"])

    def getJournalsWithTitle(self, partial_title: str) -> pd.DataFrame:
        sparql = SPARQLWrapper(self.getDbPathOrUrl())
        pt = (partial_title or "").strip().replace('"', '\\"')

        query = f"""
        PREFIX : <https://brigata.github.org/>
        SELECT ?journal
            (SAMPLE(?allid) AS ?id)
            (GROUP_CONCAT(DISTINCT STR(?allid); separator=", ") AS ?all_ids)
            ?title ?publisher ?apc ?seal ?license
            (GROUP_CONCAT(DISTINCT STR(?lang); separator=", ") AS ?languages)
        WHERE {{
            ?journal :id ?allid .
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
            "id": v(b, "all_ids"),
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
        SELECT ?journal
            (SAMPLE(?allid) AS ?id)
            (GROUP_CONCAT(DISTINCT STR(?allid); separator=", ") AS ?all_ids)
            ?title ?publisher ?apc ?seal ?license
            (GROUP_CONCAT(DISTINCT STR(?lang); separator=", ") AS ?languages)
        WHERE {{
        ?journal :id ?allid .
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
            "id": v(b, "all_ids"),
            "title": v(b, "title"),
            "publisher": v(b, "publisher"),
            "apc": v(b, "apc"),
            "seal": v(b, "seal"),
            "license": v(b, "license"),
            "languages": v(b, "languages"),
        } for b in bindings]

        return pd.DataFrame(data).drop_duplicates(subset=["id"])        # updated 10/02/26

    def getJournalsWithLicense(self, licenses: set[str]) -> pd.DataFrame:

        sparql = SPARQLWrapper(self.getDbPathOrUrl())

        lics = [
            (lic or "").strip().replace('"', '\\"')
            for lic in (licenses or set())
            if (lic or "").strip()
        ]

        if not lics:
            return pd.DataFrame(columns=[
                "journal", "id", "all_ids", "title", "publisher",
                "apc", "seal", "license", "languages"
            ])

        filter_conditions = " || ".join(
            [f'LCASE(STR(?license)) = LCASE("{lic}")' for lic in lics]
        )

        query = f"""
        PREFIX : <https://brigata.github.org/>
        SELECT ?journal
            (SAMPLE(?allid) AS ?id)
            (GROUP_CONCAT(DISTINCT STR(?allid); separator=", ") AS ?all_ids)
            ?title ?publisher ?apc ?seal ?license
            (GROUP_CONCAT(DISTINCT STR(?lang); separator=", ") AS ?languages)
        WHERE {{
        ?journal :id ?allid .
        ?journal :title ?title .
        ?journal :license ?license .

        FILTER({filter_conditions})

        OPTIONAL {{ ?journal :publisher ?publisher }}
        OPTIONAL {{ ?journal :apc ?apc }}
        OPTIONAL {{ ?journal :seal ?seal }}
        OPTIONAL {{ ?journal :languages ?lang }}
        }}
        GROUP BY ?journal ?title ?publisher ?apc ?seal ?license
        """

        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()

        if isinstance(results, bytes):
            results = json.loads(results.decode("utf-8"))

        bindings = results.get("results", {}).get("bindings", [])

        if not bindings:
            return pd.DataFrame(columns=[
                "journal", "id", "all_ids", "title", "publisher",
                "apc", "seal", "license", "languages"
            ])

        def v(b, var):
            return b.get(var, {}).get("value")

        data = [{
            "journal": v(b, "journal"),
            "id": v(b, "id"),
            "all_ids": v(b, "all_ids"),
            "title": v(b, "title"),
            "publisher": v(b, "publisher"),
            "apc": v(b, "apc"),
            "seal": v(b, "seal"),
            "license": v(b, "license"),
            "languages": v(b, "languages"),
        } for b in bindings]

        return pd.DataFrame(data).drop_duplicates(subset=["id", "license"]) 

    def getJournalsWithAPC(self) -> pd.DataFrame:
        sparql = SPARQLWrapper(self.getDbPathOrUrl())

        query = """
        PREFIX : <https://brigata.github.org/>
        SELECT ?journal
            (SAMPLE(?allid) AS ?id)
            (GROUP_CONCAT(DISTINCT STR(?allid); separator=", ") AS ?all_ids)
            ?title ?publisher ?apc ?seal ?license
            (GROUP_CONCAT(DISTINCT STR(?lang); separator=", ") AS ?languages)
        WHERE {
        ?journal :id ?allid .
        ?journal :title ?title .

        OPTIONAL { ?journal :publisher ?publisher }
        OPTIONAL { ?journal :license ?license }
        OPTIONAL { ?journal :seal ?seal }
        OPTIONAL { ?journal :languages ?lang }
        OPTIONAL { ?journal :apc ?apc }

        FILTER(BOUND(?apc) && LCASE(STR(?apc)) = "true")
        }
        GROUP BY ?journal ?id ?title ?publisher ?apc ?seal ?license
        """

        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()

        if isinstance(results, bytes):
            results = json.loads(results.decode("utf-8"))

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
            "id": v(b, "all_ids"),
            "title": v(b, "title"),
            "publisher": v(b, "publisher"),
            "apc": v(b, "apc"),
            "seal": v(b, "seal"),
            "license": v(b, "license"),
            "languages": v(b, "languages"),
        } for b in bindings]

        return pd.DataFrame(data).drop_duplicates(subset=["id"])     # updated 10/02/26

    def getJournalsWithDOAJSeal(self) -> pd.DataFrame:
        sparql = SPARQLWrapper(self.getDbPathOrUrl())

        query = """
        PREFIX : <https://brigata.github.org/>
        SELECT ?journal
            (SAMPLE(?allid) AS ?id)
            (GROUP_CONCAT(DISTINCT STR(?allid); separator=", ") AS ?all_ids)
            ?title ?publisher ?apc ?seal ?license
            (GROUP_CONCAT(DISTINCT STR(?lang); separator=", ") AS ?languages)
        WHERE {
        ?journal :id ?allid .
        ?journal :title ?title .

        OPTIONAL { ?journal :publisher ?publisher }
        OPTIONAL { ?journal :apc ?apc }
        OPTIONAL { ?journal :license ?license }
        OPTIONAL { ?journal :languages ?lang }
        OPTIONAL { ?journal :seal ?seal }

        FILTER(BOUND(?seal) && LCASE(STR(?seal)) = "true")
        }
        GROUP BY ?journal ?id ?title ?publisher ?apc ?seal ?license
        """

        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()

        if isinstance(results, bytes):
            results = json.loads(results.decode("utf-8"))

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
            "id": v(b, "all_ids"),
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
            query = """
                SELECT 
                    j.id AS journal_id,
                    GROUP_CONCAT(DISTINCT c.id || ' (' || c.quartile || ')') AS categories_with_quartiles,
                    GROUP_CONCAT(DISTINCT a.id) AS areas
                FROM IdentifiableEntity j
                LEFT JOIN HasCategory hc ON hc.journalId = j.internalId
                LEFT JOIN IdentifiableEntity c ON c.internalId = hc.categoryId
                LEFT JOIN HasArea ha ON ha.journalId = j.internalId
                LEFT JOIN IdentifiableEntity a ON a.internalId = ha.areaId
                WHERE j.id = :eid COLLATE NOCASE
                GROUP BY j.id
            """
            df = pd.read_sql(query, engine, params={"eid": eid})
            return df

        # 3) category name -> quartile + all journals
        if internal_id.startswith("category-"):
            query = """
                SELECT 
                    c.id AS category_id,
                    c.quartile AS category_quartile,
                    j.id AS journal_id,
                    GROUP_CONCAT(DISTINCT a.id) AS areas
                FROM IdentifiableEntity c
                JOIN HasCategory hc ON hc.categoryId = c.internalId
                JOIN IdentifiableEntity j ON j.internalId = hc.journalId
                LEFT JOIN HasArea ha ON ha.journalId = j.internalId
                LEFT JOIN IdentifiableEntity a ON a.internalId = ha.areaId
                WHERE c.id = :eid COLLATE NOCASE
                GROUP BY c.id, j.id
                ORDER BY j.id
            """
            df = pd.read_sql(query, engine, params={"eid": eid})
            return df

        # 4) area name -> all journals
        if internal_id.startswith("area-"):
            query = """
                SELECT 
                    a.id AS area_id,
                    j.id AS journal_id,
                    GROUP_CONCAT(DISTINCT c.id) AS specific_categories
                FROM IdentifiableEntity a
                JOIN HasArea ha ON ha.areaId = a.internalId
                JOIN IdentifiableEntity j ON j.internalId = ha.journalId
                LEFT JOIN HasCategory hc ON hc.journalId = j.internalId
                LEFT JOIN IdentifiableEntity c ON c.internalId = hc.categoryId
                WHERE a.id = :eid COLLATE NOCASE
                GROUP BY j.id
                ORDER BY j.id
            """
            df = pd.read_sql(query, engine, params={"eid": eid})
            return df

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

    def getCategoriesWithQuartile(self, quartiles: set[str]) -> pd.DataFrame:
        if not quartiles:
            return pd.DataFrame(columns=["category_id", "quartile"])

        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        keys = [f"q{i}" for i in range(len(quartiles))]
        placeholders = ", ".join([f":{k}" for k in keys])
        params_dict = dict(zip(keys, list(quartiles)))

        query = f"""
            SELECT DISTINCT
                id       AS category_id,
                quartile AS quartile
            FROM IdentifiableEntity
            WHERE internalId LIKE 'category-%'
            AND quartile IN ({placeholders})
            ORDER BY category_id
        """
        return pd.read_sql(query, engine, params=params_dict)

    def getCategoriesAssignedToAreas(self, area_ids: set[str]) -> pd.DataFrame:
        if not area_ids:
            return pd.DataFrame(columns=["category_id"])

        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")

        keys = [f"a{i}" for i in range(len(area_ids))]
        placeholders = ", ".join([f":{k}" for k in keys])
        
        params_dict = dict(zip(keys, list(area_ids)))

        query = f"""
        SELECT DISTINCT
            c.id AS category_id
        FROM HasArea ha
        JOIN HasCategory hc
            ON hc.journalId = ha.journalId
        JOIN IdentifiableEntity a
            ON a.internalId = ha.areaId
        JOIN IdentifiableEntity c
            ON c.internalId = hc.categoryId
        WHERE a.id IN ({placeholders})
        ORDER BY category_id
        """

        return pd.read_sql(query, engine, params=params_dict)

    def getAreasAssignedToCategories(self, category_ids: set[str]) -> pd.DataFrame:
        if not category_ids:
            return pd.DataFrame(columns=["area_id"])

        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        keys = [f"c{i}" for i in range(len(category_ids))]
        placeholders = ", ".join([f":{k}" for k in keys])
        
        params_dict = dict(zip(keys, list(category_ids)))

        query = f"""
        SELECT DISTINCT
            a.id AS area_id
        FROM HasCategory hc
        JOIN HasArea ha
            ON ha.journalId = hc.journalId
        JOIN IdentifiableEntity c
            ON c.internalId = hc.categoryId
        JOIN IdentifiableEntity a
            ON a.internalId = ha.areaId
        WHERE c.id IN ({placeholders})
        ORDER BY area_id
        """

        return pd.read_sql(query, engine, params=params_dict)

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
