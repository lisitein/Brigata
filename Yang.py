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
        GROUP BY ?journal ?title ?publisher ?apc ?seal ?license
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
        GROUP BY ?journal ?title ?publisher ?apc ?seal ?license
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
    def __init__(self):
        super().__init__()
        self._engine = None
    
    def _get_engine(self):
        if self._engine is None:
            self._engine = create_engine(
                f"sqlite:///{self.getDbPathOrUrl()}",
                connect_args={"check_same_thread": False}
            )
        return self._engine
    
    def setDbPathOrUrl(self, url: str):
        super().setDbPathOrUrl(url)
        self._engine = None  
        return True

    def getById(self, entity_id: str) -> pd.DataFrame:
        engine = self._get_engine()
        eid = (entity_id or "").strip()

        if not eid:
            return pd.DataFrame()

        # recognize journal / category / area
        is_journal = pd.read_sql(
            """
            SELECT 1
            FROM IdentifiableEntity
            WHERE id = :eid COLLATE NOCASE
            AND internalId LIKE 'journal-%'
            LIMIT 1
            """,
            engine,
            params={"eid": eid},
        )

        if not is_journal.empty:
            query = """
                WITH matched_journal AS (
                    SELECT internalId
                    FROM IdentifiableEntity
                    WHERE id = :eid COLLATE NOCASE
                    AND internalId LIKE 'journal-%'
                    LIMIT 1
                )
                SELECT
                    GROUP_CONCAT(DISTINCT j_all.id) AS journal_id,
                    c.id AS category_id,
                    c.quartile AS category_quartile,
                    a.id AS area_id
                FROM matched_journal mj
                JOIN IdentifiableEntity j_all
                    ON j_all.internalId = mj.internalId
                LEFT JOIN HasCategory hc
                    ON hc.journalId = mj.internalId
                LEFT JOIN IdentifiableEntity c
                    ON c.internalId = hc.categoryId
                LEFT JOIN HasArea ha
                    ON ha.journalId = mj.internalId
                LEFT JOIN IdentifiableEntity a
                    ON a.internalId = ha.areaId
                GROUP BY c.id, c.quartile, a.id
                ORDER BY journal_id, category_id, category_quartile, area_id
            """
            df = pd.read_sql(query, engine, params={"eid": eid})
            return df if not df.empty else pd.DataFrame(
                columns=["journal_id", "category_id", "category_quartile", "area_id"]
            )

        is_category = pd.read_sql(
            """
            SELECT 1
            FROM IdentifiableEntity
            WHERE id = :eid COLLATE NOCASE
            AND internalId LIKE 'category-%'
            LIMIT 1
            """,
            engine,
            params={"eid": eid},
        )

        if not is_category.empty:
            query = """
                SELECT DISTINCT
                    c.id AS category_id,
                    c.quartile AS category_quartile,
                    j.id AS journal_id
                FROM IdentifiableEntity c
                JOIN HasCategory hc
                    ON hc.categoryId = c.internalId
                JOIN IdentifiableEntity j
                    ON j.internalId = hc.journalId
                WHERE c.id = :eid COLLATE NOCASE
                ORDER BY category_id, category_quartile, journal_id
            """
            df = pd.read_sql(query, engine, params={"eid": eid})
            return df if not df.empty else pd.DataFrame(
                columns=["category_id", "category_quartile", "journal_id"]
            )

        is_area = pd.read_sql(
            """
            SELECT 1
            FROM IdentifiableEntity
            WHERE id = :eid COLLATE NOCASE
            AND internalId LIKE 'area-%'
            LIMIT 1
            """,
            engine,
            params={"eid": eid},
        )

        if not is_area.empty:
            query = """
                SELECT DISTINCT
                    a.id AS area_id,
                    j.id AS journal_id
                FROM IdentifiableEntity a
                JOIN HasArea ha
                    ON ha.areaId = a.internalId
                JOIN IdentifiableEntity j
                    ON j.internalId = ha.journalId
                WHERE a.id = :eid COLLATE NOCASE
                ORDER BY area_id, journal_id
            """
            df = pd.read_sql(query, engine, params={"eid": eid})
            return df if not df.empty else pd.DataFrame(
                columns=["area_id", "journal_id"]
            )

        return pd.DataFrame()


    def getAllCategories(self) -> pd.DataFrame:
        engine = self._get_engine()
        query = """
        SELECT DISTINCT i.id AS category_id, i.quartile AS quartile
        FROM IdentifiableEntity i
        WHERE i.internalId LIKE 'category-%'
        ORDER BY category_id
        """
        df = pd.read_sql(query, engine)
        if "category_id" not in df.columns and "id" in df.columns:
            df = df.rename(columns={"id": "category_id"})
        return df if not df.empty else pd.DataFrame(columns=["category_id", "quartile"])

    def getAllAreas(self) -> pd.DataFrame:
        engine = self._get_engine()
        query = """
        SELECT DISTINCT i.id AS id
        FROM IdentifiableEntity i
        WHERE i.internalId LIKE 'area-%'
        ORDER BY id
        """
        return pd.read_sql(query, engine)

    def getCategoriesWithQuartile(self, quartiles: set[str]) -> pd.DataFrame:
        engine = self._get_engine()

        # 1.  Normalization
        normalized_quartiles = {
            str(q).strip().upper()
            for q in (quartiles or set())
            if q is not None and str(q).strip()
        }

        # 2. IF IT IS EMPTY  = “ALL quartiles”
        if not normalized_quartiles:
            query = """
                SELECT DISTINCT
                    id AS category_id,
                    quartile AS quartile
                FROM IdentifiableEntity
                WHERE internalId LIKE 'category-%'
                ORDER BY category_id, quartile
            """
            return pd.read_sql(query, engine)

        # 3. OR DEPENDS ON SPECIFIC quartiles 
        keys = [f"q{i}" for i in range(len(normalized_quartiles))]
        placeholders = ", ".join([f":{k}" for k in keys])
        params_dict = dict(zip(keys, list(normalized_quartiles)))

        query = f"""
            SELECT DISTINCT
                id AS category_id,
                quartile AS quartile
            FROM IdentifiableEntity
            WHERE internalId LIKE 'category-%'
            AND UPPER(TRIM(quartile)) IN ({placeholders})
            ORDER BY category_id, quartile
        """
        return pd.read_sql(query, engine, params=params_dict)

    def getCategoriesAssignedToAreas(self, area_ids: set[str]) -> pd.DataFrame:
        engine = self._get_engine()

        # NORMALIZATION
        normalized_areas = {
            str(a).strip()
            for a in (area_ids or set())
            if a is not None and str(a).strip()
        }

        # IF IT IS EMPTY THEN “ALL areas”
        if not normalized_areas:
            query = """
                SELECT DISTINCT
                    c.id AS category_id,
                    c.quartile AS quartile
                FROM HasArea ha
                JOIN HasCategory hc
                    ON hc.journalId = ha.journalId
                JOIN IdentifiableEntity c
                    ON c.internalId = hc.categoryId
                JOIN IdentifiableEntity a
                    ON a.internalId = ha.areaId
                WHERE c.internalId LIKE 'category-%'
                AND a.internalId LIKE 'area-%'
                ORDER BY category_id, quartile
            """
            return pd.read_sql(query, engine)

        # OR DEPENDS ON SPECIFIC area
        keys = [f"a{i}" for i in range(len(normalized_areas))]
        placeholders = ", ".join([f":{k}" for k in keys])
        params_dict = dict(zip(keys, list(normalized_areas)))

        query = f"""
            SELECT DISTINCT
                c.id AS category_id,
                c.quartile AS quartile
            FROM HasArea ha
            JOIN IdentifiableEntity a
                ON a.internalId = ha.areaId
            JOIN HasCategory hc
                ON hc.journalId = ha.journalId
            JOIN IdentifiableEntity c
                ON c.internalId = hc.categoryId
            WHERE a.id IN ({placeholders})
            AND a.internalId LIKE 'area-%'
            AND c.internalId LIKE 'category-%'
            ORDER BY category_id, quartile
        """
        return pd.read_sql(query, engine, params=params_dict)

    def getAreasAssignedToCategories(self, category_ids: set[str]) -> pd.DataFrame:
        engine = self._get_engine()

        normalized_categories = {
            str(c).strip()
            for c in (category_ids or set())
            if c is not None and str(c).strip()
        }

        if not normalized_categories:
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
                WHERE c.internalId LIKE 'category-%'
                AND a.internalId LIKE 'area-%'
                ORDER BY area_id
            """
            return pd.read_sql(query, engine)

        keys = [f"c{i}" for i in range(len(normalized_categories))]
        placeholders = ", ".join([f":{k}" for k in keys])
        params_dict = dict(zip(keys, list(normalized_categories)))

        query = f"""
            SELECT DISTINCT
                a.id AS area_id
            FROM HasCategory hc
            JOIN IdentifiableEntity c
                ON c.internalId = hc.categoryId
            JOIN HasArea ha
                ON ha.journalId = hc.journalId
            JOIN IdentifiableEntity a
                ON a.internalId = ha.areaId
            WHERE c.id IN ({placeholders})
            AND c.internalId LIKE 'category-%'
            AND a.internalId LIKE 'area-%'
            ORDER BY area_id
        """
        return pd.read_sql(query, engine, params=params_dict)

    # ---------------------------------------------------------------
    # Laura's methods for Peroni
    # ---------------------------------------------------------------

    def getCategoryWithName(self, name: str) -> pd.DataFrame:
        engine = self._get_engine()
        name_clean = (name or "").strip()
        if not name_clean:
            return pd.DataFrame(columns=["category_id", "quartile"])
        # LIKE with % on both sides = partial, case-insensitive match
        query = """
            SELECT DISTINCT
                id       AS category_id,
                quartile AS quartile
            FROM IdentifiableEntity
            WHERE internalId LIKE 'category-%'
            AND LOWER(id) LIKE LOWER(:pattern)
            ORDER BY category_id, quartile
        """
        return pd.read_sql(query, engine, params={"pattern": f"%{name_clean}%"})

    def getAreaWithName(self, name: str) -> pd.DataFrame:
        engine = self._get_engine()
        name_clean = (name or "").strip()
        if not name_clean:
            return pd.DataFrame(columns=["id"])
        # same partial match logic as getCategoryWithName
        query = """
            SELECT DISTINCT
                id AS id
            FROM IdentifiableEntity
            WHERE internalId LIKE 'area-%'
            AND LOWER(id) LIKE LOWER(:pattern)
            ORDER BY id
        """
        return pd.read_sql(query, engine, params={"pattern": f"%{name_clean}%"})

    # ---------------------------------------------------------------
    # end Laura's methods for Peroni
    # ---------------------------------------------------------------

    def getAllCategoryAssignments(self) -> pd.DataFrame:
        engine = self._get_engine()
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
        engine = self._get_engine()
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

