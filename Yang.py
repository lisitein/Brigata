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
    def getById(self, journal_id: str) -> pd.DataFrame:
        print("function getById by Yang started")
        sparql = SPARQLWrapper(self.getDbPathOrUrl())
        query = f"""
        PREFIX : <http://Brigata.github.org/journal/>
        SELECT ?journal ?title ?publisher ?license ?apc
        WHERE {{
            ?journal a :Journal ;
                     :id "{journal_id}" ;
                     :title ?title ;
                     :publisher ?publisher ;
                     :license ?license ;
                     :apc ?apc .
        }}
        """
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        print("These are the results produced by Yang:\n", results)
        data = [{
            "id": journal_id,
            "title": r["title"]["value"],
            "publisher": r["publisher"]["value"],
            "license": r["license"]["value"],
            "apc": r["apc"]["value"]
        } for r in results["results"]["bindings"]]
        
        print ("query done!")

        if not data:
            print("I did not find any data!!!")
            return pd.DataFrame(columns=["id", "title", "publisher", "license", "apc"])
        
        print('\nThis is the dataframe Yang returns:\n', pd.DataFrame(data))
        return pd.DataFrame(data)

    def getAllJournals(self) -> pd.DataFrame:
        sparql = SPARQLWrapper(self.getDbPathOrUrl())
        query = """
        PREFIX : <http://Brigata.github.org/journal/>
        SELECT ?journal ?title ?publisher ?apc ?seal ?license
        WHERE {
            ?journal a :Journal ;
                    :title ?title ;
                    :publisher ?publisher .
            OPTIONAL { ?journal :apc ?apc }
            OPTIONAL { ?journal :seal ?seal }
            OPTIONAL { ?journal :license ?license }
        }
        """
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        data = [{
            "id": r["journal"]["value"].split("/")[-1],
            "title": r["title"]["value"],
            "publisher": r["publisher"]["value"],
            "apc": r.get("apc", {}).get("value", "No"),
            "seal": r.get("seal", {}).get("value", "No"),
            "license": r.get("license", {}).get("value", "")
        } for r in results["results"]["bindings"]]
        
        if not data:
            return pd.DataFrame(columns=["id", "title", "publisher", "apc", "seal", "license"])
        return pd.DataFrame(data)

    def getJournalsWithTitle(self, partial_title: str) -> pd.DataFrame:
        sparql = SPARQLWrapper(self.getDbPathOrUrl())
        query = f"""
        PREFIX : <http://Brigata.github.org/journal/>
        SELECT ?journal ?title
        WHERE {{
            ?journal a :Journal ;
                     :title ?title .
            FILTER CONTAINS(LCASE(?title), "{partial_title.lower()}")
        }}
        """
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        data = [{
            "id": r["journal"]["value"].split("/")[-1],
            "title": r["title"]["value"]
        } for r in results["results"]["bindings"]]
        
        if not data:
            return pd.DataFrame(columns=["id", "title"])
        return pd.DataFrame(data)

    def getJournalsPublishedBy(self, partial_name: str) -> pd.DataFrame:
        sparql = SPARQLWrapper(self.getDbPathOrUrl())
        query = f"""
        PREFIX : <http://Brigata.github.org/journal/>
        SELECT ?journal ?title ?publisher
        WHERE {{
            ?journal a :Journal ;
                     :title ?title ;
                     :publisher ?publisher .
            FILTER CONTAINS(LCASE(?publisher), "{partial_name.lower()}")
        }}
        """
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        data = [{
            "id": r["journal"]["value"].split("/")[-1],
            "title": r["title"]["value"],
            "publisher": r["publisher"]["value"]
        } for r in results["results"]["bindings"]]
        
        if not data:
            return pd.DataFrame(columns=["id", "title", "publisher"])
        return pd.DataFrame(data)

    def getJournalsWithLicense(self, licenses: set[str]) -> pd.DataFrame:
        filter_clause = "FILTER BOUND(?license)" if not licenses else f"FILTER (?license IN ({', '.join(f'\"{l}\"' for l in licenses)}))"
        sparql = SPARQLWrapper(self.getDbPathOrUrl())
        query = f"""
        PREFIX : <http://Brigata.github.org/journal/>
        SELECT ?journal ?title ?license
        WHERE {{
            ?journal a :Journal ;
                     :title ?title ;
                     :license ?license .
            {filter_clause}
        }}
        """
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        data = [{
            "id": r["journal"]["value"].split("/")[-1],
            "title": r["title"]["value"],
            "license": r["license"]["value"]
        } for r in results["results"]["bindings"]]
        
        if not data:
            return pd.DataFrame(columns=["id", "title", "license"])
        return pd.DataFrame(data)

    def getJournalsWithAPC(self, apc: bool=True) -> pd.DataFrame:
        sparql = SPARQLWrapper(self.getDbPathOrUrl())
        if apc:
            query = """
            PREFIX : <http://Brigata.github.org/journal/>
            SELECT DISTINCT ?journal ?title ?publisher ?apc
            WHERE {
                ?journal a :Journal ;
                        :title ?title ;
                        :publisher ?publisher .
                OPTIONAL { ?journal :apc ?apc }
                FILTER (
                BOUND(?apc) &&
                NOT (LCASE(STR(?apc)) IN ("none","no","false","0",""))
                )
            }
            """
        else:
            query = """
            PREFIX : <http://Brigata.github.org/journal/>
            SELECT DISTINCT ?journal ?title ?publisher ?apc
            WHERE {
                ?journal a :Journal ;
                        :title ?title ;
                        :publisher ?publisher .
                OPTIONAL { ?journal :apc ?apc }
                FILTER (
                !BOUND(?apc) ||
                LCASE(STR(?apc)) IN ("none","no","false","0","")
                )
            }
            """
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        data = [{
            "id": r["journal"]["value"].split("/")[-1],
            "title": r["title"]["value"],
            "publisher": r.get("publisher", {}).get("value", ""),
            "apc": apc
        } for r in results["results"]["bindings"]]
        return pd.DataFrame(data) if data else pd.DataFrame(columns=["id","title","publisher","apc"])


    def getJournalsWithDOAJSeal(self, seal: bool=True) -> pd.DataFrame:
        sparql = SPARQLWrapper(self.getDbPathOrUrl())
        if seal:
            query = """
            PREFIX : <http://Brigata.github.org/journal/>
            SELECT DISTINCT ?journal ?title ?publisher ?seal
            WHERE {
                ?journal a :Journal ;
                        :title ?title ;
                        :publisher ?publisher ;
                        :seal ?seal .
                FILTER (
                (?seal = true) || (LCASE(STR(?seal)) = "true")
                )
            }
            """
        else:
            query = """
            PREFIX : <http://Brigata.github.org/journal/>
            SELECT DISTINCT ?journal ?title ?publisher
            WHERE {
                ?journal a :Journal ;
                        :title ?title ;
                        :publisher ?publisher .
                OPTIONAL { ?journal :seal ?seal }
                FILTER (
                !BOUND(?seal) || (?seal = false) || (LCASE(STR(?seal)) = "false")
                )
            }
            """
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        data = [{
            "id": r["journal"]["value"].split("/")[-1],
            "title": r["title"]["value"],
            "publisher": r.get("publisher", {}).get("value", ""),
            "seal": seal
        } for r in results["results"]["bindings"]]
        return pd.DataFrame(data) if data else pd.DataFrame(columns=["id","title","publisher","seal"])

class CategoryQueryHandler(QueryHandler):
    # Yang you should search not just category but also those areas id toooooooo-------
    def getById(self, category_id: str) -> pd.DataFrame:
        print("function getById by Yang started")
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        print("engine created")
        query = """
        SELECT i.id AS id, i.quartile AS quartile
        FROM IdentifiableEntity i
        WHERE i.internalId LIKE 'category-%'
          AND i.id = :category_id
        LIMIT 1
        """
        return pd.read_sql(query, engine, params={"category_id": (category_id or "").strip()})

    def getAllCategories(self) -> pd.DataFrame:
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
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
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        query = """
        SELECT DISTINCT i.id AS id
        FROM IdentifiableEntity i
        WHERE i.internalId LIKE 'area-%'
        ORDER BY id
        """
        return pd.read_sql(query, engine)

    def getCategoriesWithQuartile(self, quartiles: set[str]) -> pd.DataFrame:
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        qs = [(q or "").strip().upper() for q in (quartiles or set()) if (q or "").strip()]
        if not qs:
            return pd.DataFrame(columns=["category_id", "quartile"])
        placeholders = ",".join(f":q{i}" for i in range(len(qs)))
        params = {f"q{i}": qs[i] for i in range(len(qs))}
        query = f"""
        SELECT DISTINCT i.id AS category_id, i.quartile AS quartile
        FROM IdentifiableEntity i
        WHERE i.internalId LIKE 'category-%'
        AND UPPER(i.quartile) IN ({placeholders})
        ORDER BY category_id
        """
        df = pd.read_sql(query, engine, params=params)
        if "category_id" not in df.columns and "id" in df.columns:
            df = df.rename(columns={"id": "category_id"})
        return df if not df.empty else pd.DataFrame(columns=["category_id", "quartile"])

    def getCategoriesAssignedToAreas(self, area_ids: set[str]) -> pd.DataFrame:
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        aids = [ (a or "").strip() for a in (area_ids or set()) if (a or "").strip() ]
        if not aids:
            return pd.DataFrame(columns=["id", "quartile"])
        
        placeholders = ",".join(f":a{i}" for i in range(len(aids)))
        params = { f"a{i}": aids[i] for i in range(len(aids)) }

        query = f"""
        SELECT DISTINCT c.id AS id, c.quartile AS quartile
        FROM HasCategory hc
        JOIN IdentifiableEntity c ON c.internalId = hc.categoryId
        JOIN HasArea ha           ON ha.journalId = hc.journalId
        JOIN IdentifiableEntity a ON a.internalId = ha.areaId
        WHERE a.id IN ({placeholders})
        ORDER BY id
        """
        return pd.read_sql(query, engine, params=params)

    def getAreasAssignedToCategories(self, category_ids: set[str]) -> pd.DataFrame:
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        cids = [ (c or "").strip() for c in (category_ids or set()) if (c or "").strip() ]
        if not cids:
            return pd.DataFrame(columns=["area"])

        placeholders = ",".join(f":c{i}" for i in range(len(cids)))
        params = { f"c{i}": cids[i] for i in range(len(cids)) }

        query = f"""
        SELECT DISTINCT a.id AS area
        FROM HasCategory hc
        JOIN IdentifiableEntity c ON c.internalId = hc.categoryId
        JOIN HasArea ha           ON ha.journalId = hc.journalId
        JOIN IdentifiableEntity a ON a.internalId = ha.areaId
        WHERE c.id IN ({placeholders})
        ORDER BY area
        """
        return pd.read_sql(query, engine, params=params)

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

# li 6.12
    def getAllAssignments(self) -> pd.DataFrame:
        """
        Return a DataFrame where each row is a (journal, category, area) assignment.
        Required columns (for laura.py):
          - id           : external journal identifier (ISSN/EISSN)
          - category_id  : category name
          - quartile     : quartile of that category for that journal
          - area_id      : area name
        """
        db_path = self.getDbPathOrUrl()
        engine = create_engine(f"sqlite:///{db_path}")

        query = """
        SELECT
            j.id AS id,             -- journal external id
            c.id AS category_id,    -- category name
            c.quartile AS quartile, -- category quartile
            a.id AS area_id         -- area name
        FROM HasCategory hc
        JOIN IdentifiableEntity c ON c.internalId = hc.categoryId
        JOIN IdentifiableEntity j ON j.internalId = hc.journalId
        JOIN HasArea ha           ON ha.journalId  = hc.journalId
        JOIN IdentifiableEntity a ON a.internalId  = ha.areaId
        """
        return pd.read_sql(query, engine)