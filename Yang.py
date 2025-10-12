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
        data = [{
            "id": journal_id,
            "title": r["title"]["value"],
            "publisher": r["publisher"]["value"],
            "license": r["license"]["value"],
            "apc": r["apc"]["value"]
        } for r in results["results"]["bindings"]]
        
        if not data:
            return pd.DataFrame(columns=["id", "title", "publisher", "license", "apc"])
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

    def getJournalsWithAPC(self) -> pd.DataFrame:
        sparql = SPARQLWrapper(self.getDbPathOrUrl())
        query = """
        PREFIX : <http://Brigata.github.org/journal/>
        SELECT ?journal ?title ?apc
        WHERE {
            ?journal a :Journal ;
                     :title ?title ;
                     :apc ?apc .
            FILTER (?apc != "None")
        }
        """
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        data = [{
            "id": r["journal"]["value"].split("/")[-1],
            "title": r["title"]["value"],
            "apc": r["apc"]["value"]
        } for r in results["results"]["bindings"]]
        
        if not data:
            return pd.DataFrame(columns=["id", "title", "apc"])
        return pd.DataFrame(data)

    def getJournalsWithDOAJSeal(self) -> pd.DataFrame:
        sparql = SPARQLWrapper(self.getDbPathOrUrl())
        query = """
        PREFIX : <http://Brigata.github.org/journal/>
        SELECT ?journal ?title ?seal
        WHERE {
            ?journal a :Journal ;
                     :title ?title ;
                     :seal ?seal .
            FILTER (?seal = true)
        }
        """
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        data = [{
            "id": r["journal"]["value"].split("/")[-1],
            "title": r["title"]["value"],
            "seal": r["seal"]["value"]
        } for r in results["results"]["bindings"]]
        
        if not data:
            return pd.DataFrame(columns=["id", "title", "seal"])
        return pd.DataFrame(data)

class CategoryQueryHandler(QueryHandler):
    def getById(self, category_id: str) -> pd.DataFrame:
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        query = """
        SELECT i.id AS category_id, i.quartile
        FROM IdentifiableEntity i
        WHERE i.internalId LIKE 'category-%'
          AND i.id = :category_id
        LIMIT 1
        """
        return pd.read_sql(query, engine, params={"category_id": (category_id or "").strip()})

    def getAllCategories(self) -> pd.DataFrame:
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        query = """
        SELECT DISTINCT i.id AS category_id, i.quartile
        FROM IdentifiableEntity i
        WHERE i.internalId LIKE 'category-%'
        ORDER BY category_id
        """
        return pd.read_sql(query, engine)

    def getAllAreas(self) -> pd.DataFrame:
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        query = """
        SELECT DISTINCT i.id AS area_id
        FROM IdentifiableEntity i
        WHERE i.internalId LIKE 'area-%'
        ORDER BY area_id
        """
        return pd.read_sql(query, engine)

    def getCategoriesWithQuartile(self, quartiles: set[str]) -> pd.DataFrame:
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        qs = [ (q or "").strip().upper() for q in (quartiles or set()) if (q or "").strip() ]
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        if not qs:
            return pd.DataFrame(columns=["category_id"])

        placeholders = ",".join(f":q{i}" for i in range(len(qs)))
        params = { f"q{i}": qs[i] for i in range(len(qs)) }

        query = f"""
        SELECT DISTINCT i.id AS category_id
        FROM IdentifiableEntity i
        WHERE i.internalId LIKE 'category-%'
          AND i.quartile IN ({placeholders})
        ORDER BY category_id
        """
        return pd.read_sql(query, engine, params=params)

    def getCategoriesAssignedToAreas(self, area_ids: set[str]) -> pd.DataFrame:
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        aids = [ (a or "").strip() for a in (area_ids or set()) if (a or "").strip() ]
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        if not aids:
            return pd.DataFrame(columns=["category_id","quartile"])

        placeholders = ",".join(f":a{i}" for i in range(len(aids)))
        params = { f"a{i}": aids[i] for i in range(len(aids)) }

        query = f"""
        SELECT DISTINCT c.id AS category_id, c.quartile
        FROM HasCategory hc
        JOIN IdentifiableEntity c ON c.internalId = hc.categoryId
        JOIN HasArea ha          ON ha.journalId  = hc.journalId
        JOIN IdentifiableEntity a ON a.internalId = ha.areaId
        WHERE a.id IN ({placeholders})
        ORDER BY category_id
        """
        return pd.read_sql(query, engine, params=params)

    def getAreasAssignedToCategories(self, category_ids: set[str]) -> pd.DataFrame:
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        cids = [ (c or "").strip() for c in (category_ids or set()) if (c or "").strip() ]
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        if not cids:
            return pd.DataFrame(columns=["area_id"])

        placeholders = ",".join(f":c{i}" for i in range(len(cids)))
        params = { f"c{i}": cids[i] for i in range(len(cids)) }

        query = f"""
        SELECT DISTINCT a.id AS area_id
        FROM HasCategory hc
        JOIN IdentifiableEntity c ON c.internalId = hc.categoryId
        JOIN HasArea ha          ON ha.journalId  = hc.journalId
        JOIN IdentifiableEntity a ON a.internalId = ha.areaId
        WHERE c.id IN ({placeholders})
        ORDER BY area_id
        """
        return pd.read_sql(query, engine, params=params)
