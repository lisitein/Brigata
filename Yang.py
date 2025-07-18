"""
Yang.py

Module for handling queries against DOAJ (via SPARQL) and Scimago (via SQLite).

Classes:
    QueryHandler: Abstract base class defining interface for DB URL management and ID-based querying.
    JournalQueryHandler: SPARQL implementation for retrieving journal metadata.
    CategoryQueryHandler: SQLite implementation for retrieving category and area data.

Dependencies:
    pandas, SPARQLWrapper, sqlalchemy
"""

from abc import ABC, abstractmethod
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
from sqlalchemy import create_engine

class QueryHandler(ABC):
    
    """
    Abstract base class defining interface for database path/URL management
    and querying entities by their external identifiers.

    Attributes:
        dbPathOrUrl (str): The database file path.
    """
    
    def __init__(self):
        
        """
        Initialize the query handler with an empty database path or URL.
        """
        
        self.dbPathOrUrl = ''

    def getDbPathOrUrl(self) -> str:
        
        """
        Retrieve the currently configured database path.

        Returns:
            str: The database path.
        """
        
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, url: str):
        
        """
        Validate and set a new database path.

        Args:
            url (str): The new database path.

        Returns:
            bool: True if successfully set, False otherwise.
        """
        
        if not isinstance(url, str):
            raise ValueError("The path/URL of the database must be a string")
        self.dbPathOrUrl = url
        return True

    @abstractmethod
    def getById(self, entity_id: str) -> pd.DataFrame:
        
        """
        Abstract method to query the database for an entity by its external ID.

        Args:
            entity_id (str): The identifier for the target entity.

        Returns:
            pd.DataFrame: Query results as a pandas DataFrame.
        """
        
        pass

class JournalQueryHandler(QueryHandler):

    def __init__(self):
        
        """
        Initialize the SPARQL query handler by invoking the base constructor.
        """
        
        super().__init__()
    
    """
    SPARQL-based implementation of QueryHandler for retrieving journal data
    from a Blazegraph SPARQL endpoint.
    """
    
    def getById(self, journal_id: str) -> pd.DataFrame:

        """
        Retrieve journal metadata by its external identifier (e.g., ISSN).

        Args:
            journal_id (str): The external journal identifier.

        Returns:
            pd.DataFrame: DataFrame with columns ['id', 'title', 'publisher',
                                                  'license', 'apc'].
                          Returns an empty DataFrame if not found.
        """
        
        sparql = SPARQLWrapper(self.getDbPathOrUrl())
        query = f"""
        PREFIX : <http://Brigata.github.org/journal/>
        SELECT ?journal ?title ?publisher ?licence ?apc
        WHERE {{
            ?journal a :Journal ;
                     :id "{journal_id}" ;
                     :title ?title ;
                     :publisher ?publisher ;
                     :licence ?licence ;
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
            "license": r["licence"]["value"],
            "apc": r["apc"]["value"]
        } for r in results["results"]["bindings"]]
        
        if not data:
            return pd.DataFrame(columns=["id", "title", "publisher", "license", "apc"])
        return pd.DataFrame(data)

    def getAllJournals(self) -> pd.DataFrame:

        """
        Retrieve all Journal entities available in the SPARQL endpoint.

        Returns:
            pd.DataFrame: List of journals with basic attributes.
        """
    
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
            "publisher": r["publisher"]["value"]
            "apc": r.get("apc", {}).get("value", "No"),   
            "seal": r.get("seal", {}).get("value", "No"),
            "license": r.get("license", {}).get("value", "")
        } for r in results["results"]["bindings"]]
        
        if not data:
            return pd.DataFrame(columns=["id", "title", "publisher", "apc", "seal", "license"])
        return pd.DataFrame(data)

    def getJournalsWithTitle(self, partial_title: str) -> pd.DataFrame:

        """
        Search for journals whose titles contain the given substring.

        Args:
            partial_title (str): Substring to match within journal titles.

        Returns:
            pd.DataFrame: Matching journal IDs and titles.
        """
        
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

        """
        Find journals by publisher name matching the given substring.

        Args:
            partial_name (str): Substring of publisher name.

        Returns:
            pd.DataFrame: Publisher-matched journals.
        """
    
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

        """
        Fetch journals whose license property matches one of the given set.

        Args:
            licenses (set[str]): A set of license strings to filter.

        Returns:
            pd.DataFrame: IDs and licenses of matching journals.
        """
        
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

        """
        Retrieve journals that charge an Article Processing Charge (APC).

        Returns:
            pd.DataFrame: Journal IDs and APC values.
        """
        
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

        """
        List journals awarded the DOAJ Seal.

        Returns:
            pd.DataFrame: Journal IDs that have the DOAJ Seal flag.
        """
        
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

    """
    SQLite-based implementation for retrieving category, area, and assignment
    data from a Scimago database.
    """

    def __init__(self):
        """
        Initialize the SQLite category handler.
        """
        super().__init__()
    
    def getById(self, category_id: str) -> pd.DataFrame:

        """
        Retrieve a Category record by its external ID.

        Args:
            category_id (str): The external category identifier.

        Returns:
            pd.DataFrame: Matching category row.
        """
        
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        query = f"""
        SELECT c.* FROM Category c
        JOIN IdentifiableEntityId i ON c.internalId = i.internalId
        WHERE i.id = '{category_id}'
        """
        return pd.read_sql(query, engine)

    def getAllCategories(self) -> pd.DataFrame:
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        return pd.read_sql("SELECT DISTINCT * FROM Category", engine)

    def getAllAreas(self) -> pd.DataFrame:
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        return pd.read_sql("SELECT DISTINCT * FROM Area", engine)

    def getCategoriesWithQuartile(self, quartiles: set[str]) -> pd.DataFrame:

        """
        Filter Category records by quartile values.

        Args:
            quartiles (set[str]): Set of quartile strings (e.g., {'Q1','Q2'}).

        Returns:
            pd.DataFrame: Categories matching the specified quartiles.
        """
        
        if not quartiles:
            quartiles = {"Q1", "Q2", "Q3", "Q4"}
        quartiles_str = ", ".join(f"'{q}'" for q in quartiles)
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        return pd.read_sql(f"SELECT * FROM Category WHERE quartile IN ({quartiles_str})", engine)

    def getCategoriesAssignedToAreas(self, area_ids: set[str]) -> pd.DataFrame:

        """
        Retrieve category IDs and quartiles for journals assigned to given areas.

        Args:
            area_ids (set[str]): Set of external area IDs; empty for all areas.

        Returns:
            pd.DataFrame: Columns ['id','quartile'] for matching categories.
        """
        
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        if not area_ids:
            query = """
            SELECT DISTINCT i.id, c.quartile
            FROM Category c
            JOIN IdentifiableEntityId i ON c.internalId = i.internalId
            JOIN HasCategory hc ON i.id = hc.hasCategoryId
            JOIN HasArea ha ON hc.hasCategoryId = ha.hasAreaId
            """
        else:
            area_ids_str = ", ".join(f"'{a}'" for a in area_ids)
            query = f"""
            SELECT DISTINCT i.id, c.quartile
            FROM Category c
            JOIN IdentifiableEntityId i ON c.internalId = i.internalId
            JOIN HasCategory hc ON i.id = hc.hasCategoryId
            JOIN HasArea ha ON hc.hasCategoryId = ha.hasAreaId
            JOIN Area a ON ha.areaId = a.internalId
            JOIN IdentifiableEntityId i2 ON a.internalId = i2.internalId
            WHERE i2.id IN ({area_ids_str})
            """
        return pd.read_sql(query, engine)

    def getAreasAssignedToCategories(self, category_ids: set[str]) -> pd.DataFrame:

        """
        Retrieve area IDs for journals assigned to given categories.

        Args:
            category_ids (set[str]): Set of category IDs; empty for all categories.

        Returns:
            pd.DataFrame: Column ['area'] listing area IDs.
        """
        
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        if not category_ids:
            query = """
            SELECT DISTINCT i.id AS area
            FROM Area a
            JOIN IdentifiableEntityId i ON a.internalId = i.internalId
            JOIN HasArea ha ON i.id = ha.areaId
            """
        else:
            category_ids_str = ", ".join(f"'{cat}'" for cat in category_ids)
            query = f"""
            SELECT DISTINCT i.id AS area
            FROM Area a
            JOIN IdentifiableEntityId i ON a.internalId = i.internalId
            JOIN HasArea ha ON i.id = ha.areaId
            WHERE ha.hasAreaId IN (
                SELECT hc.hasCategoryId
                FROM HasCategory hc
                WHERE hc.categoryId IN (
                    SELECT c.internalId FROM Category c
                    JOIN IdentifiableEntityId i2 ON c.internalId = i2.internalId
                    WHERE i2.id IN ({category_ids_str})
                )
            )
            """
        return pd.read_sql(query, engine)

    # NEW METHODS NEEDED BY FULLQUERYENGINE
    def getAllCategoryAssignments(self) -> pd.DataFrame:

        """
        List all journal-category-quartile assignments across database.

        Returns:
            pd.DataFrame: Columns ['identifiers','category_id','category_quartile','area'].
        """
        
        """Get all journal-category assignments with identifiers"""
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        query = """
        SELECT DISTINCT 
            i_journal.id as identifiers,
            i_category.id as category_id,
            c.quartile as category_quartile
        FROM HasCategory hc
        JOIN Category c ON hc.categoryId = c.categoryWithQuartileId
        JOIN IdentifiableEntityId i_journal ON hc.hasCategoryId = i_journal.internalId
        JOIN IdentifiableEntityId i_category ON c.internalId = i_category.internalId
        """
        return pd.read_sql(query, engine)

    def getAllAreaAssignments(self) -> pd.DataFrame:

        """
        List all journal-area assignments.

        Returns:
            pd.DataFrame: Columns ['identifiers','area'].
        """
        
        """Get all journal-area assignments with identifiers"""
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        query = """
        SELECT DISTINCT 
            i_journal.id as identifiers,
            i_area.id as area
        FROM HasArea ha
        JOIN Area a ON ha.areaId = a.internalId
        JOIN IdentifiableEntityId i_journal ON ha.hasAreaId = i_journal.internalId
        JOIN IdentifiableEntityId i_area ON a.internalId = i_area.internalId
        """
        return pd.read_sql(query, engine)

    def getAllAssignments(self) -> pd.DataFrame:

        """
        Retrieve all journal-category and journal-area assignments combined.

        Returns:
            pd.DataFrame: Unified assignment table.
        """
        
        """Get all journal assignments (both categories and areas)"""
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        query = """
        SELECT DISTINCT 
            i_journal.id as identifiers,
            i_category.id as category_id,
            c.quartile as category_quartile,
            i_area.id as area
        FROM HasCategory hc
        JOIN Category c ON hc.categoryId = c.categoryWithQuartileId
        JOIN IdentifiableEntityId i_journal ON hc.hasCategoryId = i_journal.internalId
        JOIN IdentifiableEntityId i_category ON c.internalId = i_category.internalId
        JOIN HasArea ha ON ha.hasAreaId = i_journal.internalId
        JOIN Area a ON ha.areaId = a.internalId
        JOIN IdentifiableEntityId i_area ON a.internalId = i_area.internalId
        """
        return pd.read_sql(query, engine)
