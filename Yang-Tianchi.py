from abc import ABC, abstractmethod
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
from sqlalchemy import create_engine

class QueryHandler(ABC):
    def __init__(self):
        self.dpPathOrUrl = ''
    
    def getDbPathOrUrl(self) -> str:
        return self._dbPathOrUrl          #return the path or URL of the current database 
    
    def setDbPathOrUrl(self, path_or_url: str):
        if not isinstance(path_or_url, str):           #set path or URL of the database
            raise ValueError("The datatype of path/URL of the database must be string")
        self._dbPathOrUrl = path_or_url
    
    @abstractmethod                                    #Use the @abstractmethod decorator to mark getById as an abstract method, forcing subclasses to implement it.
    def getById(self, entity_id: str) -> pd.DataFrame:
        #query the instance according to the ID
        pass

# All the query of sparql refers to the code given by LLM

class JournalQueryHandler(QueryHandler):
    def getById(self, journal_id: str) -> pd.DataFrame:   #If the subclass does not implement getById, Python will report an error during instantiation.
    #Query detailed information by journal ID
        if not self.getDbPathOrUrl():
            raise ValueError("The graph database endpoint is not set, please call setDbPathOrUrl() first")
    
        sparql = SPARQLWrapper(self.getDbPathOrUrl())     #Constructing SPARQL queries from the path or URL of the current database
        query = f"""                                      
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
        sparql.setQuery(query)              #Set the query statement
        sparql.setReturnFormat(JSON)        #Set the return format

        try:
            results = sparql.query().convert()    #Convert the SPARQL query results to a Python dictionary in JSON format
        except Exception as e:
            raise ConnectionError(f"SPARQL query fails: {str(e)}") #If an error occurs, a prompt will pop up
        
        data = []                                        #Initialize the data list
        for result in results["results"]["bindings"]:    #results["results"]["bindings"] is a list containing all query results
            data.append({
                "id": journal_id,
                "title": result["title"]["value"],
                "publisher": result["publisher"]["value"],
                "license": result["license"]["value"],
                "apc": result["apc"]["value"]
                })
        return pd.DataFrame(data)                         # return data frame
    
    def getAllJournals(self) -> pd.DataFrame:
    #Get all journals
        if not self.getDbPathOrUrl():
            raise ValueError("Graph database endpoint not set")   
        
        sparql = SPARQLWrapper(self.getDbPathOrUrl())       #Constructing SPARQL queries from the path or URL of the current database
        query = """
        SELECT ?journal ?title ?publisher
        WHERE {
            ?journal a :Journal ;
                     :title ?title ;
                     :publisher ?publisher .
        }
        """

        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)

        try:
            results = sparql.query().convert()      #The query results are returned in JSON format and converted to a Python dictionary using `sparql.query().convert()
        except Exception as e:
            raise ConnectionError(f"SPARQL query fails: {str(e)}")

        data = []
        for result in results["results"]["bindings"]: #results["results"]["bindings"] is a list containing all query results
            data.append({
                "id": result["journal"]["value"].split("/")[-1],  # result["journal"]["value"] is the journal's URI (e.g. "http://example.org/journal/123")
                "title": result["title"]["value"],                # Directly get result["title"]["value"]
                "publisher": result["publisher"]["value"]         # Directly get result["publisher"]["value"] 
            })
        return pd.DataFrame(data)
    
    def getJournalsWithTitle(self, keyword: str) -> pd.DataFrame:
        """Fuzzy matching of keywords in the title"""
        if not self.getDbPathOrUrl():
            raise ValueError("Graph database endpoint not set")
        
        sparql = SPARQLWrapper(self.getDbPathOrUrl())             #constructing queries
        query = f"""                                              
        SELECT ?journal ?title
        WHERE {{
            ?journal a :Journal ;
                     :title ?title .
            FILTER CONTAINS(LCASE(?title), "{keyword.lower()}")
        }}
        """

        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)

        try:
            results = sparql.query().convert()
        except Exception as e:
            raise ConnectionError(f"SPARQL query fails: {str(e)}")

        data = []
        for result in results["results"]["bindings"]:
            data.append({
                "id": result["journal"]["value"].split("/")[-1],
                "title": result["title"]["value"]
            })
        return pd.DataFrame(data)
    
    def getJournalsPublishedBy(self, partial_name: str) -> pd.DataFrame:
        """Search for journals by publisher name (partial match)"""
        if not self.getDbPathOrUrl():
            raise ValueError("The graph database endpoint is not set. Please call setDbPathOrUrl()")

        # Escaping special characters to avoid SPARQL injection
        safe_partial = partial_name.replace('"', '\\"')
        
        sparql = SPARQLWrapper(self.getDbPathOrUrl())
        query = f"""
        SELECT ?journal ?title ?publisher
        WHERE {{
            ?journal a :Journal ;
                    :title ?title ;
                    :publisher ?publisher .
            FILTER CONTAINS(LCASE(?publisher), "{safe_partial.lower()}")
        }}
        """
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        
        try:
            results = sparql.query().convert()
        except Exception as e:
            raise ConnectionError(f"SPARQL query fails: {str(e)}")

        data = []
        for result in results["results"]["bindings"]:
            data.append({
                "id": result["journal"]["value"].split("/")[-1],
                "title": result["title"]["value"],
                "publisher": result["publisher"]["value"]
            })
        return pd.DataFrame(data)
    
    def getJournalsWithLicense(self, licenses: set[str]) -> pd.DataFrame:
        """Query journals that use a specified license set"""
        if not self.getDbPathOrUrl():
            raise ValueError("Graph database endpoint not set")

        if not licenses:
            # If an empty set is input, all journals with licenses are returned
            filter_clause = "FILTER BOUND(?license)"
        else:
            # Escape and construct license filter
            safe_licenses = [f'"{license}"' for license in licenses]
            licenses_str = ", ".join(safe_licenses)
            filter_clause = f"FILTER (?license IN ({licenses_str}))"

        sparql = SPARQLWrapper(self.getDbPathOrUrl())
        query = f"""
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
        
        try:
            results = sparql.query().convert()
        except Exception as e:
            raise ConnectionError(f"SPARQL query fails: {str(e)}")
        
        data = [{
            "id": result["journal"]["value"].split("/")[-1],
            "title": result["title"]["value"],
            "license": result["license"]["value"]
        } for result in results["results"]["bindings"]]
        
        return pd.DataFrame(data)
    
    def getJournalsWithAPC(self) -> pd.DataFrame:
        """Search for journals that clearly list their APC fees"""
        if not self.getDbPathOrUrl():
            raise ValueError("Graph database endpoint not set")

        sparql = SPARQLWrapper(self.getDbPathOrUrl())
        query = """
        SELECT ?journal ?title ?apc
        WHERE {
            ?journal a :Journal ;
                    :title ?title ;
                    :apc ?apc .
            FILTER (?apc != "None")  # Assume "None" means no APC is specified
        }
        """
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        
        try:
            results = sparql.query().convert()
        except Exception as e:
            raise ConnectionError(f"SPARQL query fails: {str(e)}")
        
        data = [{
            "id": result["journal"]["value"].split("/")[-1],
            "title": result["title"]["value"],
            "apc": result["apc"]["value"]
        } for result in results["results"]["bindings"]]
        
        return pd.DataFrame(data)
    
    def getJournalsWithDOAJSeal(self) -> pd.DataFrame:
        """Search for journals with DOAJ accreditation"""
        if not self.getDbPathOrUrl():
            raise ValueError("Graph database endpoint not set")

        sparql = SPARQLWrapper(self.getDbPathOrUrl())
        query = """
        SELECT ?journal ?title ?seal
        WHERE {
            ?journal a :Journal ;
                    :title ?title ;
                    :hasDOAJSeal ?seal .
            FILTER (?seal = true)  # Assuming a Boolean flag is used
        }
        """
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        
        try:
            results = sparql.query().convert()
        except Exception as e:
            raise ConnectionError(f"SPARQL query fails: {str(e)}")
        
        data = [{
            "id": result["journal"]["value"].split("/")[-1],
            "title": result["title"]["value"],
            "has_seal": result["seal"]["value"]
        } for result in results["results"]["bindings"]]
        
        return pd.DataFrame(data)
    
class CategoryQueryHandler(QueryHandler):
    def getById(self, category_id: str) -> pd.DataFrame: #If the subclass does not implement getById, Python will report an error during instantiation.
        """Query detailed information by category ID"""
        if not self.getDbPathOrUrl():
            raise ValueError("The relational database path is not set")

        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}") #connect the engine with the URL
        query = f"SELECT * FROM Categories WHERE id = '{category_id}'" #the SQL query
        return pd.read_sql(query, engine)

    
    def getAllCategories(self) -> pd.DataFrame:
        """Get all categories (de-duplicate)"""
        if not self.getDbPathOrUrl():
            raise ValueError("The relational database path is not set")

        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}") #connect the engine with the URL
        query = "SELECT DISTINCT * FROM Categories"                  #the SQL query
        return pd.read_sql(query, engine)
    
    def getCategoriesWithQuartile(self, quartiles: set[str]) -> pd.DataFrame:
        """Filter categories by quartile"""
        if not self.getDbPathOrUrl():
            raise ValueError("The relational database path is not set")

        if not quartiles:
            quartiles = {"Q1", "Q2", "Q3", "Q4"}  # Default all quartiles

        quartiles_str = ", ".join(f"'{q}'" for q in quartiles)      
        #Input: A set quartiles containing quartile identifiers (e.g. "Q1", "Q2").
        #Output: A string that conforms to the SQL IN clause, in the format 'Q1', 'Q2', 'Q3'.
        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")
        query = f"SELECT * FROM Categories WHERE quartile IN ({quartiles_str})"
        return pd.read_sql(query, engine)
    
    def getCategoriesAssignedToAreas(self, area_ids: set[str]) -> pd.DataFrame:
        """Get all categories assigned to a specified area (de-duplicated)"""
        if not self.getDbPathOrUrl():
            raise ValueError("The relational database path is not set")

        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")

        # Handling empty input: return all categories by default (assuming all possible combinations exist in the association table)
        if not area_ids:
            query = """
            SELECT DISTINCT c.id, c.name, c.quartile
            FROM Categories c
            JOIN CategoryArea ca ON c.id = ca.category_id
            """
        else:
            # Constructing safe IN clauses (avoiding SQL injection)
            area_ids_str = ", ".join(f"'{area_id}'" for area_id in area_ids)
            query = f"""
            SELECT DISTINCT c.id, c.name, c.quartile
            FROM Categories c
            JOIN CategoryArea ca ON c.id = ca.category_id
            WHERE ca.area_id IN ({area_ids_str})
            """

        return pd.read_sql(query, engine)
    
    def getAreasAssignedToCategories(self, category_ids: set[str]) -> pd.DataFrame:
        """Get all regions assigned to a specified category (de-duplicated)"""
        if not self.getDbPathOrUrl():
            raise ValueError("The relational database path is not set")

        engine = create_engine(f"sqlite:///{self.getDbPathOrUrl()}")

        # Handling empty input: returning all regions by default
        if not category_ids:
            query = """
            SELECT DISTINCT a.id, a.name
            FROM Areas a
            JOIN CategoryArea ca ON a.id = ca.area_id
            """
        else:
            # Constructing safe IN clauses
            category_ids_str = ", ".join(f"'{cat_id}'" for cat_id in category_ids)
            query = f"""
            SELECT DISTINCT a.id, a.name
            FROM Areas a
            JOIN CategoryArea ca ON a.id = ca.area_id
            WHERE ca.category_id IN ({category_ids_str})
            """

        return pd.read_sql(query, engine)