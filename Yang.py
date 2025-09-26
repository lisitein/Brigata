from abc import ABC, abstractmethod
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
from sqlalchemy import create_engine
import sqlite3  


class CategoryQueryHandler:
    def __init__(self, db_path: str=None):
        self._db_path = db_path
    def setDbPathOrUrl(self, path: str): self._db_path = path
    def getDbPathOrUrl(self): return self._db_path  
    def _conn(self):
        if not self._db_path:
            raise RuntimeError("SQLite path not configured.")
        return sqlite3.connect(self._db_path)
    def getById(self, category_or_area_id: str) -> pd.DataFrame:
        with self._conn() as con:
            q_cat = """
            SELECT i.id AS id, c.quartile
            FROM Category c
            JOIN IdentifiableEntityId i ON c.internalId = i.internalId
            WHERE i.id = ?
            """
            df = pd.read_sql_query(q_cat, con, params=[category_or_area_id])
            if not df.empty: return df
            q_area = """
            SELECT i.id AS id
            FROM Area a
            JOIN IdentifiableEntityId i ON a.internalId = i.internalId
            WHERE i.id = ?
            """
            return pd.read_sql_query(q_area, con, params=[category_or_area_id])
    def getAllCategories(self) -> pd.DataFrame:
        with self._conn() as con:
            q = """
            SELECT DISTINCT i.id AS id, c.quartile
            FROM Category c
            JOIN IdentifiableEntityId i ON c.internalId = i.internalId
            """
            return pd.read_sql_query(q, con)
    def getCategoriesWithQuartile(self, quartiles:set) -> pd.DataFrame:
        with self._conn() as con:
            if quartiles:
                placeholders = ",".join(["?"]*len(quartiles))
                q = f"""
                SELECT DISTINCT i.id AS id, c.quartile
                FROM Category c
                JOIN IdentifiableEntityId i ON c.internalId = i.internalId
                WHERE c.quartile IN ({placeholders})
                """
                return pd.read_sql_query(q, con, params=list(quartiles))
            else:
                return self.getAllCategories()
    def getAllAreas(self) -> pd.DataFrame:
        with self._conn() as con:
            q = """
            SELECT DISTINCT i.id AS id
            FROM Area a
            JOIN IdentifiableEntityId i ON a.internalId = i.internalId
            """
            return pd.read_sql_query(q, con)
    def getCategoriesAssignedToAreas(self, area_ids:set) -> pd.DataFrame:
        with self._conn() as con:
            if area_ids:
                placeholders = ",".join(["?"]*len(area_ids))
                params = list(area_ids)
                q = f"""
                SELECT DISTINCT i_cat.id AS id, c.quartile
                FROM HasArea ha
                JOIN Area a ON ha.areaId = a.internalId
                JOIN IdentifiableEntityId i_area ON a.internalId = i_area.internalId
                JOIN HasCategory hc ON ha.hasAreaId = hc.hasCategoryId
                JOIN Category c ON hc.categoryId = c.internalId
                JOIN IdentifiableEntityId i_cat ON c.internalId = i_cat.internalId
                WHERE i_area.id IN ({placeholders})
                """
                return pd.read_sql_query(q, con, params=params)
            else:
                q = """
                SELECT DISTINCT i_cat.id AS id, c.quartile
                FROM HasArea ha
                JOIN HasCategory hc ON ha.hasAreaId = hc.hasCategoryId
                JOIN Category c ON hc.categoryId = c.internalId
                JOIN IdentifiableEntityId i_cat ON c.internalId = i_cat.internalId
                """
                return pd.read_sql_query(q, con)
    def getAreasAssignedToCategories(self, category_ids:set) -> pd.DataFrame:
        with self._conn() as con:
            if category_ids:
                placeholders = ",".join(["?"]*len(category_ids))
                params = list(category_ids)
                q = f"""
                SELECT DISTINCT i_area.id AS area
                FROM HasCategory hc
                JOIN Category c ON hc.categoryId = c.internalId
                JOIN IdentifiableEntityId i_cat ON c.internalId = i_cat.internalId
                JOIN HasArea ha ON hc.hasCategoryId = ha.hasAreaId
                JOIN Area a ON ha.areaId = a.internalId
                JOIN IdentifiableEntityId i_area ON a.internalId = i_area.internalId
                WHERE i_cat.id IN ({placeholders})
                """
                return pd.read_sql_query(q, con, params=params)
            else:
                q = """
                SELECT DISTINCT i_area.id AS area
                FROM HasCategory hc
                JOIN HasArea ha ON hc.hasCategoryId = ha.hasAreaId
                JOIN Area a ON ha.areaId = a.internalId
                JOIN IdentifiableEntityId i_area ON a.internalId = i_area.internalId
                """
                return pd.read_sql_query(q, con)
    def getAllCategoryAssignments(self) -> pd.DataFrame:
        with self._conn() as con:
            q = """
            SELECT DISTINCT 
                i_journal.id AS identifiers,
                i_cat.id AS category_id,
                c.quartile AS category_quartile
            FROM HasCategory hc
            JOIN Category c ON hc.categoryId = c.internalId
            JOIN IdentifiableEntityId i_journal ON hc.hasCategoryId = i_journal.internalId
            JOIN IdentifiableEntityId i_cat ON c.internalId = i_cat.internalId
            """
            return pd.read_sql_query(q, con)
    def getAllAreaAssignments(self) -> pd.DataFrame:
        with self._conn() as con:
            q = """
            SELECT DISTINCT 
                i_journal.id AS identifiers,
                i_area.id AS area
            FROM HasArea ha
            JOIN Area a ON ha.areaId = a.internalId
            JOIN IdentifiableEntityId i_journal ON ha.hasAreaId = i_journal.internalId
            JOIN IdentifiableEntityId i_area ON a.internalId = i_area.internalId
            """
            return pd.read_sql_query(q, con)

class JournalQueryHandler:
    def __init__(self, sparql_endpoint: str=None): self._endpoint = sparql_endpoint  
    def setDbPathOrUrl(self, url: str): self._endpoint = url
    def getDbPathOrUrl(self): return self._endpoint
    def _empty(self):
        return pd.DataFrame(columns=["id","title","publisher","apc","seal","license"])
    def getAllJournals(self): return self._empty()  
    def getJournalsWithTitle(self, term: str): return self._empty()  
    def getJournalsPublishedBy(self, publisher: str): return self._empty()
    def getJournalsWithLicense(self, license_str: str): return self._empty()
    def getJournalsWithAPC(self, apc: bool=True): return self._empty()
    def getJournalsWithDOAJSeal(self, seal: bool=True): return self._empty()
    def getById(self, jid: str): return self._empty()
