from typing import List, Set, Optional, Union
import pandas as pd


# ============================
# DATA MODEL
# ============================

class IdentifiableEntity:
    """Base class for entities with an identifier."""
    def __init__(self, id: str):
        self.id = id

    def getId(self) -> str:
        return self.id

    def getIds(self) -> Set[str]:
        # Default: entity has a single identifier
        return {self.id}


class Area(IdentifiableEntity):
    # Simple identifiable Area
    pass


class Category(IdentifiableEntity):
    # Category with optional quartile
    def __init__(self, id: str, quartile: Optional[str] = None):
        super().__init__(id)
        self.quartile = quartile

    def getQuartile(self) -> Optional[str]:
        return self.quartile


class Journal(IdentifiableEntity):
    """
    Journal entity supporting multiple identifiers (e.g. ISSN, eISSN)
    plus metadata (title, languages, publisher, etc.).
    """
    def __init__(
        self,
        id: List[str],
        title: str,
        languages: List[str],
        publisher: Optional[str],
        seal: bool,
        license: Optional[str],
        apc: bool,
        hasCategory: List[str],
        hasArea: List[str],
    ):
        # Main identifier = first in the list
        super().__init__(id[0] if id else "")
        self.identifiers = id or []
        self.title = title
        self.languages = languages
        self.publisher = publisher
        self.seal = seal
        self.license = license
        self.apc = apc
        self.hasCategory = hasCategory
        self.hasArea = hasArea

    def getIds(self) -> Set[str]:
        # Override: return all identifiers
        return set(self.identifiers)

    # --- Getters ---
    def getTitle(self) -> str:
        return self.title

    def getLanguages(self) -> List[str]:
        return self.languages

    def getPublisher(self) -> Optional[str]:
        return self.publisher

    def hasSeal(self) -> bool:
        return self.seal

    def getLicense(self) -> Optional[str]:
        return self.license

    def hasAPC(self) -> bool:
        return self.apc

    def getHasCategory(self) -> List[str]:
        return self.hasCategory

    def getHasArea(self) -> List[str]:
        return self.hasArea


# ============================
# BASIC QUERY ENGINE
# ============================

class BasicQueryEngine:
    """
    Collects one or more JournalQueryHandler (RDF)
    and CategoryQueryHandler (SQL), and exposes
    a unified API that returns Python objects.
    """

    def __init__(self):
        self.journalHandlers = []
        self.categoryHandlers = []

    # --- Handler registration ---

    def addJournalHandler(self, handler) -> bool:
        self.journalHandlers.append(handler)
        return True

    def addCategoryHandler(self, handler) -> bool:
        self.categoryHandlers.append(handler)
        return True

    def cleanJournalHandlers(self) -> bool:
        self.journalHandlers.clear()
        return True

    def cleanCategoryHandlers(self) -> bool:
        self.categoryHandlers.clear()
        return True

    # --- Journal queries (delegated to RDF handlers) ---

    def getAllJournals(self) -> List[Journal]:
        result = []
        for h in self.journalHandlers:
            df = h.getAllJournals()
            if not df.empty:
                result.extend(self._makeJournals(df))
        return result

    def getJournalsWithTitle(self, title: str) -> List[Journal]:
        result = []
        for h in self.journalHandlers:
            df = h.getJournalsWithTitle(title)
            if not df.empty:
                result.extend(self._makeJournals(df))
        return result

    def getJournalsPublishedBy(self, publisher: str) -> List[Journal]:
        result = []
        for h in self.journalHandlers:
            df = h.getJournalsPublishedBy(publisher)
            if not df.empty:
                result.extend(self._makeJournals(df))
        return result

    def getJournalsWithLicense(self, licenses: Set[str]) -> List[Journal]:
        result = []
        for h in self.journalHandlers:
            df = h.getJournalsWithLicense(licenses)
            if not df.empty:
                result.extend(self._makeJournals(df))
        return result

    def getJournalsWithAPC(self) -> List[Journal]:
        result = []
        for h in self.journalHandlers:
            df = h.getJournalsWithAPC()
            if not df.empty:
                result.extend(self._makeJournals(df))
        return result

    def getJournalsWithDOAJSeal(self) -> List[Journal]:
        result = []
        for h in self.journalHandlers:
            df = h.getJournalsWithDOAJSeal()
            if not df.empty:
                result.extend(self._makeJournals(df))
        return result

    # --- Category & Area queries (delegated to SQL handlers) ---

    def getAllCategories(self) -> List[Category]:
        result = []
        for h in self.categoryHandlers:
            df = h.getAllCategories()
            if not df.empty:
                df = df.drop_duplicates(subset=["category_id"])
                for _, r in df.iterrows():
                    result.append(Category(r["category_id"], r.get("quartile")))
        return result

    def getCategoriesWithQuartile(self, quartiles: Set[str]) -> List[Category]:
        result = []
        for h in self.categoryHandlers:
            df = h.getCategoriesWithQuartile(quartiles)
            if not df.empty:
                for _, r in df.iterrows():
                    result.append(Category(r["category_id"], r.get("quartile")))
        return result

    def getAllAreas(self) -> List[Area]:
        result = []
        for h in self.categoryHandlers:
            df = h.getAllAreas()
            if not df.empty:
                df = df.drop_duplicates(subset=["area_id"])
                for _, r in df.iterrows():
                    result.append(Area(r["area_id"]))
        return result

    # --- Richer base queries (following the UML!) ---

    def getEntityById(self, id: str) -> Optional[Union[Journal, Category, Area]]:
        # Look up in journal handlers
        for h in self.journalHandlers:
            df = h.getById(id)
            if not df.empty:
                return self._makeJournals(df)[0]

        # Look up in category/area handlers
        for h in self.categoryHandlers:
            df = h.getById(id)
            if df.empty:
                continue

            if "category_id" in df.columns:
                r = df.iloc[0]
                return Category(r["category_id"], r.get("quartile"))
            if "area_id" in df.columns:
                r = df.iloc[0]
                return Area(r["area_id"])

        return None

    def getCategoriesAssignedToAreas(self, areas: Set[str]) -> List[Category]:
        # Return categories linked to these areas
        result = []
        for h in self.categoryHandlers:
            df = h.getCategoriesAssignedToAreas(areas)
            if not df.empty:
                for _, r in df.iterrows():
                    result.append(Category(r["category_id"], r.get("quartile")))
        return result

    def getAreasAssignedToCategories(self, categories: Set[str]) -> List[Area]:
        # Return areas linked to these categories
        result = []
        for h in self.categoryHandlers:
            df = h.getAreasAssignedToCategories(categories)
            if not df.empty:
                for _, r in df.iterrows():
                    result.append(Area(r["area_id"]))
        return result

    # --- Helper: DF → Journal objects ---

    def _makeJournals(self, df: pd.DataFrame) -> List[Journal]:
        if df.empty:
            return []

        journals = []
        for _, r in df.iterrows():
            identifiers = r["id"] if isinstance(r["id"], list) else [r["id"]]

            # Normalize booleans
            seal = str(r.get("seal", "")).lower() in ["true", "yes", "1", "y", "t"]
            apc = str(r.get("apc", "")).lower() in ["true", "yes", "1", "y", "t"]

            journals.append(
                Journal(
                    id=identifiers,
                    title=r.get("title", ""),
                    languages=r.get("languages", []),
                    publisher=r.get("publisher"),
                    seal=seal,
                    license=r.get("license"),
                    apc=apc,
                    hasCategory=r.get("hasCategory", []),
                    hasArea=r.get("hasArea", []),
                )
            )
        return journals


# ============================
# FULL QUERY ENGINE
# ============================

class FullQueryEngine(BasicQueryEngine):
    """
    Provides complex queries combining:
    - journal metadata (RDF)
    - category/area assignments (SQL)
    """

    def getJournalsInCategoriesWithQuartile(
        self,
        category_ids: Set[str],
        quartiles: Set[str],
    ) -> List[Journal]:

        all_ids = set()

        # Get matching rows from SQL assignment table
        for h in self.categoryHandlers:
            df = h.getAllAssignments()
            if df.empty:
                continue

            if category_ids:
                df = df[df["category_id"].isins(category_ids)]
            if quartiles:
                df = df[df["quartile"].isin(quartiles)]

            all_ids.update(df["id"].dropna().tolist())

        if not all_ids:
            return []

        # Filter journals by id
        result = []
        for h in self.journalHandlers:
            df_j = h.getAllJournals()
            if df_j.empty:
                continue
            mask = df_j["id"].isin(all_ids)
            result.extend(self._makeJournals(df_j[mask]))

        return result

    def getJournalsInAreasWithLicense(
        self,
        areas: Set[str],
        licenses: Set[str],
    ) -> List[Journal]:

        all_ids = set()

        # Filter assignment table by areas
        for h in self.categoryHandlers:
            df = h.getAllAssignments()
            if df.empty or not areas:
                continue

            df = df[df["area_id"].isin(areas)]
            all_ids.update(df["id"].dropna().tolist())

        if not all_ids:
            return []

        # Filter RDF journals by license and id
        result = []
        for h in self.journalHandlers:
            df_j = h.getJournalsWithLicense(licenses)
            if df_j.empty:
                continue
            mask = df_j["id"].isins(all_ids)
            result.extend(self._makeJournals(df_j[mask]))

        return result

    def getDiamondJournalsInAreasAndCategoriesWithQuartile(
        self,
        areas: Set[str],
        category_ids: Set[str],
        quartiles: Set[str],
    ) -> List[Journal]:

        all_ids = set()

        # Full assignment filtering
        for h in self.categoryHandlers:
            df = h.getAllAssignments()
            if df.empty:
                continue

            if areas:
                df = df[df["area_id"].isin(areas)]
            if category_ids:
                df = df[df["category_id"].isin(category_ids)]
            if quartiles:
                df = df[df["quartile"].isin(quartiles)]

            all_ids.update(df["id"].dropna().tolist())

        if not all_ids:
            return []

        # Select only journals with APC = false
        result = []
        for h in self.journalHandlers:
            df_j = h.getAllJournals()
            if df_j.empty:
                continue

            mask_ids = df_j["id"].isin(all_ids)
            apc_values = df_j["apc"].astype(str).str.lower()
            mask_diamond = apc_values.isin(["no", "false", "0", "n"])

            mask = mask_ids & mask_diamond
            result.extend(self._makeJournals(df_j[mask]))

        return result
