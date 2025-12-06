from typing import List, Set, Optional, Union

import pandas as pd


# ============================
# DATA MODEL
# ============================


class IdentifiableEntity:
    def __init__(self, id: str):
        self.id = id

    def getId(self) -> str:
        return self.id

    def getIds(self) -> Set[str]:
        """Default: a single identifier."""
        return {self.id}
class Area(IdentifiableEntity):
    """Simple identifiable Area."""
    pass
class Category(IdentifiableEntity):
    def __init__(self, id: str, quartile: Optional[str] = None):
        super().__init__(id)
        self.quartile = quartile

    def getQuartile(self) -> Optional[str]:
        return self.quartile
class Journal(IdentifiableEntity):
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
        # use the first identifier as "main" id
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

    # Override: now returns *all* identifiers
    def getIds(self) -> Set[str]:
        return set(self.identifiers)

    # Getter methods
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
    Aggregates one or more JournalQueryHandler and CategoryDataQueryHandler
    and exposes a unified API that returns Python objects.
    """

    def __init__(self):
        self.journalHandlers = []
        self.categoryHandlers = []

    # ---- Handler registration ----

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

    # ---- Journal queries ----

    def getAllJournals(self) -> List[Journal]:
        result: List[Journal] = []
        for h in self.journalHandlers:
            df = h.getAllJournals()
            if not df.empty:
                result.extend(self._makeJournals(df))
        return result

    def getJournalsWithTitle(self, title: str) -> List[Journal]:
        result: List[Journal] = []
        for h in self.journalHandlers:
            df = h.getJournalsWithTitle(title)
            if not df.empty:
                result.extend(self._makeJournals(df))
        return result

    def getJournalsPublishedBy(self, publisher: str) -> List[Journal]:
        result: List[Journal] = []
        for h in self.journalHandlers:
            df = h.getJournalsPublishedBy(publisher)
            if not df.empty:
                result.extend(self._makeJournals(df))
        return result

    def getJournalsWithLicense(self, licenses: Set[str]) -> List[Journal]:
        result: List[Journal] = []
        for h in self.journalHandlers:
            df = h.getJournalsWithLicense(licenses)
            if not df.empty:
                result.extend(self._makeJournals(df))
        return result

    def getJournalsWithAPC(self) -> List[Journal]:
        result: List[Journal] = []
        for h in self.journalHandlers:
            df = h.getJournalsWithAPC()
            if not df.empty:
                result.extend(self._makeJournals(df))
        return result

    def getJournalsWithDOAJSeal(self) -> List[Journal]:
        result: List[Journal] = []
        for h in self.journalHandlers:
            df = h.getJournalsWithDOAJSeal()
            if not df.empty:
                result.extend(self._makeJournals(df))
        return result

    # ---- Category and Area queries ----

    def getAllCategories(self) -> List[Category]:
        result: List[Category] = []
        for h in self.categoryHandlers:
            df = h.getAllCategories()
            if not df.empty:
                df = df.drop_duplicates(subset=["category_id"])
                for _, r in df.iterrows():
                    result.append(Category(r["category_id"], r.get("quartile")))
        return result

    def getCategoriesWithQuartile(self, quartiles: Set[str]) -> List[Category]:
        result: List[Category] = []
        for h in self.categoryHandlers:
            df = h.getCategoriesWithQuartile(quartiles)
            if not df.empty:
                # il DF può avere solo category_id oppure anche quartile:
                for _, r in df.iterrows():
                    result.append(Category(r["category_id"], r.get("quartile")))
        return result

    def getAllAreas(self) -> List[Area]:
        result: List[Area] = []
        for h in self.categoryHandlers:
            df = h.getAllAreas()
            if not df.empty:
                df = df.drop_duplicates(subset=["area_id"])
                for _, r in df.iterrows():
                    result.append(Area(r["area_id"]))
        return result

    # ---- “Base but richer” queries (come da UML) ----

    def getEntityById(self, id: str) -> Optional[Union[Journal, Category, Area]]:
        """
        Look up an entity by id across all handlers.
        Returns a Journal, Category, Area, or None.
        """
        # Search among Journals
        for h in self.journalHandlers:
            df = h.getById(id)
            if not df.empty:
                # assume unique id → one row
                return self._makeJournals(df)[0]

        # Search among Categories and Areas
        for h in self.categoryHandlers:
            df = h.getById(id)
            if df.empty:
                continue

            if "category_id" in df.columns:
                row = df.iloc[0]
                return Category(row["category_id"], row.get("quartile"))

            if "area_id" in df.columns:
                row = df.iloc[0]
                return Area(row["area_id"])

        return None

    def getCategoriesAssignedToAreas(self, areas: Set[str]) -> List[Category]:
        """
        Delegates to CategoryDataQueryHandler.getCategoriesAssignedToAreas
        and wraps the result into Category objects.
        """
        result: List[Category] = []
        for h in self.categoryHandlers:
            df = h.getCategoriesAssignedToAreas(areas)
            if not df.empty:
                for _, r in df.iterrows():
                    result.append(Category(r["category_id"], r.get("quartile")))
        return result

    def getAreasAssignedToCategories(self, categories: Set[str]) -> List[Area]:
        """
        Delegates to CategoryDataQueryHandler.getAreasAssignedToCategories
        and wraps the result into Area objects.
        """
        result: List[Area] = []
        for h in self.categoryHandlers:
            df = h.getAreasAssignedToCategories(categories)
            if not df.empty:
                for _, r in df.iterrows():
                    result.append(Area(r["area_id"]))
        return result

    # ---- Helper ----

    def _makeJournals(self, df: pd.DataFrame) -> List[Journal]:
        """Convert DataFrame rows into Journal objects."""
        if df.empty:
            return []

        journals: List[Journal] = []
        for _, r in df.iterrows():
            identifiers = r["id"] if isinstance(r["id"], list) else [r["id"]]

            seal_str = str(r.get("seal", "")).lower()
            apc_str = str(r.get("apc", "")).lower()

            seal = seal_str in ["true", "yes", "1", "y", "t"]
            apc = apc_str in ["true", "yes", "1", "y", "t"]

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
    Extends BasicQueryEngine with more complex queries
    combining journals, categories and areas.
    """

    def getJournalsInCategoriesWithQuartile(
        self,
        category_ids: Set[str],
        quartiles: Set[str],
    ) -> List[Journal]:
        """
        Journals that are assigned to (some of) the given categories
        and whose categories have one of the given quartiles.
        """
        all_ids: Set[str] = set()

        for h in self.categoryHandlers:
            # This method must exist on CategoryDataQueryHandler
            df = h.getAllAssignments()
            if df.empty:
                continue

            if category_ids:
                df = df[df["category_id"].isin(category_ids)]
            if quartiles:
                df = df[df["quartile"].isin(quartiles)]

            all_ids.update(df["id"].dropna().tolist())

        if not all_ids:
            return []

        result: List[Journal] = []
        for h in self.journalHandlers:
            df_journals = h.getAllJournals()
            if df_journals.empty:
                continue

            mask = df_journals["id"].isin(all_ids)
            result.extend(self._makeJournals(df_journals[mask]))

        return result

    def getJournalsInAreasWithLicense(
        self,
        areas: Set[str],
        licenses: Set[str],
    ) -> List[Journal]:
        """
        Journals that are assigned to given areas and have
        one of the specified licenses.
        """
        all_ids: Set[str] = set()

        for h in self.categoryHandlers:
            df = h.getAllAssignments()
            if df.empty or not areas:
                continue

            df = df[df["area_id"].isin(areas)]
            all_ids.update(df["id"].dropna().tolist())

        if not all_ids:
            return []

        result: List[Journal] = []
        for h in self.journalHandlers:
            df_journals = h.getJournalsWithLicense(licenses)
            if df_journals.empty:
                continue

            mask = df_journals["id"].isin(all_ids)
            result.extend(self._makeJournals(df_journals[mask]))

        return result

    def getDiamondJournalsInAreasAndCategoriesWithQuartile(
        self,
        areas: Set[str],
        category_ids: Set[str],
        quartiles: Set[str],
    ) -> List[Journal]:
        """
        Diamond journals (no APC) that are:
        - in one of the given areas
        - in one of the given categories
        - whose categories have one of the given quartiles.
        """
        all_ids: Set[str] = set()

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

        result: List[Journal] = []
        for h in self.journalHandlers:
            df_journals = h.getAllJournals()
            if df_journals.empty:
                continue

            mask_ids = df_journals["id"].isin(all_ids)
            apc_str = df_journals["apc"].astype(str).str.lower()
            mask_diamond = apc_str.isin(["no", "false", "0", "n"])

            mask = mask_ids & mask_diamond
            result.extend(self._makeJournals(df_journals[mask]))

        return result
