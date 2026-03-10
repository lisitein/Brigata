from typing import List, Set, Optional, Union
import pandas as pd

from daniele import CategoryQueryHandler as DanieleCategoryHandler
from li import CategoryQueryHandler as LiCategoryHandler
from Yang import JournalQueryHandler


# ============================
# DATA MODEL
# ============================

class IdentifiableEntity:
    def __init__(self, id: str):
        self.id = id

    def getId(self) -> str:
        return self.id

    def getIds(self) -> Set[str]:
        return {self.id}


class Area(IdentifiableEntity):
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
        return set(self.identifiers)

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
    def __init__(self):
        self.journalHandlers: List[JournalQueryHandler] = []
        self.categoryHandlers: List[Union[DanieleCategoryHandler, LiCategoryHandler]] = []

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
                for _, r in df.iterrows():
                    result.append(Category(r["category_id"], r.get("quartile")))
        return result

    def getAllAreas(self) -> List[Area]:
        result: List[Area] = []
        for h in self.categoryHandlers:
            df = h.getAllAreas()
            if not df.empty:
                col = "area_id" if "area_id" in df.columns else "id"
                df = df.drop_duplicates(subset=[col])
                for _, r in df.iterrows():
                    result.append(Area(r[col]))
        return result

    # ---- Entity lookup ----

    def getEntityById(self, id: str) -> Optional[Union[Journal, Category, Area]]:
        for h in self.journalHandlers:
            df = h.getById(id)
            if not df.empty:
                journals = self._makeJournals(df)
                for j in journals:
                    if id in j.getIds():
                        return j

        for h in self.categoryHandlers:
            df = h.getById(id)
            if df.empty:
                continue

            if "category_id" in df.columns or "id" in df.columns:
                col = "category_id" if "category_id" in df.columns else "id"
                row = df.iloc[0]
                return Category(row[col], row.get("quartile"))

            if "area_id" in df.columns or "id" in df.columns:
                col = "area_id" if "area_id" in df.columns else "id"
                row = df.iloc[0]
                return Area(row[col])

        return None

    # ---- Category/Area relationships ----

    def getCategoriesAssignedToAreas(self, areas: Set[str]) -> List[Category]:
        result: List[Category] = []
        for h in self.categoryHandlers:
            df = h.getCategoriesAssignedToAreas(areas)
            if not df.empty:
                for _, r in df.iterrows():
                    cid = r.get("category_id", r.get("id"))
                    result.append(Category(cid, r.get("quartile")))
        return result

    def getAreasAssignedToCategories(self, categories: Set[str]) -> List[Area]:
        result: List[Area] = []
        for h in self.categoryHandlers:
            df = h.getAreasAssignedToCategories(categories)
            if not df.empty:
                for _, r in df.iterrows():
                    aid = r.get("area_id", r.get("area", r.get("id")))
                    result.append(Area(aid))
        return result

    # ---- Helper ----

    def _makeJournals(self, df: pd.DataFrame) -> List[Journal]:
        if df.empty:
            return []

        journals: List[Journal] = []

        for _, r in df.iterrows():
            raw_id = r.get("id", "")

            # FIX: Yang returns ids as a comma-separated string (from GROUP_CONCAT),
            # so we split it into a proper list instead of wrapping the whole string.
            if isinstance(raw_id, list):
                identifiers = raw_id
            elif isinstance(raw_id, str) and raw_id:
                identifiers = [s.strip() for s in raw_id.split(",") if s.strip()]
            else:
                identifiers = []

            title = r.get("title", "") or ""

            langs = r.get("languages", [])
            if isinstance(langs, str):
                langs = [s.strip() for s in langs.split(",") if s.strip()]
            elif not isinstance(langs, list):
                langs = []

            seal = str(r.get("seal", "")).lower() in ["true", "yes", "1", "y", "t"]
            apc = str(r.get("apc", "")).lower() in ["true", "yes", "1", "y", "t"]

            has_category = r.get("hasCategory", [])
            if isinstance(has_category, str):
                has_category = [c.strip() for c in has_category.split(",") if c.strip()]
            elif not isinstance(has_category, list):
                has_category = []

            has_area = r.get("hasArea", [])
            if isinstance(has_area, str):
                has_area = [a.strip() for a in has_area.split(",") if a.strip()]
            elif not isinstance(has_area, list):
                has_area = []

            journals.append(
                Journal(
                    id=identifiers,
                    title=title,
                    languages=langs,
                    publisher=r.get("publisher"),
                    seal=seal,
                    license=r.get("license"),
                    apc=apc,
                    hasCategory=has_category,
                    hasArea=has_area,
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
        area_ids: Set[str],
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

            if area_ids:
                df = df[df["area_id"].isin(area_ids)]
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
            mask_diamond = apc_str.isin(["no", "false", "0", "n", "none", ""])

            mask = mask_ids & mask_diamond
            result.extend(self._makeJournals(df_journals[mask]))

        return result
