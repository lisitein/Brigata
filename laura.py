from typing import List, Set, Optional
import pandas as pd

from Yang import JournalQueryHandler, CategoryQueryHandler

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
        self.categoryHandlers: List[CategoryQueryHandler] = []

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

    def getEntityById(self, id: str):
        # Try journals
        for h in self.journalHandlers:
            df = h.getById(id)
            if df is not None and not df.empty:
                journals = self._makeJournals(df)
                for j in journals:
                    if id in j.getIds():
                        return j

        # Try categories / areas
        for h in self.categoryHandlers:
            df = h.getById(id)
            if df.empty:
                continue

            if "category_id" in df.columns:
                row = df.iloc[0]
                return Category(row["category_id"], row.get("category_quartile") or row.get("quartile"))

            if "area_id" in df.columns:
                row = df.iloc[0]
                return Area(row["area_id"])

        return None

    # ---- Category/Area relationships ----

    def getCategoriesAssignedToAreas(self, areas: Set[str]) -> List[Category]:
        result: List[Category] = []
        for h in self.categoryHandlers:
            df = h.getCategoriesAssignedToAreas(areas)
            if not df.empty:
                for _, r in df.iterrows():
                    result.append(Category(r["category_id"], r.get("quartile")))
        return result

    def getAreasAssignedToCategories(self, categories: Set[str]) -> List[Area]:
        result: List[Area] = []
        for h in self.categoryHandlers:
            df = h.getAreasAssignedToCategories(categories)
            if not df.empty:
                for _, r in df.iterrows():
                    result.append(Area(r["area_id"]))
        return result

    # ---- Helper ----

    def _makeJournals(self, df: pd.DataFrame) -> List[Journal]:
        journals: List[Journal] = []

        for _, r in df.iterrows():
            raw_id = r.get("id", "")
            if isinstance(raw_id, list):
                identifiers = raw_id
            elif isinstance(raw_id, str) and raw_id:
                identifiers = [s.strip() for s in raw_id.split(",") if s.strip()]
            else:
                identifiers = []

            langs = r.get("languages", [])
            if isinstance(langs, str):
                langs = [s.strip() for s in langs.split(",") if s.strip()]
            elif not isinstance(langs, list):
                langs = []

            has_category = r.get("hasCategory", [])
            if isinstance(has_category, str):
                has_category = [c.strip() for c in has_category.split(",") if c.strip()]

            has_area = r.get("hasArea", [])
            if isinstance(has_area, str):
                has_area = [a.strip() for a in has_area.split(",") if a.strip()]

            journals.append(
                Journal(
                    id=identifiers,
                    title=r.get("title", "") or "",
                    languages=langs,
                    publisher=r.get("publisher"),
                    seal=str(r.get("seal", "")).lower() in ["true", "yes", "1"],
                    license=r.get("license"),
                    apc=str(r.get("apc", "")).lower() in ["true", "yes", "1"],
                    hasCategory=has_category,
                    hasArea=has_area,
                )
            )

        return journals


# ============================
# FULL QUERY ENGINE
# ============================

class FullQueryEngine(BasicQueryEngine):

    def _collect_ids_from_category_assignments(
        self,
        h: CategoryQueryHandler,
        category_ids: Set[str],
        quartiles: Set[str],
    ) -> Set[str]:

        df = h.getAllCategoryAssignments()
        if df.empty:
            return set()

        if category_ids:
            df = df[df["category"].isin(category_ids)]
        if quartiles:
            df = df[df["category_quartile"].isin(quartiles)]

        ids = set()
        for _, r in df.iterrows():
            for s in r["identifiers"].split(","):
                s = s.strip()
                if s:
                    ids.add(s)
        return ids

    def _collect_ids_from_area_assignments(
        self,
        h: CategoryQueryHandler,
        area_ids: Set[str],
    ) -> Set[str]:

        df = h.getAllAreaAssignments()
        if df.empty:
            return set()

        if area_ids:
            df = df[df["area"].isin(area_ids)]

        ids = set()
        for _, r in df.iterrows():
            for s in r["identifiers"].split(","):
                s = s.strip()
                if s:
                    ids.add(s)
        return ids

    def getJournalsInCategoriesWithQuartile(
        self,
        category_ids: Set[str],
        quartiles: Set[str],
    ) -> List[Journal]:

        all_ids = set()
        for h in self.categoryHandlers:
            all_ids.update(
                self._collect_ids_from_category_assignments(h, category_ids, quartiles)
            )

        if not all_ids:
            return []

        result = []
        for h in self.journalHandlers:
            df = h.getAllJournals()
            if not df.empty:
                mask = df["id"].isin(all_ids)
                result.extend(self._makeJournals(df[mask]))

        return result

    def getJournalsInAreasWithLicense(
        self,
        areas: Set[str],
        licenses: Set[str],
    ) -> List[Journal]:

        all_ids = set()
        for h in self.categoryHandlers:
            all_ids.update(
                self._collect_ids_from_area_assignments(h, areas)
            )

        if not all_ids:
            return []

        result = []
        for h in self.journalHandlers:
            df = h.getJournalsWithLicense(licenses)
            if not df.empty:
                mask = df["id"].isin(all_ids)
                result.extend(self._makeJournals(df[mask]))

        return result

    def getDiamondJournalsInAreasAndCategoriesWithQuartile(
        self,
        area_ids: Set[str],
        category_ids: Set[str],
        quartiles: Set[str],
    ) -> List[Journal]:

        ids_cat = set()
        ids_area = set()

        for h in self.categoryHandlers:
            ids_cat.update(
                self._collect_ids_from_category_assignments(h, category_ids, quartiles)
            )
            ids_area.update(
                self._collect_ids_from_area_assignments(h, area_ids)
            )

        all_ids = ids_cat & ids_area
        if not all_ids:
            return []

        result = []
        for h in self.journalHandlers:
            df = h.getAllJournals()
            if df.empty:
                continue

            mask_ids = df["id"].isin(all_ids)
            apc_mask = df["apc"].astype(str).str.lower().isin(["no", "false", "0", ""])
            mask = mask_ids & apc_mask

            result.extend(self._makeJournals(df[mask]))

        return result
