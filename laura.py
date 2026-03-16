from typing import List, Set, Optional
import pandas as pd

from Yang import JournalQueryHandler, CategoryQueryHandler

# ============================
# DATA MODEL
# ============================

class IdentifiableEntity:
    def __init__(self, ids: List[str]):
        self.id = ids

    def getId(self) -> List[str]:
        return self.id

    def getIds(self) -> Set[str]:
        return set(self.id)


class Area(IdentifiableEntity):
    pass


class Category(IdentifiableEntity):
    def __init__(self, id: str, quartile: Optional[str] = None):
        super().__init__([id])
        self.quartile = quartile

    def getQuartile(self) -> Optional[str]:
        return self.quartile


class Journal(IdentifiableEntity):
    def __init__(
        self,
        ids: List[str],
        title: str,
        languages: List[str],
        publisher: Optional[str],
        seal: bool,
        license: Optional[str],
        apc: bool,
        hasCategory: List[str],
        hasArea: List[str],
    ):
        super().__init__(ids)
        self.title = title
        self.languages = languages
        self.publisher = publisher
        self.seal = seal
        self.license = license
        self.apc = apc
        self.hasCategory = hasCategory
        self.hasArea = hasArea

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
            try:
                df = h.getAllJournals()
                if df is not None and not df.empty:
                    result.extend(self._makeJournals(df))
            except Exception:
                continue
        return result

    def getJournalsWithTitle(self, title: str) -> List[Journal]:
        result: List[Journal] = []
        for h in self.journalHandlers:
            try:
                df = h.getJournalsWithTitle(title)
                if df is not None and not df.empty:
                    result.extend(self._makeJournals(df))
            except Exception:
                continue
        return result

    def getJournalsPublishedBy(self, publisher: str) -> List[Journal]:
        result: List[Journal] = []
        for h in self.journalHandlers:
            try:
                df = h.getJournalsPublishedBy(publisher)
                if df is not None and not df.empty:
                    result.extend(self._makeJournals(df))
            except Exception:
                continue
        return result

    def getJournalsWithLicense(self, licenses: Set[str]) -> List[Journal]:
        result: List[Journal] = []
        for h in self.journalHandlers:
            try:
                df = h.getJournalsWithLicense(licenses)
                if df is not None and not df.empty:
                    result.extend(self._makeJournals(df))
            except Exception:
                continue
        return result

    def getJournalsWithAPC(self) -> List[Journal]:
        result: List[Journal] = []
        for h in self.journalHandlers:
            try:
                df = h.getJournalsWithAPC()
                if df is not None and not df.empty:
                    result.extend(self._makeJournals(df))
            except Exception:
                continue
        return result

    def getJournalsWithDOAJSeal(self) -> List[Journal]:
        result: List[Journal] = []
        for h in self.journalHandlers:
            try:
                df = h.getJournalsWithDOAJSeal()
                if df is not None and not df.empty:
                    result.extend(self._makeJournals(df))
            except Exception:
                continue
        return result

    # ---- Category and Area queries ----

    def getAllCategories(self) -> List[Category]:
        result: List[Category] = []
        for h in self.categoryHandlers:
            try:
                df = h.getAllCategories()
                if df is None or df.empty:
                    continue

                # 🔥 PATCH: NON deduplicare solo per category_id
                # Yang restituisce category_id + quartile
                df = df.drop_duplicates(subset=["category_id", "quartile"])

                for _, r in df.iterrows():
                    result.append(Category(r["category_id"], r.get("quartile")))
            except Exception:
                continue
        return result

    def getCategoriesWithQuartile(self, quartiles: Set[str]) -> List[Category]:
        result: List[Category] = []
        for h in self.categoryHandlers:
            try:
                df = h.getCategoriesWithQuartile(quartiles)
                if df is None or df.empty:
                    continue
                for _, r in df.iterrows():
                    result.append(Category(r["category_id"], r.get("quartile")))
            except Exception:
                continue
        return result

    def getAllAreas(self) -> List[Area]:
        result: List[Area] = []
        for h in self.categoryHandlers:
            try:
                df = h.getAllAreas()
                if df is None or df.empty:
                    continue

                # Qui il dedup è corretto
                if "area_id" in df.columns:
                    col = "area_id"
                elif "id" in df.columns:
                    col = "id"
                else:
                    continue

                df = df.drop_duplicates(subset=[col])
                for _, r in df.iterrows():
                    result.append(Area([r[col]]))
            except Exception:
                continue
        return result

    # ---- Entity lookup ----

    def getEntityById(self, id: str):
        # Try journals
        for h in self.journalHandlers:
            try:
                df = h.getById(id)
                if df is not None and not df.empty:
                    journals = self._makeJournals(df)
                    for j in journals:
                        if id in j.getIds():
                            return j
            except Exception:
                continue

        # Try categories / areas
        for h in self.categoryHandlers:
            try:
                df = h.getById(id)
                if df is None or df.empty:
                    continue

                if "category_id" in df.columns:
                    row = df.iloc[0]
                    return Category(row["category_id"], row.get("category_quartile") or row.get("quartile"))

                if "area_id" in df.columns:
                    row = df.iloc[0]
                    return Area([row["area_id"]])
            except Exception:
                continue

        return None

    # ---- Category/Area relationships ----

    def getCategoriesAssignedToAreas(self, areas: Set[str]) -> List[Category]:
        result: List[Category] = []
        for h in self.categoryHandlers:
            try:
                df = h.getCategoriesAssignedToAreas(areas)
                if df is None or df.empty:
                    continue
                for _, r in df.iterrows():
                    cid = r.get("category_id")
                    if cid:
                        result.append(Category(cid, r.get("quartile")))
            except Exception:
                continue
        return result

    def getAreasAssignedToCategories(self, categories: Set[str]) -> List[Area]:
        result: List[Area] = []
        for h in self.categoryHandlers:
            try:
                df = h.getAreasAssignedToCategories(categories)
                if df is None or df.empty:
                    continue
                for _, r in df.iterrows():
                    aid = r.get("area_id")
                    if aid:
                        result.append(Area([aid]))
            except Exception:
                continue
        return result

    # ---- Helper ----

    def _makeJournals(self, df: pd.DataFrame) -> List[Journal]:
        journals: List[Journal] = []
        if df is None or df.empty:
            return journals

        for _, r in df.iterrows():
            try:
                raw_id = r.get("id", "")
                if isinstance(raw_id, list):
                    ids = raw_id
                elif isinstance(raw_id, str) and raw_id:
                    ids = [s.strip() for s in raw_id.split(",") if s.strip()]
                else:
                    ids = []

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
                        ids=ids,
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
            except Exception:
                continue

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

        try:
            df = h.getAllCategoryAssignments()
        except Exception:
            return set()

        if df is None or df.empty:
            return set()

        if "category" not in df.columns or "identifiers" not in df.columns:
            return set()

        if category_ids:
            df = df[df["category"].isin(category_ids)]
        if quartiles and "category_quartile" in df.columns:
            df = df[df["category_quartile"].isin(quartiles)]

        ids = set()
        for _, r in df.iterrows():
            try:
                for s in str(r.get("identifiers", "")).split(","):
                    s = s.strip()
                    if s:
                        ids.add(s)
            except Exception:
                continue
        return ids

    def _collect_ids_from_area_assignments(
        self,
        h: CategoryQueryHandler,
        area_ids: Set[str],
    ) -> Set[str]:

        try:
            df = h.getAllAreaAssignments()
        except Exception:
            return set()

        if df is None or df.empty:
            return set()

        if "area" not in df.columns or "identifiers" not in df.columns:
            return set()

        if area_ids:
            df = df[df["area"].isin(area_ids)]

        ids = set()
        for _, r in df.iterrows():
            try:
                for s in str(r.get("identifiers", "")).split(","):
                    s = s.strip()
                    if s:
                        ids.add(s)
            except Exception:
                continue
        return ids

    def getJournalsInCategoriesWithQuartile(
        self,
        category_ids: Set[str],
        quartiles: Set[str],
    ) -> List[Journal]:

        all_ids = set()
        for h in self.categoryHandlers:
            try:
                all_ids.update(
                    self._collect_ids_from_category_assignments(h, category_ids, quartiles)
                )
            except Exception:
                continue

        if not all_ids:
            return []

        result = []
        for h in self.journalHandlers:
            try:
                df = h.getAllJournals()
                if df is None or df.empty:
                    continue
                mask = df["id"].isin(all_ids)
                result.extend(self._makeJournals(df[mask]))
            except Exception:
                continue

        return result

    def getJournalsInAreasWithLicense(
        self,
        areas: Set[str],
        licenses: Set[str],
    ) -> List[Journal]:

        all_ids = set()
        for h in self.categoryHandlers:
            try:
                all_ids.update(
                    self._collect_ids_from_area_assignments(h, areas)
                )
            except Exception:
                continue

        if not all_ids:
            return []

        result = []
        for h in self.journalHandlers:
            try:
                df = h.getJournalsWithLicense(licenses)
                if df is None or df.empty:
                    continue
                mask = df["id"].isin(all_ids)
                result.extend(self._makeJournals(df[mask]))
            except Exception:
                continue

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
            try:
                ids_cat.update(
                    self._collect_ids_from_category_assignments(h, category_ids, quartiles)
                )
                ids_area.update(
                    self._collect_ids_from_area_assignments(h, area_ids)
                )
            except Exception:
                continue

        all_ids = ids_cat & ids_area
        if not all_ids:
            return []

        result = []
        for h in self.journalHandlers:
            try:
                df = h.getAllJournals()
                if df is None or df.empty:
                    continue

                mask_ids = df["id"].isin(all_ids)
                apc_mask = df["apc"].astype(str).str.lower().isin(["no", "false", "0", ""])
                mask = mask_ids & apc_mask

                result.extend(self._makeJournals(df[mask]))
            except Exception:
                continue

        return result
