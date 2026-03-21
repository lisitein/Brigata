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
        if quartile is None or str(quartile).strip() in ("", "None", "NULL", "null"):
            self.quartile = None
        else:
            self.quartile = str(quartile).strip()

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
        categories: List[Category],
        areas: List[Area],
    ):
        super().__init__(ids)
        self.title = title
        self.languages = languages
        self.publisher = publisher
        self.seal = seal
        self.license = license
        self.apc = apc
        self.categories = categories
        self.areas = areas

    def getTitle(self) -> str:
        return self.title

    def getLanguages(self) -> List[str]:
        return self.languages

    def getPublisher(self) -> Optional[str]:
        return self.publisher

    def hasDOAJSeal(self) -> bool:
        return self.seal

    def getLicence(self) -> Optional[str]:
        return self.license

    def hasAPC(self) -> bool:
        return self.apc

    def getCategories(self) -> List[Category]:
        return self.categories

    def getAreas(self) -> List[Area]:
        return self.areas


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

    def _clean_str(self, x) -> str:
        try:
            if x is None:
                return ""
            return str(x).encode("utf-8", errors="replace").decode("utf-8")
        except Exception:
            return ""

    def _id_matches(self, cell, ids: Set[str]) -> bool:
        return any(i.strip() in ids for i in str(cell).split(","))

    # -----------------------------------------
    # Entity lookup
    # -----------------------------------------
    def getEntityById(self, id: str):
        # 1. Try journals via SPARQL
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

        # 2. Try categories and areas via SQLite
        for h in self.categoryHandlers:
            try:
                df = h.getById(id)
                if df is None or df.empty:
                    continue

                cols = set(df.columns)

                # Category result: has category_id but not area_id
                if "category_id" in cols and "area_id" not in cols and "journal_id" in cols:
                    row = df.iloc[0]
                    q = row.get("category_quartile") or row.get("quartile")
                    return Category(str(id), q)

                # Area result: has area_id but not category_id
                if "area_id" in cols and "category_id" not in cols:
                    return Area([str(id)])

                # Journal result: not found in Blazegraph, build minimal object from SQLite
                if "journal_id" in cols and ("category_id" in cols or "area_id" in cols):
                    cats_for_journal: List[Category] = []
                    areas_for_journal: List[Area] = []
                    seen_cats: Set[str] = set()
                    seen_areas: Set[str] = set()

                    for _, rr in df.iterrows():
                        cid = rr.get("category_id")
                        q = rr.get("category_quartile")
                        if cid and str(cid) not in seen_cats:
                            seen_cats.add(str(cid))
                            cats_for_journal.append(Category(str(cid), q))

                        aid = rr.get("area_id")
                        if aid and str(aid) not in seen_areas:
                            seen_areas.add(str(aid))
                            areas_for_journal.append(Area([str(aid)]))

                    all_jids = set()
                    for _, rr in df.iterrows():
                        jid_cell = rr.get("journal_id", "")
                        for s in str(jid_cell).split(","):
                            s = s.strip()
                            if s:
                                all_jids.add(s)

                    return Journal(
                        ids=sorted(all_jids),
                        title="",
                        languages=[],
                        publisher=None,
                        seal=False,
                        license=None,
                        apc=False,
                        categories=cats_for_journal,
                        areas=areas_for_journal,
                    )

            except Exception:
                continue

        return None

    # -----------------------------------------
    # Journal queries
    # -----------------------------------------
    def getAllJournals(self) -> List['Journal']:
        result: List[Journal] = []
        for h in self.journalHandlers:
            try:
                df = h.getAllJournals()
                if df is not None and not df.empty:
                    result.extend(self._makeJournals(df))
            except Exception:
                continue
        return result

    def getJournalsWithTitle(self, title: str) -> List['Journal']:
        result: List[Journal] = []
        for h in self.journalHandlers:
            try:
                df = h.getJournalsWithTitle(title)
                if df is not None and not df.empty:
                    result.extend(self._makeJournals(df))
            except Exception:
                continue
        return result

    def getJournalsPublishedBy(self, publisher: str) -> List['Journal']:
        result: List[Journal] = []
        for h in self.journalHandlers:
            try:
                df = h.getJournalsPublishedBy(publisher)
                if df is not None and not df.empty:
                    result.extend(self._makeJournals(df))
            except Exception:
                continue
        return result

    def getJournalsWithLicense(self, licenses: Set[str]) -> List['Journal']:
        result: List[Journal] = []
        for h in self.journalHandlers:
            try:
                df = h.getJournalsWithLicense(licenses)
                if df is not None and not df.empty:
                    result.extend(self._makeJournals(df))
            except Exception:
                continue
        return result

    def getJournalsWithAPC(self) -> List['Journal']:
        result: List[Journal] = []
        for h in self.journalHandlers:
            try:
                df = h.getJournalsWithAPC()
                if df is not None and not df.empty:
                    result.extend(self._makeJournals(df))
            except Exception:
                continue
        return result

    def getJournalsWithDOAJSeal(self) -> List['Journal']:
        result: List[Journal] = []
        for h in self.journalHandlers:
            try:
                df = h.getJournalsWithDOAJSeal()
                if df is not None and not df.empty:
                    result.extend(self._makeJournals(df))
            except Exception:
                continue
        return result

    # -----------------------------------------
    # Category and Area queries
    # -----------------------------------------
    def getAllCategories(self) -> List[Category]:
        result: List[Category] = []
        for h in self.categoryHandlers:
            try:
                df = h.getAllCategories()
                if df is None or df.empty:
                    continue
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
                col = "area_id" if "area_id" in df.columns else "id"
                df = df.drop_duplicates(subset=[col])
                for _, r in df.iterrows():
                    result.append(Area([r[col]]))
            except Exception:
                continue
        return result

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

    # -----------------------------------------
    # Journal construction
    # -----------------------------------------
    def _makeJournals(self, df: pd.DataFrame) -> List['Journal']:
        journals: List[Journal] = []
        if df is None or df.empty:
            return journals

        for _, r in df.iterrows():
            try:
                # Prefer 'all_ids' if available (returned by getJournalsWithLicense)
                raw_id = r.get("all_ids") or r.get("id", "")
                if raw_id is None:
                    ids = []
                else:
                    ids = [s.strip() for s in str(raw_id).split(",") if s.strip()]

                langs = r.get("languages", [])
                if isinstance(langs, str):
                    langs = [s.strip() for s in langs.split(",") if s.strip()]
                elif isinstance(langs, list):
                    langs = [str(s).strip() for s in langs if str(s).strip()]
                else:
                    langs = []

                title = self._clean_str(r.get("title", ""))
                publisher = self._clean_str(r.get("publisher", ""))
                license_clean = self._clean_str(r.get("license", ""))
                seal = str(r.get("seal", "")).strip().lower() in ["true", "yes", "1"]
                apc = str(r.get("apc", "")).strip().lower() in ["true", "yes", "1"]

                journal = Journal(
                    ids=ids,
                    title=title,
                    languages=langs,
                    publisher=publisher,
                    seal=seal,
                    license=license_clean,
                    apc=apc,
                    categories=[],
                    areas=[],
                )

                # Enrich with categories and areas from SQLite
                cats_dict: dict = {}  # category_id -> quartile
                areas_set: Set[str] = set()

                for jid in ids:
                    for h in self.categoryHandlers:
                        try:
                            df_rel = h.getById(jid)
                            if df_rel is None or df_rel.empty:
                                continue

                            if "category_id" in df_rel.columns:
                                for _, rr in df_rel.iterrows():
                                    cid = rr.get("category_id")
                                    if cid and str(cid).strip():
                                        cid_str = str(cid).strip()
                                        q = rr.get("category_quartile") or rr.get("quartile")
                                        if cid_str not in cats_dict:
                                            cats_dict[cid_str] = q

                            if "area_id" in df_rel.columns:
                                for _, rr in df_rel.iterrows():
                                    aid = rr.get("area_id")
                                    if aid and str(aid).strip():
                                        areas_set.add(str(aid).strip())

                        except Exception:
                            continue

                journal.categories = [Category(cid, q) for cid, q in sorted(cats_dict.items())]
                journal.areas = [Area([aid]) for aid in sorted(areas_set)]

                journals.append(journal)

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
                mask = df["id"].apply(lambda x: self._id_matches(x, all_ids))
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
                id_col = "all_ids" if "all_ids" in df.columns else "id"
                mask = df[id_col].apply(lambda x: self._id_matches(x, all_ids))
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
                mask_ids = df["id"].apply(lambda x: self._id_matches(x, all_ids))
                apc_mask = df["apc"].astype(str).str.lower().isin(["no", "false", "0", ""])
                mask = mask_ids & apc_mask
                result.extend(self._makeJournals(df[mask]))
            except Exception:
                continue
        return result
