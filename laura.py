from typing import List, Set, Optional
from collections import defaultdict
import pandas as pd
from sqlalchemy import create_engine as _ce
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

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return set(self.id) == set(other.id)

    def __hash__(self):
        return hash(frozenset(self.id))


class Area(IdentifiableEntity):
    pass


class Category(IdentifiableEntity):
    def __init__(self, id: str, quartile: Optional[str] = None):
        super().__init__([id])
        if quartile is None or str(quartile).strip() in ("", "None", "NULL", "null", "nan"):
            self.quartile = None
        else:
            self.quartile = str(quartile).strip()

    def getQuartile(self) -> Optional[str]:
        return self.quartile

    def __eq__(self, other):
        if not isinstance(other, Category):
            return False
        return set(self.id) == set(other.id) and self.quartile == other.quartile

    def __hash__(self):
        return hash((frozenset(self.id), self.quartile))


class Journal(IdentifiableEntity):
    def __init__(
        self,
        ids: List[str],
        title: Optional[str],
        languages: Optional[List[str]],
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
        self.hasCategory = categories
        self.hasArea = areas

    def getTitle(self) -> Optional[str]:
        return self.title

    def getLanguages(self) -> Optional[List[str]]:
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
        return self.hasCategory

    def getAreas(self) -> List[Area]:
        return self.hasArea

    def __eq__(self, other):
        if not isinstance(other, Journal):
            return False
        return set(self.id) == set(other.id)

    def __hash__(self):
        return hash(frozenset(self.id))


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

    def _clean_str(self, x) -> Optional[str]:
        try:
            if x is None or str(x).strip() in ("", "None", "nan"):
                return None
            return str(x).encode("utf-8", errors="replace").decode("utf-8")
        except Exception:
            return None

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
                        license="",
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
        seen_ids: Set[str] = set()

        # 1. Journals from Blazegraph (DOAJ)
        for h in self.journalHandlers:
            try:
                df = h.getAllJournals()
                if df is not None and not df.empty:
                    journals = self._makeJournals(df)
                    for j in journals:
                        result.append(j)
                        for jid in j.getIds():
                            seen_ids.add(jid)
            except Exception:
                continue

        # 2. Journals present only in SQLite (Scimago) — not in Blazegraph
        for h in self.categoryHandlers:
            try:
                engine = _ce(f"sqlite:///{h.getDbPathOrUrl()}")
                df_sql = pd.read_sql(
                    "SELECT DISTINCT internalId, id FROM IdentifiableEntity WHERE internalId LIKE 'journal-%'",
                    engine
                )
                if df_sql.empty:
                    continue

                groups = defaultdict(list)
                for _, row in df_sql.iterrows():
                    groups[row["internalId"]].append(row["id"])

                for internal_id, ids in groups.items():
                    if not any(i in seen_ids for i in ids):
                        cats_dict: dict = {}
                        areas_set: Set[str] = set()
                        for jid in ids:
                            try:
                                df_rel = h.getById(jid)
                                if df_rel is None or df_rel.empty:
                                    continue
                                if "category_id" in df_rel.columns:
                                    for _, rr in df_rel.iterrows():
                                        cid = rr.get("category_id")
                                        q = rr.get("category_quartile")
                                        if cid and str(cid).strip() and str(cid).strip() not in cats_dict:
                                            cats_dict[str(cid).strip()] = q
                                if "area_id" in df_rel.columns:
                                    for _, rr in df_rel.iterrows():
                                        aid = rr.get("area_id")
                                        if aid and str(aid).strip():
                                            areas_set.add(str(aid).strip())
                            except Exception:
                                continue

                        result.append(Journal(
                            ids=sorted(ids),
                            title="",
                            languages=[],
                            publisher=None,
                            seal=False,
                            license="",
                            apc=False,
                            categories=[Category(cid, q) for cid, q in sorted(cats_dict.items())],
                            areas=[Area([aid]) for aid in sorted(areas_set)],
                        ))
                        for i in ids:
                            seen_ids.add(i)
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
                raw_id = r.get("all_ids") or r.get("id", "")
                if raw_id is None:
                    ids = []
                else:
                    ids = [s.strip() for s in str(raw_id).split(",") if s.strip()]

                langs_raw = r.get("languages")
                if langs_raw is None or str(langs_raw).strip() in ("", "None", "nan"):
                    langs = None
                elif isinstance(langs_raw, str):
                    langs = [s.strip() for s in langs_raw.split(",") if s.strip()] or None
                elif isinstance(langs_raw, list):
                    langs = [str(s).strip() for s in langs_raw if str(s).strip()] or None
                else:
                    langs = None

                title         = self._clean_str(r.get("title"))
                publisher     = self._clean_str(r.get("publisher"))
                license_clean = self._clean_str(r.get("license"))
                seal = str(r.get("seal", "")).strip().lower() in ["true", "yes", "1"]
                apc  = str(r.get("apc",  "")).strip().lower() in ["true", "yes", "1"]

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
                cats_dict: dict = {}
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

                journal.hasCategory = [Category(cid, q) for cid, q in sorted(cats_dict.items())]
                journal.hasArea     = [Area([aid]) for aid in sorted(areas_set)]
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

        ids: Set[str] = set()
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

        ids: Set[str] = set()
        for _, r in df.iterrows():
            try:
                for s in str(r.get("identifiers", "")).split(","):
                    s = s.strip()
                    if s:
                        ids.add(s)
            except Exception:
                continue
        return ids

    def _makeJournalsFromSQLiteIds(
        self,
        all_ids: Set[str],
        blazegraph_ids: Set[str],
        apc_filter: Optional[bool] = None,
    ) -> List[Journal]:
        # Builds minimal Journal objects for IDs found in SQLite but not in Blazegraph.
        # apc_filter: True = only apc journals, False = only non-apc, None = all.
        # SQLite-only journals have no APC info from DOAJ, so they are treated as apc=False.
        # If apc_filter=True, they are excluded; otherwise they are included.
        result: List[Journal] = []
        sqlite_only_ids = all_ids - blazegraph_ids
        if not sqlite_only_ids:
            return result

        for h in self.categoryHandlers:
            try:
                engine = _ce(f"sqlite:///{h.getDbPathOrUrl()}")
                df_sql = pd.read_sql(
                    "SELECT DISTINCT internalId, id FROM IdentifiableEntity WHERE internalId LIKE 'journal-%'",
                    engine
                )
                if df_sql.empty:
                    continue

                groups = defaultdict(list)
                for _, row in df_sql.iterrows():
                    groups[row["internalId"]].append(row["id"])

                seen: Set[str] = set()
                for internal_id, ids in groups.items():
                    if not any(i in sqlite_only_ids for i in ids):
                        continue
                    key = str(frozenset(ids))
                    if key in seen:
                        continue
                    seen.add(key)

                    if apc_filter is True:
                        continue  # SQLite-only journals can't have confirmed APC

                    cats_dict: dict = {}
                    areas_set: Set[str] = set()
                    for jid in ids:
                        try:
                            df_rel = h.getById(jid)
                            if df_rel is None or df_rel.empty:
                                continue
                            if "category_id" in df_rel.columns:
                                for _, rr in df_rel.iterrows():
                                    cid = rr.get("category_id")
                                    q = rr.get("category_quartile")
                                    if cid and str(cid).strip() and str(cid).strip() not in cats_dict:
                                        cats_dict[str(cid).strip()] = q
                            if "area_id" in df_rel.columns:
                                for _, rr in df_rel.iterrows():
                                    aid = rr.get("area_id")
                                    if aid and str(aid).strip():
                                        areas_set.add(str(aid).strip())
                        except Exception:
                            continue

                    result.append(Journal(
                        ids=sorted(ids),
                        title="",
                        languages=[],
                        publisher=None,
                        seal=False,
                        license="",
                        apc=False,
                        categories=[Category(cid, q) for cid, q in sorted(cats_dict.items())],
                        areas=[Area([aid]) for aid in sorted(areas_set)],
                    ))
            except Exception:
                continue

        return result

    def getJournalsInCategoriesWithQuartile(
        self,
        category_ids: Set[str],
        quartiles: Set[str],
    ) -> List[Journal]:
        all_ids: Set[str] = set()
        for h in self.categoryHandlers:
            try:
                all_ids.update(
                    self._collect_ids_from_category_assignments(h, category_ids, quartiles)
                )
            except Exception:
                continue

        if not all_ids:
            return []

        result: List[Journal] = []
        blazegraph_ids: Set[str] = set()

        # Journals found in Blazegraph
        for h in self.journalHandlers:
            try:
                df = h.getAllJournals()
                if df is None or df.empty:
                    continue
                mask = df["id"].apply(lambda x: self._id_matches(x, all_ids))
                matched = self._makeJournals(df[mask])
                for j in matched:
                    result.append(j)
                    blazegraph_ids.update(j.getIds())
            except Exception:
                continue

        # Journals present only in SQLite
        result.extend(self._makeJournalsFromSQLiteIds(all_ids, blazegraph_ids))
        return result

    def getJournalsInAreasWithLicense(
        self,
        areas: Set[str],
        licenses: Set[str],
    ) -> List[Journal]:
        all_ids: Set[str] = set()
        for h in self.categoryHandlers:
            try:
                all_ids.update(
                    self._collect_ids_from_area_assignments(h, areas)
                )
            except Exception:
                continue

        if not all_ids:
            return []

        result: List[Journal] = []
        blazegraph_ids: Set[str] = set()

        # Journals found in Blazegraph, filtered by license
        normalized_licenses = {lic.strip().lower() for lic in licenses if lic} if licenses else set()

        for h in self.journalHandlers:
            try:
                if licenses:
                    df = h.getJournalsWithLicense(licenses)
                else:
                    df = h.getAllJournals()

                if df is None or df.empty:
                    continue

                # Exact-match filter on license: some journals have multiple license triples
                # in Blazegraph (e.g. "CC BY" and "CC BY-NC"), so Yang returns one row per
                # license. We keep only rows whose license is exactly one of the requested ones.
                if normalized_licenses:
                    df = df[df["license"].apply(
                        lambda x: str(x).strip().lower() in normalized_licenses
                    )]
                    if df.empty:
                        continue

                id_col = "all_ids" if "all_ids" in df.columns else "id"
                mask = df[id_col].apply(lambda x: self._id_matches(x, all_ids))
                matched = self._makeJournals(df[mask])
                for j in matched:
                    result.append(j)
                    blazegraph_ids.update(j.getIds())
            except Exception:
                continue

        # SQLite-only journals have no license info from DOAJ.
        # Include them only when no license filter is applied (empty set = all licenses).
        if not licenses:
            result.extend(self._makeJournalsFromSQLiteIds(all_ids, blazegraph_ids))

        return result

    # ---------------------------------------------------------------
    # Laura's methods for Peroni
    # ---------------------------------------------------------------

    def getJournalByName(self, name: str) -> List[Journal]:
        name_clean = (name or "").strip()
        if not name_clean:
            return []

        # Step 1: journals whose title contains the input string (from Blazegraph)
        title_matches: List[Journal] = []
        for h in self.journalHandlers:
            try:
                df = h.getJournalsWithTitle(name_clean)
                if df is not None and not df.empty:
                    title_matches.extend(self._makeJournals(df))
            except Exception:
                continue

        # Step 2: category and area names that contain the input string (from SQLite)
        matching_category_names: Set[str] = set()
        matching_area_names: Set[str] = set()
        for h in self.categoryHandlers:
            try:
                df_cat = h.getCategoryWithName(name_clean)
                if df_cat is not None and not df_cat.empty:
                    matching_category_names.update(df_cat["category_id"].dropna().tolist())
                df_area = h.getAreaWithName(name_clean)
                if df_area is not None and not df_area.empty:
                    matching_area_names.update(df_area["id"].dropna().tolist())
            except Exception:
                continue

        # Step 3: keep only journals that have at least one matching category OR area
        result: List[Journal] = []
        seen: Set[frozenset] = set()
        for j in title_matches:
            cat_names = {c.getId()[0] for c in j.getCategories()}
            area_names = {a.getId()[0] for a in j.getAreas()}
            if cat_names & matching_category_names or area_names & matching_area_names:
                key = frozenset(j.getIds())
                if key not in seen:
                    seen.add(key)
                    result.append(j)

        return result

    # ---------------------------------------------------------------
    # end Laura's methods for Peroni
    # ---------------------------------------------------------------

    def getDiamondJournalsInAreasAndCategoriesWithQuartile(
        self,
        area_ids: Set[str],
        category_ids: Set[str],
        quartiles: Set[str],
    ) -> List[Journal]:
        ids_cat:  Set[str] = set()
        ids_area: Set[str] = set()

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

        # Intersection: journal must appear in BOTH the requested categories AND areas
        all_ids = ids_cat & ids_area

        if not all_ids:
            return []

        result: List[Journal] = []
        blazegraph_ids: Set[str] = set()

        # Journals found in Blazegraph, filtered by apc=False (diamond = no APC).
        # We track ALL Blazegraph IDs (including apc=True ones that are excluded)
        # so that SQLite does not re-add them as apc=False later.
        for h in self.journalHandlers:
            try:
                df = h.getAllJournals()
                if df is None or df.empty:
                    continue

                mask_ids = df["id"].apply(lambda x: self._id_matches(x, all_ids))
                df_candidates = df[mask_ids]

                # Register all candidate IDs as seen in Blazegraph (regardless of APC)
                for _, row in df_candidates.iterrows():
                    raw = row.get("id") or row.get("all_ids", "")
                    for s in str(raw).split(","):
                        s = s.strip()
                        if s:
                            blazegraph_ids.add(s)

                # Only keep diamond journals (apc=False)
                apc_mask = df_candidates["apc"].astype(str).str.lower().isin(["no", "false", "0", ""])
                matched = self._makeJournals(df_candidates[apc_mask])
                result.extend(matched)
            except Exception:
                continue

        # SQLite-only journals are treated as apc=False, so they qualify as diamond.
        # Blazegraph journals with apc=True are already in blazegraph_ids and won't be re-added.
        result.extend(self._makeJournalsFromSQLiteIds(all_ids, blazegraph_ids, apc_filter=False))
        return result
