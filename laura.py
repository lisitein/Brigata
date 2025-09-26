import pandas as pd  
from typing import List, Set, Optional, Union

class IdentifiableEntity:
    def __init__(self, id: str):
        self.id = id
    def getId(self) -> str:
        return self.id
    def getIds(self):
        return [self.id]

class Area(IdentifiableEntity):
    pass

class Category(IdentifiableEntity):
    def __init__(self, id: str, quartile: str):
        super().__init__(id)
        self.quartile = quartile
    def getQuartile(self) -> str:
        return self.quartile

class Journal:
    def __init__(self, id, title: str, languages=None, publisher: str=None,
                 seal: bool=False, license: str=None, apc: bool=False,
                 hasCategory=None, hasArea=None):
        self.ids = id if isinstance(id, list) else [id]
        self.title = title or ""
        self.languages = languages or []
        self.publisher = publisher
        self.seal = bool(seal)
        self.license = license
        self.apc = bool(apc)
        self.hasCategory = hasCategory or []
        self.hasArea = hasArea or []
    def getIds(self): return self.ids
    def getId(self): return self.ids[0] if self.ids else None
    def getTitle(self): return self.title
    def getPublisher(self): return self.publisher
    def getLicense(self): return self.license
    def getAPC(self): return self.apc
    def getDOAJSeal(self): return self.seal
    def __repr__(self): return f"Journal(id={self.ids}, title={self.title!r})"

class BasicQueryEngine:
    def __init__(self, journalHandlers=None, categoryHandlers=None):
        self.journalHandlers = journalHandlers or []
        self.categoryHandlers = categoryHandlers or []
    def _empty_journal_df(self):
        return pd.DataFrame(columns=["id", "title", "publisher", "apc", "seal", "license"])
    def _concat_journal_dfs(self, dfs):
        if not dfs: return self._empty_journal_df()
        dfs = [df if df is not None and not df.empty else self._empty_journal_df() for df in dfs]
        all_cols = set().union(*[set(d.columns) for d in dfs]) if dfs else set()
        dfs = [d.reindex(columns=list(all_cols)) for d in dfs]
        out = pd.concat(dfs, ignore_index=True).drop_duplicates()
        for col in ["apc", "seal"]:
            if col in out.columns:
                out[col] = out[col].apply(lambda x: str(x).strip().lower() in ("1","true","yes"))
        return out
    def _makeJournals(self, df: pd.DataFrame):
        if df is None or df.empty: return []
        df = df.copy()
        for col in ["id","title","publisher","apc","seal","license","languages","hasCategory","hasArea"]:
            if col not in df.columns: df[col] = None
        journals = []
        for _, r in df.iterrows():
            jid = r["id"]
            if not isinstance(jid, list): jid = [jid] if pd.notna(jid) else []
            journals.append(Journal(
                id=jid, title=r["title"] or "", languages=r["languages"] if isinstance(r["languages"], list) else [],
                publisher=r["publisher"],
                seal=(str(r["seal"]).strip().lower() in ("1","true","yes")) if r["seal"] is not None else False,
                license=r["license"],
                apc=(str(r["apc"]).strip().lower() in ("1","true","yes")) if r["apc"] is not None else False,
                hasCategory=r["hasCategory"] if isinstance(r["hasCategory"], list) else [],
                hasArea=r["hasArea"] if isinstance(r["hasArea"], list) else []
            ))
        return journals

    # ===== Dataset export helpers =====
    @staticmethod
    def journals_to_dataframe(journals: list['Journal']) -> pd.DataFrame:
        rows = []
        for j in journals:
            rows.append({
                "id": "|".join([i for i in j.getIds() if i]),
                "title": j.getTitle(),
                "publisher": j.getPublisher(),
                "license": j.getLicense(),
                "apc": j.getAPC(),
                "seal": j.getDOAJSeal(),
                "languages": "|".join(j.languages) if isinstance(j.languages, list) else "",
                "hasCategory": "|".join(j.hasCategory) if isinstance(j.hasCategory, list) else "",
                "hasArea": "|".join(j.hasArea) if isinstance(j.hasArea, list) else "",
            })
        return pd.DataFrame(rows, columns=[
            "id","title","publisher","license","apc","seal","languages","hasCategory","hasArea"
        ])

    @staticmethod
    def export_journals(journals: list['Journal'], path: str, fmt: str="csv") -> str:
        df = BasicQueryEngine.journals_to_dataframe(journals)
        fmt = (fmt or "csv").lower()
        if fmt == "csv":
            df.to_csv(path, index=False)
        elif fmt == "json":
            df.to_json(path, orient="records", force_ascii=False, indent=2)
        else:
            raise ValueError("Unsupported format. Use 'csv' or 'json'.")
        return path

    # ---- Basic journal queries (graph side) ----
    def getAllJournals(self):
        dfs = [h.getAllJournals() for h in self.journalHandlers]
        return self._makeJournals(self._concat_journal_dfs(dfs))
    def getJournalsWithTitle(self, term: str):
        dfs = [h.getJournalsWithTitle(term) for h in self.journalHandlers]
        return self._makeJournals(self._concat_journal_dfs(dfs))
    def getJournalsPublishedBy(self, publisher: str):
        dfs = [h.getJournalsPublishedBy(publisher) for h in self.journalHandlers]
        return self._makeJournals(self._concat_journal_dfs(dfs))
    def getJournalsWithLicense(self, license_str: str):
        dfs = [h.getJournalsWithLicense(license_str) for h in self.journalHandlers]
        return self._makeJournals(self._concat_journal_dfs(dfs))
    def getJournalsWithAPC(self, apc: bool=True):
        dfs = [h.getJournalsWithAPC(apc) for h in self.journalHandlers]
        return self._makeJournals(self._concat_journal_dfs(dfs))
    def getJournalsWithDOAJSeal(self, seal: bool=True):
        dfs = [h.getJournalsWithDOAJSeal(seal) for h in self.journalHandlers]
        return self._makeJournals(self._concat_journal_dfs(dfs))

class FullQueryEngine(BasicQueryEngine):
    def getEntityById(self, id: str):
        for h in self.journalHandlers:
            df = h.getById(id)
            if df is not None and not df.empty:
                js = self._makeJournals(df)
                if js: return js[0]
        for h in self.categoryHandlers:
            df = h.getById(id)
            if df is not None and not df.empty:
                r = df.iloc[0]
                if "quartile" in df.columns and pd.notna(r.get("quartile", None)):
                    return Category(r["id"], r["quartile"])
                return Area(r["id"])
        return None

    def getAllAreas(self):
        import pandas as pd
        dfs = [h.getAllAreas() for h in self.categoryHandlers]
        if not dfs: return []
        df = pd.concat(dfs, ignore_index=True).drop_duplicates()
        return [Area(r["id"]) for _, r in df.iterrows()]

    def getAllCategories(self):
        import pandas as pd
        dfs = [h.getAllCategories() for h in self.categoryHandlers]
        if not dfs: return []
        df = pd.concat(dfs, ignore_index=True).drop_duplicates()
        return [Category(r["id"], r["quartile"]) for _, r in df.iterrows()]

    def getCategoriesWithQuartile(self, quartiles:set):
        import pandas as pd
        dfs = [h.getCategoriesWithQuartile(quartiles) for h in self.categoryHandlers]
        if not dfs: return []
        df = pd.concat(dfs, ignore_index=True).drop_duplicates()
        return [Category(r["id"], r["quartile"]) for _, r in df.iterrows()]

    def _ids_from_relational(self, df_ids, col="identifiers"):
        import pandas as pd
        if df_ids is None or df_ids.empty or col not in df_ids.columns:
            return set()
        out = set()
        for val in df_ids[col].astype(str).tolist():
            if val is None or val == "nan": continue
            if isinstance(val, str) and ("," in val or "|" in val):
                for piece in val.replace("|", ",").split(","):
                    p = piece.strip()
                    if p: out.add(p)
            else:
                out.add(val.strip())
        return out

    def _filter_journals_by_external_ids(self, ids:set):
        if not ids: return []
        all_j = self.getAllJournals()
        picked = []
        idset = set(ids)
        for j in all_j:
            if set([i for i in j.getIds() if i]).intersection(idset):
                picked.append(j)
        return picked

    def getJournalsInCategoriesWithQuartile(self, quartiles:set):
        import pandas as pd
        dfs = [h.getAllCategoryAssignments() for h in self.categoryHandlers]
        if not dfs: return []
        df_rel = pd.concat(dfs, ignore_index=True).drop_duplicates()
        if quartiles:
            df_rel = df_rel[df_rel["category_quartile"].isin(list(quartiles))]
        ids = self._ids_from_relational(df_rel, col="identifiers")
        return self._filter_journals_by_external_ids(ids)

    def getJournalsInAreasWithLicense(self, areas:set, license_str:str):
        import pandas as pd
        dfs = [h.getAllAreaAssignments() for h in self.categoryHandlers]
        if not dfs: return []
        df_rel = pd.concat(dfs, ignore_index=True).drop_duplicates()
        if areas:
            df_rel = df_rel[df_rel["area"].isin(list(areas))]
        ids = self._ids_from_relational(df_rel, col="identifiers")
        if not ids: return []
        dfs_j = [h.getJournalsWithLicense(license_str) for h in self.journalHandlers]
        df_j = self._concat_journal_dfs(dfs_j)
        if df_j.empty: return []
        df_j = df_j[df_j["id"].astype(str).isin(ids)]
        return self._makeJournals(df_j)

    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, areas:set, quartiles:set):
        import pandas as pd
        dfs_cat = [h.getAllCategoryAssignments() for h in self.categoryHandlers]
        dfs_area = [h.getAllAreaAssignments() for h in self.categoryHandlers]
        if not dfs_cat or not dfs_area: return []
        df_cat = pd.concat(dfs_cat, ignore_index=True).drop_duplicates()
        df_area = pd.concat(dfs_area, ignore_index=True).drop_duplicates()
        if quartiles:
            df_cat = df_cat[df_cat["category_quartile"].isin(list(quartiles))]
        if areas:
            df_area = df_area[df_area["area"].isin(list(areas))]
        ids = self._ids_from_relational(df_cat, "identifiers") & self._ids_from_relational(df_area, "identifiers")
        if not ids: return []
        dfs_j = [h.getJournalsWithAPC(False) for h in self.journalHandlers]
        df_j = self._concat_journal_dfs(dfs_j)
        if df_j.empty: return []
        df_j = df_j[df_j["id"].astype(str).isin(ids)]
        return self._makeJournals(df_j)
