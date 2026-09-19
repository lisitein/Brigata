from typing import List, Set, Optional, Dict
from collections import defaultdict
import pandas as pd
from sqlalchemy import create_engine as _ce
from Yang import JournalQueryHandler, CategoryQueryHandler
from tqdm.auto import tqdm


# ============================
# DATA MODEL
# ============================

class IdentifiableEntity:
    def __init__(self, ids: List[str]):
        self.id = ids

    def getId(self) -> List[str]:
        return self.id

    def getIds(self) -> List[str]:
        return self.id

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

    def query_relational(self, query):
        lista_df = []
        for h in self.categoryHandlers:
            engine = _ce(f"sqlite:///{h.getDbPathOrUrl()}")
            with engine.connect() as connection:
                df_parziale = pd.read_sql(query, con=connection)
                lista_df.append(df_parziale)
            #engine.dispose()

        if lista_df:
            df_finale = pd.concat(lista_df, ignore_index=True).drop_duplicates()
        else:
            df_finale = pd.DataFrame()

        return df_finale

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

    def _license_matches(self, license_str: str, requested: Set[str]) -> bool:
        # Split stored license string on commas and check each token.
        # e.g. "CC BY, CC0" -> {"cc by", "cc0"}
        # "CC BY-NC" does NOT match {"CC BY"} because tokens differ.
        tokens = {t.strip().lower() for t in str(license_str).split(',')}
        return any(lic.strip().lower() in tokens for lic in requested)

    def complete_ids(self, t_result, set_ids_to_output):
        for j in t_result:
            obj_ids = j.getIds()
            obj_ids_check = set(obj_ids)
            tutti_id_lista = list(set_ids_to_output)
            if obj_ids_check not in tutti_id_lista:
                for el in tutti_id_lista:
                    for i in obj_ids:
                        if i in el:
                            print(i,'\t =>  ',el, type(i), type(el))
                            j.id=list(el)
        return t_result

    def enrich_entity_with_categories_and_areas(self, obj: IdentifiableEntity, id: str) -> IdentifiableEntity:
        lista_df = []

        for h in self.categoryHandlers:
            d_parziale = h.getById(id)
            lista_df.append(d_parziale)

        if lista_df:
            df_finale = pd.concat(lista_df, ignore_index=True).drop_duplicates()
        else:
            df_finale = pd.DataFrame()


        if df_finale is None or df_finale.empty:
            return obj
        obj.hasCategory = [Category(c,q) for c,q in df_finale[['category_id', 'category_quartile']].drop_duplicates().values]
        obj.hasArea     = [Area([a]) for a in df_finale['area_id'].drop_duplicates().values]
        return obj

    def enrich_journals_with_categories_and_areas(self, output_journals, tutti_gli_id_validi):

        journal_ids_for_query ='(' + ', '.join(f"'{t}'" for t in tutti_gli_id_validi) + ')'

        query_for_areas='''SELECT DISTINCT  ie1.internalId AS journal_internal_id,
                                            ie1.id AS journal_id,
                                            ie2.id AS area_id
                                    FROM IdentifiableEntity AS ie1
                                        LEFT JOIN HasArea AS ha
                                            ON ie1.internalId == ha.journalId
                                        LEFT JOIN IdentifiableEntity AS ie2
                                        ON ha.areaId == ie2.internalId
                                    WHERE ie1.id IN ''' + journal_ids_for_query

        df_with_areas = self.query_relational(query_for_areas)


        query_for_categories='''SELECT DISTINCT ie1.internalId AS journal_internal_id,
                                                ie1.id AS journal_id,
                                                ie2.id AS category_id,
                                                ie2.quartile
                                        FROM IdentifiableEntity AS ie1
                                            LEFT JOIN HasCategory AS hc
                                                ON ie1.internalId == hc.journalId
                                            LEFT JOIN IdentifiableEntity AS ie2
                                            ON hc.categoryId == ie2.internalId
                                        WHERE ie1.id IN''' + journal_ids_for_query

        df_with_categories = self.query_relational(query_for_categories)

        for obj in tqdm(output_journals, desc='Enriching journals'):
            obj_ids = obj.getIds()

            for obj_id in obj_ids:
                df_with_areas_filtered = df_with_areas[df_with_areas['journal_id'] == obj_id]
                obj.hasArea     = [Area([a]) for a in df_with_areas_filtered['area_id'].drop_duplicates().values]
                df_with_categories_filtered = df_with_categories[df_with_categories['journal_id'] == obj_id]
                obj.hasCategory = [Category(c,q) for c,q in df_with_categories_filtered[['category_id', 'quartile']].drop_duplicates().values]
                if obj.hasCategory or obj.hasArea:
                    break 
           
        return output_journals

    def add_missing_ids_daniele(self, journals):
        query = 'SELECT * FROM IdentifiableEntity'
        df = self.query_relational(query)
        for j in journals:
            for id in j.getIds():
                if id in df['id']:
                    id_to_use = id
            internal_id = df.loc[df['id'] == id_to_use, 'internalId'].values[0]
            #print(internal_id)
            df_f = df[df['internalId'] == internal_id]
            a=set(j.getIds())
            b=set(df_f['id'])
            if not a.isdisjoint(b) and not b.issubset(a):
                new_ids = a.union(b)
                j.id=list(new_ids)
        return journals

    def add_missing_ids(self, journals):
        query = 'SELECT * FROM IdentifiableEntity'
        df = self.query_relational(query)
        
        df_id_set = set(df['id'])
        
        for j in tqdm(journals, desc='Completing ids'):
            j_ids = j.getIds()
            
            if not any(id in df_id_set for id in j_ids):
                continue
                
            id_to_use = None
            for id in j_ids:
                if id in df_id_set:
                    id_to_use = id
            
            internal_id = df.loc[df['id'] == id_to_use, 'internalId'].values[0]
            #print(internal_id)
            
            df_f = df[df['internalId'] == internal_id]
            a = set(j_ids)
            b = set(df_f['id'])
            
            if not a.isdisjoint(b) and not b.issubset(a):
                new_ids = a.union(b)
                j.id = list(new_ids)
                

        lista_df = []
        for h in self.journalHandlers:      
            df_parziale = h.getAllJournals()
            lista_df.append(df_parziale)

        if lista_df:
            df_graph = pd.concat(lista_df, ignore_index=True).drop_duplicates()
        else:
            df_graph = pd.DataFrame()

        for j in tqdm(journals, desc='Completing journals'):
            if j.getTitle() == '':
                possible_ids='|'.join(j.getIds())
                j_to_complete = df_graph[df_graph['id'].str.contains(possible_ids, na=False)]
                for _, r in j_to_complete.iterrows():
                    j = self._completeJournal(j, r)

        return journals




    # -----------------------------------------
    # Entity lookup
    # -----------------------------------------

    def getEntityById_core(self, id: str):

        lista_df_graph = []


        # 1. Try journals via SPARQL
        for h in self.journalHandlers:
            df_parziale = h.getById(id)
            lista_df_graph.append(df_parziale)

        if lista_df_graph:
            df_finale_graph = pd.concat(lista_df_graph, ignore_index=True).drop_duplicates()
        else:
            df_finale_graph = pd.DataFrame()

        if df_finale_graph is not None and not df_finale_graph.empty:
            journals = self._makeJournals(df_finale_graph)
            for j in journals:
                if id in j.getIds():
                    return j

        # 2. Try categories and areas via SQLite
        lista_df = []

        for h in self.categoryHandlers:
            df_parziale = h.getById(id)
            lista_df.append(df_parziale)

        if lista_df:
            df = pd.concat(lista_df, ignore_index=True).drop_duplicates()
        else:
            df = pd.DataFrame()



#        if df is None or df.empty:
#            continue
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

        return None

    def getEntityById(self, id: str):
        entity=self.getEntityById_core(id)
        if type(entity)==Journal:
            entity=self.enrich_entity_with_categories_and_areas(entity, id)
            entity = self.add_missing_ids([entity])[0]
        return entity

    # -----------------------------------------
    # Journal queries
    # -----------------------------------------

    def getAllJournals(self) -> List['Journal']:
        result: List[Journal] = []
        seen_ids: Set[str] = set()

        # 1. Journals from Blazegraph (DOAJ)

        for h in self.journalHandlers:
            df = h.getAllJournals()
            if df is not None and not df.empty:
                journals = self._makeJournals(df)
                for j in journals:
                    result.append(j)
                    for jid in j.getIds():
                        seen_ids.add(jid)


        '''we don't need the journals in the relational database, this is why I commented the following lines:

        # 2. Journals present only in SQLite (Scimago) — not in Blazegraph
        for h in self.categoryHandlers:
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

            
            for internal_id, ids in tqdm(groups.items(), desc='Processing SQLite-only journals', total=len(groups), unit='journal'):
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
            '''

        return self.add_missing_ids(list(set(self.enrich_journals_with_categories_and_areas(result, seen_ids))))

    def getJournalsWithTitle(self, title: str) -> List['Journal']:
        result: List[Journal] = []
        seen_ids: Set[str] = set()

        for h in self.journalHandlers:
            df = h.getJournalsWithTitle(title)
            if df is not None and not df.empty:
                result.extend(self._makeJournals(df))
            for j in result:
                for jid in j.getIds():
                    seen_ids.add(jid)

        return self.add_missing_ids(list(set(self.enrich_journals_with_categories_and_areas(result, seen_ids))))

    def getJournalsPublishedBy(self, publisher: str) -> List['Journal']:
        result: List[Journal] = []
        seen_ids: Set[str] = set()

        for h in self.journalHandlers:
            df = h.getJournalsPublishedBy(publisher)
            if df is not None and not df.empty:
                result.extend(self._makeJournals(df))
            for j in result:
                for jid in j.getIds():
                    seen_ids.add(jid)

        return self.add_missing_ids(list(set(self.enrich_journals_with_categories_and_areas(result, seen_ids))))

    def getJournalsWithLicense(self, licenses: Set[str]) -> List['Journal']:
        if not licenses:
            return []
        result: List[Journal] = []
        seen: Set[frozenset] = set()
        for h in self.journalHandlers:
            df = h.getAllJournals()
            if df is None or df.empty:
                continue
            df = df[df["license"].apply(
                lambda x: self._license_matches(x, licenses)
            )]
            if not df.empty:
                for j in self._makeJournals(df):
                    key = frozenset(j.getIds())
                    if key not in seen:
                        seen.add(key)
                        result.append(j)
        
        seen_ids = set().union(*seen)
        
        return self.add_missing_ids(list(set(self.enrich_journals_with_categories_and_areas(result, seen_ids))))

    def getJournalsWithAPC(self) -> List['Journal']:
        result: List[Journal] = []
        seen_ids: Set[str] = set()
        for h in self.journalHandlers:
            df = h.getJournalsWithAPC()
            if df is not None and not df.empty:
                result.extend(self._makeJournals(df))
            for j in result:
                for jid in j.getIds():
                    seen_ids.add(jid)

        return self.add_missing_ids(list(set(self.enrich_journals_with_categories_and_areas(result, seen_ids))))

    def getJournalsWithDOAJSeal(self) -> List['Journal']:
        result: List[Journal] = []
        seen_ids: Set[str] = set()
        for h in self.journalHandlers:
            df = h.getJournalsWithDOAJSeal()
            if df is not None and not df.empty:
                result.extend(self._makeJournals(df))
            for j in result:
                for jid in j.getIds():
                    seen_ids.add(jid)

        return self.add_missing_ids(list(set(self.enrich_journals_with_categories_and_areas(result, seen_ids))))

    # -----------------------------------------
    # Category and Area queries
    # -----------------------------------------

    def getAllCategories(self) -> List[Category]:
        result: List[Category] = []
        for h in self.categoryHandlers:
            df = h.getAllCategories()
            if df is None or df.empty:
                continue
            df = df.drop_duplicates(subset=["category_id","quartile"])
            for _, r in df.iterrows():
                result.append(Category(r["category_id"], r.get("quartile")))
        return list(set(result))

    def getCategoriesWithQuartile(self, quartiles: Set[str]) -> List[Category]:
        result: List[Category] = []
        for h in self.categoryHandlers:
            df = h.getCategoriesWithQuartile(quartiles)
            if df is None or df.empty:
                continue
            for _, r in df.iterrows():
                result.append(Category(r["category_id"], r.get("quartile")))
        return list(set(result))

    def getAllAreas(self) -> List[Area]:
        result: List[Area] = []
        for h in self.categoryHandlers:
            df = h.getAllAreas()
            if df is None or df.empty:
                continue
            col = "area_id" if "area_id" in df.columns else "id"
            df = df.drop_duplicates(subset=[col])
            for _, r in df.iterrows():
                result.append(Area([r[col]]))
        return list(set(result))

    def getCategoriesAssignedToAreas(self, areas: Set[str]) -> List[Category]:
        result: List[Category] = []
        for h in self.categoryHandlers:
            df = h.getCategoriesAssignedToAreas(areas)
            if df is None or df.empty:
                continue
            for _, r in df.iterrows():
                cid = r.get("category_id")
                if cid:
                    result.append(Category(cid, r.get("quartile")))
        return list(set(result))

    def getAreasAssignedToCategories(self, categories: Set[str]) -> List[Area]:
        result: List[Area] = []
        for h in self.categoryHandlers:
            df = h.getAreasAssignedToCategories(categories)
            if df is None or df.empty:
                continue
            for _, r in df.iterrows():
                aid = r.get("area_id")
                if aid:
                    result.append(Area([aid]))
        return list(set(result))

    # -----------------------------------------
    # Journal construction
    # -----------------------------------------

    def _makeJournals(self, df: pd.DataFrame) -> List['Journal']:
        journals: List[Journal] = []
        if df is None or df.empty:
            return journals

        for _, r in tqdm(df.iterrows(), desc='Making Journal objects', total=len(df), unit='journal'):
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

            
            '''
            
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
            '''

            journals.append(journal)

        return journals

    def _completeJournal(self, journal, r) -> List['Journal']:

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

        journal.languages = langs
        journal.publisher = publisher
        journal.seal = seal
        journal.licence = license_clean
        journal.apc = apc
        journal.title = title

        return journal

# ============================
# FULL QUERY ENGINE
# ============================

class FullQueryEngine(BasicQueryEngine):

    def intersect(self, rel, graph):

        r = {
                frozenset(f1 | f2) 
                for f1 in tqdm(list(rel), desc="Intersecting journals ids")
                for f2 in graph 
                if not f1.isdisjoint(f2)
            }

        return r
    

    # Funzione per verificare se la cella contiene almeno un ID valido
    def controlla_id(self, cella, id_validi):
        # Trasforma la cella in un set di stringhe pulite
        set_cella = set(i.strip() for i in str(cella).split(','))
        # Se l'intersezione tra i due set NON è vuota, restituisce True
        return bool(set_cella & id_validi)

    def get_journals_ids_based_on_categories_and_quartiles(self, category_ids: Set[str], quartiles: Set[str]) -> Set[str]:

        condizioni = []

        if category_ids:
            cat_string = ', '.join(f"'{cid}'" for cid in category_ids)
            condizioni.append(f"id IN ({cat_string})")

        if quartiles:
            quart_string = ', '.join(f"'{q}'" for q in quartiles)
            condizioni.append(f"quartile IN ({quart_string})")

        query = '''
            SELECT *
            FROM IdentifiableEntity 
            LEFT JOIN HasCategory ON IdentifiableEntity.internalId = HasCategory.categoryId
        '''
        
        if condizioni:
            query += " WHERE " + " AND ".join(condizioni)
        else:
            query += " WHERE internalId LIKE 'category-%' "

        df_finale=self.query_relational(query)
        

        
        

        journals_internal_ids=list(set(df_finale['journalId']))
        l='(' + ', '.join(f"'{t}'" for t in journals_internal_ids) + ')'

        query_for_journals_ids='''
            SELECT ie.internalId, ie.id
            FROM IdentifiableEntity as ie
            WHERE ie.internalId IN
        ''' + l

        ids_relational = self.query_relational(query_for_journals_ids)

        set_ids_relational = set()

        for elem in journals_internal_ids:
            df_filtered = ids_relational[ids_relational['internalId'] == elem]
            current_set = set()
            for id in df_filtered['id']:
                current_set.add(id)
            set_ids_relational.add(frozenset(current_set))

        return set_ids_relational

    def get_journals_ids_based_on_areas(self, areas_ids: Set[str]) -> Set[str]:
        
        query = '''
            SELECT *
            FROM IdentifiableEntity 
            LEFT JOIN HasArea ON IdentifiableEntity.internalId = HasArea.areaId
        '''

        if areas_ids:
            area_string = ', '.join(f"'{cid}'" for cid in areas_ids)
            condizione = f"id IN ({area_string})"
            query += " WHERE " + condizione
        else:
            query += " WHERE internalId LIKE 'area-%' "

        df_finale=self.query_relational(query)

        
        

        journals_internal_ids=list(set(df_finale['journalId']))
        l='(' + ', '.join(f"'{t}'" for t in journals_internal_ids) + ')'

        query_for_journals_ids='''
            SELECT ie.internalId, ie.id
            FROM IdentifiableEntity as ie
            WHERE ie.internalId IN
        ''' + l

        ids_relational = self.query_relational(query_for_journals_ids)

        set_ids_relational = set()

        for elem in tqdm(journals_internal_ids):
            df_filtered = ids_relational[ids_relational['internalId'] == elem]
            current_set = set()
            for id in df_filtered['id']:
                current_set.add(id)
            set_ids_relational.add(frozenset(current_set))

        return set_ids_relational


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

    def _makeJournalsFromSQLiteIds(         # THIS DOES NOT WORK
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

        return result

    def getJournalsInCategoriesWithQuartile_by_laura(
        self,
        category_ids: Set[str],
        quartiles: Set[str],
    ) -> List[Journal]:
        all_ids: Set[str] = set()
        for h in self.categoryHandlers:
            all_ids.update(
                self._collect_ids_from_category_assignments(h, category_ids, quartiles)
            )

        if not all_ids:
            return []

        result: List[Journal] = []
        blazegraph_ids: Set[str] = set()

        # Journals found in Blazegraph
        for h in self.journalHandlers:
            df = h.getAllJournals()
            if df is None or df.empty:
                continue
            mask = df["id"].apply(lambda x: self._id_matches(x, all_ids))
            matched = self._makeJournals(df[mask])
            for j in matched:
                result.append(j)
                blazegraph_ids.update(j.getIds())

        # Journals present only in SQLite
        result.extend(self._makeJournalsFromSQLiteIds(all_ids, blazegraph_ids))
        return result


    def getJournalsInCategoriesWithQuartile(
        self,
        category_ids: Set[str],
        quartiles: Set[str],
    ) -> List[Journal]:


        set_ids_relational = self.get_journals_ids_based_on_categories_and_quartiles(category_ids, quartiles)

        lista_df = []
        for h in self.journalHandlers:      
            df_parziale = h.getAllJournals()
            lista_df.append(df_parziale)

        if lista_df:
            df_graph = pd.concat(lista_df, ignore_index=True).drop_duplicates()
        else:
            df_graph = pd.DataFrame()
        

        set_ids_graph = {frozenset(i.strip() for i in str(cella).split(',')) for cella in df_graph['id']}

        print(len(set_ids_relational), len(set_ids_graph))

        #set_ids_to_output = set_ids_relational & set_ids_graph
        #print('Previous Len:', len(set_ids_to_output))

        set_ids_to_output = self.intersect(set_ids_relational, set_ids_graph)
        print('Intersection Len:', len(set_ids_to_output))



        tutti_gli_id_validi = set().union(*set_ids_to_output)

        output_journals = self._makeJournals(df_graph[df_graph['id'].apply(lambda x: self.controlla_id(x, tutti_gli_id_validi))])

        final_result = list(set(self.enrich_journals_with_categories_and_areas(output_journals, tutti_gli_id_validi)))

        #return self.complete_ids(final_result, set_ids_to_output)
        return self.add_missing_ids(final_result)

    def getJournalsInAreasWithLicense(
        self,
        areas_ids: Set[str],
        licenses: Set[str],
    ) -> List[Journal]:


        set_ids_relational = self.get_journals_ids_based_on_areas(areas_ids)

        lista_df = []
        if licenses:
            for h in self.journalHandlers:     
                df_parziale = h.getJournalsWithLicense(licenses)
                lista_df.append(df_parziale)
        else:
            for h in self.journalHandlers:      
                df_parziale = h.getAllJournals()
                lista_df.append(df_parziale)

        if lista_df:
            df_graph = pd.concat(lista_df, ignore_index=True).drop_duplicates()
        else:
            df_graph = pd.DataFrame()



        set_ids_graph = {frozenset(i.strip() for i in str(cella).split(',')) for cella in df_graph['id']}

        print(len(set_ids_relational), len(set_ids_graph))
        set_ids_to_output = self.intersect(set_ids_relational, set_ids_graph)
        print('Intersection Len:', len(set_ids_to_output))


        tutti_gli_id_validi = set().union(*set_ids_to_output)

        output_journals = self._makeJournals(df_graph[df_graph['id'].apply(lambda x: self.controlla_id(x, tutti_gli_id_validi))])

        final_result = list(set(self.enrich_journals_with_categories_and_areas(output_journals, tutti_gli_id_validi)))

        #return self.complete_ids(final_result, set_ids_to_output)
        return self.add_missing_ids(final_result)


    def getJournalsInAreasWithLicense_by_laura(
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
                        lambda x: self._license_matches(x, licenses)
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

        # _makeJournals no longer fills in hasCategory/hasArea by itself (that
        # is now done separately by enrich_journals_with_categories_and_areas),
        # so without this step every journal here would have empty categories
        # and areas and step 3 below would never keep anything.
        title_ids: Set[str] = set()
        for j in title_matches:
            title_ids.update(j.getIds())
        title_matches = self.enrich_journals_with_categories_and_areas(title_matches, title_ids)

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

    def getDiamondJournalsInAreasAndCategoriesWithQuartile_by_laura(
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

    def getDiamondJournalsInAreasAndCategoriesWithQuartile(
        self,
        area_ids: Set[str],
        category_ids: Set[str],
        quartiles: Set[str],
    ) -> List[Journal]:


        set_ids_relational_categories_quartiles = self.get_journals_ids_based_on_categories_and_quartiles(category_ids, quartiles)
        set_ids_relational_areas = self.get_journals_ids_based_on_areas(area_ids)

        lista_df = []
        for h in self.journalHandlers:     
            df_parziale = h.getAllJournals()
            lista_df.append(df_parziale)


        if lista_df:
            df_graph = pd.concat(lista_df, ignore_index=True).drop_duplicates()
        else:
            df_graph = pd.DataFrame()



        df_graph = df_graph[df_graph['apc']=='false']

        set_ids_graph = {frozenset(i.strip() for i in str(cella).split(',')) for cella in df_graph['id']}

        print(len(set_ids_relational_categories_quartiles), len(set_ids_relational_areas), len(set_ids_graph))
        set_ids_to_output_partial = self.intersect(set_ids_relational_categories_quartiles, set_ids_relational_areas)
        set_ids_to_output = self.intersect(set_ids_to_output_partial, set_ids_graph)
        print('Intersection Len:', len(set_ids_to_output))


        tutti_gli_id_validi = set().union(*set_ids_to_output)

        output_journals = self._makeJournals(df_graph[df_graph['id'].apply(lambda x: self.controlla_id(x, tutti_gli_id_validi))])

        final_result = list(set(self.enrich_journals_with_categories_and_areas(output_journals, tutti_gli_id_validi)))

        #return self.complete_ids(final_result, set_ids_to_output)
        return self.add_missing_ids(final_result)
