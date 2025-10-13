import pandas as pd
from typing import List, Set, Optional, Union

# DATA MODEL

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
    def __init__(self, id: List[str], title: str, languages: List[str],
                 publisher: Optional[str], seal: bool, license: Optional[str],
                 apc: bool, hasCategory: List[str], hasArea: List[str]):
        super().__init__(id[0] if id else '')
        self.identifiers = id
        self.title = title
        self.languages = languages
        self.publisher = publisher
        self.seal = seal
        self.license = license
        self.apc = apc
        self.hasCategory = hasCategory
        self.hasArea = hasArea
                     
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

# BASIC QUERY ENGINE

class BasicQueryEngine:
    def __init__(self):
        self.journalHandlers = []
        self.categoryHandlers = []

    # Handler registration
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

    # Journal queries
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

    # Category and Area queries
    def getAllCategories(self) -> List[Category]:
        result = []
        for h in self.categoryHandlers:
            df = h.getAllCategories()
            if not df.empty:
                df = df.drop_duplicates(subset=['category_id'])
                result.extend([Category(r['category_id'], r.get('quartile')) for _, r in df.iterrows()])
        return result

    def getCategoriesWithQuartile(self, quartiles: Set[str]) -> List[Category]:
        result = []
        for h in self.categoryHandlers:
            df = h.getCategoriesWithQuartile(quartiles)
            if not df.empty:
                result.extend([Category(r['category_id'], r['quartile']) for _, r in df.iterrows()])
        return result

    def getAllAreas(self) -> List[Area]:
        result = []
        for h in self.categoryHandlers:
            df = h.getAllAreas()
            if not df.empty:
                df = df.drop_duplicates(subset=['area_id'])
                result.extend([Area(r['area_id']) for _, r in df.iterrows()])
        return result

    # Helper method: convert DataFrame rows into Journal objects
    def _makeJournals(self, df: pd.DataFrame) -> List[Journal]:
        if df.empty:
            return []
        return [
            Journal(
                id=r['id'] if isinstance(r['id'], list) else [r['id']],
                title=r.get('title', ''),
                languages=r.get('languages', []),
                publisher=r.get('publisher'),
                seal=bool(r.get('seal', False)),
                license=r.get('license'),
                apc=str(r.get('apc', '')).lower() in ["true", "yes"],
                hasCategory=r.get('hasCategory', []),
                hasArea=r.get('hasArea', [])
            )
            for _, r in df.iterrows()
        ]

# FULL QUERY ENGINE

class FullQueryEngine(BasicQueryEngine):

    def getEntityById(self, id: str) -> Optional[Union[Journal, Category, Area]]:
        # Search among Journals
        for h in self.journalHandlers:
            df = h.getById(id)
            if not df.empty:
                return self._makeJournals(df)[0]
        # Search among Categories and Areas
        for h in self.categoryHandlers:
            df = h.getById(id)
            if not df.empty:
                if 'category_id' in df.columns:
                    return Category(df.iloc[0]['category_id'], df.iloc[0].get('quartile'))
                elif 'area_id' in df.columns:
                    return Area(df.iloc[0]['area_id'])

        return None

    def getCategoriesAssignedToAreas(self, areas: Set[str]) -> List[Category]:
        result = []
        for h in self.categoryHandlers:
            df = h.getCategoriesAssignedToAreas(areas)
            if not df.empty:
                result.extend([Category(r['category_id'], r['quartile']) for _, r in df.iterrows()])
        return result

    def getAreasAssignedToCategories(self, categories: Set[str]) -> List[Area]:
        result = []
        for h in self.categoryHandlers:
            df = h.getAreasAssignedToCategories(categories)
            if not df.empty:
                result.extend([Area(r['area_id']) for _, r in df.iterrows()])
        return result

    def getJournalsInCategoriesWithQuartile(self, category_ids: Set[str], quartiles: Set[str]) -> List[Journal]:
        all_ids = set()
        for h in self.categoryHandlers:
            df = h.getAllAssignments()  # metodo standard nei test
            if not df.empty:
                if category_ids:
                    df = df[df['category_id'].isin(category_ids)]
                if quartiles:
                    df = df[df['quartile'].isin(quartiles)]
                all_ids.update(df['id'].dropna().tolist())

        if not all_ids:
            return []

        result = []
        for h in self.journalHandlers:
            df_journals = h.getAllJournals()
            if not df_journals.empty:
                mask = df_journals['id'].isin(all_ids)
                result.extend(self._makeJournals(df_journals[mask]))
        return result

    def getJournalsInAreasWithLicense(self, areas: Set[str], licenses: Set[str]) -> List[Journal]:
        all_ids = set()
        for h in self.categoryHandlers:
            df = h.getAllAssignments()
            if not df.empty and areas:
                df = df[df['area_id'].isin(areas)]
                all_ids.update(df['id'].dropna().tolist())

        if not all_ids:
            return []

        result = []
        for h in self.journalHandlers:
            df_journals = h.getJournalsWithLicense(licenses)
            if not df_journals.empty:
                mask = df_journals['id'].isin(all_ids)
                result.extend(self._makeJournals(df_journals[mask]))
        return result

    def getDiamondJournalsInAreasAndCategoriesWithQuartile(
        self, areas: Set[str], category_ids: Set[str], quartiles: Set[str]
    ) -> List[Journal]:

        all_ids = set()
        for h in self.categoryHandlers:
            df = h.getAllAssignments()
            if not df.empty:
                if areas:
                    df = df[df['area_id'].isin(areas)]
                if category_ids:
                    df = df[df['category_id'].isin(category_ids)]
                if quartiles:
                    df = df[df['quartile'].isin(quartiles)]
                all_ids.update(df['id'].dropna().tolist())

        if not all_ids:
            return []

        result = []
        for h in self.journalHandlers:
            df_journals = h.getAllJournals()
            if not df_journals.empty:
                # diamond = no APC (i.e., apc == False or “no”)
                mask = (df_journals['id'].isin(all_ids)) & (
                    df_journals['apc'].astype(str).str.lower().isin(["no", "false"])
                )
                result.extend(self._makeJournals(df_journals[mask]))
        return result
