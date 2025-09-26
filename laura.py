import pandas as pd
from typing import List, Set, Optional, Union

# DATA MODEL

class IdentifiableEntity:
    def __init__(self, id: str):
        self.id = id

    def getId(self) -> str:
        return self.id

    # Compatibility method: some tests might expect getIds()
    def getIds(self) -> List[str]:
        return [self.id]


class Area(IdentifiableEntity): 
    pass


class Category(IdentifiableEntity):
    def __init__(self, id: str, quartile: str):
        super().__init__(id)
        self.quartile = quartile

    def getQuartile(self) -> str:
        return self.quartile


class Journal(IdentifiableEntity):
    def __init__(self, id: List[str], title: str, languages: List[str],
                 publisher: Optional[str], seal: bool, license: Optional[str],
                 apc: bool, hasCategory: List[str], hasArea: List[str]):
        # Use the first id as the "main" id
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
        return [j for h in self.journalHandlers for j in self._makeJournals(h.getAllJournals())]

    def getJournalsWithTitle(self, title: str) -> List[Journal]:
        return [j for h in self.journalHandlers for j in self._makeJournals(h.getJournalsWithTitle(title))]

    def getJournalsPublishedBy(self, publisher: str) -> List[Journal]:
        return [j for h in self.journalHandlers for j in self._makeJournals(h.getJournalsPublishedBy(publisher))]

    def getJournalsWithLicense(self, licenses: Set[str]) -> List[Journal]:
        return [j for h in self.journalHandlers for j in self._makeJournals(h.getJournalsWithLicense(licenses))]

    def getJournalsWithAPC(self) -> List[Journal]:
        return [j for h in self.journalHandlers for j in self._makeJournals(h.getJournalsWithAPC())]

    def getJournalsWithDOAJSeal(self) -> List[Journal]:
        return [j for h in self.journalHandlers for j in self._makeJournals(h.getJournalsWithDOAJSeal())]

    # Category and Area queries
    def getAllCategories(self) -> List[Category]:
        result = []
        for h in self.categoryHandlers:
            df = h.getAllCategories()
            result.extend([Category(r['category_id'], r['quartile']) for _, r in df.iterrows()])
        return result

    def getCategoriesWithQuartile(self, quartiles: Set[str]) -> List[Category]:
        result = []
        for h in self.categoryHandlers:
            df = h.getCategoriesWithQuartile(quartiles)
            result.extend([Category(r['category_id'], r['quartile']) for _, r in df.iterrows()])
        return result

    def getAllAreas(self) -> List[Area]:
        result = []
        for h in self.categoryHandlers:
            df = h.getAllAreas()
            result.extend([Area(r['area_id']) for _, r in df.iterrows()])
        return result

    # Helper method: convert DataFrame rows into Journal objects
    def _makeJournals(self, df: pd.DataFrame) -> List[Journal]:
        if df.empty:
            return []
        return [Journal(
            id=r['id'] if isinstance(r['id'], list) else [r['id']],
            title=r.get('title', ''),
            languages=r.get('languages', []),
            publisher=r.get('publisher'),
            seal=bool(r.get('seal', False)),
            license=r.get('license'),
            apc=bool(r.get('apc', False)),
            hasCategory=r.get('hasCategory', []),
            hasArea=r.get('hasArea', [])
        ) for _, r in df.iterrows()]

# FULL QUERY ENGINE

class FullQueryEngine(BasicQueryEngine):

    def getEntityById(self, id: str) -> Optional[Union[Journal, Category, Area]]:
        # Search among Journals
        for j in self.getAllJournals():
            if j.getId() == id or id in getattr(j, "identifiers", []):
                return j
        # Search among Categories
        for c in self.getAllCategories():
            if c.getId() == id:
                return c
        # Search among Areas
        for a in self.getAllAreas():
            if a.getId() == id:
                return a
        return None

    def getCategoriesAssignedToAreas(self, areas: Set[str]) -> List[Category]:
        result = []
        for h in self.categoryHandlers:
            df = h.getCategoriesAssignedToAreas(areas)
            if not df.empty:
                for _, r in df.iterrows():
                    result.append(Category(r['category_id'], r['quartile']))
        return result

    def getAreasAssignedToCategories(self, categories: Set[str]) -> List[Area]:
        result = []
        for h in self.categoryHandlers:
            df = h.getAreasAssignedToCategories(categories)
            if not df.empty:
                for _, r in df.iterrows():
                    result.append(Area(r['area_id']))
        return result

    def getJournalsInCategoriesWithQuartile(self, category_ids: Set[str], quartiles: Set[str]) -> List[Journal]:
        all_ids = set()
        for handler in self.categoryHandlers:
            df = handler.getAllCategoryAssignments()
            if not df.empty:
                if category_ids:
                    df = df[df['category_id'].isin(category_ids)]
                if quartiles:
                    df = df[df['quartile'].isin(quartiles)]
                all_ids.update(df['id'].dropna().tolist())

        if not all_ids:
            return []

        result = []
        for handler in self.journalHandlers:
            df_journals = handler.getAllJournals()
            if not df_journals.empty:
                mask = df_journals['id'].isin(all_ids)
                result.extend(self._makeJournals(df_journals[mask]))
        return result

    def getJournalsInAreasWithLicense(self, areas: Set[str], licenses: Set[str]) -> List[Journal]:
        all_ids = set()
        for handler in self.categoryHandlers:
            df = handler.getAllAreaAssignments()
            if not df.empty:
                if areas:
                    df = df[df['area_id'].isin(areas)]
                all_ids.update(df['id'].dropna().tolist())

        if not all_ids:
            return []

        result = []
        for handler in self.journalHandlers:
            df_journals = handler.getJournalsWithLicense(licenses)
            if not df_journals.empty:
                mask = df_journals['id'].isin(all_ids)
                result.extend(self._makeJournals(df_journals[mask]))
        return result

    def getDiamondJournalsInAreasAndCategoriesWithQuartile(
        self, areas: Set[str], category_ids: Set[str], quartiles: Set[str]
    ) -> List[Journal]:
        all_ids = set()
        for handler in self.categoryHandlers:
            df = handler.getAllAssignments()
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
        for handler in self.journalHandlers:
            df_journals = handler.getAllJournals()
            if not df_journals.empty:
                mask = (df_journals['id'].isin(all_ids)) & (df_journals['apc'] == False)  # FIX: diamond = no APC
                result.extend(self._makeJournals(df_journals[mask]))
        return result
