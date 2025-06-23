import pandas as pd
from typing import List, Set, Optional, Union

# DATA MODEL

class IdentifiableEntity:
    def __init__(self, id: str):
        self.id = id
    def getId(self) -> str:
        return self.id

class Area(IdentifiableEntity): pass

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

# BASIC QUERY ENGINE

class BasicQueryEngine:
    def __init__(self):
        self.journalHandlers = []
        self.categoryHandlers = []

    def addJournalHandler(self, handler): self.journalHandlers.append(handler)
    def addCategoryHandler(self, handler): self.categoryHandlers.append(handler)
    def cleanJournalHandlers(self): self.journalHandlers.clear()
    def cleanCategoryHandlers(self): self.categoryHandlers.clear()

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

    def getAllCategories(self) -> List[Category]:
        return [Category(r['internalId'], r['quartile']) for h in self.categoryHandlers for _, r in h.getAllCategories().iterrows()]

    def getCategoriesWithQuartile(self, quartiles: Set[str]) -> List[Category]:
        return [Category(r['internalId'], r['quartile']) for h in self.categoryHandlers for _, r in h.getCategoriesWithQuartile(quartiles).iterrows()]

    def getAllAreas(self) -> List[Area]:
        return [Area(r['area']) for h in self.categoryHandlers for _, r in h.getAllAreas().iterrows()]

    def _makeJournals(self, df: pd.DataFrame) -> List[Journal]:
        return [Journal(
            id=r['id'] if isinstance(r['id'], list) else [r['id']],
            title=r.get('title', ''),
            languages=r.get('languages', []),
            publisher=r.get('publisher'),
            seal=str(r.get('seal')).lower() == 'yes',
            license=r.get('license'),
            apc=str(r.get('apc')).lower() == 'yes',
            hasCategory=r.get('hasCategory', []),
            hasArea=r.get('hasArea', [])
        ) for _, r in df.iterrows()]

# FULL QUERY ENGINE

class FullQueryEngine(BasicQueryEngine):
    def getEntityById(self, id: str) -> Optional[Union[Journal, Category, Area]]:
        for h in self.journalHandlers:
            df = h.getById(id)
            if not df.empty:
                return self._makeJournals(df)[0]
        for h in self.categoryHandlers:
            df = h.getById(id)
            if not df.empty:
                r = df.iloc[0]
                return Category(r['internalId'], r['quartile']) if 'quartile' in r else Area(r['area'])
        return None

    def getCategoriesAssignedToAreas(self, areas: Set[str]) -> List[Category]:
        return [Category(r['internalId'], r['quartile']) for h in self.categoryHandlers for _, r in h.getCategoriesAssignedToAreas(areas).iterrows()]

    def getAreasAssignedToCategories(self, categories: Set[str]) -> List[Area]:
        return [Area(r['area']) for h in self.categoryHandlers for _, r in h.getAreasAssignedToCategories(categories).iterrows()]

    def getJournalsInCategoriesWithQuartile(self, category_ids: Set[str], quartiles: Set[str]) -> List[Journal]:
        return self._filterJournalsFromAssignments('getAllCategoryAssignments', category_ids, quartiles, None, None)

    def getJournalsInAreasWithLicense(self, areas: Set[str], licenses: Set[str]) -> List[Journal]:
        return self._filterJournalsFromAssignments('getAllAreaAssignments', None, None, areas, licenses)

    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, areas: Set[str], category_ids: Set[str], quartiles: Set[str]) -> List[Journal]:
        return self._filterJournalsFromAssignments('getAllAssignments', category_ids, quartiles, areas, None, diamond=True)

    def _filterJournalsFromAssignments(self, method, cat_ids, quarts, area_ids, licenses, diamond=False):
        df_match = pd.DataFrame()
        for h in self.categoryHandlers:
            df = getattr(h, method)()
            if cat_ids: df = df[df['internalId'].isin(cat_ids)]
            if quarts: df = df[df['quartile'].isin(quarts)]
            if area_ids: df = df[df['area'].isin(area_ids)]
            df_match = pd.concat([df_match, df])
        ids = df_match['id'].dropna().unique().tolist()

        journals = []
        for h in self.journalHandlers:
            df = h.getAllJournals() if not licenses else h.getJournalsWithLicense(licenses)
            if diamond:
                df = df[df['apc'].isin(['No', False])]
            df['id'] = df['id'].apply(lambda x: x if isinstance(x, list) else [x])
            df = df.explode('id')
            df = df[df['id'].isin(ids)]
            journals.extend(self._makeJournals(df))
        return journals
