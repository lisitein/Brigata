import pandas as pd
from typing import List, Set, Optional, Union

# DATA MODEL

class IdentifiableEntity:
    def __init__(self, id: str):
        self.id = id
    def getId(self) -> str:
        return self.id

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
        categories = []
        for h in self.categoryHandlers:
            df = h.getAllCategories()
            for _, r in df.iterrows():
                categories.append(Category(r['internalId'], r['quartile']))
        return categories

    def getCategoriesWithQuartile(self, quartiles: Set[str]) -> List[Category]:
        categories = []
        for h in self.categoryHandlers:
            df = h.getCategoriesWithQuartile(quartiles)
            for _, r in df.iterrows():
                categories.append(Category(r['internalId'], r['quartile']))
        return categories

    def getAllAreas(self) -> List[Area]:
        areas = []
        for h in self.categoryHandlers:
            df = h.getAllAreas()
            for _, r in df.iterrows():
                areas.append(Area(r['internalId']))
        return areas

    def _makeJournals(self, df: pd.DataFrame) -> List[Journal]:
        journals = []
        for _, r in df.iterrows():
            journal = Journal(
                id=r['id'] if isinstance(r['id'], list) else [r['id']],
                title=r.get('title', ''),
                languages=r.get('languages', []),
                publisher=r.get('publisher'),
                seal=str(r.get('seal')).lower() == 'yes',
                license=r.get('license'),
                apc=str(r.get('apc')).lower() == 'yes',
                hasCategory=r.get('hasCategory', []),
                hasArea=r.get('hasArea', [])
            )
            journals.append(journal)
        return journals

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
                return Category(r['internalId'], r['quartile']) if 'quartile' in r else Area(r['internalId'])
        return None

    def getCategoriesAssignedToAreas(self, areas: Set[str]) -> List[Category]:
        categories = []
        for h in self.categoryHandlers:
            df = h.getCategoriesAssignedToAreas(areas)
            for _, r in df.iterrows():
                categories.append(Category(r['id'], r['quartile']))
        return categories

    def getAreasAssignedToCategories(self, categories: Set[str]) -> List[Area]:
        areas = []
        for h in self.categoryHandlers:
            df = h.getAreasAssignedToCategories(categories)
            for _, r in df.iterrows():
                areas.append(Area(r['area']))
        return areas

    def getJournalsInCategoriesWithQuartile(self, category_ids: Set[str], quartiles: Set[str]) -> List[Journal]:
        df_match = pd.DataFrame()
        for handler in self.categoryHandlers:
            df = handler.getAllCategoryAssignments()
            if category_ids:
                df = df[df['category_id'].isin(category_ids)]
            if quartiles:
                df = df[df['category_quartile'].isin(quartiles)]
            df_match = pd.concat([df_match, df])
        
        if df_match.empty:
            return []
            
        identifiers = df_match['identifiers'].dropna().unique().tolist()
        result = []
        for handler in self.journalHandlers:
            df_journals = handler.getAllJournals()
            if not df_journals.empty and 'id' in df_journals.columns:
                df_journals['id'] = df_journals['id'].apply(lambda x: x if isinstance(x, list) else [x])
                df_journals = df_journals.explode('id')
                df_journals = df_journals[df_journals['id'].isin(identifiers)]
                result.extend(self._makeJournals(df_journals))
        return result

    def getJournalsInAreasWithLicense(self, areas: Set[str], licenses: Set[str]) -> List[Journal]:
        df_match = pd.DataFrame()
        for handler in self.categoryHandlers:
            df = handler.getAllAreaAssignments()
            if areas:
                df = df[df['area'].isin(areas)]
            df_match = pd.concat([df_match, df])
        
        if df_match.empty:
            return []
            
        identifiers = df_match['identifiers'].dropna().unique().tolist()
        result = []
        for handler in self.journalHandlers:
            df_journals = handler.getJournalsWithLicense(licenses)
            if not df_journals.empty and 'id' in df_journals.columns:
                df_journals['id'] = df_journals['id'].apply(lambda x: x if isinstance(x, list) else [x])
                df_journals = df_journals.explode('id')
                df_journals = df_journals[df_journals['id'].isin(identifiers)]
                result.extend(self._makeJournals(df_journals))
        return result

    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, areas: Set[str], category_ids: Set[str], quartiles: Set[str]) -> List[Journal]:
        df_match = pd.DataFrame()
        for handler in self.categoryHandlers:
            df = handler.getAllAssignments()
            if areas:
                df = df[df['area'].isin(areas)]
            if category_ids:
                df = df[df['category_id'].isin(category_ids)]
            if quartiles:
                df = df[df['category_quartile'].isin(quartiles)]
            df_match = pd.concat([df_match, df])
        
        if df_match.empty:
            return []
            
        identifiers = df_match['identifiers'].dropna().unique().tolist()
        result = []
        for handler in self.journalHandlers:
            df_journals = handler.getAllJournals()
            if not df_journals.empty:
                df_journals = df_journals[df_journals['apc'].isin(['No', False])]
                if 'id' in df_journals.columns:
                    df_journals['id'] = df_journals['id'].apply(lambda x: x if isinstance(x, list) else [x])
                    df_journals = df_journals.explode('id')
                    df_journals = df_journals[df_journals['id'].isin(identifiers)]
                    result.extend(self._makeJournals(df_journals))
        return result