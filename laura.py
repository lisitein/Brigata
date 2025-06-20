# hey guys! I will add the comements between today and tomorrow

# importing libraries & tools

import pandas as pd
from typing import List, Set, Optional

# DATA MODEL

class IdentifiableEntity:
    def __init__(self, id: str):
        self.id = id

    def getId(self) -> str:
        return self.id

class Area(IdentifiableEntity):
    def __init__(self, id: str):
        super().__init__(id)

class Category(IdentifiableEntity):
    def __init__(self, id: str, quartile: str):
        super().__init__(id)
        self.quartile = quartile

    def getQuartile(self) -> str:
        return self.quartile

class Journal(IdentifiableEntity):
    def __init__(self,
                 id: List[str],
                 title: str,
                 languages: List[str],
                 publisher: Optional[str],
                 seal: bool,
                 license: Optional[str],
                 apc: bool,
                 hasCategory: List[str],
                 hasArea: List[str]):
        super().__init__(id[0] if isinstance(id, list) and id else id)
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

    def addJournalHandler(self, handler):
        self.journalHandlers.append(handler)

    def addCategoryHandler(self, handler):
        self.categoryHandlers.append(handler)

    def getAllJournals(self) -> List[Journal]:
        journals = []
        for handler in self.journalHandlers:
            df = handler.getAllJournals()
            journals.extend(self._createJournalObjects(df))
        return journals

    def getJournalsWithTitle(self, partialTitle: str) -> List[Journal]:
        journals = []
        for handler in self.journalHandlers:
            df = handler.getJournalsWithTitle(partialTitle)
            journals.extend(self._createJournalObjects(df))
        return journals

    def getJournalsWithLicense(self, licenses: Set[str]) -> List[Journal]:
        journals = []
        for handler in self.journalHandlers:
            df = handler.getJournalsWithLicense(licenses)
            journals.extend(self._createJournalObjects(df))
        return journals

    def getJournalsWithAPC(self) -> List[Journal]:
        journals = []
        for handler in self.journalHandlers:
            df = handler.getJournalsWithAPC()
            journals.extend(self._createJournalObjects(df))
        return journals

    def getJournalsWithDOAJSeal(self) -> List[Journal]:
        journals = []
        for handler in self.journalHandlers:
            df = handler.getJournalsWithDOAJSeal()
            journals.extend(self._createJournalObjects(df))
        return journals

    def getAllCategories(self) -> List[Category]:
        categories = []
        for handler in self.categoryHandlers:
            df = handler.getAllCategories()
            for _, row in df.iterrows():
                categories.append(Category(row['category_id'], row['category_quartile']))
        return categories

    def getCategoriesWithQuartile(self, quartiles: List[str]) -> List[Category]:
        categories = []
        for handler in self.categoryHandlers:
            df = handler.getCategoriesWithQuartile(quartiles)
            for _, row in df.iterrows():
                categories.append(Category(row['category_id'], row['category_quartile']))
        return categories

    def getAllAreas(self) -> List[Area]:
        areas = []
        for handler in self.categoryHandlers:
            df = handler.getAllAreas()
            for _, row in df.iterrows():
                areas.append(Area(row['area']))
        return areas

    def _createJournalObjects(self, df: pd.DataFrame) -> List[Journal]:
        journals = []
        for _, row in df.iterrows():
            journal = Journal(
                id=row['identifiers'] if isinstance(row['identifiers'], list) else [row['identifiers']],
                title=row.get('title', ''),
                languages=row.get('languages', []),
                publisher=row.get('publisher'),
                seal=str(row.get('seal')).lower() == 'yes',
                license=row.get('license'),
                apc=str(row.get('apc')).lower() == 'yes',
                hasCategory=row.get('hasCategory', []),
                hasArea=row.get('hasArea', [])
            )
            journals.append(journal)
        return journals

# FULL QUERY ENGINE

class FullQueryEngine(BasicQueryEngine):
    def getJournalsInCategoriesWithQuartile(self, category_ids: Set[str], quartiles: Set[str]) -> List:
        df_match = pd.DataFrame()
        for handler in self.categoryHandlers:
            df = handler.getAllCategoryAssignments()
            if category_ids:
                df = df[df['category_id'].isin(category_ids)]
            if quartiles:
                df = df[df['category_quartile'].isin(quartiles)]
            df_match = pd.concat([df_match, df])

        identifiers = df_match['identifiers'].dropna().unique().tolist()

        result = []
        for handler in self.journalHandlers:
            df_journals = handler.getAllJournals()
            df_journals['identifiers'] = df_journals['identifiers'].apply(lambda x: x if isinstance(x, list) else [x])
            df_journals = df_journals.explode('identifiers')
            df_journals = df_journals[df_journals['identifiers'].isin(identifiers)]
            result.extend(self._createJournalObjects(df_journals))
        return result

    def getJournalsInAreasWithLicense(self, areas: Set[str], licenses: Set[str]) -> List:
        df_match = pd.DataFrame()
        for handler in self.categoryHandlers:
            df = handler.getAllAreaAssignments()
            if areas:
                df = df[df['area'].isin(areas)]
            df_match = pd.concat([df_match, df])

        identifiers = df_match['identifiers'].dropna().unique().tolist()

        result = []
        for handler in self.journalHandlers:
            df_journals = handler.getJournalsWithLicense(licenses)
            df_journals['identifiers'] = df_journals['identifiers'].apply(lambda x: x if isinstance(x, list) else [x])
            df_journals = df_journals.explode('identifiers')
            df_journals = df_journals[df_journals['identifiers'].isin(identifiers)]
            result.extend(self._createJournalObjects(df_journals))
        return result

    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, areas: Set[str], category_ids: Set[str], quartiles: Set[str]) -> List:
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

        identifiers = df_match['identifiers'].dropna().unique().tolist()

        result = []
        for handler in self.journalHandlers:
            df_journals = handler.getAllJournals()
            df_journals = df_journals[df_journals['apc'].isin(['No', False])]
            df_journals['identifiers'] = df_journals['identifiers'].apply(lambda x: x if isinstance(x, list) else [x])
            df_journals = df_journals.explode('identifiers')
            df_journals = df_journals[df_journals['identifiers'].isin(identifiers)]
            result.extend(self._createJournalObjects(df_journals))
        return result
