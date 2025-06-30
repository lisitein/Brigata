import pandas as pd
from typing import List, Set, Optional, Union

# DATA MODEL

class IdentifiableEntity:
    def __init__(self, id: str):  # defines a generic class that represents any entity with an id (string)
        self.id = id  # saves the id value as an attribute of the object, accessible as self.id
    def getId(self) -> str:  # classic getter" method, used to access in a controlled way an internal attribute of the object
        return self.id

class Area(IdentifiableEntity): # subclass with no additional attributes: it inherits everything from IdentifiableEntity
    pass  # null operation; nothing more is being added to Area at this time

class Category(IdentifiableEntity): # Category is an IdentifiableEntity with an additional attribute: quartile
    def __init__(self, id: str, quartile: str):
        super().__init__(id)
        self.quartile = quartile  # saves the quartile
    def getQuartile(self) -> str:
        return self.quartile  # method to obtain the Category's quartile

class Journal(IdentifiableEntity):
    def __init__(self, id: List[str], title: str, languages: List[str],   #  Journal constructor, representing an academic journal - it has many attributes
                 publisher: Optional[str], seal: bool, license: Optional[str],
                 apc: bool, hasCategory: List[str], hasArea: List[str]):
        super().__init__(id[0] if id else '')  # only the first identifier (id[0]) is used as the "main identifier"; if id is empty, an empty string is assigned
        self.identifiers = id  # this saves the entire id list as a separate attribute, called identifiers, so the user can have access to both the "main" ID (self.id) and all the others (self.identifiers)
        self.title = title
        self.languages = languages
        self.publisher = publisher
        self.seal = seal
        self.license = license
        self.apc = apc
        self.hasCategory = hasCategory
        self.hasArea = hasArea

    def getTitle(self) -> str:  # series of public methods of the Journal class that return the values ​​of the various attributes
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

    def getHasCategory(self) -> List[str]:  # contains a list of category IDs to which the journal belongs
        return self.hasCategory

    def getHasArea(self) -> List[str]:  # contains a list of subject area IDs to which the journal is linked
        return self.hasArea

# BASIC QUERY ENGINE

class BasicQueryEngine:  # we start by defining a Python class to run queries
    def __init__(self):  #  the __init__ method initializes two empty lists
        self.journalHandlers = []  # handler to access journal data
        self.categoryHandlers = []  # handler to access category and area data

    def addJournalHandler(self, handler) -> bool:  # public method which has as parameter "handler", that is an object that must implement methods like getAllJournals() / -> bool means that the method returns a boolean value
        self.journalHandlers.append(handler) #  list containing all registered journalHandlers; with .append(handler) one adds the handler object to the end of the list
        return True  # the method simply returns True to indicate that the operation was successful
        
    def addCategoryHandler(self, handler) -> bool:  # the same, but for categoryHandlers
        self.categoryHandlers.append(handler)
        return True

    # the cleanJournalHandlers() method is used to completely empty the list of journal handlers (self.journalHandlers) previously registered with addJournalHandler(...)
    
    def cleanJournalHandlers(self) -> bool:
        self.journalHandlers.clear()
        return True
        
    def cleanCategoryHandlers(self) -> bool:
        self.categoryHandlers.clear()
        return True
        
    # QUERY ON JOURNALS
    #    all of these functions do the same thing:
    # 1. iterate through each handler in journalHandlers
    # 2. call the corresponding method (e.g. getAllJournals())
    # 3. pass the result to _makeJournals(), which converts a DataFrame into Journal objects
    # 4. get a list of Journal objects
    # 5. concatenate everything into a single list to return

    def getAllJournals(self) -> List[Journal]:
        return [j for h in self.journalHandlers for j in self._makeJournals(h.getAllJournals())] # double list comprehension, which can be read as: "for each handler in the self.journalHandlers list, call getAllJournals() on it, convert the result to Journal objects using _makeJournals(), and add them to the final list"

    def getJournalsWithTitle(self, title: str) -> List[Journal]:  #  return all Journals whose title matches a certain string, searching all data sources saved in the engine
        return [j for h in self.journalHandlers for j in self._makeJournals(h.getJournalsWithTitle(title))]

    def getJournalsPublishedBy(self, publisher: str) -> List[Journal]:
        return [j for h in self.journalHandlers for j in self._makeJournals(h.getJournalsPublishedBy(publisher))]

    def getJournalsWithLicense(self, licenses: Set[str]) -> List[Journal]:
        return [j for h in self.journalHandlers for j in self._makeJournals(h.getJournalsWithLicense(licenses))]

    def getJournalsWithAPC(self) -> List[Journal]:
        return [j for h in self.journalHandlers for j in self._makeJournals(h.getJournalsWithAPC())]

    def getJournalsWithDOAJSeal(self) -> List[Journal]:
        return [j for h in self.journalHandlers for j in self._makeJournals(h.getJournalsWithDOAJSeal())]
        
    # QUERY ON CATEGORIES AND AREAS

    #  goal: return a list of Category objects, combining data from all handlers registered in the categoryHandlers list
    def getAllCategories(self) -> List[Category]: # it returns a list of Category objects without distinctions: all available categories
        result = []  # it creates an empty list which will collect all Category objects found
        for h in self.categoryHandlers: # iterates through all previously registered category handlers via addCategoryHandler(...); each h is an object that implements a getAllCategories() method, which returns a pandas.DataFrame
            df = h.getAllCategories()  # this line of code calls getAllCategories() on the h handler
                                       # the method should return a pandas DataFrame with at least two columns:
                                       #   - internalId: the identifier of the category
                                       #   - quartile: the quartile of the category (e.g. “Q1”, “Q2”…)
            result.extend([Category(r['internalId'], r['quartile']) for _, r in df.iterrows()])  #  for each row, this creates a Category(...) object by passing:
                                                                                                 #    - r['internalId']: the internal ID of the category.
                                                                                                 #    - r['quartile']: the quartile of the category (e.g. "Q1").
                                                                                                 #  result.extend(...) adds all newly created Category objects from that DataFrame to the result list
                                                                                                 #  the first element _ is the index (which is not needed), so it should be ignored
        return result  # returns the final list "result", which now contains all categories from all data sources

    # goal: return a list of Category objects, but only those that belong to one of the quartiles specified in input (e.g. “Q1”, “Q2”)
    def getCategoriesWithQuartile(self, quartiles: Set[str]) -> List[Category]:  # the line takes as input a set of quartiles, for example: {"Q1", "Q2"} and returns a list of Category objects filtered by those quartiles
        result = []  # it creates an empty list to accumulate Category objects found in all handlers
        for h in self.categoryHandlers:  #  it iterates through each registered category handler: these handlers are responsible for returning data filtered by quartile
            df = h.getCategoriesWithQuartile(quartiles)   # this line calls the getCategoriesWithQuartile(...) method of the h handler, passing the "quartiles" set received as input
                                                          # this method filters the data internally and returns a pandas.DataFrame with only the categories that belong to the requested quartiles
            result.extend([Category(r['internalId'], r['quartile']) for _, r in df.iterrows()])  #  df.iterrows(): iterates through each row r of the DataFrame
                                                                                                 #  r['internalId'] is the category identifier
                                                                                                 #  r['quartile'] is the quartile (e.g. "Q1")
                                                                                                 #  Category(r['internalId'], r['quartile']): creates a Category object with these values
                                                                                                 #  [ ... for _, r in df.iterrows()]: builds a list of Category objects
                                                                                                 #  result.extend(...): adds these objects to the list result
        return result  # at the end of the loop, "result" contains all categories from all handlers, filtered by quartile and is returned as output

    # goal: collect all available scientific/disciplinary areas, coming from all registered category handlers, and transform them into Area objects that can be managed in the data model
    def getAllAreas(self) -> List[Area]:  # this public method of the BasicQueryEngine class returns a list of Area objects
        result = []  #  we start by initializing an empty list called result, which will be progressively filled with Area objects, one for each row found
        for h in self.categoryHandlers:  #  each h is a handler registered with addCategoryHandler(...); it is an object that knows the subject areas (starting from a CSV, a Jason file, a database...) and can provide them
            df = h.getAllAreas()  #  it calls the getAllAreas() method on the h handler / this method returns a pandas.DataFrame, which represents all the areas available from that source
            result.extend([Area(r['internalId']) for _, r in df.iterrows()])  #  this line does 4 things in one:
                                                                              #  1. df.iterrows() iterates through each row r of the DataFrame df
                                                                              #      - r is a Series, which is an ordered dictionary representing a row
                                                                              #      - r['internalId'] accesses the area identifier
                                                                              #  2. [Area(r['internalId']) for _, r in df.iterrows()] is a list comprehension
                                                                              #      - it creates a list of Area objects, one for each row.
                                                                              #  3. Area(r['internalId']) creates an instance of the Area class, using the row identifier
                                                                              #  4. result.extend([...]) adds these objects to the result list
        return result  #  it returns the complete list of Area objects collected by all queried handlers / each Area object has only one attribute: id

    # goal: the method _makeJournals(self, df: pd.DataFrame) -> List[Journal] has the goal of transforming a pandas DataFrame into a list of Journal objects (academic journals)
    def _makeJournals(self, df: pd.DataFrame) -> List[Journal]:  # _makeJournals is a private method, i.e. intended for internal use by the BasicQueryEngine / the df: pd.DataFrame parte means that it expects a DataFrame as input, i.e. a table
        if df.empty:  # if the DataFrame contains no rows, there is nothing to convert
            return []  # returns an empty list of journals
        # building of each Journal
        return [Journal(
            id=r['id'] if isinstance(r['id'], list) else [r['id']],  #  checks if the 'id' field is a list
                                                                     #      - if it's already a list, it uses it as is
                                                                     #      - if it's a single string, put it into a list ([r['id']])
                                                                     #  this is because the Journal constructor always expects a list of identifiers, not a single string
            title=r.get('title', ''),  # it uses .get() to get the title (if the 'title' field does not exist in the row, it returns an empty string)
            languages=r.get('languages', []),  # it gets the list of languages ​​for the journal (if not present, it uses an empty list)
            publisher=r.get('publisher'),  # it gets the publisher (can be None if not present)
            seal=str(r.get('seal')).lower() == 'yes', # gets the seal value, converts it to a string, lowercases it (.lower()). then checks if it is equal to "yes" and returns True or False
            license=r.get('license'),  # gets the license or None if missing.
            apc=str(r.get('apc')).lower() == 'yes',  # as above, but for the APC (article processing charge) field. returns a boolean: True if APC is "yes", otherwise False
            hasCategory=r.get('hasCategory', []),  # gets the list of related category IDs. if missing, assumes no categories ([])
            hasArea=r.get('hasArea', [])  # gets the list of IDs of the connected areas, and as above if missing, returns an empty list
        ) for _, r in df.iterrows()]  # df.iterrows() allows you to scroll through a DataFrame row by row / _ is the row index (not used)

# FULL QUERY ENGINE

class FullQueryEngine(BasicQueryEngine):  # this class inherits everything from the BasicQueryEngine (including handlers and base methods) and adds advanced functionality for combined queries
    # goal: return an entity (Journal, Category or Area), given its identification string, without the caller of the method having to know what type it belongs to
    def getEntityById(self, id: str) -> Optional[Union[Journal, Category, Area]]:  # the method takes only an ID as input and searches all available handlers:
                                                                                   #     - first among journals (journalHandlers)
                                                                                   #     - then among categories and areas (categoryHandlers)
                                                                                   # as soon as it finds a match, it returns the correct Python object, already instantiated:
                                                                                   #     - a Journal object
                                                                                   #     - or a Category
                                                                                   #     - or an Area
        for h in self.journalHandlers:  # it loops through all handlers that have been registered as journalHandlers
            df = h.getById(id)  # asks the handler h to return data for the journal with that id / the handler's getById(id) method should return a DataFrame (typically with a single row) containing the data for the journal with that identifier
            if not df.empty:  # checks if the returned DataFrame is not empty
                return self._makeJournals(df)[0]  # converts the DataFrame into a list of Journal objects, using the internal _makeJournals(df) method, and then returns the first element
        for h in self.categoryHandlers:  # iterates through all handlers registered in self.categoryHandlers, which are responsible for managing categories and areas
            df = h.getById(id)  # it tries to retrieve a DataFrame from the data managed by h, looking for a row that has the past ID as its one / this getById(id) method must therefore work for both category IDs and area IDs, always returning a DataFrame
            if not df.empty:  # if the DataFrame is not empty, it means that we have found an entity (or Category, or Area) with that ID
                r = df.iloc[0]  # extracts the first row of the DataFrame / r is a pandas Series that contains all the fields of the found row
                return Category(r['internalId'], r['quartile']) if 'quartile' in r else Area(r['internalId'])  # if there is a 'quartile' column in row r, then we are talking about a Category; otherwise, we are talking about an Area
                                                                                                               # this is possible because both categories and areas use the same ID (internalId), but only categories also have the quartile field
        return None  # if the ID is not found, returns None

    # goal: get all categories related to a set of areas
    def getCategoriesAssignedToAreas(self, areas: Set[str]) -> List[Category]:  # the method takes as input an areas parameter, which is a set of strings (Set[str]), each representing the ID of a thematic area
                                                                                # it returns a list of Category objects (List[Category]), i.e. the categories associated with these areas
        result = []  # initializes an empty list result, which will be filled with the found Category objects
        for h in self.categoryHandlers:  # iterates over all handlers registered in the self.categoryHandlers list
            df = h.getCategoriesAssignedToAreas(areas)  # for each handler, it calls the getCategoriesAssignedToAreas method, handing to it the desired areas; this method is expected to return a Pandas DataFrame, where each row represents a category associated with one of the provided areas
            result.extend([Category(r['id'], r['quartile']) for _, r in df.iterrows()])  # for each row r of the DataFrame df, it creates a Category object, using:
                                                                                         #     - r['id']: the category identifier.
                                                                                         #     - r['quartile']: the quartile of the category (e.g. Q1, Q2…).
                                                                                         # adds each Category object to the result list
        return result  # once it has iterated over all handlers, it returns the complete list of categories assigned to the specified areas

    # goal: get all areas related to a set of categories
    def getAreasAssignedToCategories(self, categories: Set[str]) -> List[Area]:  # this method takes as input categories, i.e. a set of category identifiers (Set[str])
                                                                                 # it returns a list of Area objects (List[Area]), representing the areas assigned to these categories
        result = []  # initializes an empty result list, which will be filled with the found Area objects
        for h in self.categoryHandlers:  # iterates over each registered categoryHandler
            df = h.getAreasAssignedToCategories(categories)  # calls the getAreasAssignedToCategories method on each handler, handing to it the set of categories; this method returns a DataFrame (df) where each row represents a category–area pair, but only for those categories included in "categories"
            result.extend([Area(r['area']) for _, r in df.iterrows()])  # for each row r of the DataFrame df, it creates an Area object using the field r['area'], which is the ID of the area / these objects are added to the result list
        return result  # at the end of the iteration, it returns the result list, containing all the areas found associated with the given categories

    # goal: to obtain journals belonging to certain categories and with certain quartiles
    def getJournalsInCategoriesWithQuartile(self, category_ids: Set[str], quartiles: Set[str]) -> List[Journal]: # this method takes as input:
                                                                                                                 #     - category_ids: a set (Set[str]) of category IDs (e.g. "cs.AI")
                                                                                                                 #     - quartiles: a set of quartiles (e.g. "Q1", "Q2", …)
                                                                                                                 # and it returns a list of Journal objects that are related to the requested categories and quartiles
        # fast path: get all matching identifiers in one go
        all_identifiers = set()  # it initializes an empty set to collect the unique identifiers of journals that match the filters
        for handler in self.categoryHandlers:  # it loops through all categoryHandlers
            df = handler.getAllCategoryAssignments()  # gets from each handler a DataFrame df with all the category-journal-quartile assignments
            if category_ids:  # if a subset of categories is specified, these lines filter the DataFrame to keep only rows with category_id among those given
                df = df[df['category_id'].isin(category_ids)]
            if quartiles:  # if a subset of quartiles was specified, further filter for those (e.g. just "Q1" or "Q1" and "Q2")
                df = df[df['category_quartile'].isin(quartiles)]
            if not df.empty:  # if the resulting DataFrame is not empty, it takes the "identifiers" column, removes the NaNs, turns it into a list, and adds it to the all_identifiers set / these identifiers represent the journals that match the criteria
                all_identifiers.update(df['identifiers'].dropna().tolist())
        
        if not all_identifiers:  # if no identifier is found, it stops here and returns an empty list: there are no journals that match the criteria
            return []
        
        # get journals matching these identifiers
        result = []  # it creates an empty result list that will contain the found and built Journals
        for handler in self.journalHandlers:  # iterates over all journalHandlers registered in the engine
            df_journals = handler.getAllJournals()  # calls the getAllJournals() method on the handler to get all journals in DataFrame format
            if not df_journals.empty and 'id' in df_journals.columns:  # makes sure that:
                                                                       #     - the DataFrame is not empty
                                                                       #     - it contains a column called 'id', which is needed for filtering
                # vectorized operations
                mask = df_journals['id'].isin(all_identifiers)  # creates a boolean mask (a series of True/False) that says which rows in the DataFrame have an id contained in the all_identifiers set
                result.extend(self._makeJournals(df_journals[mask]))  # applies the mask to select only rows with valid ids
                                                                      # calls _makeJournals() to convert each row into a Journal object
                                                                      # adds the found Journals to the result list
        return result  # returns the complete list of Journals found and built from the filtered identifiers

    # goal: to obtain journals that belong to specific areas and have one of the specified licenses
    def getJournalsInAreasWithLicense(self, areas: Set[str], licenses: Set[str]) -> List[Journal]:  # this method accepts two sets of strings (areas, licenses) and returns a list of Journal objects
        # fast path: get all matching identifiers in one go
        all_identifiers = set()  # initializes an empty set: it will contain the unique identifiers of the journals found in the indicated areas
        for handler in self.categoryHandlers:  # iterates over all (relational) handlers that contain information about assigned categories/areas
            df = handler.getAllAreaAssignments()  # requires each handler to have a DataFrame that shows the assignments between journals and subject areas; this df has at least two columns:
                                                  #     - 'area' → subject area
                                                  #     - 'identifiers' → journal ids or list of ids
            if areas:  # if the areas set is not empty, filter the DataFrame keeping only the rows where the 'area' column is present in areas
                df = df[df['area'].isin(areas)]
            if not df.empty:                                                    #  if the filtered DataFrame is not empty:
                all_identifiers.update(df['identifiers'].dropna().tolist())     #     - df['identifiers']: column with journal IDs     
                                                                                #     - .dropna(): remove rows with null values
                                                                                #     - .tolist(): transform the column into a list
                                                                                #     - .update(...): add all IDs to the all_identifiers set (without duplicates)
        
        if not all_identifiers:  # if no identifier was found, it means that no journal belongs to the indicated areas → returns empty list immediately
            return []
        
        # get journals with license filter AND identifier filter
        result = []  # initializes an empty list in which journals that satisfy both conditions will be collected
        for handler in self.journalHandlers:  # iterates over all handlers that handle journals
            df_journals = handler.getJournalsWithLicense(licenses)  # requests the single handler to return all journals that have one of the specified licenses and returns a DataFrame (e.g. with columns id, title, apc, etc.).
            if not df_journals.empty and 'id' in df_journals.columns:  # checks that:
                                                                       #     - the DataFrame is not empty
                                                                       #     - it contains an 'id' column (needed to compare identifiers)
                # vectorized operations (same as above)
                mask = df_journals['id'].isin(all_identifiers)
                result.extend(self._makeJournals(df_journals[mask]))
        return result  # returns the final list: all journals that are in one of the areas and have one of the indicated licenses

    # goal: to obtain journals that:
    # 1. belong to at least one of the areas indicated
    # 2. belong to at least one of the categories indicated
    # 3. these categories have one of the quartiles indicated
    # 4. DO NOT require APC (i.e. they are Diamond journals)
    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, areas: Set[str], category_ids: Set[str], quartiles: Set[str]) -> List[Journal]:  # this method takes as input:
                                                                                                                                                  #     - areas: a set of subject area identifiers (Set[str])
                                                                                                                                                  #     - category_ids: a set of category identifiers (Set[str])
                                                                                                                                                  #     - quartiles: a set of quartile values ​​(Set[str])
                                                                                                                                                  # and it returns a list of Journal objects (List[Journal]) that satisfy all of the above criteria
        # fast path: get all matching identifiers in one go
        all_identifiers = set()  # initializes an empty set: it will contain the IDs of journals that satisfy the conditions on areas, categories and quartiles.
        for handler in self.categoryHandlers:  # iterate over all categoryHandlers, asking to return a DataFrame with all the area-category-magazine assignments
            df = handler.getAllAssignments()
            if areas:  # filter the DataFrame keeping only the rows where the area is among those indicated
                df = df[df['area'].isin(areas)]
            if category_ids:  # filter further: keep only rows with compatible category IDs
                df = df[df['category_id'].isin(category_ids)]
            if quartiles:  #filter again: keep only rows with the desired quartile
                df = df[df['category_quartile'].isin(quartiles)]
            if not df.empty:  # if after all the filters the DataFrame is not empty, take the identifiers column (which contains the journal ID) and add all the non-null IDs to the all_identifiers set
                all_identifiers.update(df['identifiers'].dropna().tolist())
        
        if not all_identifiers:  # this means: "if, after all the filters, I have not found any journal identifiers, then there is no point in continuing: I return an empty list directly"
            return []
        
        # get all journals and filter for Diamond (no APC) and matching identifiers
        result = []  # initializes the list that will contain the resulting Journal objects
        for handler in self.journalHandlers:  # iterates through all registered journalHandlers
            df_journals = handler.getAllJournals()  # retrieves all journals from that handler (in pandas.DataFrame format)
            if not df_journals.empty and 'id' in df_journals.columns:  # proceeds only if:
                                                                       #     - the DataFrame is not empty
                                                                       #     - the 'id' column exists, which is necessary to filter on identifiers
                # apply both filters in one vectorized operation
                apc_mask = df_journals['apc'].isin(['No', False])  # creates a Boolean filter that is True only for journals that do not require APC (i.e. are Diamond); the value can be 'No' or False, depending on the representation in the dataset
                id_mask = df_journals['id'].isin(all_identifiers)  # filters the df_journals DataFrame to keep only rows where the value of the 'id' column is contained in the all_identifiers set
                combined_mask = apc_mask & id_mask  # creates a combined filter that selects only rows from a DataFrame (df_journals) that:
                                                    #     - DO NOT require APC (apc_mask)
                                                    #     - HAVE an identifier among the relevant ones (id_mask)
                                                    # the result is a single Boolean Series called combined_mask, which has the value True only where both filters are True
                result.extend(self._makeJournals(df_journals[combined_mask]))  # applies the combined_mask filter to the DataFrame
                                                                               # converts the resulting rows into Journal objects using _makeJournals
                                                                               # adds these objects to the result list
        return result  # returns the final list of Diamond journals that:
                       #     - belong to the specified areas
                       #     - belong to the specified categories
                       #     - belong to the specified quartiles (those selected in the previous block via identifiers)
