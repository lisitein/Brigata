from pandas import *
from json import load
from sqlite3 import connect
from baseHandler import UploadHandler

#I created an image of the relational database and I uploaded on GitHub: yangish_database.png

class CategoryUploadHandler(UploadHandler):
    def pushDataToDb(self, path):
        with open(path, mode="r", encoding="UTF-8") as f:
            json_content = load(f)
            #print('Number of journals in the dataset:', len(json_content))     #the json file contains len(json_content) journals

        # let's see if there are already tables in the database. In case, let's 
        # understand what are the last internal ids used, so we continue from them
        with connect(self.dbPathOrUrl) as con:
            try:
                existing = read_sql("SELECT internalId FROM IdentifiableEntity", con)
                last_journal = max([int(id.split('-')[1]) for id in existing['internalId'] if id.startswith('journal-')], default=0) + 1
                last_area = max([int(id.split('-')[1]) for id in existing['internalId'] if id.startswith('area-')], default=0) + 1
                last_category = max([int(id.split('-')[1]) for id in existing['internalId'] if id.startswith('category-')], default=0) + 1
            except:
                last_journal = 0
                last_area = 0
                last_category = 0

        # let's collect the journals 
        journal_internal_id=[]
        journal_id=[]
        placeholder=[]
        for n in range(len(json_content)):
            number_of_identifiers=len(json_content[n]['identifiers'])
            for m in range(number_of_identifiers):
                journal_internal_id.append(f'journal-{n+last_journal}')
                journal_id.append(json_content[n]['identifiers'][m])
                placeholder.append('')

        journal=DataFrame()                 
        journal.insert(0, 'internalId', Series(journal_internal_id, dtype="string"))
        journal.insert(1, 'id', Series(journal_id, dtype="string"))
        journal.insert(2, 'quartile', Series(placeholder, dtype="string"))
        
        # let's collect the areas
        all_areas_set=set()
        for j in json_content:
            for elem in j['areas']:
                all_areas_set.add(elem)
        all_areas_list=list(all_areas_set)

        area_internal_id=[]
        area_id=[]
        placeholder=[]
        for n in range(len(all_areas_list)):
            area_internal_id.append(f'area-{n+last_area}')
            area_id.append(all_areas_list[n])
            placeholder.append('')

        area=DataFrame()
        area.insert(0, 'internalId', Series(area_internal_id, dtype="string"))
        area.insert(1, 'id', Series(area_id, dtype="string"))
        area.insert(2, 'quartile', Series(placeholder, dtype="string"))

    # let's collect the categories

        all_categories_set=set()                
        for j in json_content:
            for elem in j['categories']:
                if "quartile" not in elem:
                    elem["quartile"]=''
                all_categories_set.add((elem['id'],elem['quartile']))
        all_categories_list=list(all_categories_set)

        category_internal_id=[]
        category_id=[]
        quartile=[]
        for n in range(len(all_categories_list)):
            category_internal_id.append(f'category-{n+last_category}')
            category_id.append(all_categories_list[n][0])
            quartile.append(all_categories_list[n][1])

        category=DataFrame()
        category.insert(0, 'internalId', Series(category_internal_id, dtype="string"))
        category.insert(1, 'id', Series(category_id, dtype="string"))
        category.insert(2, 'quartile', Series(quartile, dtype="string"))


    #let's create the TABLES for the relational database operating on the dataframes

    #I start with the table IdentifiableEntity

        identifiable_entity=concat([journal,area,category],axis=0)
  
    #Now I work to create the table HasCategory
        
        starting_journal=[]
        matching_category=[]
        matching_quartile=[]
        for n in range(len(json_content)):
            for categ in json_content[n]['categories']:
                starting_journal.append(f'journal-{n+last_journal}')
                matching_category.append(categ['id'])
                matching_quartile.append(categ['quartile'])

        has_category=DataFrame()   
        has_category.insert(0, 'journalId', Series(starting_journal, dtype="string"))
        has_category.insert(1, 'categoryName', Series(matching_category, dtype="string")) 
        has_category.insert(2, 'quartile', Series(matching_quartile, dtype="string"))

        # Add a filter to distinguish categories from areas in the identifiable_entity table
        only_categories = identifiable_entity[identifiable_entity['internalId'].str.startswith('category-')]
        has_category=merge(only_categories, has_category, left_on=["id", 'quartile'], right_on=['categoryName', 'quartile'])[['journalId', 'internalId']]
        has_category=has_category.rename(columns={"internalId":"categoryId"})


    #Now I work for table HasArea:
        
        starting_journal=[]
        matching_area=[]
        for n in range(len(json_content)):
            for ar in json_content[n]['areas']:
                starting_journal.append(f'journal-{n+last_journal}')
                matching_area.append(ar)

        has_area=DataFrame()   
        has_area.insert(0, 'journalId', Series(starting_journal, dtype="string"))
        has_area.insert(1, 'areaName', Series(matching_area, dtype="string")) 

        # Add a filter to distinguish areas from categories in the identifiable_entity table
        only_areas = identifiable_entity[identifiable_entity['internalId'].str.startswith('area-')]
        has_area=merge(only_areas, has_area, left_on="id", right_on="areaName")[['journalId',"internalId"]]
        has_area=has_area.rename(columns={"internalId":"areaId"})

    #I upload the tables in the relational database:

        with connect(self.dbPathOrUrl) as con:  
            identifiable_entity.to_sql("IdentifiableEntity", con, if_exists="append", index=False)
            has_category.to_sql("HasCategory", con, if_exists="append", index=False)
            has_area.to_sql("HasArea", con, if_exists="append", index=False)
            con.commit()            

        return True
