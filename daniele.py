from pandas import *
from json import load
from sqlite3 import connect
from baseHandler import UploadHandler

class CategoryUploadHandler(UploadHandler):
    def pushDataToDb(self, path):
        with open(path, mode="r", encoding="UTF-8") as f:
            json_content = load(f)
            print('Number of journals in the dataset:', len(json_content))     #the json file contains len(json_content) journals
        

        #I created an image of the tables in my relational database and I uploaded on GitHub: daniele_relational_database_structure.png

        # let's create the table "Journal"              
        journal_internal_id=[]
        journal_id=[]
        for n in range(len(json_content)):
            number_of_identifiers=len(json_content[n]['identifiers'])
            for m in range(number_of_identifiers):
                journal_internal_id.append(f'journal-{n}')
                journal_id.append(json_content[n]['identifiers'][m])

        journal=DataFrame()                 
        journal.insert(0, 'internalId', Series(journal_internal_id, dtype="string"))
        journal.insert(1, 'id', Series(journal_id, dtype="string"))
        
        # let's create the table "Area"
        all_areas_list=[]                #these 3 lines are necessary to understand how many unique areas' names are there in the set
        for j in json_content:
            this_journal_areas=[]
            for elem in j['areas']:
                if [elem] not in all_areas_list:            #this instruction aims to avoid duplicates
                    this_journal_areas.append([elem])
            all_areas_list.extend(this_journal_areas)        #in the end, I got a list of lists

        area_internal_id=[]
        area_id=[]
        for n in range(len(all_areas_list)):
            number_of_identifiers=len(all_areas_list[n])    #actually, here the 'identifiers' are simply the names of the areas
            for m in range(number_of_identifiers):
                area_internal_id.append(f'area-{n}')
                area_id.append(all_areas_list[n][m])

        area=DataFrame()
        area.insert(0, 'internalId', Series(area_internal_id, dtype="string"))
        area.insert(1, 'id', Series(area_id, dtype="string"))

        # let's create the table "Category"
        all_categories_list=[]                #these 3 lines are necessary to understand how many unique categories' names are there in the set
        couples_category_quartile=[]          #these 3 variables are useful tu understand what are the couples category-quartile in the dataset
        categories_for_quartiles=[]           
        quartiles_for_categories=[]
        for j in json_content:
            this_journal_categories=[]
            for elem in j['categories']:
                if [elem["id"]] not in all_categories_list:            #this instruction aims to avoid duplicates
                    this_journal_categories.append([elem["id"]])
                if "quartile" in elem:
                    if ([elem["id"]][0],elem["quartile"]) not in couples_category_quartile: #a list of tuples category-quartile
                        couples_category_quartile.append(([elem["id"]][0],elem["quartile"]))  #for every couple, as id I use only the first of the hypothetical many
                    else:
                        couples_category_quartile.append(([elem["id"]][0],''))
            all_categories_list.extend(this_journal_categories)        #in the end, I got a list of lists

        category_internal_id=[]
        category_id=[]
        for n in range(len(all_categories_list)):
            number_of_identifiers=len(all_categories_list[n])    #actually, here the 'identifiers' are simply the names of categories
            for m in range(number_of_identifiers):
                category_internal_id.append(f'category-{n}')
                category_id.append(all_categories_list[n][m])

        category=DataFrame()
        category.insert(0, 'internalId', Series(category_internal_id, dtype="string"))
        category.insert(1, 'id', Series(category_id, dtype="string"))

        for a,b in couples_category_quartile:
            categories_for_quartiles.append(a)    
            quartiles_for_categories.append(b)
        category_with_quartile_id=[]
        for n in range(len(categories_for_quartiles)):          #assign id to couples category-quartile
            category_with_quartile_id.append(f'category-with-quartile-{n}')

        category_quartile=DataFrame()
        category_quartile.insert(0, 'name', Series(categories_for_quartiles, dtype="string"))
        category_quartile.insert(1, 'quartile', Series(quartiles_for_categories, dtype="string"))
        category_quartile.insert(2, 'categoryWithQuartileId', Series(category_with_quartile_id, dtype="string"))



        #let's define the real TABLES for the relational database, by performing some operations
        table_journal=journal[["internalId"]].drop_duplicates()
        table_area=area[["internalId"]].drop_duplicates()
        table_category=merge(category, category_quartile, left_on="id", right_on="name")[["internalId","quartile","categoryWithQuartileId"]]
        table_identifiable_entity_id=concat([journal,area,category],axis=0)

        
            #now let's work to create the table HasCategory
        journals_for_categories_quartiles=[]      
        categories_quartiles_for_journals=[]
        
        for n in range(len(json_content)):
            number_of_categories_quartiles=len(json_content[n]['categories'])
            for m in range(number_of_categories_quartiles):
                journals_for_categories_quartiles.append(f'journal-{n}')
                categories_quartiles_for_journals.append(f'{[json_content[n]['categories'][m]["id"]][0]} - {json_content[n]['categories'][m]["quartile"]}') 
        
        journal_category=DataFrame()     
        journal_category.insert(0, 'Journal', Series(journals_for_categories_quartiles, dtype="string"))
        journal_category.insert(1, 'Category-Quartile', Series(categories_quartiles_for_journals, dtype="string"))


        couples_category_quartile_string_version=[] #tuples cannot be passed into a dataframe, that is why I am transforming them into strings
        for a,b in couples_category_quartile:
            couples_category_quartile_string_version.append(f'{a} - {b}')

        temporary_frame_cat_quart_id=DataFrame()
        temporary_frame_cat_quart_id.insert(0, 'Category-Quartile', Series(couples_category_quartile_string_version, dtype="string"))
        temporary_frame_cat_quart_id.insert(1, 'categoryWithQuartileId', Series(category_with_quartile_id, dtype="string"))


        table_has_category=merge(journal_category, temporary_frame_cat_quart_id, left_on='Category-Quartile', right_on='Category-Quartile')[['Journal','categoryWithQuartileId']]
        table_has_category=table_has_category.rename(columns={'Journal':"hasCategoryId",'categoryWithQuartileId':'categoryId'})

            #table_has_category is ready, now let's work for table Has Area:

        journals_for_areas=[]      
        areas_for_journals=[]
        
        for n in range(len(json_content)):
            number_of_areas=len(json_content[n]['areas'])
            for m in range(number_of_areas):
                journals_for_areas.append(f'journal-{n}')
                areas_for_journals.append([json_content[n]['areas'][m]][0]) 
        
        journal_area=DataFrame()     
        journal_area.insert(0, 'Journal', Series(journals_for_areas, dtype="string"))
        journal_area.insert(1, 'Area', Series(areas_for_journals, dtype="string"))

        table_has_area=merge(journal_area, area, left_on="Area", right_on="id")[['Journal',"internalId"]]
        table_has_area=table_has_area.rename(columns={'Journal':"hasAreaId","internalId":"areaId"})

            #table_has_area is now ready

        with connect(self.dbPathOrUrl) as con:
            table_journal.to_sql("Journal", con, if_exists="replace", index=False)
            table_area.to_sql("Area", con, if_exists="replace", index=False)
            table_category.to_sql("Category", con, if_exists="replace", index=False)
            table_identifiable_entity_id.to_sql("IdentifiableEntityId", con, if_exists="replace", index=False)
            table_has_category.to_sql("HasCategory", con, if_exists="replace", index=False)
            table_has_area.to_sql("HasArea", con, if_exists="replace", index=False)
            con.commit()

        return True



#PERPLEXITIES:
#how do we handle possible repeated items in the dataset provided?  
#A PROBLEM THAT STILL EXIST: APPARENTLY THE SOFTWARE CAN'T HANDLE CASES IN WHICH CATEGORIES (AND MAYBE AREAS) HAVE MULTIPLE IDs
#OTHER PROBLEM: WHAT IF AREA OR CATEGORY ARE ABSENT? IT CAN HAPPEN  (actually that problem is already handled I think)
#make the software more robust at classes level; think how to handle if many datasets are loaded one after the other


#let's try to create a relational database using a small part of the json file we are provided:
#rel_path = "small_database.db"
#cat = CategoryUploadHandler()
#cat.setDbPathOrUrl(rel_path)
#cat.pushDataToDb("reduced_dataset.json")
#I load on github the small database: daniele_small_database.db
