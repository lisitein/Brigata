from pandas import *
from json import load
from sqlite3 import connect
from baseHandler import UploadHandler

class CategoryUploadHandler(UploadHandler):
    def pushDataToDb(self, path):
        with open(path, mode="r", encoding="UTF-8") as f:
            json_content = load(f)

        # determine last internal ids already present
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

        # collect journals (one row per external identifier)
        journal_internal_id = []
        journal_id = []
        placeholder = []
        # additional metadata columns to keep compatibility with laura.py / Yang
        languages_col = []
        publisher_col = []
        license_col = []
        apc_col = []
        seal_col = []

        for n in range(len(json_content)):
            number_of_identifiers = len(json_content[n].get('identifiers', []))
            # metadata for this journal (same for each identifier row)
            langs = ",".join(json_content[n].get('languages', [])) if json_content[n].get('languages') else ""
            pub = json_content[n].get('publisher', "") or ""
            lic = json_content[n].get('license', "") or ""
            apc_val = str(json_content[n].get('apc', "") or "")
            seal_val = str(json_content[n].get('seal', "") or "")

            for m in range(number_of_identifiers):
                journal_internal_id.append(f'journal-{n+last_journal}')
                journal_id.append(json_content[n]['identifiers'][m])
                placeholder.append('')
                languages_col.append(langs)
                publisher_col.append(pub)
                license_col.append(lic)
                apc_col.append(apc_val)
                seal_col.append(seal_val)

        journal = DataFrame()
        journal.insert(0, 'internalId', Series(journal_internal_id, dtype="string"))
        journal.insert(1, 'id', Series(journal_id, dtype="string"))
        journal.insert(2, 'quartile', Series(placeholder, dtype="string"))
        # add compatibility metadata columns (may be empty)
        journal.insert(3, 'languages', Series(languages_col, dtype="string"))
        journal.insert(4, 'publisher', Series(publisher_col, dtype="string"))
        journal.insert(5, 'license', Series(license_col, dtype="string"))
        journal.insert(6, 'apc', Series(apc_col, dtype="string"))
        journal.insert(7, 'seal', Series(seal_col, dtype="string"))

        # collect areas
        all_areas_set = set()
        for j in json_content:
            for elem in j.get('areas', []):
                all_areas_set.add(elem)
        all_areas_list = list(all_areas_set)

        area_internal_id = []
        area_id = []
        placeholder = []
        for n in range(len(all_areas_list)):
            area_internal_id.append(f'area-{n+last_area}')
            area_id.append(all_areas_list[n])
            placeholder.append('')

        area = DataFrame()
        area.insert(0, 'internalId', Series(area_internal_id, dtype="string"))
        area.insert(1, 'id', Series(area_id, dtype="string"))
        area.insert(2, 'quartile', Series(placeholder, dtype="string"))

        # collect categories
        all_categories_set = set()
        for j in json_content:
            for elem in j.get('categories', []):
                if "quartile" not in elem:
                    elem["quartile"] = ''
                all_categories_set.add((elem['id'], elem['quartile']))
        all_categories_list = list(all_categories_set)

        category_internal_id = []
        category_id = []
        quartile = []
        for n in range(len(all_categories_list)):
            category_internal_id.append(f'category-{n+last_category}')
            category_id.append(all_categories_list[n][0])
            quartile.append(all_categories_list[n][1])

        category = DataFrame()
        category.insert(0, 'internalId', Series(category_internal_id, dtype="string"))
        category.insert(1, 'id', Series(category_id, dtype="string"))
        category.insert(2, 'quartile', Series(quartile, dtype="string"))

        # create IdentifiableEntity table (journals, areas, categories)
        identifiable_entity = concat([journal, area, category], axis=0)

        # Now create HasCategory
        starting_journal = []
        matching_category = []
        matching_quartile = []
        for n in range(len(json_content)):
            for categ in json_content[n].get('categories', []):
                starting_journal.append(f'journal-{n+last_journal}')
                matching_category.append(categ['id'])
                matching_quartile.append(categ.get('quartile', ''))

        has_category = DataFrame()
        has_category.insert(0, 'journalId', Series(starting_journal, dtype="string"))
        has_category.insert(1, 'categoryName', Series(matching_category, dtype="string"))
        has_category.insert(2, 'quartile', Series(matching_quartile, dtype="string"))

        # merge to get category internalId
        has_category = merge(
            identifiable_entity,
            has_category,
            left_on=["id", 'quartile'],
            right_on=['categoryName', 'quartile']
        )[['journalId', 'internalId']]
        has_category = has_category.rename(columns={"internalId": "categoryId"})

        # Now create HasArea
        starting_journal = []
        matching_area = []
        for n in range(len(json_content)):
            for ar in json_content[n].get('areas', []):
                starting_journal.append(f'journal-{n+last_journal}')
                matching_area.append(ar)

        has_area = DataFrame()
        has_area.insert(0, 'journalId', Series(starting_journal, dtype="string"))
        has_area.insert(1, 'areaName', Series(matching_area, dtype="string"))

        has_area = merge(identifiable_entity, has_area, left_on="id", right_on="areaName")[['journalId', "internalId"]]
        has_area = has_area.rename(columns={"internalId": "areaId"})

        # Upload tables into the relational database
        with connect(self.dbPathOrUrl) as con:
            identifiable_entity.to_sql("IdentifiableEntity", con, if_exists="append", index=False)
            has_category.to_sql("HasCategory", con, if_exists="append", index=False)
            has_area.to_sql("HasArea", con, if_exists="append", index=False)
            con.commit()

        return True
