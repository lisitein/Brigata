# main.py - Example of using the project

import os

from laura import FullQueryEngine
from daniele import CategoryUploadHandler
from Yang import JournalQueryHandler, CategoryQueryHandler
from li import JournalUploadHandler


def main():
    # 1. set the db path
    relational_db = "my_journals.db"  # SQLite db file
    graph_db = "http://192.168.1.226:9999/blazegraph/namespace/kb/sparql"  # Blazegraph endpoint
    
    # reset relational DB to avoid duplicates
    if os.path.exists(relational_db):
        os.remove(relational_db)

    print("=== upload data to database ===")
    
    # upload csv to graph db
    journal_uploader = JournalUploadHandler()
    journal_uploader.setDbPathOrUrl(graph_db)
    print("uploading journal data...")
    journal_uploader.pushDataToDb("data/doaj.csv")
    print("Journal data uploaded!")
    
    # upload json to relational db
    category_uploader = CategoryUploadHandler()
    category_uploader.setDbPathOrUrl(relational_db)
    print("uploading category data...")
    category_uploader.pushDataToDb("data/scimago.json")
    print("Category data uploaded!")
    
    # 3. create query engine
    print("\n=== set query engine ===")
    
    journal_query = JournalQueryHandler()
    journal_query.setDbPathOrUrl(graph_db)
    
    category_query = CategoryQueryHandler()
    category_query.setDbPathOrUrl(relational_db)
    
    engine = FullQueryEngine()
    engine.addJournalHandler(journal_query)
    engine.addCategoryHandler(category_query)
    
    # 4. Actual query and use
    print("\n=== start querying ===")
    
    print("1. get all journals (first 5):")
    all_journals = engine.getAllJournals()
    for journal in all_journals[:5]:
        print(f"   - {journal.getTitle()} (publisher: {journal.getPublisher()})")
    print(f"   Total journals: {len(all_journals)}")
    
    print("\n2. query journals with title 'Science' (first 3):")
    science_journals = engine.getJournalsWithTitle("Science")
    for journal in science_journals[:3]:
        print(f"   - {journal.getTitle()}")
    
    print("\n3. query journals by publisher 'Elsevier' (first 3):")
    elsevier_journals = engine.getJournalsPublishedBy("Elsevier")
    print(f"   found {len(elsevier_journals)} journals")
    for journal in elsevier_journals[:3]:
        print(f"   - {journal.getTitle()}")
    
    print("\n4. query journals with DOAJ Seal:")
    doaj_journals = engine.getJournalsWithDOAJSeal()
    print(f"   found {len(doaj_journals)} journals")
    
    print("\n5. query journals with CC BY license:")
    cc_by_journals = engine.getJournalsWithLicense({"CC BY"})
    print(f"   found {len(cc_by_journals)} journals")
    
    print("\n6. get all categories (first 5):")
    all_categories = engine.getAllCategories()
    print(f"   Total categories: {len(all_categories)}")
    for cat in all_categories[:5]:
        print(f"   - {cat.getId()} (quartile: {cat.getQuartile()})")
    
    print("\n7. query diamond open access journals (example):")
    diamond_journals = engine.getDiamondJournalsInAreasAndCategoriesWithQuartile(
    area_ids={"Medicine"},
    category_ids={"Drug Discovery"},
    quartiles={"Q1"}
    )
    print(f"   found {len(diamond_journals)} diamond journals")


if __name__ == "__main__":
    main()
