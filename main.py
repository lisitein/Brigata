# main.py - Example of using the project

from impl import *

def main():
    # 1. set the db path
    relational_db = "my_journals.db"  # SQLite db file
    graph_db = "http://127.0.0.1:9999/blazegraph/sparql"  # Blazegraph
    
    # 2. upload data
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
    
    # create query handler
    journal_query = JournalQueryHandler()
    journal_query.setDbPathOrUrl(graph_db)
    
    category_query = CategoryQueryHandler()
    category_query.setDbPathOrUrl(relational_db)
    
    #  create FullQueryEngine
    engine = FullQueryEngine()
    engine.addJournalHandler(journal_query)
    engine.addCategoryHandler(category_query)
    
    # 4. Actual query and use
    print("\n=== start querying ===")
    
    # query all journals
    print("1. get all journals (first 5):")
    all_journals = engine.getAllJournals()
    for i, journal in enumerate(all_journals[:5]):
        print(f"   - {journal.getTitle()} (publisher: {journal.getPublisher()})")
    print(f"   There are {len(all_journals)} journals in total")
    
    # query journals with title
    print("\n2. query journals with title 'Science' (first 3):")
    science_journals = engine.getJournalsWithTitle("Science")
    for journal in science_journals[:3]:
        print(f"   - {journal.getTitle()}")
    
    # query journals by publisher
    print("\n3. query journals by publisher Elsevier (first 3):")
    elsevier_journals = engine.getJournalsPublishedBy("Elsevier")
    print(f"   found {len(elsevier_journals)} journals published by Elsevier")
    for journal in elsevier_journals[:3]:
        print(f"   - {journal.getTitle()}")
    
    # query journals with DOAJ Seal
    print("\n4. query journals with DOAJ Seal (first 3):")
    doaj_journals = engine.getJournalsWithDOAJSeal()
    print(f"   found {len(doaj_journals)} journals with DOAJ Seal")
    
    # query journals with CC BY license
    print("\n5. query journals with CC BY license (first 3):")
    cc_by_journals = engine.getJournalsWithLicense({"CC BY"})
    print(f"   found {len(cc_by_journals)} journals with CC BY license")
    
    # query all categories
    print("\n6. get all categories (first 5):")
    all_categories = engine.getAllCategories()
    print(f"   There are{len(all_categories)} categories in total")
    for cat in all_categories[:5]:
        print(f"   - {cat.getId()} (quartile: {cat.getQuartile()})")
    
    # complex query: diamond open access journals
    print("\n7. query diamond open access journals (no APC fee):")
    diamond_journals = engine.getDiamondJournalsInAreasAndCategoriesWithQuartile(
        areas={"Computer Science"}, 
        category_ids={"Artificial Intelligence"}, 
        quartiles={"Q1"}
    )
    print(f"   found {len(diamond_journals)} diamond open access journals according with the conditions")
    
    print("\n=== query finished ===")

def search_by_user_input():
    """Interactive query function"""
    # set query engine (assuming data has been uploaded)
    journal_query = JournalQueryHandler()
    journal_query.setDbPathOrUrl("http://127.0.0.1:9999/blazegraph/sparql")
    
    category_query = CategoryQueryHandler()
    category_query.setDbPathOrUrl("my_journals.db")
    
    engine = FullQueryEngine()
    engine.addJournalHandler(journal_query)
    engine.addCategoryHandler(category_query)
    
    while True:
        print("\n=== Journal query system ===")
        print("1. query journals with title")
        print("2. query journals by publisher")
        print("3. query journals with DOAJ Seal")
        print("4. quit")
        
        choice = input("please choose function (1-4): ").strip()
        
        if choice == "1":
            title = input("please type in the title keyword: ").strip()
            journals = engine.getJournalsWithTitle(title)
            print(f"\nfound {len(journals)} journals (first 10):")
            for journal in journals[:10]:  
                print(f"- {journal.getTitle()} ({journal.getPublisher()})")
                
        elif choice == "2":
            publisher = input("please type in the publisher name: ").strip()
            journals = engine.getJournalsPublishedBy(publisher)
            print(f"\nfound {len(journals)} journals published by {publisher}:")
            for journal in journals[:10]:
                print(f"- {journal.getTitle()}")
                
        elif choice == "3":
            journals = engine.getJournalsWithDOAJSeal()
            print(f"\nfound {len(journals)} journals with DOAJ Seal:")
            for journal in journals[:10]:
                print(f"- {journal.getTitle()} ({journal.getPublisher()})")
                
        elif choice == "4":
            print("Goodbye！")
            break
        else:
            print("Invalid choice, please try again.")

if __name__ == "__main__":
    # choose the mode
    mode = input("choose mode - 1: demonstration query, 2: interactive search: ").strip()
    
    if mode == "1":
        main()
    elif mode == "2":
        search_by_user_input()
    else:
        print("running default demonstration...")
        main()