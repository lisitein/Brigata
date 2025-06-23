from impl import *

# Setup databases
relational_db = "journals.db"
graph_db = "http://127.0.0.1:9999/blazegraph/sparql"

# Upload data (do this once)
journal_uploader = JournalUploadHandler()
journal_uploader.setDbPathOrUrl(graph_db)
journal_uploader.pushDataToDb("data/doaj.csv")

category_uploader = CategoryUploadHandler()
category_uploader.setDbPathOrUrl(relational_db)
category_uploader.pushDataToDb("data/scimago.json")

# Create query engine
journal_query = JournalQueryHandler()
journal_query.setDbPathOrUrl(graph_db)

category_query = CategoryQueryHandler()
category_query.setDbPathOrUrl(relational_db)

engine = FullQueryEngine()
engine.addJournalHandler(journal_query)
engine.addCategoryHandler(category_query)

# Use the system
all_journals = engine.getAllJournals()
open_access = engine.getJournalsWithDOAJSeal()

# Get results
for journal in science_journals[:5]:
    print(journal.getTitle())
