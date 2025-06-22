# -*- coding: utf-8 -*-
from impl import (
    CategoryUploadHandler, CategoryQueryHandler,
    JournalUploadHandler, JournalQueryHandler,
    FullQueryEngine
)

# === 1. Set up relational database (scimago.json) ===
rel_path = "relational.db"
cat_uploader = CategoryUploadHandler()
cat_uploader.setDbPathOrUrl(rel_path)
cat_uploader.pushDataToDb("data/scimago.json")

# === 2. Set up graph database (doaj.csv) ===
graph_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
jou_uploader = JournalUploadHandler()
jou_uploader.setDbPathOrUrl(graph_endpoint)
jou_uploader.pushDataToDb("data/doaj.csv")

# === 3. Build query handlers ===
cat_handler = CategoryQueryHandler()
cat_handler.setDbPathOrUrl(rel_path)

jou_handler = JournalQueryHandler()
jou_handler.setDbPathOrUrl(graph_endpoint)

# === 4. Build FullQueryEngine and register handlers ===
engine = FullQueryEngine()
engine.addCategoryHandler(cat_handler)
engine.addJournalHandler(jou_handler)

# === 5. Run example queries ===

print("\n--- All Journals ---")
journals = engine.getAllJournals()
for j in journals[:3]:
    print("-", j.getTitle())

print("\n--- Journals with 'Artificial Intelligence' in Q1 ---")
ai_q1_journals = engine.getJournalsInCategoriesWithQuartile(
    {"Artificial Intelligence"}, {"Q1"}
)
for j in ai_q1_journals[:3]:
    print("-", j.getTitle())

print("\n--- Entity by ID: '2532-8816' ---")
entity = engine.getEntityById("2532-8816")
print(entity.__class__.__name__, "→", entity.getId())

print("\n--- Diamond Open Access Journals in Oncology, Q1 ---")
diamond_journals = engine.getDiamondJournalsInAreasAndCategoriesWithQuartile(
    {"Oncology"}, {"Oncology"}, {"Q1"}
)
for j in diamond_journals[:3]:
    print("-", j.getTitle())