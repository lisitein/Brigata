# 📘 Brigata — DHDK 2025
### Scholarly Journal Integration & Query Engine

## 🎯 Project Goal

This project integrates scholarly journal metadata coming from two heterogeneous sources:
- **a graph database** (Blazegraph + RDF triples)
- **a relational database** (SQLite)

and exposes a unified Python query engine that returns results as Python objects (`Journal`, `Category`, `Area`).
The architecture is modular, extensible, and designed to support heterogeneous data ingestion and querying.

---

## 📚 Data Sources

### Graph Database (CSV → RDF → Blazegraph)
- Source: *Directory of Open Access Journals (DOAJ)*
- Contains: ISSN, title, languages, publisher, license, APC, DOAJ Seal
- Loaded via: **JournalUploadHandler**

### Relational Database (JSON → SQLite)
- Source: *Scimago Journal Rank (SJR)*
- Contains: categories, areas, quartiles
- Loaded via: **CategoryUploadHandler**

---

## 🏗️ System Architecture

### Overall Workflow

<img src="images/workflow.png" width="900"/>

1. **CategoryUploadHandler** loads JSON into the relational database (SQLite).
2. **JournalUploadHandler** loads CSV into the graph database (Blazegraph).
3. **CategoryQueryHandler** and **JournalQueryHandler** retrieve data as Pandas DataFrames.
4. **FullQueryEngine** integrates both sources and returns Python objects.

---

## 🗄️ Relational Database Schema

<img src="images/relational_database_structure.png" width="900"/>

The schema includes:
- `IdentifiableEntity`
- `HasCategory`
- `HasArea`

This structure supports many-to-many relationships between journals, categories, and areas.

---

## 🧩 Python Data Model

<img src="images/datamodel.png" width="700"/>

### IdentifiableEntity
Base class for all entities with one or more identifiers.

### Journal
- title
- languages
- publisher
- seal
- license
- apc
- hasCategory
- hasArea

### Category
- id
- quartile

### Area
- id

---

## 🔍 Query Engine

### CategoryQueryHandler
Queries the relational database (SQLite):
- `getAllCategories()`
- `getAllAreas()`
- `getCategoriesWithQuartile()`
- `getCategoriesAssignedToAreas()`
- `getAreasAssignedToCategories()`
- `getCategoryWithName()` — returns all categories whose name partially matches the input string
- `getAreaWithName()` — returns all areas whose name partially matches the input string

### BasicQueryEngine
Provides simple filters over the graph database:
- `getAllJournals()`
- `getJournalsWithTitle()`
- `getJournalsPublishedBy()`
- `getJournalsWithLicense()`
- `getJournalsWithAPC()`
- `getJournalsWithDOAJSeal()`
- `getAllCategories()`
- `getAllAreas()`
- `getCategoriesWithQuartile()`

### FullQueryEngine
Provides composite queries combining graph + relational data:
- `getJournalsInCategoriesWithQuartile()`
- `getJournalsInAreasWithLicense()`
- `getDiamondJournalsInAreasAndCategoriesWithQuartile()`
- `getJournalByName()` — returns journals whose title contains the input string and that have at least one category or area whose name also contains it

---

## 📦 Project Structure

```
Brigata/
├── main.py
├── impl.py
├── laura.py
├── Yang.py
├── daniele.py
├── li.py
├── baseHandler.py
├── images/
│   ├── workflow.png
│   ├── relational_database_structure.png
│   ├── datamodel.png
├── data/
│   ├── doaj.csv
│   ├── scimago.json
└── relational.db        # auto-generated
```

---

## ⚙️ Installation

Install dependencies:

```
pip install pandas rdflib SPARQLWrapper sqlalchemy requests
```

---

## 🔥 Running Blazegraph

Download Blazegraph:

```
wget https://github.com/blazegraph/database/releases/download/BLAZEGRAPH_2_1_6_RC/blazegraph.jar
```

Start the server:

```
java -server -Xmx1g -jar blazegraph.jar
```

Verify at `http://localhost:9999/blazegraph/`.

> ⚠️ Update the Blazegraph endpoint URL in `main.py` to match your local setup. The default is `http://127.0.0.1:9999/blazegraph/namespace/kb/sparql`.

---

## 🚀 Running the Project

```
python main.py
```

This will:
- upload CSV to Blazegraph
- upload JSON to SQLite
- initialize the query engine
- run example queries

---

## 👥 Team Members

| Name | Role | Email |
|------|------|-------|
| **Tianchi Yang** | Graph Query Handler (SPARQL) | tianchi.yang@studio.unibo.it |
| **Yihua Li** | Graph Upload Handler (RDF → Blazegraph) | yihua.li@studio.unibo.it |
| **Daniele Camagna** | Relational DB + Upload Handler | daniele.camagna@studio.unibo.it |
| **Laura Bortoli** | Query Engine (Basic & Full) | laura.bortoli@studio.unibo.it |
