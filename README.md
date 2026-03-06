# Brigata — DHDK 2025
group: Members of the Brigata   

## Project Goal  

The goal of the project is to integrate and query scholarly journal metadata using both a **relational database** and a **graph database**, and to implement a **query engine** capable of returning results as Python objects. The overall architecture is modular and supports handlers for uploading and querying data from heterogeneous sources.

---

## Data Sources

- **CSV (graph database)**: journal metadata from [DOAJ](https://doaj.org/)
- **JSON (relational database)**: category and area information from [Scimago Journal Rank](https://www.scimagojr.com/)
  
Each dataset has been normalized and loaded into its respective database type through custom upload handlers.

---

## Project Architecture

<img width="1136" alt="workflow" src="https://github.com/user-attachments/assets/aeb1a702-6430-49d2-84a0-c5673705d637" />

1. **CategoryUploadHandler** loads JSON data into the **relational database** (SQLite).  
2. **JournalUploadHandler** loads CSV data into the **graph database** (RDF triples or Neo4j-like structure).
3. **CategoryQueryHandler** and **JournalQueryHandler** retrieve relevant data as Pandas DataFrames.
4. **FullQueryEngine** integrates the data from both sources and transforms it into Python objects.

   The structure of relational database:
  <img width="1136" alt="daniele_relational_database_structure" src="https://github.com/lisitein/Brigata/blob/d9acf3ae581000e87c7c6de25154ea8920c245ac/yangish_database.png" />


---

## Python Data Model

We defined three core entities:

- `Journal` with metadata like `title`, `language`, `license`, `apc`, `hasCategory`, `hasArea`
- `Category` with `id` and `quartile`
- `Area` with `id`

All classes extend from the abstract `IdentifiableEntity`.

---

## Query Engine Functionality

Implemented in two levels:

- `BasicQueryEngine`: simple filters such as title, license, seal, APC, quartile
- `FullQueryEngine`: composite queries combining multiple sources, such as:
  - Journals in specific **categories** and **quartiles**
  - Journals in specific **areas** and with a given **license**
  - **Diamond Open Access** journals matching multiple conditions  

---

## How to Run the Project🚀

### 1. Project Setup

First, download all the python files and organize them in a folder with the following structure:

```
your_project/
├── main.py    
├── impl.py    
├── baseHandler.py           
├── daniele.py
├── li.py 
├── Yang.py 
├── laura.py 
├── data/
│   ├── XX.csv              
│   └── XXX.json            
│   └── ...                 # additional .csv or .json files
└── relational.db             # (auto-generated, no need to create manually)
```

**Important**: Create a `data/` folder and place your query files inside. Only **CSV** and **JSON** file formats are supported.

### 2. Install Dependencies

Install the required Python packages if they're not already in your environment:

```bash
pip install pandas pyparsing rdflib SPARQLWrapper sqlalchemy
```

To check which packages you already have installed:
```bash
pip list
```

### 3. Setup Blazegraph

Activate Blazegraph in a separate terminal window:

**Download Blazegraph** (if you don't have it):
```bash
wget https://github.com/blazegraph/database/releases/download/BLAZEGRAPH_2_1_6_RC/blazegraph.jar
```

**Start Blazegraph**:
```bash
java -server -Xmx1g -jar blazegraph.jar
```
*Make sure the path to the JAR file is correct.*

**Verify Blazegraph is running** by visiting `http://localhost:9999/blazegraph/` in your browser.

### 4. Run the Project

Once all dependencies are installed and Blazegraph is running, you can execute the project by run the main.py.
```bash
python main.py
```

---

## Team Members

| Name | Role | Email |
|------|------|-------|
| **Tianchi Yang** | Category & Journal QueryHandlers | tianchi.yang@studio.unibo.it |
| **Yihua Li** | Graph database & JournalUploadHandler | yihua.li@studio.unibo.it |
| **Daniele Camagna** | Relational database & CategoryUploadHandler | daniele.camagna@studio.unibo.it |
| **Laura Bortoli** | Query Engine (Basic & Full) | laura.bortoli@studio.unibo.it |

---

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

<img src="images/pipeline.png" width="900"/>

**1. CategoryUploadHandler** loads JSON into the relational database (SQLite).
**2. JournalUploadHandler** loads CSV into the graph database (Blazegraph).
**3. CategoryQueryHandler** and **JournalQueryHandler** retrieve data as Pandas DataFrames.
**4. FullQueryEngine** integrates both sources and returns Python objects.

---

## 🗄️ Relational Database Schema

<img src="images/relational_schema.png" width="900"/>

The schema includes:

- `IdentifiableEntity`
- `HasCategory`
- `HasArea`

This structure supports many-to-many relationships between journals, categories, and areas.

---

## 🧩 Python Data Model

<img src="images/data_model.png" width="700"/>

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

### BasicQueryEngine

Provides simple filters:

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

---

## 📦 Project Structure

```
your_project/
├── main.py
├── laura.py
├── Yang.py
├── daniele.py
├── li.py
├── baseHandler.py
├── test.py
├── images/
│   ├── pipeline.png
│   ├── relational_schema.png
│   ├── data_model.png
│   ├── uml_query_handlers.png
├── data/
│   ├── doaj.csv
│   ├── scimago.json
└── my_journals.db   # auto-generated
```

---

## ⚙️ Installation

Install dependencies:
```
pip install pandas rdflib SPARQLWrapper sqlalchemy
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

Verify:
```
http://localhost:9999/blazegraph/
```

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
