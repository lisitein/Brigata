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

![Workflow](workflow.png)  

1. **CategoryUploadHandler** loads JSON data into the **relational database** (SQLite).
2. **JournalUploadHandler** loads CSV data into the **graph database** (RDF triples or Neo4j-like structure).
3. **CategoryQueryHandler** and **JournalQueryHandler** retrieve relevant data as Pandas DataFrames.
4. **FullQueryEngine** integrates the data from both sources and transforms it into Python objects.

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

## Team Members

| Name | Role | Email |
|------|------|-------|
| **Tianchi Yang** | Category & Journal QueryHandlers | tianchi.yang@studio.unibo.it |
| **Yihua Li** | Graph database & JournalUploadHandler | yihua.li@studio.unibo.it |
| **Daniele Camagna** | Relational database & CategoryUploadHandler | daniele.camagna@studio.unibo.it |
| **Laura Bortoli** | Query Engine (Basic & Full) | laura.bortoli@studio.unibo.it |

---
