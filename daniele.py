from pandas import *
from json import load
from sqlite3 import connect
from baseHandler import UploadHandler, QueryHandler

#I created an image of the relational database and I uploaded on GitHub: yangish_database.png

# ============================================================
# CATEGORY UPLOAD HANDLER
# ============================================================

class CategoryUploadHandler(UploadHandler):
    """
    Uploads JSON data into the relational SQLite database.
    This version preserves the original logic but fixes fragile merges,
    ensures consistent quartile handling, and improves ID generation safety.
    """

    def pushDataToDb(self, path):
        # ---- Load JSON ----
        with open(path, mode="r", encoding="UTF-8") as f:
            json_content = load(f)

        # ---- Determine next internal IDs safely ----
        with connect(self.dbPathOrUrl) as con:
            try:
                existing = read_sql("SELECT internalId FROM IdentifiableEntity", con)

                def next_id(prefix):
                    ids = [
                        int(id.split('-')[1])
                        for id in existing['internalId']
                        if id.startswith(prefix)
                    ]
                    return (max(ids) + 1) if ids else 0

                last_journal = next_id("journal-")
                last_area = next_id("area-")
                last_category = next_id("category-")

            except Exception:
                last_journal = last_area = last_category = 0

        # ============================================================
        # JOURNALS
        # ============================================================

        journal_internal_id = []
        journal_id = []
        languages_col = []
        publisher_col = []
        license_col = []
        apc_col = []
        seal_col = []
        placeholder = []

        for n, entry in enumerate(json_content):
            identifiers = entry.get("identifiers", [])
            langs = ",".join(entry.get("languages", [])) if entry.get("languages") else ""
            pub = entry.get("publisher", "") or ""
            lic = entry.get("license", "") or ""
            apc_val = str(entry.get("apc", "") or "")
            seal_val = str(entry.get("seal", "") or "")

            for ident in identifiers:
                journal_internal_id.append(f"journal-{n + last_journal}")
                journal_id.append(ident)
                languages_col.append(langs)
                publisher_col.append(pub)
                license_col.append(lic)
                apc_col.append(apc_val)
                seal_col.append(seal_val)
                placeholder.append("")

        journal = DataFrame({
            "internalId": Series(journal_internal_id, dtype="string"),
            "id": Series(journal_id, dtype="string"),
            "quartile": Series(placeholder, dtype="string"),
            "languages": Series(languages_col, dtype="string"),
            "publisher": Series(publisher_col, dtype="string"),
            "license": Series(license_col, dtype="string"),
            "apc": Series(apc_col, dtype="string"),
            "seal": Series(seal_col, dtype="string"),
        })

        # ============================================================
        # AREAS
        # ============================================================

        all_areas = {ar for j in json_content for ar in j.get("areas", [])}
        all_areas = list(all_areas)

        area = DataFrame({
            "internalId": Series([f"area-{i + last_area}" for i in range(len(all_areas))], dtype="string"),
            "id": Series(all_areas, dtype="string"),
            "quartile": Series([""] * len(all_areas), dtype="string"),
        })

        # ============================================================
        # CATEGORIES
        # ============================================================

        all_categories = set()
        for j in json_content:
            for elem in j.get("categories", []):
                cid = elem.get("id", "")
                q = elem.get("quartile", "") or ""
                all_categories.add((cid, q))

        all_categories = list(all_categories)

        category = DataFrame({
            "internalId": Series([f"category-{i + last_category}" for i in range(len(all_categories))], dtype="string"),
            "id": Series([c[0] for c in all_categories], dtype="string"),
            "quartile": Series([c[1] for c in all_categories], dtype="string"),
        })

        # ============================================================
        # IDENTIFIABLE ENTITY TABLE
        # ============================================================

        identifiable_entity = concat([journal, area, category], axis=0)

        # ============================================================
        # HAS CATEGORY (robust merge)
        # ============================================================

        hc_journal = []
        hc_category = []
        hc_quartile = []

        for n, entry in enumerate(json_content):
            for categ in entry.get("categories", []):
                hc_journal.append(f"journal-{n + last_journal}")
                hc_category.append(categ.get("id", ""))
                hc_quartile.append(categ.get("quartile", "") or "")

        has_category = DataFrame({
            "journalId": Series(hc_journal, dtype="string"),
            "categoryName": Series(hc_category, dtype="string"),
            "quartile": Series(hc_quartile, dtype="string"),
        })

        category_lookup = identifiable_entity[
            identifiable_entity["internalId"].str.startswith("category-")
        ][["id", "internalId"]]

        has_category = merge(
            has_category,
            category_lookup,
            left_on="categoryName",
            right_on="id",
            how="inner"
        )[["journalId", "internalId"]].rename(columns={"internalId": "categoryId"})

        # ============================================================
        # HAS AREA (robust merge)
        # ============================================================

        ha_journal = []
        ha_area = []

        for n, entry in enumerate(json_content):
            for ar in entry.get("areas", []):
                ha_journal.append(f"journal-{n + last_journal}")
                ha_area.append(ar)

        has_area = DataFrame({
            "journalId": Series(ha_journal, dtype="string"),
            "areaName": Series(ha_area, dtype="string"),
        })

        area_lookup = identifiable_entity[
            identifiable_entity["internalId"].str.startswith("area-")
        ][["id", "internalId"]]

        has_area = merge(
            has_area,
            area_lookup,
            left_on="areaName",
            right_on="id",
            how="inner"
        )[["journalId", "internalId"]].rename(columns={"internalId": "areaId"})

        # ============================================================
        # WRITE TO DATABASE
        # ============================================================

        with connect(self.dbPathOrUrl) as con:
            identifiable_entity.to_sql("IdentifiableEntity", con, if_exists="append", index=False)
            has_category.to_sql("HasCategory", con, if_exists="append", index=False)
            has_area.to_sql("HasArea", con, if_exists="append", index=False)
            con.commit()

        return True


# ============================================================
# CATEGORY QUERY HANDLER
# ============================================================

class CategoryQueryHandler(QueryHandler):
    """
    Handles all queries to the relational SQLite database.
    Returns pandas DataFrames in a format compatible with BasicQueryEngine.
    """

    def getById(self, category_id: str) -> DataFrame:
        cid = (category_id or "").strip()
        if not cid:
            return DataFrame()

        with connect(self.dbPathOrUrl) as con:
            query = """
                SELECT id, quartile
                FROM IdentifiableEntity
                WHERE id = ?
                AND internalId LIKE 'category-%'
                LIMIT 1
            """
            df = read_sql(query, con, params=[cid])

        return df

    def getAllCategories(self) -> DataFrame:
        with connect(self.dbPathOrUrl) as con:
            query = """
                SELECT id, quartile
                FROM IdentifiableEntity
                WHERE internalId LIKE 'category-%'
            """
            df = read_sql(query, con)

        return df

    def getCategoriesForJournal(self, journal_id: str) -> DataFrame:
        jid = (journal_id or "").strip()
        if not jid:
            return DataFrame()

        with connect(self.dbPathOrUrl) as con:
            query = """
                SELECT c.id, c.quartile
                FROM HasCategory hc
                JOIN IdentifiableEntity c
                    ON hc.categoryId = c.internalId
                WHERE hc.journalId = (
                    SELECT internalId
                    FROM IdentifiableEntity
                    WHERE id = ?
                    AND internalId LIKE 'journal-%'
                    LIMIT 1
                )
            """
            df = read_sql(query, con, params=[jid])

        return df

    def getJournalsInCategory(self, category_id: str) -> DataFrame:
        cid = (category_id or "").strip()
        if not cid:
            return DataFrame()

        with connect(self.dbPathOrUrl) as con:
            query = """
                SELECT j.id, j.publisher, j.license, j.apc, j.seal, j.languages
                FROM HasCategory hc
                JOIN IdentifiableEntity c
                    ON hc.categoryId = c.internalId
                JOIN IdentifiableEntity j
                    ON hc.journalId = j.internalId
                WHERE c.id = ?
                AND j.internalId LIKE 'journal-%'
            """
            df = read_sql(query, con, params=[cid])

        return df
