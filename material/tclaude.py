"""
tclaude.py  —  Complete compliance test for the Brigata project.

Structure:
  - Section 1: Upload handlers (just check they run and return True)
  - Section 2: QueryHandler layer (return types = DataFrame, basic sanity)
  - Section 3: BasicQueryEngine / FullQueryEngine (return types + content logic)
  - Section 4: Content correctness (spot-checks against the real datasets)

HOW TO RUN:
  1. Make sure Blazegraph is running.
  2. Edit the four path/URL variables just below the imports.
  3. python tclaude.py

The test does NOT re-upload data by default (UPLOAD = False).
Set UPLOAD = True the first time, or after clearing the databases.
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

# ─────────────────────────────────────────────
#  !! EDIT THESE FOUR LINES BEFORE RUNNING !!
# ─────────────────────────────────────────────
GRAPH_ENDPOINT = "http://127.0.0.1:9999/blazegraph/sparql"
RELATIONAL_DB  = "data/relational.db"
DOAJ_CSV       = "data/doaj.csv"
SCIMAGO_JSON   = "data/scimago.json"
UPLOAD         = False   # set True to (re-)upload data into both DBs
# ─────────────────────────────────────────────

from pandas import DataFrame
from impl import (
    CategoryUploadHandler, CategoryQueryHandler,
    JournalUploadHandler,  JournalQueryHandler,
    FullQueryEngine,
    Journal, Category, Area, IdentifiableEntity,
)

# ══════════════════════════════════════════════
#  Helpers
# ══════════════════════════════════════════════

PASS  = "\033[92mPASS\033[0m"
FAIL  = "\033[91mFAIL\033[0m"
SKIP  = "\033[93mSKIP\033[0m"
_results = []

def ok(name, condition, detail=""):
    tag = PASS if condition else FAIL
    _results.append((name, bool(condition)))
    line = f"  [{tag}] {name}"
    if detail:
        line += f"\n         → {detail}"
    print(line)
    return bool(condition)

def skip(name, reason=""):
    _results.append((name, None))
    print(f"  [{SKIP}] {name}  ({reason})")

def section(title):
    print(f"\n{'━'*62}")
    print(f"  {title}")
    print(f"{'━'*62}")

def summary():
    section("SUMMARY")
    passed  = sum(1 for _, r in _results if r is True)
    failed  = sum(1 for _, r in _results if r is False)
    skipped = sum(1 for _, r in _results if r is None)
    for name, r in _results:
        tag = PASS if r is True else (FAIL if r is False else SKIP)
        print(f"  [{tag}] {name}")
    print(f"\n  Passed: {passed}  Failed: {failed}  Skipped: {skipped}  "
          f"Total: {len(_results)}")
    if failed:
        print(f"\n  \033[91m{failed} check(s) FAILED — see details above.\033[0m")
    else:
        print(f"\n  \033[92mAll executed checks passed.\033[0m")


# ══════════════════════════════════════════════
#  SECTION 1 — Upload handlers
# ══════════════════════════════════════════════
section("1 — Upload handlers")

# 1a. JournalUploadHandler interface
ju = JournalUploadHandler()
ok("JournalUploadHandler() needs no args",  True)   # if we got here, constructor works
ok("ju.setDbPathOrUrl() returns bool",
   isinstance(ju.setDbPathOrUrl(GRAPH_ENDPOINT), bool))
ok("ju.getDbPathOrUrl() returns the value set",
   ju.getDbPathOrUrl() == GRAPH_ENDPOINT)

# 1b. CategoryUploadHandler interface
cu = CategoryUploadHandler()
ok("CategoryUploadHandler() needs no args", True)
ok("cu.setDbPathOrUrl() returns bool",
   isinstance(cu.setDbPathOrUrl(RELATIONAL_DB), bool))
ok("cu.getDbPathOrUrl() returns the value set",
   cu.getDbPathOrUrl() == RELATIONAL_DB)

# 1c. Optional actual upload
if UPLOAD:
    if os.path.exists(RELATIONAL_DB):
        os.remove(RELATIONAL_DB)
    ok("cu.pushDataToDb(scimago.json) returns True",
       cu.pushDataToDb(SCIMAGO_JSON) is True)
    ok("ju.pushDataToDb(doaj.csv) returns True",
       ju.pushDataToDb(DOAJ_CSV) is True)
else:
    skip("pushDataToDb (upload)", "UPLOAD=False — skipped to save time")


# ══════════════════════════════════════════════
#  SECTION 2 — QueryHandler layer (DataFrames)
# ══════════════════════════════════════════════
section("2 — QueryHandler layer: must return DataFrame")

# ── 2a. JournalQueryHandler ──
jqh = JournalQueryHandler()
ok("JournalQueryHandler() needs no args", True)
ok("jqh.setDbPathOrUrl() returns bool",
   isinstance(jqh.setDbPathOrUrl(GRAPH_ENDPOINT), bool))
ok("jqh.getDbPathOrUrl() matches",
   jqh.getDbPathOrUrl() == GRAPH_ENDPOINT)

for method, args in [
    ("getById",                ["nonexistent-id-xyz"]),
    ("getAllJournals",          []),
    ("getJournalsWithTitle",   ["nonexistent-xyz"]),
    ("getJournalsPublishedBy", ["nonexistent-xyz"]),
    ("getJournalsWithLicense", [{"nonexistent-xyz"}]),
    ("getJournalsWithAPC",     []),
    ("getJournalsWithDOAJSeal",[]),
]:
    try:
        result = getattr(jqh, method)(*args)
        ok(f"jqh.{method}() returns DataFrame",
           isinstance(result, DataFrame),
           f"got {type(result).__name__}")
    except Exception as e:
        ok(f"jqh.{method}() returns DataFrame", False, f"Exception: {e}")

# ── 2b. CategoryQueryHandler ──
cqh = CategoryQueryHandler()
ok("CategoryQueryHandler() needs no args", True)
ok("cqh.setDbPathOrUrl() returns bool",
   isinstance(cqh.setDbPathOrUrl(RELATIONAL_DB), bool))
ok("cqh.getDbPathOrUrl() matches",
   cqh.getDbPathOrUrl() == RELATIONAL_DB)

for method, args in [
    ("getById",                       ["nonexistent-id-xyz"]),
    ("getAllCategories",               []),
    ("getAllAreas",                    []),
    ("getCategoriesWithQuartile",     [{"nonexistent-xyz"}]),
    ("getCategoriesAssignedToAreas",  [{"nonexistent-xyz"}]),
    ("getAreasAssignedToCategories",  [{"nonexistent-xyz"}]),
]:
    try:
        result = getattr(cqh, method)(*args)
        ok(f"cqh.{method}() returns DataFrame",
           isinstance(result, DataFrame),
           f"got {type(result).__name__}")
    except Exception as e:
        ok(f"cqh.{method}() returns DataFrame", False, f"Exception: {e}")


# ══════════════════════════════════════════════
#  SECTION 3 — BasicQueryEngine / FullQueryEngine: return types
# ══════════════════════════════════════════════
section("3 — Engine layer: return types and handler management")

engine = FullQueryEngine()
ok("FullQueryEngine() needs no args", True)
ok("cleanJournalHandlers() returns bool",   isinstance(engine.cleanJournalHandlers(),  bool))
ok("cleanCategoryHandlers() returns bool",  isinstance(engine.cleanCategoryHandlers(), bool))
ok("addJournalHandler() returns bool",      isinstance(engine.addJournalHandler(jqh),  bool))
ok("addCategoryHandler() returns bool",     isinstance(engine.addCategoryHandler(cqh), bool))

# Every engine method must return the right Python type
engine_checks = [
    # (method_name, args, expected_item_type_or_None_for_entity)
    ("getAllJournals",          [],                              Journal),
    ("getJournalsWithTitle",   ["nonexistent-xyz"],             Journal),
    ("getJournalsPublishedBy", ["nonexistent-xyz"],             Journal),
    ("getJournalsWithLicense", [{"nonexistent-xyz"}],           Journal),
    ("getJournalsWithAPC",     [],                              Journal),
    ("getJournalsWithDOAJSeal",[],                              Journal),
    ("getAllCategories",        [],                              Category),
    ("getAllAreas",             [],                              Area),
    ("getCategoriesWithQuartile",            [{"nonexistent"}], Category),
    ("getCategoriesAssignedToAreas",         [{"nonexistent"}], Category),
    ("getAreasAssignedToCategories",         [{"nonexistent"}], Area),
    ("getJournalsInCategoriesWithQuartile",  [{"nonexistent"}, {"nonexistent"}], Journal),
    ("getJournalsInAreasWithLicense",        [{"nonexistent"}, {"nonexistent"}], Journal),
    ("getDiamondJournalsInAreasAndCategoriesWithQuartile",
                                             [{"nonexistent"}, {"nonexistent"}, {"nonexistent"}], Journal),
]

for method, args, item_type in engine_checks:
    try:
        result = getattr(engine, method)(*args)
        is_list = isinstance(result, list)
        items_ok = all(isinstance(i, item_type) for i in result)
        ok(f"engine.{method}() → list[{item_type.__name__}]",
           is_list and items_ok,
           f"list={is_list}, all items {item_type.__name__}={items_ok}, len={len(result) if is_list else '?'}")
    except Exception as e:
        ok(f"engine.{method}() → list[{item_type.__name__}]", False, f"Exception: {e}")

# getEntityById with nonexistent id → None
try:
    r = engine.getEntityById("nonexistent-id-xyz-abc")
    ok("getEntityById(nonexistent) returns None", r is None, f"got {r}")
except Exception as e:
    ok("getEntityById(nonexistent) returns None", False, f"Exception: {e}")


# ══════════════════════════════════════════════
#  SECTION 4 — Content correctness (real data spot-checks)
# ══════════════════════════════════════════════
section("4 — Content correctness (requires real data in both DBs)")

# ── 4a. getAllJournals ──
try:
    all_j = engine.getAllJournals()
    ok("getAllJournals() returns a non-empty list", len(all_j) > 0,
       f"{len(all_j)} journals")

    # Every Journal must have getId() returning a non-empty list of strings
    ids_ok = all(
        isinstance(j.getId(), list) and len(j.getId()) > 0
        and all(isinstance(x, str) for x in j.getId())
        for j in all_j
    )
    ok("Every Journal has getId() → non-empty list[str]", ids_ok)

    # getTitle() must return str or None (never crash)
    titles_ok = all(j.getTitle() is None or isinstance(j.getTitle(), str) for j in all_j)
    ok("Every Journal has getTitle() → str or None", titles_ok)

    # getLicence() must return str or None
    lic_ok = all(j.getLicence() is None or isinstance(j.getLicence(), str) for j in all_j)
    ok("Every Journal has getLicence() → str or None", lic_ok)

    # hasAPC() must return bool
    apc_ok = all(isinstance(j.hasAPC(), bool) for j in all_j)
    ok("Every Journal has hasAPC() → bool", apc_ok)

    # hasDOAJSeal() must return bool
    seal_ok = all(isinstance(j.hasDOAJSeal(), bool) for j in all_j)
    ok("Every Journal has hasDOAJSeal() → bool", seal_ok)

    # getCategories() must return list (possibly empty) of Category objects
    cats_ok = all(
        isinstance(j.getCategories(), list)
        and all(isinstance(c, Category) for c in j.getCategories())
        for j in all_j
    )
    ok("Every Journal has getCategories() → list[Category]", cats_ok)

    # getAreas() must return list (possibly empty) of Area objects
    areas_ok = all(
        isinstance(j.getAreas(), list)
        and all(isinstance(a, Area) for a in j.getAreas())
        for j in all_j
    )
    ok("Every Journal has getAreas() → list[Area]", areas_ok)

    # No duplicate journals (by ID set)
    seen = set()
    dupes = 0
    for j in all_j:
        key = frozenset(j.getId())
        if key in seen:
            dupes += 1
        seen.add(key)
    ok("getAllJournals() has no duplicate journals", dupes == 0,
       f"{dupes} duplicate(s) found")

except Exception as e:
    ok("getAllJournals() content checks", False, f"Exception: {e}")

# ── 4b. getAllCategories ──
try:
    all_c = engine.getAllCategories()
    ok("getAllCategories() returns a non-empty list", len(all_c) > 0,
       f"{len(all_c)} categories")

    # Every Category: getId() → list with one string, getQuartile() → str or None
    cat_ids_ok = all(
        isinstance(c.getId(), list) and len(c.getId()) >= 1
        for c in all_c
    )
    ok("Every Category has getId() → non-empty list", cat_ids_ok)

    quartile_ok = all(
        c.getQuartile() is None or isinstance(c.getQuartile(), str)
        for c in all_c
    )
    ok("Every Category has getQuartile() → str or None", quartile_ok)

    # No duplicate (name, quartile) pairs
    cat_pairs = [(c.getId()[0], c.getQuartile()) for c in all_c]
    ok("getAllCategories() has no duplicate (name, quartile) pairs",
       len(cat_pairs) == len(set(cat_pairs)),
       f"{len(cat_pairs) - len(set(cat_pairs))} duplicate(s)")

except Exception as e:
    ok("getAllCategories() content checks", False, f"Exception: {e}")

# ── 4c. getAllAreas ──
try:
    all_a = engine.getAllAreas()
    ok("getAllAreas() returns a non-empty list", len(all_a) > 0,
       f"{len(all_a)} areas")

    area_ids_ok = all(
        isinstance(a.getId(), list) and len(a.getId()) >= 1
        for a in all_a
    )
    ok("Every Area has getId() → non-empty list", area_ids_ok)

    # No duplicate area names
    area_names = [a.getId()[0] for a in all_a]
    ok("getAllAreas() has no duplicate area names",
       len(area_names) == len(set(area_names)),
       f"{len(area_names) - len(set(area_names))} duplicate(s)")

except Exception as e:
    ok("getAllAreas() content checks", False, f"Exception: {e}")

# ── 4d. getJournalsWithTitle (partial match) ──
try:
    r = engine.getJournalsWithTitle("Science")
    ok("getJournalsWithTitle('Science') returns results", len(r) > 0,
       f"{len(r)} journals")
    title_match = all(
        j.getTitle() is not None and "science" in j.getTitle().lower()
        for j in r
    )
    ok("getJournalsWithTitle('Science'): all results contain 'Science' in title",
       title_match,
       "some titles don't contain 'Science'" if not title_match else "")

    r_empty = engine.getJournalsWithTitle("xyzzy-nonexistent-brigata")
    ok("getJournalsWithTitle(nonexistent) returns empty list",
       isinstance(r_empty, list) and len(r_empty) == 0,
       f"got {len(r_empty)} results")
except Exception as e:
    ok("getJournalsWithTitle content checks", False, f"Exception: {e}")

# ── 4e. getJournalsPublishedBy (partial match) ──
try:
    r = engine.getJournalsPublishedBy("Elsevier")
    ok("getJournalsPublishedBy('Elsevier') returns results", len(r) > 0,
       f"{len(r)} journals")
    pub_match = all(
        j.getPublisher() is not None and "elsevier" in j.getPublisher().lower()
        for j in r
    )
    ok("getJournalsPublishedBy('Elsevier'): all results match publisher",
       pub_match)

    r_empty = engine.getJournalsPublishedBy("xyzzy-nonexistent-brigata")
    ok("getJournalsPublishedBy(nonexistent) returns empty list",
       isinstance(r_empty, list) and len(r_empty) == 0)
except Exception as e:
    ok("getJournalsPublishedBy content checks", False, f"Exception: {e}")

# ── 4f. getJournalsWithLicense ──
try:
    r_ccby = engine.getJournalsWithLicense({"CC BY"})
    ok("getJournalsWithLicense({'CC BY'}) returns results", len(r_ccby) > 0,
       f"{len(r_ccby)} journals")
    lic_match = all(
        j.getLicence() is not None and "cc by" in j.getLicence().lower()
        for j in r_ccby
    )
    ok("getJournalsWithLicense({'CC BY'}): all results have CC BY licence",
       lic_match,
       "some journals have wrong licence" if not lic_match else "")

    # CC BY must NOT include CC BY-NC results
    no_nc = all(
        j.getLicence() is not None and j.getLicence().strip().lower() == "cc by"
        for j in r_ccby
    )
    ok("getJournalsWithLicense({'CC BY'}): no CC BY-NC results included",
       no_nc,
       "CC BY-NC or other variants found" if not no_nc else "")

    r_empty = engine.getJournalsWithLicense({"xyzzy-nonexistent"})
    ok("getJournalsWithLicense(nonexistent) returns empty list",
       isinstance(r_empty, list) and len(r_empty) == 0,
       f"got {len(r_empty)}")
except Exception as e:
    ok("getJournalsWithLicense content checks", False, f"Exception: {e}")

# ── 4g. getJournalsWithAPC ──
try:
    r_apc = engine.getJournalsWithAPC()
    ok("getJournalsWithAPC() returns results", len(r_apc) > 0,
       f"{len(r_apc)} journals")
    apc_match = all(j.hasAPC() is True for j in r_apc)
    ok("getJournalsWithAPC(): every result has hasAPC()==True", apc_match)
except Exception as e:
    ok("getJournalsWithAPC content checks", False, f"Exception: {e}")

# ── 4h. getJournalsWithDOAJSeal ──
try:
    r_seal = engine.getJournalsWithDOAJSeal()
    ok("getJournalsWithDOAJSeal() returns results", len(r_seal) > 0,
       f"{len(r_seal)} journals")
    seal_match = all(j.hasDOAJSeal() is True for j in r_seal)
    ok("getJournalsWithDOAJSeal(): every result has hasDOAJSeal()==True", seal_match)
except Exception as e:
    ok("getJournalsWithDOAJSeal content checks", False, f"Exception: {e}")

# ── 4i. getCategoriesWithQuartile ──
try:
    r_q1 = engine.getCategoriesWithQuartile({"Q1"})
    ok("getCategoriesWithQuartile({'Q1'}) returns results", len(r_q1) > 0,
       f"{len(r_q1)} categories")
    q1_match = all(c.getQuartile() == "Q1" for c in r_q1)
    ok("getCategoriesWithQuartile({'Q1'}): every result has quartile Q1", q1_match)

    # empty set → all quartiles
    r_all = engine.getCategoriesWithQuartile(set())
    all_cats = engine.getAllCategories()
    ok("getCategoriesWithQuartile(empty) returns same count as getAllCategories()",
       len(r_all) == len(all_cats),
       f"getCategoriesWithQuartile(empty)={len(r_all)}, getAllCategories={len(all_cats)}")
except Exception as e:
    ok("getCategoriesWithQuartile content checks", False, f"Exception: {e}")

# ── 4j. getCategoriesAssignedToAreas ──
try:
    r = engine.getCategoriesAssignedToAreas({"Medicine"})
    ok("getCategoriesAssignedToAreas({'Medicine'}) returns results",
       len(r) > 0, f"{len(r)} categories")
    all_category = all(isinstance(c, Category) for c in r)
    ok("getCategoriesAssignedToAreas: all items are Category objects", all_category)

    # empty → same as getAllCategories
    r_all = engine.getCategoriesAssignedToAreas(set())
    ok("getCategoriesAssignedToAreas(empty) returns results (= all categories)",
       len(r_all) > 0, f"{len(r_all)} categories")

    # nonexistent area → empty
    r_none = engine.getCategoriesAssignedToAreas({"xyzzy-nonexistent"})
    ok("getCategoriesAssignedToAreas(nonexistent) returns empty list",
       isinstance(r_none, list) and len(r_none) == 0)
except Exception as e:
    ok("getCategoriesAssignedToAreas content checks", False, f"Exception: {e}")

# ── 4k. getAreasAssignedToCategories ──
try:
    r = engine.getAreasAssignedToCategories({"Drug Discovery"})
    ok("getAreasAssignedToCategories({'Drug Discovery'}) returns results",
       len(r) > 0, f"{len(r)} areas")
    all_area = all(isinstance(a, Area) for a in r)
    ok("getAreasAssignedToCategories: all items are Area objects", all_area)

    r_none = engine.getAreasAssignedToCategories({"xyzzy-nonexistent"})
    ok("getAreasAssignedToCategories(nonexistent) returns empty list",
       isinstance(r_none, list) and len(r_none) == 0)
except Exception as e:
    ok("getAreasAssignedToCategories content checks", False, f"Exception: {e}")

# ── 4l. getEntityById — Journal ──
try:
    # pick a real ISSN from your dataset; this one is common in DOAJ
    test_issn = "2072-4292"   # Remote Sensing (MDPI) — present in most DOAJ exports
    result = engine.getEntityById(test_issn)
    if result is None:
        skip(f"getEntityById('{test_issn}') → Journal",
             "ISSN not in current dataset — update test_issn")
    else:
        ok(f"getEntityById('{test_issn}') returns Journal",
           isinstance(result, Journal),
           f"got {type(result).__name__}")
        if isinstance(result, Journal):
            ok(f"getEntityById(journal): getId() contains '{test_issn}'",
               test_issn in result.getId(),
               f"got {result.getId()}")
            ok(f"getEntityById(journal): getCategories() → list[Category]",
               isinstance(result.getCategories(), list)
               and all(isinstance(c, Category) for c in result.getCategories()))
            ok(f"getEntityById(journal): getAreas() → list[Area]",
               isinstance(result.getAreas(), list)
               and all(isinstance(a, Area) for a in result.getAreas()))
except Exception as e:
    ok("getEntityById(journal) checks", False, f"Exception: {e}")

# ── 4m. getEntityById — Area ──
try:
    test_area = "Medicine"
    result = engine.getEntityById(test_area)
    if result is None:
        skip(f"getEntityById('{test_area}') → Area", "not in dataset")
    else:
        ok(f"getEntityById('{test_area}') returns Area",
           isinstance(result, Area),
           f"got {type(result).__name__}")
        if isinstance(result, Area):
            ok(f"getEntityById(area): getId() contains '{test_area}'",
               test_area in result.getId())
except Exception as e:
    ok("getEntityById(area) checks", False, f"Exception: {e}")

# ── 4n. getEntityById — Category ──
try:
    test_cat = "Drug Discovery"
    result = engine.getEntityById(test_cat)
    if result is None:
        skip(f"getEntityById('{test_cat}') → Category", "not in dataset")
    else:
        ok(f"getEntityById('{test_cat}') returns Category",
           isinstance(result, Category),
           f"got {type(result).__name__}")
        if isinstance(result, Category):
            ok(f"getEntityById(category): getId() contains '{test_cat}'",
               test_cat in result.getId())
            ok(f"getEntityById(category): getQuartile() → str or None",
               result.getQuartile() is None or isinstance(result.getQuartile(), str))
except Exception as e:
    ok("getEntityById(category) checks", False, f"Exception: {e}")

# ── 4o. getEntityById — nonexistent ──
try:
    r = engine.getEntityById("xyzzy-totally-nonexistent-id-brigata")
    ok("getEntityById(nonexistent) returns None", r is None, f"got {r}")
except Exception as e:
    ok("getEntityById(nonexistent) returns None", False, f"Exception: {e}")

# ── 4p. getJournalsInCategoriesWithQuartile ──
try:
    r = engine.getJournalsInCategoriesWithQuartile({"Drug Discovery"}, {"Q1"})
    ok("getJournalsInCategoriesWithQuartile({'Drug Discovery'},{'Q1'}) returns results",
       len(r) > 0, f"{len(r)} journals")
    ok("getJournalsInCategoriesWithQuartile: all items are Journal", all(isinstance(j, Journal) for j in r))

    # every returned journal must have Drug Discovery Q1 in its categories
    cat_check = all(
        any(c.getId()[0] == "Drug Discovery" and c.getQuartile() == "Q1"
            for c in j.getCategories())
        for j in r
    )
    ok("getJournalsInCategoriesWithQuartile: every journal has 'Drug Discovery Q1' category",
       cat_check,
       "some journals missing the category" if not cat_check else "")

    # empty, empty → all journals that appear in scimago
    r_all = engine.getJournalsInCategoriesWithQuartile(set(), set())
    ok("getJournalsInCategoriesWithQuartile(empty,empty) returns results",
       len(r_all) > 0, f"{len(r_all)} journals")

    # no duplicates
    ids_seen = set()
    dupes = 0
    for j in r_all:
        key = frozenset(j.getId())
        if key in ids_seen: dupes += 1
        ids_seen.add(key)
    ok("getJournalsInCategoriesWithQuartile: no duplicates", dupes == 0,
       f"{dupes} duplicate(s)")
except Exception as e:
    ok("getJournalsInCategoriesWithQuartile content checks", False, f"Exception: {e}")

# ── 4q. getJournalsInAreasWithLicense ──
try:
    r = engine.getJournalsInAreasWithLicense({"Medicine"}, {"CC BY"})
    ok("getJournalsInAreasWithLicense({'Medicine'},{'CC BY'}) returns results",
       len(r) > 0, f"{len(r)} journals")
    ok("getJournalsInAreasWithLicense: all items are Journal",
       all(isinstance(j, Journal) for j in r))

    # every result must have CC BY licence
    lic_check = all(
        j.getLicence() is not None and j.getLicence().strip().lower() == "cc by"
        for j in r
    )
    ok("getJournalsInAreasWithLicense: every journal has 'CC BY' licence",
       lic_check,
       "some journals have wrong licence" if not lic_check else "")

    # every result must be in the Medicine area
    area_check = all(
        any(a.getId()[0] == "Medicine" for a in j.getAreas())
        for j in r
    )
    ok("getJournalsInAreasWithLicense: every journal is in 'Medicine' area",
       area_check,
       "some journals not in Medicine area" if not area_check else "")

    r_none = engine.getJournalsInAreasWithLicense({"xyzzy"}, {"CC BY"})
    ok("getJournalsInAreasWithLicense(nonexistent area) returns empty list",
       isinstance(r_none, list) and len(r_none) == 0)
except Exception as e:
    ok("getJournalsInAreasWithLicense content checks", False, f"Exception: {e}")

# ── 4r. getDiamondJournalsInAreasAndCategoriesWithQuartile ──
try:
    r = engine.getDiamondJournalsInAreasAndCategoriesWithQuartile(
        {"Medicine"}, {"Drug Discovery"}, {"Q1"}
    )
    ok("getDiamond({'Medicine'},{'Drug Discovery'},{'Q1'}) returns results",
       len(r) > 0, f"{len(r)} journals")
    ok("getDiamond: all items are Journal", all(isinstance(j, Journal) for j in r))

    # diamond = no APC
    diamond_check = all(j.hasAPC() is False for j in r)
    ok("getDiamond: every result has hasAPC()==False (diamond = no APC)",
       diamond_check,
       "some journals have APC=True" if not diamond_check else "")

    # must be in Medicine area
    area_check = all(
        any(a.getId()[0] == "Medicine" for a in j.getAreas())
        for j in r
    )
    ok("getDiamond: every journal is in 'Medicine' area",
       area_check,
       "some journals not in Medicine area" if not area_check else "")

    # must have Drug Discovery Q1 category
    cat_check = all(
        any(c.getId()[0] == "Drug Discovery" and c.getQuartile() == "Q1"
            for c in j.getCategories())
        for j in r
    )
    ok("getDiamond: every journal has 'Drug Discovery Q1' category",
       cat_check,
       "some journals missing the category" if not cat_check else "")

    r_none = engine.getDiamondJournalsInAreasAndCategoriesWithQuartile(
        {"xyzzy"}, {"Drug Discovery"}, {"Q1"}
    )
    ok("getDiamond(nonexistent area) returns empty list",
       isinstance(r_none, list) and len(r_none) == 0)
except Exception as e:
    ok("getDiamondJournalsInAreasAndCategoriesWithQuartile content checks",
       False, f"Exception: {e}")


# ══════════════════════════════════════════════
#  SECTION 5 — Edge cases
# ══════════════════════════════════════════════
section("5 — Edge cases")

# 5a. getJournalsWithLicense with empty set → empty list
try:
    r = engine.getJournalsWithLicense(set())
    ok("getJournalsWithLicense(empty set) returns empty list (no license = no results)",
       isinstance(r, list) and len(r) == 0,
       f"got {len(r)} results")
except Exception as e:
    ok("getJournalsWithLicense(empty set)", False, f"Exception: {e}")

# 5b. getCategoriesWithQuartile empty → same as getAllCategories
try:
    all_cats  = engine.getAllCategories()
    cats_empty = engine.getCategoriesWithQuartile(set())
    ok("getCategoriesWithQuartile(empty) count == getAllCategories() count",
       len(cats_empty) == len(all_cats),
       f"{len(cats_empty)} vs {len(all_cats)}")
except Exception as e:
    ok("getCategoriesWithQuartile(empty) == getAllCategories()", False, f"Exception: {e}")

# 5c. getCategoriesAssignedToAreas empty → not empty
try:
    r = engine.getCategoriesAssignedToAreas(set())
    ok("getCategoriesAssignedToAreas(empty) returns results",
       len(r) > 0, f"{len(r)} categories")
except Exception as e:
    ok("getCategoriesAssignedToAreas(empty)", False, f"Exception: {e}")

# 5d. getAreasAssignedToCategories empty → not empty
try:
    r = engine.getAreasAssignedToCategories(set())
    ok("getAreasAssignedToCategories(empty) returns results",
       len(r) > 0, f"{len(r)} areas")
except Exception as e:
    ok("getAreasAssignedToCategories(empty)", False, f"Exception: {e}")

# 5e. getJournalsInCategoriesWithQuartile — only category empty
try:
    r = engine.getJournalsInCategoriesWithQuartile(set(), {"Q1"})
    ok("getJournalsInCategoriesWithQuartile(empty cats, Q1) returns results",
       len(r) > 0, f"{len(r)} journals")
except Exception as e:
    ok("getJournalsInCategoriesWithQuartile(empty cats, Q1)", False, f"Exception: {e}")

# 5f. Journal language parsing — must be list, not a single string
try:
    all_j = engine.getAllJournals()
    multi_lang = [j for j in all_j if j.getLanguages() and len(j.getLanguages()) > 1]
    if not multi_lang:
        skip("Multi-language journal has getLanguages() as list (not one big string)",
             "no multi-language journal found in current dataset")
    else:
        j_ml = multi_lang[0]
        ok("Multi-language journal: getLanguages() returns list with >1 element",
           isinstance(j_ml.getLanguages(), list) and len(j_ml.getLanguages()) > 1,
           f"got {j_ml.getLanguages()}")
except Exception as e:
    ok("Multi-language parsing check", False, f"Exception: {e}")

# 5g. No duplicate journals in getJournalsWithDOAJSeal
try:
    r = engine.getJournalsWithDOAJSeal()
    seen = set()
    dupes = 0
    for j in r:
        key = frozenset(j.getId())
        if key in seen: dupes += 1
        seen.add(key)
    ok("getJournalsWithDOAJSeal(): no duplicate journals", dupes == 0,
       f"{dupes} duplicate(s)")
except Exception as e:
    ok("getJournalsWithDOAJSeal duplicate check", False, f"Exception: {e}")

# 5h. IdentifiableEntity subclass check
try:
    all_j = engine.getAllJournals()
    all_c = engine.getAllCategories()
    all_a = engine.getAllAreas()
    ok("Journal is subclass of IdentifiableEntity",
       all(isinstance(j, IdentifiableEntity) for j in all_j[:5]))
    ok("Category is subclass of IdentifiableEntity",
       all(isinstance(c, IdentifiableEntity) for c in all_c[:5]))
    ok("Area is subclass of IdentifiableEntity",
       all(isinstance(a, IdentifiableEntity) for a in all_a[:5]))
except Exception as e:
    ok("IdentifiableEntity subclass checks", False, f"Exception: {e}")


# ══════════════════════════════════════════════
#  Print summary
# ══════════════════════════════════════════════
summary()