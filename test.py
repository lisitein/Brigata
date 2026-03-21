import os
import requests

try:
    response = requests.get('http://192.168.1.226:9999/blazegraph/', timeout=5)
    print("Blazegraph reachable:", response.status_code)
except Exception as e:
    print("Blazegraph NOT reachable:", e)

from laura import *
from daniele import CategoryUploadHandler
from li import JournalUploadHandler
from Yang import JournalQueryHandler, CategoryQueryHandler
from baseHandler import UploadHandler

if os.path.exists("data/relational_database.db"):
    os.remove("data/relational_database.db")

engine = FullQueryEngine()

cu = CategoryUploadHandler()
cu.setDbPathOrUrl("data/relational_database.db")
cu.pushDataToDb('data/scimago.json')

ju = JournalUploadHandler()
ju.setDbPathOrUrl("http://192.168.1.226:9999/blazegraph/namespace/kb/sparql")
ju.pushDataToDb('data/doaj.csv')

cq = CategoryQueryHandler()
cq.setDbPathOrUrl("data/relational_database.db")
engine.addCategoryHandler(cq)

jq = JournalQueryHandler()
jq.setDbPathOrUrl("http://192.168.1.226:9999/blazegraph/namespace/kb/sparql")
engine.addJournalHandler(jq)


def normalize_list(x):
    if x is None:
        return []
    return list(x)

def same_list(a, b):
    return set(normalize_list(a)) == set(normalize_list(b))


# ------------------------------------------------------------
# STATIC TEST DATA
# FIX: Area takes a list, Journal uses categories= and areas=
# ------------------------------------------------------------
a = [
    Area(["Medicine"]),
    Area(["Pharmacology, Toxicology and Pharmaceutics"]),
    Area(["Economics, Econometrics and Finance"]),
    Area(["Energy"]),
    Area(["Materials Science"]),
    Area(["Biochemistry, Genetics and Molecular Biology"]),
    Area(["Arts and Humanities"])
]

c = [
    Category(id="Drug Discovery", quartile="Q1"),
    Category(id="Medicine (miscellaneous)", quartile="Q1"),
    Category(id="Pharmacology", quartile="Q1"),
    Category(id="Economics and Econometrics", quartile="Q1"),
    Category(id="Biomaterials", quartile="Q1"),
    Category(id="Electronic, Optical and Magnetic Materials", quartile="Q1"),
    Category(id="Energy (miscellaneous)", quartile="Q1"),
    Category(id="Materials Chemistry", quartile="Q1"),
    Category(id="Surfaces, Coatings and Films", quartile="Q1"),
    Category(id="Biochemistry, Genetics and Molecular Biology (miscellaneous)", quartile="Q1"),
    Category(id="Biochemistry, Genetics and Molecular Biology (miscellaneous)", quartile="Q3"),
    Category(id="Medicine (miscellaneous)", quartile="Q4"),
    Category(id="Philosophy")
]

j = [
    Journal(ids=["1983-9979"], title="Prolíngua", languages=["Portuguese"], publisher="Universidade Federal da Paraíba", seal=False, license="CC BY-NC-SA", apc=False, categories=[], areas=[]),
    Journal(ids=["2224-9281","2414-990X"], title="Проблеми Законності", languages=["Ukrainian","Russian","English"], publisher="Yaroslav Mudryi National Law University", seal=False, license="CC BY", apc=True, categories=[c[12]], areas=[a[6]]),
    Journal(ids=["2174-548X"], title="Enlightening Tourism: A Pathmaking Journal", languages=["English"], publisher="University of Huelva", seal=False, license="CC BY-NC", apc=False, categories=[], areas=[]),
    Journal(ids=["1733-8670","2392-0378"], title="Scientific Journals of the Maritime University of Szczecin", languages=["English"], publisher="MUS", seal=False, license="CC BY", apc=True, categories=[], areas=[]),
    Journal(ids=["2238-8869"], title="Fronteiras: Journal of Social, Technological and Environmental Science", languages=["Portuguese"], publisher="Centro Universitário de Anápolis", seal=False, license="CC BY-NC", apc=False, categories=[], areas=[]),
    Journal(ids=["1676-546X","1679-0359"], title="Semina: Ciências Agrárias", languages=["Portuguese","English"], publisher="Universidade Estadual de Londrina", seal=False, license="Publisher's own license", apc=True, categories=[], areas=[]),

    Journal(ids=["1474-1784","1474-1776"], title="", languages=[], publisher="", seal=False, license="", apc=False, categories=[c[0],c[1],c[2]], areas=[a[0],a[1]]),
    Journal(ids=["1944-7981","0002-8282"], title="", languages=[], publisher="", seal=False, license="", apc=False, categories=[c[3]], areas=[a[2]]),
    Journal(ids=["2058-8437"], title="", languages=[], publisher="", seal=False, license="", apc=False, categories=c[4:9], areas=[a[3],a[4]]),
    Journal(ids=["1546-170X","1078-8956"], title="", languages=[], publisher="", seal=False, license="", apc=False, categories=[c[9],c[1]], areas=[a[5],a[0]]),
    Journal(ids=["0065-2598","2214-8019"], title="", languages=[], publisher="", seal=False, license="", apc=False, categories=[c[10],c[11]], areas=[a[5],a[0]])
]


# ------------------------------------------------------------
# TESTS
# ------------------------------------------------------------

def test_getEntityById():
    tests = [
        ("1474-1784", j[6]),
        ("santa-claus", None),
        ("2224-9281", j[1]),
        ("happy-yang", None),
        ("2238-8869", j[4]),
        ("Medicine", a[0]),
        ("Biochemistry, Genetics and Molecular Biology", a[5]),
        ("Biochemistry, Genetics and Molecular Biology (miscellaneous)", Category(id="Biochemistry, Genetics and Molecular Biology (miscellaneous)", quartile="Q1")),  # FIX: Q1 is first by DB ordering
        ("Philosophy", Category(id="Philosophy"))
    ]
 
    for i, (inp, expected) in enumerate(tests, start=1):
        out = engine.getEntityById(inp)
        if out == expected:
            print(f"TEST1 [{i}] OK")
        else:
            print(f"TEST1 [{i}] FAIL — got {out}, expected {expected}")


def test_getAllJournals():
    if same_list(engine.getAllJournals(), j):
        print("TEST2 OK")
    else:
        print("TEST2 FAIL")


def test_getJournalsWithTitle():
    tests = [
        ("Prolíngua", [j[0]]),
        ("Законності", [j[1]]),
        ("happy-Yang", []),
        ("Enlightening Tourism", [j[2]]),
        ("University of Szczecin", [j[3]]),
        ("Fronteiras", [j[4]]),
        ("Semina", [j[5]]),
        ("Enlightening", [j[2]]),
        ("Scien", [j[3], j[4]]),
        ("Semina: Ciências Agrárias", [j[5]])
    ]

    for i, (inp, expected) in enumerate(tests, start=1):
        out = engine.getJournalsWithTitle(inp)
        if same_list(out, expected):
            print(f"TEST3 [{i}] OK")
        else:
            print(f"TEST3 [{i}] FAIL — got {out}, expected {expected}")


def test_getJournalsPublishedBy():
    tests = [
        ("Universi", [j[0], j[1], j[2], j[4], j[5]]),
        ("University", [j[1], j[2]]),
        ("Universidade", [j[0], j[5]]),
        ("happy Yang", []),
        ("MUS", [j[3]]),
        ("de Anápolis", [j[4]])
    ]

    for i, (inp, expected) in enumerate(tests, start=1):
        out = engine.getJournalsPublishedBy(inp)
        if same_list(out, expected):
            print(f"TEST4 [{i}] OK")
        else:
            print(f"TEST4 [{i}] FAIL — got {out}, expected {expected}")


def test_getJournalsWithLicense():
    tests = [
        ({"CC BY-NC-SA"}, [j[0]]),
        ({"CC BY"}, [j[1], j[3]]),
        ({"Publisher's own license"}, [j[5]]),
        ({"CHI CHI"}, [])
    ]

    for i, (inp, expected) in enumerate(tests, start=1):
        out = engine.getJournalsWithLicense(inp)
        if same_list(out, expected):
            print(f"TEST5 [{i}] OK")
        else:
            print(f"TEST5 [{i}] FAIL — got {out}, expected {expected}")


def test_getJournalsWithAPC():
    expected = [x for x in j if x.apc]
    if same_list(engine.getJournalsWithAPC(), expected):
        print("TEST6 OK")
    else:
        print("TEST6 FAIL")


def test_getJournalsWithDOAJSeal():
    expected = [x for x in j if x.seal]
    if same_list(engine.getJournalsWithDOAJSeal(), expected):
        print("TEST7 OK")
    else:
        print("TEST7 FAIL")


def test_getAllCategories():
    if same_list(engine.getAllCategories(), c):
        print("TEST8 OK")
    else:
        print("TEST8 FAIL")


def test_getAllAreas():
    if same_list(engine.getAllAreas(), a):
        print("TEST9 OK")
    else:
        print("TEST9 FAIL")


def test_getCategoriesWithQuartile():
    tests = [
        ({"Q1"}, c[0:10]),
        (set(), c),
        ({"Q3", "Q4"}, [c[10], c[11]])
    ]

    for i, (inp, expected) in enumerate(tests, start=1):
        out = engine.getCategoriesWithQuartile(inp)
        if same_list(out, expected):
            print(f"TEST10 [{i}] OK")
        else:
            print(f"TEST10 [{i}] FAIL — got {out}, expected {expected}")


def test_getCategoriesAssignedToAreas():
    tests = [
        ({"Medicine"}, [c[0], c[1], c[2], c[9], c[10], c[11]]),
        (set(), c),
        ({"Medicine", "Arts and Humanities"}, [c[0], c[1], c[2], c[9], c[10], c[11], c[12]])
    ]

    for i, (inp, expected) in enumerate(tests, start=1):
        out = engine.getCategoriesAssignedToAreas(inp)
        if same_list(out, expected):
            print(f"TEST11 [{i}] OK")
        else:
            print(f"TEST11 [{i}] FAIL — got {out}, expected {expected}")


def test_getAreasAssignedToCategories():
    tests = [
        ({"Drug Discovery"}, [a[0], a[1]]),
        ({"Drug Discovery", "Philosophy"}, [a[0], a[1], a[6]]),
        ({"Medicine (miscellaneous)"}, [a[0], a[1], a[5]])
    ]

    for i, (inp, expected) in enumerate(tests, start=1):
        out = engine.getAreasAssignedToCategories(inp)
        if same_list(out, expected):
            print(f"TEST12 [{i}] OK")
        else:
            print(f"TEST12 [{i}] FAIL — got {out}, expected {expected}")


# ------------------------------------------------------------
# RUN
# ------------------------------------------------------------
test_getEntityById()
test_getAllJournals()
test_getJournalsWithTitle()
test_getJournalsPublishedBy()
test_getJournalsWithLicense()
test_getJournalsWithAPC()
test_getJournalsWithDOAJSeal()
test_getAllCategories()
test_getAllAreas()
test_getCategoriesWithQuartile()
test_getCategoriesAssignedToAreas()
test_getAreasAssignedToCategories()
