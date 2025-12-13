import requests
try:
    response = requests.get('http://127.0.0.1:9999/blazegraph/sparql', timeout=5)
    print("Server reachable:", response.status_code)
except Exception as e:
    print("Server NOT reachable:", e)


from laura import *
from daniele import *
from li import *
from Yang import *
from baseHandler import *

#We initialize a FullQueryEngine instance:
engine = FullQueryEngine()

cu=CategoryUploadHandler()
cu.setDbPathOrUrl("data/relational_database.db")
cu.pushDataToDb('data/scimago.json')
ju=JournalUploadHandler()
ju.setDbPathOrUrl("http://127.0.0.1:9999/blazegraph/sparql")
ju.pushDataToDb('data/doaj.csv')

cq=CategoryQueryHandler()
cq.setDbPathOrUrl("data/relational_database.db")
engine.addCategoryHandler(cq)
jq=JournalQueryHandler()
jq.setDbPathOrUrl("http://127.0.0.1:9999/blazegraph/sparql")
engine.addJournalHandler(jq)



a= [
    Area(id="Medicine"),
    Area(id="Pharmacology, Toxicology and Pharmaceutics"),
    Area(id="Economics, Econometrics and Finance"),
    Area(id="Energy"),
    Area(id="Materials Science"),
    Area(id="Biochemistry, Genetics and Molecular Biology"),
    Area(id="Arts and Humanities")
]

c=[
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


j= [
    Journal(title = "Prolíngua", id = ["1983-9979"], languages = ["Portuguese"], publisher = "Universidade Federal da Paraíba", seal=False, license="CC BY-NC-SA", apc=False, hasCategory=[], hasArea=[]),
    Journal(title = "Проблеми Законності", id = ["2224-9281","2414-990X"], languages = ["Ukrainian", "Russian", "English"], publisher = "Yaroslav Mudryi National Law University", seal=False, license="CC BY", apc=True, hasCategory=[c[12]], hasArea=[a[6]]),
    Journal(title = "Enlightening Tourism: A Pathmaking Journal", id = ["2174-548X"], languages = ["English"], publisher = "University of Huelva", seal=False, license="CC BY-NC", apc=False, hasCategory=[], hasArea=[]),
    Journal(title = "Scientific Journals of the Maritime University of Szczecin", id = ["1733-8670","2392-0378"], languages = ["English"], publisher = "MUS", seal=False, license="CC BY", apc=True, hasCategory=[], hasArea=[]),
    Journal(title = "Fronteiras: Journal of Social, Technological and Environmental Science", id = ["2238-8869"], languages = ["Portuguese"], publisher = "Centro Universitário de Anápolis", seal=False, license="CC BY-NC", apc=False, hasCategory=[], hasArea=[]),
    Journal(title = "Semina: Ciências Agrárias", id = ["1676-546X","1679-0359"], languages = ["Portuguese", "English"], publisher = "Universidade Estadual de Londrina", seal=False, license="Publisher's own license", apc=True, hasCategory=[], hasArea=[]),

    Journal(title = "", id = ["1474-1784","1474-1776"], languages = [],publisher = [], seal=False, license="", apc=False, hasCategory=[c[0],c[1],c[2]], hasArea=[a[0],a[1]]),
    Journal(title = "", id = ["1944-7981","0002-8282"], languages = [], publisher = [], seal=False, license="", apc=False, hasCategory=[c[3]], hasArea=[a[2]]),
    Journal(title = "", id = ["2058-8437"], languages = [], seal=False, publisher = [], license="", apc=False, hasCategory=c[4:9], hasArea=[a[3],a[4]]),
    Journal(title = "", id = ["1546-170X","1078-8956"], languages = [], publisher = [], seal=False, license="", apc=False, hasCategory=[c[9],c[1]], hasArea=[a[5],a[0]]),
    Journal(title = "", id = ["0065-2598","2214-8019"], languages = [], publisher = [], seal=False, license="", apc=False, hasCategory=[c[10],c[11]], hasArea=[a[5],a[0]])
    ]



#TEST 1
def test_getEntityById(j):
    correct_inputs_and_outputs=[
    {"input":"1474-1784",
     "output":j[6]},

    {"input":"santa-claus",
     "output":None},

    {"input":"2224-9281",
     "output":j[1]},
    
    {"input":"happy-yang",
     "output":None},
    
    {"input":"2238-8869",
     "output":j[4]},

    {"input":"Medicine",
     "output":a[0]},

    {"input":"Biochemistry, Genetics and Molecular Biology",
     "output":a[5]},

    {"input":"Biochemistry, Genetics and Molecular Biology (miscellaneous)",
     "output":Category(id="Biochemistry, Genetics and Molecular Biology (miscellaneous)")},
    #what about quartile? in the UML it is an attribute of Category class...

    {"input":"Philosophy",
     "output":Category(id="Philosophy")}
    ]
    i = 0
    for pair in correct_inputs_and_outputs:
        print("---------------------------------------------------------")
        i = i+1
        # print("---- DEBUG getById on JournalQueryHandler ----")
        # df_j = engine.getEntityById(pair["input"])
        # print(df_j)
        # print(df_j.columns)

        engine.getEntityById(pair["input"])
        if engine.getEntityById(pair["input"])==pair["output"]:
            print(True, "Yoho^^ for Test1",[i])
        else:
            print(False, "Eha... The problem occurs in TEST1, pair",[i])
        # print("Hello, you chose the input:", pair["input"])

        # if isinstance(pair["output"], Journal):
        #     expected=Journal(pair["output"])
        # elif isinstance(pair["output"], Category):
        #     expected=Category(pair["output"])
        # else:
        #     expected=Area(pair["output"])

        # print("Your goood expected output:", pair["output"])
        # print("Laura gives you the output:", engine.getEntityById(pair["input"]),"\n\n\n")

#TEST 2
def test_getAllJournals(j):
    if engine.getAllJournals()== j:
        print(True, "Yoho!^^ TEST2 is True")
    else:
        print(False, "Eha... The result of TEST2 is uncorrect.")

# TEST3
def test_getJournalsWithTitle():
    correct_inputs_and_outputs=[
    {"input":"Prolíngua",
     "output":[Journal(title = "Prolíngua", id = "1983-9979", languages = ["Portuguese"], publisher = "Universidade Federal da Paraíba", seal=False, license="CC BY-NC-SA", apc=False, hasCategory=[], hasArea=[])]},

    {"input":"Законності",
     "output":[Journal(title = "Проблеми Законності", id = ["2224-9281","2414-990X"], languages = ["Ukrainian", "Russian", "English"], publisher = "Yaroslav Mudryi National Law University", seal=False, license="CC BY", apc=True, hasCategory=[], hasArea=[])]},

    {"input":"happy-Yang",
     "output":None},

    {"input":"Enlightening Tourism",
     "output":[Journal(title = "Enlightening Tourism: A Pathmaking Journal", id = "2174-548X", languages = ["English"], publisher = "University of Huelva", seal=False, license="CC BY-NC", apc=False, hasCategory=[], hasArea=[])]},

    {"input":"University of Szczecin",
     "output":[Journal(title = "Scientific Journals of the Maritime University of Szczecin", id = ["1733-8670","2392-0378"], languages = ["English"], publisher = "MUS", seal=False, license="CC BY", apc=True, hasCategory=[], hasArea=[])]},

    {"input":"Fronteiras",
     "output":[Journal(title = "Fronteiras: Journal of Social, Technological and Environmental Science", id = "2238-8869", languages = ["Portuguese"], publisher = "Centro Universitário de Anápolis", seal=False, license="CC BY-NC", apc=False, hasCategory=[], hasArea=[])]},
    
    {"input":"Semina",
     "output":[Journal(title = "Semina: Ciências Agrárias", id = ["1676-546X","1679-0359"], languages = ["Portuguese", "English"], publisher = "Universidade Estadual de Londrina", seal=False, license="Publisher's own license", apc=True, hasCategory=[], hasArea=[])]},

    {"input":"Enlightening",
     "output":[Journal(title = "Enlightening Tourism: A Pathmaking Journal", id = "2174-548X", languages = ["English"], publisher = "University of Huelva", seal=False, license="CC BY", apc=True, hasCategory=[], hasArea=[])]},

    {"input":"Scien",
     "output":[Journal(title = "Scientific Journals of the Maritime University of Szczecin", id = ["1733-8670","2392-0378"], languages = ["English"], publisher = "MUS", seal=False, license="CC BY", apc=True, hasCategory=[], hasArea=[]),
               Journal(title = "Fronteiras: Journal of Social, Technological and Environmental Science", id = "2238-8869", languages = ["Portuguese"], publisher = "Centro Universitário de Anápolis", seal=False, license="CC BY", apc=True, hasCategory=[], hasArea=[])]},

    {"input":"Semina: Ciências Agrárias",
     "output":[Journal(title = "Semina: Ciências Agrárias", id = ["1676-546X","1679-0359"], languages = ["Portuguese", "English"], publisher = "Universidade Estadual de Londrina", seal=False, license="CC BY", apc=True, hasCategory=[], hasArea=[])]}
    ]
    i = 0
    for pair in correct_inputs_and_outputs:
        i = i+1
        if engine.getJournalsWithTitle(pair["input"])==pair["output"]:
            print(True, "Yoho^^ for TEST3",[i])
        else:
            print(False, "Eha... problem occurs in TEST3, pair",[i])
            # print(engine.getJournalsWithTitle(inputs[i]))

#TEST 4
def test_getJournalsPublishedBy():
    inputs = ["Universi","University","Universidade","happy Yang","MUS","de Anápolis"]
    outputs = [[j[0],j[1], j[2],j[4],j[5]],[j[1],j[2]],[j[0],j[5]],None, [j[3]],[j[4]]]
    i = 0
    while i < 5 :
        if engine.getJournalsPublishedBy(inputs[i]) == outputs[i]:
            i = i+1
            print(True, "Yoho^^ for TEST4", [i]) 
        else:
            i = i+1
            print(False, "Eha... problem occurs in TEST4, pair",[i])
            print(engine.getJournalsPublishedBy(inputs[i]))

#TEST 5
def test_getJournalsWithLicense():
    correct_inputs_and_outputs=[
    {"input":"CC BY-NC-SA",
     "output": [j[0]]},

    {"input":"CC BY",
     "output": [j[1],j[3]]},

    {"input":"Publisher's own license",
     "output": [j[5]]},

    {"input":"CHI CHI",
     "output": None},
    ]
    for pair in correct_inputs_and_outputs:
        if engine.getJournalsWithLicense(pair["input"])==pair["output"]:
            print(True, "Yoho^^ TEST5 is correct")
        else:
            print(False, "Eha... TEST5 is uncorrect.")

#TEST 6
def test_getJournalsWithAPC(j):
    result=[]
    for elem in j:
        if elem.apc==True:
            result.append(elem)
    if engine.getJournalsWithAPC()==result:
        print(True, "Yoho^^ TEST6 is correct")
    else:
        print(False, "Eha... TEST6 is uncorrect.")
    
#TEST 7
def test_getJournalsWithDOAJSeal(j):
    result=[]
    for elem in j:
        if elem.seal==True:
            result.append(elem)
    if engine.getJournalsWithDOAJSeal()==result:
        print(True, "Yoho^^ TEST7 is correct")
    else:
        print(False, "Eha... TEST7 is uncorrect.")

#TEST 8
def test_getAllCategories(c):
    if engine.getAllCategories()== c:
        print(True, "Yoho!^^ TEST8 is correct")
    else:
        print(False, "Eha... TEST8 is uncorrect.")

#TEST 9
def test_getAllAreas(a):
    if engine.getAllAreas()== a:
        print(True, "Yoho!^^ getAllAreas (TEST9) is correct")
    else:
        print(False, "Eha... getAllAreas (TEST9) is uncorrect.")

#TEST 10
def test_getCategoriesWithQuartile():
    correct_inputs_and_outputs=[
    {"input":{'Q1'},
     "output": c[0:10]},

    {"input":{},
     "output":c},

    {"input":{'Q3','Q4'},
     "output": [c[10], c[11]]},
    ]
    for pair in correct_inputs_and_outputs:
        if engine.getCategoriesWithQuartile(pair["input"])==pair["output"]:
            print(True, "Yoho^^ TEST10 is correct")
        else:
            print(False, "Eha... TEST10 is uncorrect.")

#TEST 11
def test_getCategoriesAssignedToAreas():
    correct_inputs_and_outputs=[
    {"input":{'Medicine'},
     "output": [c[0], c[1], c[2], c[9], c[10], c[11]]},

    {"input":{},
     "output":c},

    {"input":{'Medicine','Arts and Humanities'},
     "output": [c[0], c[1], c[2], c[9], c[10], c[11], c[12]]},
    ]
    for pair in correct_inputs_and_outputs:
        if engine.getCategoriesAssignedToAreas(pair["input"])==pair["output"]:
            print(True, "Yoho^^ TEST11 is correct")
        else:
            print(False, "Eha... TEST11 is uncorrect.")

# Test 12
def test_getAreasAssignedToCategories():
    inputs = [{"Drug Discovery"},{"Drug Discovery","Philosophy"},{"Medicine (miscellaneous)"},]
    outputs = [[a[0],a[1]],[a[0],a[1],a[6]],a,[a[0],a[1],a[5]]]
    i = 0
    while i < 2:
        if engine.getAreasAssignedToCategories(inputs[i]) == outputs[i]:
            i = i+1
            print(True,"YOHO^^ for Test12",[i])
        else:
            i = i+1
            print(False, "Eha... problem occurs in TEST12, pair",[i])
            print(engine.getAreasAssignedToCategories(inputs[i]))

#Test 13
def test_getJournalsInCategoriesWithQuartile():
    pass

def test_getJournalsInAreasWithLicense():
    pass

def test_getDiamondJournalsInAreasAndCategoriesWithQuartile():

    pass



# We call the functions:
test_getEntityById(j)
# test_getAllJournals(j)
# test_getJournalsWithTitle()
# test_getJournalsPublishedBy()
# test_getJournalsWithLicense()
# test_getJournalsWithAPC(j)
# test_getJournalsWithDOAJSeal(j)
# test_getAllCategories(c)
# test_getAllAreas(a)
# test_getCategoriesWithQuartile()
# test_getCategoriesAssignedToAreas()
# test_getAreasAssignedToCategories()
