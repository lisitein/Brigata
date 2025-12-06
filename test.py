from laura import *
from daniele import *
from li import *
from Yang import *
from baseHandler import *

j=[
    Journal(title = "Prolíngua", id = "1983-9979", language = "Portuguese", publisher = "Universidade Federal da Paraíba", seal=False, license="CC BY-NC-SA", apc=False),
    Journal(title = "Проблеми Законності", id = ["2224-9281","2414-990X"] language = ["Ukrainian", "Russian", "English"], publisher = "Yaroslav Mudryi National Law University", seal=False, license="CC BY", apc=True),
    Journal(title = "Enlightening Tourism: A Pathmaking Journal", id = "2174-548X", language = "English", publisher = "University of Huelva", seal=False, license="CC BY-NC", apc=False),
    Journal(title = "Scientific Journals of the Maritime University of Szczecin", id = ["1733-8670","2392-0378"], language = "English", publisher = "MUS", seal=False, license="CC BY", apc=True),
    Journal(title = "Fronteiras: Journal of Social, Technological and Environmental Science", id = "2238-8869", language = "Portuguese", publisher = "Centro Universitário de Anápolis", seal=False, license="CC BY-NC", apc=False),
    Journal(title = "Semina: Ciências Agrárias", id = ["1676-546X","1679-0359"], language = ["Portuguese", "English"], publisher = "Universidade Estadual de Londrina", seal=False, license="Publisher's own license", apc=True)
    ]


a=[
    Area(id="Medicine");
    Area(id="Pharmacology, Toxicology and Pharmaceutics");
    Area(id="Economics, Econometrics and Finance");
    Area(id="Energy");
    Area(id="Materials Science");
    Area(id="Biochemistry, Genetics and Molecular Biology");
    Area(id="Arts and Humanities");
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





#TEST 1

correct_inputs_and_outputs=[

    {"input":"1474-1784",
     "output":Journal(id=["1474-1784","1474-1776"])},

    {"input":"santa-claus",
     "output":None},

    {"input":"2224-9281",
     "output":Journal(id=["2224-9281", "2414-990X"], title="Проблеми Законності", languages=["Ukrainian", "Russian", "English"], publisher="Yaroslav Mudryi National Law University", seal=False, license="CC BY", apc=True)}
    
    {"input":"happy-yang",
     "output":None},
    
    {"input":"2238-8869",
     "output":Journal(id="2238-8869", title="Fronteiras: Journal of Social, Technological and Environmental Science", languages="Portuguese", publisher="Centro Universitário de Anápolis", seal=False, license="CC BY-NC", apc=False)},

    {"input":"Medicine",
     "output":Area(id="Medicine")},

    {"input":"Biochemistry, Genetics and Molecular Biology",
     "output":Area(id="Biochemistry, Genetics and Molecular Biology")},

    {"input":"Biochemistry, Genetics and Molecular Biology (miscellaneous)",
     "output":Category(id="Biochemistry, Genetics and Molecular Biology (miscellaneous)")},
    #what about quartile? in the UML it is an attribute of Category class...

    {"input":"Philosophy",
     "output":Category(id="Philosophy")}
        
    ]


def test_getEntityById(correct_inputs_and_outputs):
    i = 0
    for pair in correct_inputs_and_outputs:
        i = i+1
        if getEntityById(pair["input"])==pair["output"]:
            print(True, "Yoho^^ for Test1, [i]")
        else:
            print(False, "Eha... The problem occurs in test_getEntityById (TEST1), pair[i]")



#TEST 2
def test_getAllJournals(j):
    if getAllJournals()== j:
        print(True, "Yoho!^^ getAllJournals(TEST2) is True")
    else:
        print(False, "Eha... The result of getAllJournals (TEST2) is uncorrect.")


# TEST3

correct_inputs_and_outputs=[
    
    {"input":"Prolíngua",
     "output":[Journal(title = "Prolíngua", id = "1983-9979", language = "Portuguese", publisher = "Universidade Federal da Paraíba", seal=False, license="CC BY-NC-SA", apc=False)]},

    {"input":"Законності",
     "output":[Journal(title = "Проблеми Законності", id = ["2224-9281","2414-990X"] language = ["Ukrainian", "Russian", "English"], publisher = "Yaroslav Mudryi National Law University", seal=False, license="CC BY", apc=True)]},

    {"input":"happy-Yang",
     "output":None},

    {"input":"Enlightening Tourism",
     "output":[Journal(title = "Enlightening Tourism: A Pathmaking Journal", id = "2174-548X", language = "English", publisher = "University of Huelva", seal=False, license="CC BY-NC", apc=False)]},

    {"input":"University of Szczecin",
     "output":[Journal(title = "Scientific Journals of the Maritime University of Szczecin", id = ["1733-8670","2392-0378"], language = "English", publisher = "MUS", seal=False, license="CC BY", apc=True)]},

    {"input":"Fronteiras",
     "output":[Journal(title = "Fronteiras: Journal of Social, Technological and Environmental Science", id = "2238-8869", language = "Portuguese", publisher = "Centro Universitário de Anápolis", seal=False, license="CC BY-NC", apc=False)]},
    
    {"input":"Semina",
     "output":[Journal(title = "Semina: Ciências Agrárias", id = ["1676-546X","1679-0359"], language = ["Portuguese", "English"], publisher = "Universidade Estadual de Londrina", seal=False, license="Publisher's own license", apc=True)]},

    {"input":"Enlightening",
     "output":[Journal(title = "Enlightening Tourism: A Pathmaking Journal", id = "2174-548X", language = "English", publisher = "University of Huelva", seal=False, license="CC BY", apc=True)]},

    {"input":"Scien",
     "output":[Journal(title = "Scientific Journals of the Maritime University of Szczecin", id = ["1733-8670","2392-0378"], language = "English", publisher = "MUS", seal=False, license="CC BY", apc=True),
               Journal(title = "Fronteiras: Journal of Social, Technological and Environmental Science", id = "2238-8869", language = "Portuguese", publisher = "Centro Universitário de Anápolis", seal=False, license="CC BY", apc=True)]},

    {"input":"Semina: Ciências Agrárias",
     "output":[Journal(title = "Semina: Ciências Agrárias", id = ["1676-546X","1679-0359"], language = ["Portuguese", "English"], publisher = "Universidade Estadual de Londrina", seal=False, license="CC BY", apc=True)]}

]


def test_getJournalsWithTitle(correct_inputs_and_outputs):
    i = 0
    for pair in correct_inputs_and_outputs:
        i = i+1
        if getJournalsWithTitle(pair["input"])==pair["output"]:
            print(True, "Yoho^^ for TEST3, [i]")
        else:
            print(False, "Eha... problem occurs in getJournalsWithTitle (TEST3), pair[i].")


#TEST 4
inputs = ["Universi","University","Universidade","happy Yang","MUS","de Anápolis"]
outputs = [[j[0],j[1], j[2],j[4],j[5]],[j[1],j[2]],[j[0],j[5]],None, [j[3]],[j[4]]]
def test_getJournalsPublishedBy(inputs):
    i = 0
    for i <6:
        if getJournalsPublishedBy(inputs[i]) == outputs[i]:
            i = i+1
            print(True, "Yoho^^ for TEST4, [i]") 
        else:
            i = i+1
            print(False, "Eha... problem occurs in getJournalsPublishedBy (TEST4), pair[i].")





#TEST 5

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

def test_getJournalsWithLicense(correct_inputs_and_outputs):
    for pair in correct_inputs_and_outputs:
    if getJournalsWithLicense(pair["input"])==pair["output"]:
        print(True, "Yoho^^ getJournalsWithLicense (TEST5) is correct")
    else:
        print(False, "Eha... getJournalsWithLicense (TEST5) is uncorrect.")



#TEST 6

def test_getJournalsWithAPC(j):
    result=[]
    for elem in j:
        if elem.apc==True:
            result.append(elem)
    if getJournalsWithAPC()==result:
        print(True, "Yoho^^ getJournalsWithAPC (TEST6) is correct")
    else:
        print(False, "Eha... getJournalsWithAPC (TEST6) is uncorrect.")
    



#TEST 7

def test_getJournalsWithDOAJSeal(j):
    result=[]
    for elem in j:
        if elem.seal==True:
            result.append(elem)
    if getJournalsWithDOAJSeal()==result:
        print(True, "Yoho^^ getJournalsWithDOAJSeal (TEST7) is correct")
    else:
        print(False, "Eha... getJournalsWithDOAJSeal (TEST7) is uncorrect.")




#TEST 8
def test_getAllCategories(c):
    if getAllCategories()== c:
        print(True, "Yoho!^^ getAllCategories (TEST8) is correct")
    else:
        print(False, "Eha... getAllCategories (TEST8) is uncorrect.")


#TEST 9
def test_getAllAreas(a):
    if getAllAreas()== a:
        print(True, "Yoho!^^ getAllAreas (TEST9) is correct")
    else:
        print(False, "Eha... getAllAreas (TEST9) is uncorrect.")




#TEST 10

def test_getCategoriesWithQuartile():
    pass






#TEST 11
def test_getCategoriesAssignedToAreas():
    pass






# Test 12
inputs = [{"Drug Discovery"},{"Drug Discovery","Philosophy"},{"Medicine (miscellaneous)"},]
outputs = [[a[0],a[1]],[a[0],a[1],a[6]],a,[a[0],a[1],a[5]]]
def test_getAreasAssignedToCategories(inputs):
    i = 0
    while i < 4:
        if getAreasAssignedToCategories(inputs[i]) == outputs[i]:
            i = i+1
            print(True,"YOHO^^ for Test12 [i].")
        else:
            i = i+1
            print(False, "Eha... problem occurs in getAreasAssignedToCategories (TEST12), pair[i].")
    




def test_getJournalsInCategoriesWithQuartile():
    pass

def test_getJournalsInAreasWithLicense():
    pass

def test_getDiamondJournalsInAreasAndCategoriesWithQuartile():

    pass














