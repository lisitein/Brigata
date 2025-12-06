from laura import *
from daniele import *
from li import *
from Yang import *
from baseHandler import *



#TEST 1

correct_inputs_and_outputs=[
    {"input":"1474-1784",
     "output":Journal(id=["1474-1784","1474-1776"])}
    ("santa-claus", None)
    ("2224-9281", Journal(id=["2224-9281", "2414-990X"], title="Проблеми Законності", languages=["Ukrainian", "Russian", "English"], publisher="Yaroslav Mudryi National Law University", seal=False, license="CC BY", apc=True))
    ("happy-yang", None)
    ("2224-9281", Journal(id=["2224-9281", "2414-990X"], title="Проблеми Законності", languages=["Ukrainian", "Russian", "English"], publisher="Yaroslav Mudryi National Law University", seal=False, license="CC BY", apc=True))
    ]

def test_getEntityById(correct_inputs_and_outputs):
    for pair in correct_inputs_and_outputs:
        if getEntityById(pair["input"])==pair["output"]:
            print("Yoho^^ getEntityById is True")
        else:
            print("Eha... The result of test_getEntityById (TEST1) is uncorrect.")



#TEST 2

correct_inputs_and_outputs=[
    Journal(title = "Prolíngua", id = "1983-9979", language = "Portuguese", publisher = "Universidade Federal da Paraíba", seal=False, license="CC BY", apc=True),
    Journal(title = "Проблеми Законності", id = ["2224-9281","2414-990X"] language = ["Ukrainian", "Russian", "English"], publisher = "Yaroslav Mudryi National Law University", seal=False, license="CC BY", apc=True),
    Journal(title = "Enlightening Tourism", id = "2174-548X", language = "English", publisher = "University of Huelva", seal=False, license="CC BY", apc=True),
    Journal(title = "Scientific Journals of the Maritime University of Szczecin", id = ["1733-8670","2392-0378"], language = "English", publisher = "MUS", seal=False, license="CC BY", apc=True),
    Journal(title = "Fronteiras: Journal of Social, Technological and Environmental Science", id = "2238-8869", language = "Portuguese", publisher = "Centro Universitário de Anápolis", seal=False, license="CC BY", apc=True),
    Journal(title = "Semina: Ciências Agrárias", id = ["1676-546X","1679-0359"], language = ["Portuguese", "English"], publisher = "Universidade Estadual de Londrina", seal=False, license="CC BY", apc=True),
    ]

def test_getAllJournals():
    if getAllJournals()== correct_inputs_and_outputs:
        print("Yoho!^^ getAllJournals is True")
    else:
        print("Eha... The result of getAllJournals (TEST2) is uncorrect.")


















def test_getJournalsWithTitle():
    pass

def test_getJournalsPublishedBy():
    pass

def test_getJournalsWithLicense():
    pass

def test_getJournalsWithAPC():
    pass

def test_getJournalsWithDOAJSeal():
    pass

def test_getAllCategories():
    pass

def test_getAllAreas():
    pass

def test_getCategoriesWithQuartile():
    pass

def test_getCategoriesAssignedToAreas():
    pass

def test_getAreasAssignedToCategories():
    pass

def test_getJournalsInCategoriesWithQuartile():
    pass

def test_getJournalsInAreasWithLicense():
    pass

def test_getDiamondJournalsInAreasAndCategoriesWithQuartile():

    pass
