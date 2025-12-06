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
            print(True)
    




#TEST 2

correct_inputs_and_outputs=[
    {"input":     ,
     "output":       }

    ]

def test_getAllJournals():
    for pair in correct_inputs_and_outputs:
        if getAllJournals(pair["input"])==pair["output"]:
            print(True)


















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