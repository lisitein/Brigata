from collections import Counter
from pprint import pprint
from impl import CategoryUploadHandler, CategoryQueryHandler
from impl import JournalUploadHandler, JournalQueryHandler
from impl import FullQueryEngine

rel_path = "relational.db"
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"

def load():

    cat = CategoryUploadHandler()
    cat.setDbPathOrUrl(rel_path)
    cat.pushDataToDb("data/scimago.json")

    jou = JournalUploadHandler()
    jou.setDbPathOrUrl(grp_endpoint)
    jou.pushDataToDb("data/doaj.csv")

def test():

    cat_qh = CategoryQueryHandler()
    cat_qh.setDbPathOrUrl(rel_path)

    jou_qh = JournalQueryHandler()
    jou_qh.setDbPathOrUrl(grp_endpoint)

    que = FullQueryEngine()
    que.addCategoryHandler(cat_qh)
    que.addJournalHandler(jou_qh)


    print('\n\n1) getEntityById')
    one=que.getEntityById('1822-4547')
    two=que.getEntityById('1822-4555')
    if one != two:
        print('ERROR in getting perfect entities by id')
        return False
    else:
        print('*** CORRECT ***\n\n')

    print('\n\n13) getJournalsInCategoriesWithQuartile')
    result_q2 = que.getJournalsInCategoriesWithQuartile({"Artificial Intelligence", "Oncology"}, {"Q1"})
    print(len(result_q2))
    for j in result_q2:
        if j.getTitle=='':
            return False
        idsq=[]
        for c in j.getCategories():
            new=(c.getIds(), c.getQuartile())  
            idsq.append(new)
        if (["Artificial Intelligence"], 'Q1') not in idsq and (["Oncology"],'Q1') not in idsq:
            return False
    print('*** CORRECT ***\n\n')
    #for j in result_q2:
    #    print(j.getIds())
    
    print('\n\n14) getJournalsInAreasWithLicense')
    result_q2 = que.getJournalsInAreasWithLicense({'Arts and Humanities'},{'CC BY-NC-ND'})
    print(len(result_q2))
    for j in result_q2:
        if j.getTitle=='':
            return False
        #print(j.getIds(), j.getLicence())     
        #for ar in j.getAreas():
            #print(ar.getIds()) 
        a=[]
        for c in j.getAreas():
            a.append(c.getIds())
        if (['Arts and Humanities']) not in a:
            print('ERRORE area')
            return False
        l=j.getLicence()
        if ('CC BY-NC-ND') not in l:
            print('ERRORE lic')
            return False
    print('*** CORRECT ***\n\n')


    print('\n\n15) getDiamondJournalsInAreasAndCategoriesWithQuartile')    
    r=que.getDiamondJournalsInAreasAndCategoriesWithQuartile({'Arts and Humanities'},{'Philosophy'},{'Q3'})
    for j in r:
        if j.getTitle=='':
            return False
        #print(j.getIds())     
        a=[]
        for c in j.getAreas():
            a.append(c.getIds())
        if (['Arts and Humanities']) not in a:
            print('ERRORE area')
            return False
        idsq=[]
        for c in j.getCategories():
            new=(c.getIds(), c.getQuartile())  
            idsq.append(new)
        if (["Philosophy"], 'Q3') not in idsq:
            print('ERRORE category')
            return False
        if j.hasAPC()!=False:
            print('ERRORE APC')
            return False
        
    print('*** CORRECT ***\n\n')
    return True

load()
status=test()
print('\n\n\nDOES THE ENGINE WORK?', status)
