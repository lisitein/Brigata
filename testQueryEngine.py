import inspect
from datetime import datetime
from impl import *


def inspect_object(obj, indent='', f=None):
    t=''
    if isinstance(obj,Area):
        t='Area'
    elif isinstance(obj,Category):
        t='Category'
    elif isinstance(obj,Journal):
        t='Journal'
    else:
        t=type(obj)
    print(f"\n{indent}Type: {t}", file=f)
    for name, value in inspect.getmembers(obj):
        # Filters out methods and internal dunder attributes
        if not callable(value) and not name.startswith('__'):
            print(f"{indent}{name}: {value}", file=f)
            if isinstance(value, list):
                for elem in value:
                    if isinstance(elem,Area) or isinstance(elem,Category):
                        print('\n\t[', file=f)
                        inspect_object(elem, indent='\t', f=f)
                        print('\t]\n', file=f)
    print('\n')


rel_path = "data/relational_database.db"
cat = CategoryUploadHandler()
cat.setDbPathOrUrl(rel_path)

grp_endpoint = "http://10.201.2.51:9999/blazegraph/"  
jou = JournalUploadHandler()
jou.setDbPathOrUrl(grp_endpoint)

cat_qh = CategoryQueryHandler()
cat_qh.setDbPathOrUrl(rel_path)

jou_qh = JournalQueryHandler()
jou_qh.setDbPathOrUrl(grp_endpoint)

laura = FullQueryEngine()
laura.addCategoryHandler(cat_qh)
laura.addJournalHandler(jou_qh)



with open('resultsQueryEngine.txt','w') as f:

    f.write('***REPORT ABOUT THE BASIC QUERY ENGINE***\n')
    f.write(datetime.now().strftime("%d/%m/%Y %H:%M\n\n\n"))

    f.write('\n\n\nGET ENTITY BY ID\n')

    f.write('\n\n\n*** ')
    f.write('0002-8282\n\n')
    try:
        o=laura.getEntityById('0002-8282')
        inspect_object(o, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n*** ')
    f.write('1944-7981\n\n')
    try:
        o=laura.getEntityById('1944-7981')
        inspect_object(o, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n*** ')
    f.write('2058-8437\n\n')
    try:
        o=laura.getEntityById('2058-8437')
        inspect_object(o, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')


    f.write('\n\n\n*** ')
    f.write('2224-9281\n\n')
    try:
        o=laura.getEntityById('2224-9281')
        inspect_object(o, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')


    f.write('\n\n\n*** ')
    f.write('2238-8869\n\n')
    try:
        o=laura.getEntityById('2238-8869')
        inspect_object(o, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')


    f.write('\n\n\n*** ')
    f.write('1733-8670\n\n')
    try:
        o=laura.getEntityById('1733-8670')
        inspect_object(o, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')


    f.write('\n\n\n*** ')
    f.write('2392-0378\n\n')
    try:
        o=laura.getEntityById('2392-0378')
        inspect_object(o, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')


    f.write('\n\n\n*** ')
    f.write('Energy\n\n')
    try:
        o=laura.getEntityById('Energy')
        inspect_object(o, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')


    f.write('\n\n\n*** ')
    f.write('Philosophy\n\n')
    try:
        o=laura.getEntityById('Philosophy')
        inspect_object(o, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')


    f.write('\n\n\n*** ')
    f.write('Drug Discovery\n\n')
    try:
        o=laura.getEntityById('Drug Discovery')
        inspect_object(o, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')


    f.write('\n\n\n*** ')
    f.write('Medicine (miscellaneous)\n\n')
    try:
        o=laura.getEntityById('Medicine (miscellaneous)')
        inspect_object(o, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n*** ')
    f.write('happy-yang\n\n')
    try:
        o=laura.getEntityById('happy-yang')
        inspect_object(o, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\nGET ALL JOURNALS\n')
    try:
        j=laura.getAllJournals()
        for elem in j:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')


    f.write('\n\n\nGET JOURNALS WITH TITLE\n')

    f.write('\n\n\n*** ')
    f.write('cien\n\n')
    try:
        j=laura.getJournalsWithTitle('cien')
        for elem in j:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')


    f.write('\n\n\n*** ')
    f.write('Tourism\n\n')
    try:
        j=laura.getJournalsWithTitle('Tourism')
        for elem in j:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')


    f.write('\n\n\n*** ')
    f.write('happy yang\n\n')
    try:
        j=laura.getJournalsWithTitle('happy yang')
        for elem in j:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')


    f.write('\n\n\nGET JOURNALS PUBLISHED BY\n')

    f.write('\n\n\n*** ')
    f.write('MUS\n\n')
    try:
        j=laura.getJournalsPublishedBy('MUS')
        for elem in j:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')
       
    f.write('\n\n\n*** ')
    f.write('Univers\n\n')
    try:
        j=laura.getJournalsPublishedBy('Univers')
        for elem in j:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n*** ')
    f.write('Super Yang\n\n')
    try:
        j=laura.getJournalsPublishedBy('Super Yang')
        for elem in j:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')


    f.write('\n\n\nGET JOURNALS WITH LICENSE\n')

    f.write('\n\n\n*** ')
    f.write('CC BY     -     CC BY-NC-SA\n\n')
    try:
        j=laura.getJournalsWithLicense({'CC BY', 'CC BY-NC-SA'})
        for elem in j:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n*** ')
    f.write('CC BY-NC\n\n')
    try:
        j=laura.getJournalsWithLicense({'CC BY-NC'})
        for elem in j:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n*** ')
    f.write('ChiChi\n\n')
    try:
        j=laura.getJournalsWithLicense({'ChiChi'})
        for elem in j:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n*** ')
    f.write('no license specified\n\n')
    try:
        j=laura.getJournalsWithLicense({})
        for elem in j:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\nGET JOURNALS WITH APC\n')
    try:
        j=laura.getJournalsWithAPC()
        for elem in j:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\nGET JOURNALS WITH DOAJ SEAL\n')
    try:
        j=laura.getJournalsWithDOAJSeal()
        for elem in j:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\nGET ALL CATEGORIES\n')
    try:
        c=laura.getAllCategories()
        for elem in c:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\nGET ALL AREAS\n')
    try:
        a=laura.getAllAreas()
        for elem in a:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')


    f.write('\n\n\nGET CATEGORIES WITH QUARTILE\n')

    f.write('\n\n\n*** ')
    f.write('no quartile specified\n\n')
    try:
        c=laura.getCategoriesWithQuartile({})
        for elem in c:
            inspect_object(elem, f=f) 
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n*** ')
    f.write('Q3\n\n')
    try:
        c=laura.getCategoriesWithQuartile({'Q3'})
        for elem in c:
            inspect_object(elem, f=f) 
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n*** ')
    f.write('Q3 - Q4\n\n')
    try:
        c=laura.getCategoriesWithQuartile({'Q3','Q4'})
        for elem in c:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\nGET CATEGORIES ASSIGNED TO AREAS\n')

    f.write('\n\n\n*** ')
    f.write('no area specified\n\n')
    try:
        c=laura.getCategoriesAssignedToAreas({})
        for elem in c:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n*** ')
    f.write('Medicine\n\n')
    try:
        c=laura.getCategoriesAssignedToAreas({'Medicine'})
        for elem in c:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n*** ')
    f.write('Energy - Arts and Humanities\n\n')
    try:
        c=laura.getCategoriesAssignedToAreas({'Energy','Arts and Humanities'})
        for elem in c:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n*** ')
    f.write('happy-yang\n\n')
    try:
        c=laura.getCategoriesAssignedToAreas({'happy-yang'})
        for elem in c:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\nGET AREAS ASSIGNED TO CATEGORIES\n')

    f.write('\n\n\n*** ')
    f.write('no category specified\n\n')
    try:
        c=laura.getAreasAssignedToCategories({})
        for elem in c:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n*** ')
    f.write('Biomaterials\n\n')
    try:
        c=laura.getAreasAssignedToCategories({'Biomaterials'})
        for elem in c:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n*** ')
    f.write('Pharmacology - Philosophy\n\n')
    try:
        c=laura.getAreasAssignedToCategories({'Pharmacology','Philosophy'})
        for elem in c:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n*** ')
    f.write('Medicine (miscellaneous)\n\n')
    try:
        c=laura.getAreasAssignedToCategories({'Medicine (miscellaneous)'})
        for elem in c:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n*** ')
    f.write('happy-yang\n\n')
    try:
        c=laura.getAreasAssignedToCategories({'happy-yang'})
        for elem in c:
            inspect_object(elem, f=f)
    except:
        f.write('!!! ERROR IN EXECUTION')




print('Done - Send immediately the results.txt file to Daniele ;P')