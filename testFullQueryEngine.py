import inspect
from datetime import datetime
from impl import *


def inspect_object(obj, indent='', f=None):
    if isinstance(obj, Area):
        t = 'Area'
    elif isinstance(obj, Category):
        t = 'Category'
    elif isinstance(obj, Journal):
        t = 'Journal'
    elif obj is None:
        print(f"\n{indent}Type: None", file=f)
        return
    else:
        t = type(obj).__name__

    print(f"\n{indent}Type: {t}", file=f)

    attrs = {name: value for name, value in inspect.getmembers(obj)
             if not callable(value) and not name.startswith('__')}

    for name, value in attrs.items():

        # List of complex objects (Journal, Category, Area)
        if isinstance(value, list) and all(isinstance(e, (Area, Category, Journal)) for e in value):
            print(f"{indent}{name}:", file=f)
            if not value:
                print(f"{indent}  (empty)", file=f)
            for elem in value:
                print(f"{indent}  [", file=f)
                inspect_object(elem, indent=indent + '    ', f=f)
                print(f"{indent}  ]", file=f)
            continue

        # Simple list
        if isinstance(value, list):
            try:
                compact = ', '.join(str(v) for v in value)
            except Exception:
                compact = '<unprintable>'
            print(f"{indent}{name}: [{compact}]", file=f)
            continue

        # Simple value
        try:
            safe_val = str(value).encode('utf-8', errors='replace').decode('utf-8')
        except Exception:
            safe_val = '<unprintable>'
        print(f"{indent}{name}: {safe_val}", file=f)

    print('', file=f)


# -------------------------------------------------------
# Setup
# !! UPDATE grp_endpoint with your current Blazegraph IP
#    before running (it changes with the network).
#    Check the working IP in testQueryEngine.py.
# -------------------------------------------------------
rel_path = "data/relational_database.db"
cat = CategoryUploadHandler()
cat.setDbPathOrUrl(rel_path)

grp_endpoint = "http://172.20.10.2:9999/blazegraph/sparql"
jou = JournalUploadHandler()
jou.setDbPathOrUrl(grp_endpoint)

cat_qh = CategoryQueryHandler()
cat_qh.setDbPathOrUrl(rel_path)

jou_qh = JournalQueryHandler()
jou_qh.setDbPathOrUrl(grp_endpoint)

laura = FullQueryEngine()
laura.addCategoryHandler(cat_qh)
laura.addJournalHandler(jou_qh)


with open('resultsFullQueryEngine.txt', 'w', encoding='utf-8') as f:

    f.write('***REPORT ABOUT THE FULL QUERY ENGINE***\n')
    f.write(datetime.now().strftime("%d/%m/%Y %H:%M\n\n\n"))


    # ===========================================================
    # GET JOURNALS IN CATEGORIES WITH QUARTILE
    # ===========================================================
    f.write('\n\n\nGET JOURNALS IN CATEGORIES WITH QUARTILE\n')

    f.write('\n\n\n*** ')
    f.write('category={"Drug Discovery"}, quartile={"Q1"}\n\n')
    try:
        j = laura.getJournalsInCategoriesWithQuartile({'Drug Discovery'}, {'Q1'})
        if not j:
            f.write('(empty list)\n')
        for elem in j:
            inspect_object(elem, f=f)
    except Exception as e:
        f.write(f'!!! ERROR IN EXECUTION: {e}\n')

    f.write('\n\n\n*** ')
    f.write('category={"Philosophy"}, quartile={} (all quartiles)\n\n')
    try:
        j = laura.getJournalsInCategoriesWithQuartile({'Philosophy'}, {})
        if not j:
            f.write('(empty list)\n')
        for elem in j:
            inspect_object(elem, f=f)
    except Exception as e:
        f.write(f'!!! ERROR IN EXECUTION: {e}\n')

    f.write('\n\n\n*** ')
    f.write('category={"Biomaterials", "Drug Discovery"}, quartile={"Q1"}\n\n')
    try:
        j = laura.getJournalsInCategoriesWithQuartile({'Biomaterials', 'Drug Discovery'}, {'Q1'})
        if not j:
            f.write('(empty list)\n')
        for elem in j:
            inspect_object(elem, f=f)
    except Exception as e:
        f.write(f'!!! ERROR IN EXECUTION: {e}\n')

    f.write('\n\n\n*** ')
    f.write('category={"Biochemistry, Genetics and Molecular Biology (miscellaneous)"}, quartile={"Q3"}\n\n')
    try:
        j = laura.getJournalsInCategoriesWithQuartile(
            {'Biochemistry, Genetics and Molecular Biology (miscellaneous)'}, {'Q3'}
        )
        if not j:
            f.write('(empty list)\n')
        for elem in j:
            inspect_object(elem, f=f)
    except Exception as e:
        f.write(f'!!! ERROR IN EXECUTION: {e}\n')

    f.write('\n\n\n*** ')
    f.write('category={} (all categories), quartile={"Q1"}\n\n')
    try:
        j = laura.getJournalsInCategoriesWithQuartile({}, {'Q1'})
        if not j:
            f.write('(empty list)\n')
        for elem in j:
            inspect_object(elem, f=f)
    except Exception as e:
        f.write(f'!!! ERROR IN EXECUTION: {e}\n')

    f.write('\n\n\n*** ')
    f.write('category={} (all), quartile={} (all)\n\n')
    try:
        j = laura.getJournalsInCategoriesWithQuartile({}, {})
        if not j:
            f.write('(empty list)\n')
        for elem in j:
            inspect_object(elem, f=f)
    except Exception as e:
        f.write(f'!!! ERROR IN EXECUTION: {e}\n')

    f.write('\n\n\n*** ')
    f.write('category={"happy-yang"}, quartile={"Q1"}\n\n')
    try:
        j = laura.getJournalsInCategoriesWithQuartile({'happy-yang'}, {'Q1'})
        if not j:
            f.write('(empty list)\n')
        for elem in j:
            inspect_object(elem, f=f)
    except Exception as e:
        f.write(f'!!! ERROR IN EXECUTION: {e}\n')


    # ===========================================================
    # GET JOURNALS IN AREAS WITH LICENSE
    # ===========================================================
    f.write('\n\n\nGET JOURNALS IN AREAS WITH LICENSE\n')

    f.write('\n\n\n*** ')
    f.write('area={"Arts and Humanities"}, license={"CC BY"}\n\n')
    try:
        j = laura.getJournalsInAreasWithLicense({'Arts and Humanities'}, {'CC BY'})
        if not j:
            f.write('(empty list)\n')
        for elem in j:
            inspect_object(elem, f=f)
    except Exception as e:
        f.write(f'!!! ERROR IN EXECUTION: {e}\n')

    f.write('\n\n\n*** ')
    f.write('area={"Medicine"}, license={"CC BY"}\n\n')
    try:
        j = laura.getJournalsInAreasWithLicense({'Medicine'}, {'CC BY'})
        if not j:
            f.write('(empty list)\n')
        for elem in j:
            inspect_object(elem, f=f)
    except Exception as e:
        f.write(f'!!! ERROR IN EXECUTION: {e}\n')

    f.write('\n\n\n*** ')
    f.write('area={"Arts and Humanities", "Economics, Econometrics and Finance"}, license={"CC BY", "CC BY-NC-SA"}\n\n')
    try:
        j = laura.getJournalsInAreasWithLicense(
            {'Arts and Humanities', 'Economics, Econometrics and Finance'},
            {'CC BY', 'CC BY-NC-SA'}
        )
        if not j:
            f.write('(empty list)\n')
        for elem in j:
            inspect_object(elem, f=f)
    except Exception as e:
        f.write(f'!!! ERROR IN EXECUTION: {e}\n')

    f.write('\n\n\n*** ')
    f.write('area={"Arts and Humanities"}, license={} (all licenses)\n\n')
    try:
        j = laura.getJournalsInAreasWithLicense({'Arts and Humanities'}, {})
        if not j:
            f.write('(empty list)\n')
        for elem in j:
            inspect_object(elem, f=f)
    except Exception as e:
        f.write(f'!!! ERROR IN EXECUTION: {e}\n')

    f.write('\n\n\n*** ')
    f.write('area={} (all areas), license={"CC BY"}\n\n')
    try:
        j = laura.getJournalsInAreasWithLicense({}, {'CC BY'})
        if not j:
            f.write('(empty list)\n')
        for elem in j:
            inspect_object(elem, f=f)
    except Exception as e:
        f.write(f'!!! ERROR IN EXECUTION: {e}\n')

    f.write('\n\n\n*** ')
    f.write('area={} (all), license={} (all)\n\n')
    try:
        j = laura.getJournalsInAreasWithLicense({}, {})
        if not j:
            f.write('(empty list)\n')
        for elem in j:
            inspect_object(elem, f=f)
    except Exception as e:
        f.write(f'!!! ERROR IN EXECUTION: {e}\n')

    f.write('\n\n\n*** ')
    f.write('area={"happy-yang"}, license={"CC BY"}\n\n')
    try:
        j = laura.getJournalsInAreasWithLicense({'happy-yang'}, {'CC BY'})
        if not j:
            f.write('(empty list)\n')
        for elem in j:
            inspect_object(elem, f=f)
    except Exception as e:
        f.write(f'!!! ERROR IN EXECUTION: {e}\n')

    f.write('\n\n\n*** ')
    f.write('area={"Medicine"}, license={"ChiChi"}\n\n')
    try:
        j = laura.getJournalsInAreasWithLicense({'Medicine'}, {'ChiChi'})
        if not j:
            f.write('(empty list)\n')
        for elem in j:
            inspect_object(elem, f=f)
    except Exception as e:
        f.write(f'!!! ERROR IN EXECUTION: {e}\n')


    # ===========================================================
    # GET DIAMOND JOURNALS IN AREAS AND CATEGORIES WITH QUARTILE
    # ===========================================================
    f.write('\n\n\nGET DIAMOND JOURNALS IN AREAS AND CATEGORIES WITH QUARTILE\n')

    f.write('\n\n\n*** ')
    f.write('area={"Arts and Humanities"}, category={"Philosophy"}, quartile={} (all quartiles)\n\n')
    try:
        j = laura.getDiamondJournalsInAreasAndCategoriesWithQuartile(
            {'Arts and Humanities'}, {'Philosophy'}, {}
        )
        if not j:
            f.write('(empty list)\n')
        for elem in j:
            inspect_object(elem, f=f)
    except Exception as e:
        f.write(f'!!! ERROR IN EXECUTION: {e}\n')

    f.write('\n\n\n*** ')
    f.write('area={"Medicine"}, category={"Drug Discovery"}, quartile={"Q1"}\n\n')
    try:
        j = laura.getDiamondJournalsInAreasAndCategoriesWithQuartile(
            {'Medicine'}, {'Drug Discovery'}, {'Q1'}
        )
        if not j:
            f.write('(empty list)\n')
        for elem in j:
            inspect_object(elem, f=f)
    except Exception as e:
        f.write(f'!!! ERROR IN EXECUTION: {e}\n')

    f.write('\n\n\n*** ')
    f.write('area={"Energy"}, category={"Energy (miscellaneous)"}, quartile={"Q1"}\n\n')
    try:
        j = laura.getDiamondJournalsInAreasAndCategoriesWithQuartile(
            {'Energy'}, {'Energy (miscellaneous)'}, {'Q1'}
        )
        if not j:
            f.write('(empty list)\n')
        for elem in j:
            inspect_object(elem, f=f)
    except Exception as e:
        f.write(f'!!! ERROR IN EXECUTION: {e}\n')

    f.write('\n\n\n*** ')
    f.write('area={} (all areas), category={"Philosophy"}, quartile={} (all quartiles)\n\n')
    try:
        j = laura.getDiamondJournalsInAreasAndCategoriesWithQuartile(
            {}, {'Philosophy'}, {}
        )
        if not j:
            f.write('(empty list)\n')
        for elem in j:
            inspect_object(elem, f=f)
    except Exception as e:
        f.write(f'!!! ERROR IN EXECUTION: {e}\n')

    f.write('\n\n\n*** ')
    f.write('area={"Energy"}, category={} (all categories), quartile={"Q1"}\n\n')
    try:
        j = laura.getDiamondJournalsInAreasAndCategoriesWithQuartile(
            {'Energy'}, {}, {'Q1'}
        )
        if not j:
            f.write('(empty list)\n')
        for elem in j:
            inspect_object(elem, f=f)
    except Exception as e:
        f.write(f'!!! ERROR IN EXECUTION: {e}\n')

    f.write('\n\n\n*** ')
    f.write('area={} (all), category={} (all), quartile={} (all)\n\n')
    try:
        j = laura.getDiamondJournalsInAreasAndCategoriesWithQuartile({}, {}, {})
        if not j:
            f.write('(empty list)\n')
        for elem in j:
            inspect_object(elem, f=f)
    except Exception as e:
        f.write(f'!!! ERROR IN EXECUTION: {e}\n')

    f.write('\n\n\n*** ')
    f.write('area={"happy-yang"}, category={"Philosophy"}, quartile={} (all quartiles)\n\n')
    try:
        j = laura.getDiamondJournalsInAreasAndCategoriesWithQuartile(
            {'happy-yang'}, {'Philosophy'}, {}
        )
        if not j:
            f.write('(empty list)\n')
        for elem in j:
            inspect_object(elem, f=f)
    except Exception as e:
        f.write(f'!!! ERROR IN EXECUTION: {e}\n')

    f.write('\n\n\n*** ')
    f.write('area={"Arts and Humanities"}, category={"happy-yang"}, quartile={"Q1"}\n\n')
    try:
        j = laura.getDiamondJournalsInAreasAndCategoriesWithQuartile(
            {'Arts and Humanities'}, {'happy-yang'}, {'Q1'}
        )
        if not j:
            f.write('(empty list)\n')
        for elem in j:
            inspect_object(elem, f=f)
    except Exception as e:
        f.write(f'!!! ERROR IN EXECUTION: {e}\n')


print('Done - Send immediately the results.txt file to Daniele ;P')
