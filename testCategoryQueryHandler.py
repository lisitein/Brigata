from datetime import datetime
from impl import *


with open('resultsCategoryQueryHandler.txt','w') as f:

    yang=CategoryQueryHandler()
    yang.setDbPathOrUrl('data/relational_database.db')

    f.write('***REPORT ABOUT THE CATEGORY QUERY HANDLER***\n')
    f.write(datetime.now().strftime("%d/%m/%Y %H:%M\n\n\n"))


    f.write('\n\n\nGET BY ID\n')

    f.write('\n\n\n***')
    f.write('0002-8282\n\n')
    f.write(yang.getById('0002-8282').to_string())

    f.write('\n\n\n***')
    f.write('1944-7981\n\n')
    f.write(yang.getById('1944-7981').to_string())

    f.write('\n\n\n***')
    f.write('2058-8437\n\n')
    f.write(yang.getById('2058-8437').to_string())

    f.write('\n\n\n***')
    f.write('2224-9281\n\n')
    f.write(yang.getById('2224-9281').to_string())

    f.write('\n\n\n***')
    f.write('Energy\n\n')
    f.write(yang.getById('Energy').to_string())

    f.write('\n\n\n***')
    f.write('Philosophy\n\n')
    f.write(yang.getById('Philosophy').to_string())

    f.write('\n\n\n***')
    f.write('Drug Discovery\n\n')
    f.write(yang.getById('Drug Discovery').to_string())

    f.write('\n\n\n***')
    f.write('Medicine (miscellaneous)\n\n')
    f.write(yang.getById('Medicine (miscellaneous)').to_string())

    f.write('\n\n\n***')
    f.write('happy-yang\n\n')
    f.write(yang.getById('happy-yang').to_string())

    f.write('\n\n\nGET ALL CATEGORIES\n\n')
    f.write(yang.getAllCategories().to_string())


    f.write('\n\n\nGET ALL AREAS\n\n')
    f.write(yang.getAllAreas().to_string())


    f.write('\n\nGET CATEGORIES WITH QUARTILE\n')

    f.write('\n\n\n***')
    f.write('no quartile specified\n\n')
    f.write(yang.getCategoriesWithQuartile({}).to_string())

    f.write('\n\n\n***')
    f.write('Q3\n\n')
    f.write(yang.getCategoriesWithQuartile({'Q3'}).to_string())

    f.write('\n\n\n***')
    f.write('Q3-Q4\n\n')
    f.write(yang.getCategoriesWithQuartile({'Q3','Q4'}).to_string())


    f.write('\n\n\nGET CATEGORIES ASSIGNED TO AREAS\n')

    f.write('\n\n\n***')
    f.write('no area specified\n\n')
    f.write(yang.getCategoriesAssignedToAreas({}).to_string())

    f.write('\n\n\n***')
    f.write('Medicine\n\n')
    f.write(yang.getCategoriesAssignedToAreas({'Medicine'}).to_string())

    f.write('\n\n\n***')
    f.write('Energy - Arts and Humanities\n\n')
    f.write(yang.getCategoriesAssignedToAreas({'Energy','Arts and Humanities'}).to_string())

    f.write('\n\n\n***')
    f.write('happy-yang\n\n')
    f.write(yang.getCategoriesAssignedToAreas({'happy-yang'}).to_string())


    f.write('\n\n\nGET AREAS ASSIGNED TO CATEGORIES\n')

    f.write('\n\n\n***')
    f.write('no category specified\n\n')
    f.write(yang.getAreasAssignedToCategories({}).to_string())

    f.write('\n\n\n***')
    f.write('Biomaterials\n\n')
    f.write(yang.getAreasAssignedToCategories({'Biomaterials'}).to_string())

    f.write('\n\n\n***')
    f.write('Pharmacology - Philosophy\n\n')
    f.write(yang.getAreasAssignedToCategories({'Pharmacology','Philosophy'}).to_string())

    f.write('\n\n\n***')
    f.write('Medicine (miscellaneous)\n\n')
    f.write(yang.getAreasAssignedToCategories({'Medicine (miscellaneous)'}).to_string())

    f.write('\n\n\n***')
    f.write('happy-yang\n\n')
    f.write(yang.getAreasAssignedToCategories({'happy-yang'}).to_string())


f.close()

print('Done - Send immediately the results.txt file to Daniele ;P')