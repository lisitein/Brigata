from datetime import datetime
from impl import *


with open('resultsCategoryQueryHandler.txt','w') as f:

    yang=CategoryQueryHandler()
    yang.setDbPathOrUrl('data/relational_database.db')

    f.write('***REPORT ABOUT THE CATEGORY QUERY HANDLER***\n')
    f.write(datetime.now().strftime("%d/%m/%Y %H:%M\n\n\n"))


    f.write('GET BY ID\n')

    f.write('\n\n\n***\n')
    f.write('0002-8282')
    f.write(yang.getById('0002-8282').to_string())

    f.write('\n\n\n***\n')
    f.write('1944-7981')
    f.write(yang.getById('1944-7981').to_string())

    f.write('\n\n\n***\n')
    f.write('2058-8437')
    f.write(yang.getById('2058-8437').to_string())

    f.write('\n\n\n***\n')
    f.write('2224-9281')
    f.write(yang.getById('2224-9281').to_string())

    f.write('\n\n\n***\n')
    f.write('Energy')
    f.write(yang.getById('Energy').to_string())

    f.write('\n\n\n***\n')
    f.write('Philosophy')
    f.write(yang.getById('Philosophy').to_string())

    f.write('\n\n\n***\n')
    f.write('Drug Discovery')
    f.write(yang.getById('Drug Discovery').to_string())

    f.write('\n\n\n***\n')
    f.write('Medicine (miscellaneous)')
    f.write(yang.getById('Medicine (miscellaneous)').to_string())

    f.write('\n\n\n***\n')
    f.write('happy-yang')
    f.write(yang.getById('happy-yang').to_string())

    f.write('GET ALL CATEGORIES\n')
    f.write(yang.getAllCategories().to_string())

    f.write('\n\n\n***\n')

    f.write('GET ALL AREAS\n')
    f.write(yang.getAllAreas().to_string())

    f.write('\n\n\n***\n')

    f.write('GET CATEGORIES WITH QUARTILE\n')

    f.write('\n\n\n***\n')
    f.write('no quartile specified')
    f.write(yang.getCategoriesWithQuartile({}).to_string())

    f.write('\n\n\n***\n')
    f.write('Q3')
    f.write(yang.getCategoriesWithQuartile({'Q3'}).to_string())

    f.write('\n\n\n***\n')
    f.write('Q3-Q4')
    f.write(yang.getCategoriesWithQuartile({'Q3','Q4'}).to_string())


    f.write('\n\n\n***\n')

    f.write('GET CATEGORIES ASSIGNED TO AREAS\n')

    f.write('\n\n\n***\n')
    f.write('no area specified')
    f.write(yang.getCategoriesAssignedToAreas({}).to_string())

    f.write('\n\n\n***\n')
    f.write('Medicine')
    f.write(yang.getCategoriesAssignedToAreas({'Medicine'}).to_string())

    f.write('\n\n\n***\n')
    f.write('Energy - Arts and Humanities')
    f.write(yang.getCategoriesAssignedToAreas({'Energy','Arts and Humanities'}).to_string())

    f.write('\n\n\n***\n')
    f.write('happy-yang')
    f.write(yang.getCategoriesAssignedToAreas({'happy-yang'}).to_string())

    f.write('\n\n\n***\n')

    f.write('GET AREAS ASSIGNED TO CATEGORIES\n')

    f.write('\n\n\n***\n')
    f.write('no category specified')
    f.write(yang.getAreasAssignedToCategories({}).to_string())

    f.write('\n\n\n***\n')
    f.write('Biomaterials')
    f.write(yang.getAreasAssignedToCategories({'Biomaterials'}).to_string())

    f.write('\n\n\n***\n')
    f.write('Pharmacology - Philosophy')
    f.write(yang.getAreasAssignedToCategories({'Pharmacology','Philosophy'}).to_string())

    f.write('\n\n\n***\n')
    f.write('Medicine (miscellaneous)')
    f.write(yang.getAreasAssignedToCategories({'Medicine (miscellaneous)'}).to_string())

    f.write('\n\n\n***\n')
    f.write('happy-yang')
    f.write(yang.getAreasAssignedToCategories({'happy-yang'}).to_string())


f.close()

print('Done - Send immediately the results.txt file to Daniele ;P')