from datetime import datetime
from impl import *


with open('resultsJournalQueryHandler.txt','w') as f:

    yang=JournalQueryHandler()
    yang.setDbPathOrUrl('http://192.168.1.115:9999/blazegraph/')                     #http://10.201.2.51:9999/blazegraph/

    f.write('***REPORT ABOUT THE JOURNAL QUERY HANDLER***\n')
    f.write(datetime.now().strftime("%d/%m/%Y %H:%M\n\n\n"))


    f.write('\n\n\nGET BY ID\n')

    f.write('\n\n\n***')
    f.write('2174-548X\n\n')
    try:
        f.write(yang.getById('2174-548X').to_string())
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n***')
    f.write('2224-9281\n\n')
    try:
        f.write(yang.getById('2224-9281').to_string())
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n***')
    f.write('happy-yang\n\n')
    try:
        f.write(yang.getById('happy-yang').to_string())
    except:
        f.write('!!! ERROR IN EXECUTION')



    f.write('\n\n\nGET ALL JOURNALS\n')
    try:
        f.write(yang.getAllJournals().to_string())
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n\n')



    f.write('\n\n\nGET JOURNALS WITH TITLE\n')

    f.write('\n\n\n***')
    f.write('happy-yang\n\n')
    try:    
        f.write(yang.getJournalsWithTitle('happy-yang').to_string())
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n***')
    f.write('cien\n\n')
    try:    
        f.write(yang.getJournalsWithTitle('cien').to_string())
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n***')
    f.write('Tourism\n\n')
    try:    
        f.write(yang.getJournalsWithTitle('Tourism').to_string())
    except:
        f.write('!!! ERROR IN EXECUTION')



    f.write('\n\n\nGET JOURNALS PUBLISHED BY\n')

    f.write('\n\n\n*** ')
    f.write('MUS\n\n')
    try:
        f.write(yang.getJournalsPublishedBy('MUS').to_string())
    except:
        f.write('!!! ERROR IN EXECUTION')
       
    f.write('\n\n\n*** ')
    f.write('Univers\n\n')
    try:
        f.write(yang.getJournalsPublishedBy('Univers').to_string())
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n*** ')
    f.write('Super Yang\n\n')
    try:
        f.write(yang.getJournalsPublishedBy('Super Yang').to_string())
    except:
        f.write('!!! ERROR IN EXECUTION')



    f.write('\n\n\nGET JOURNALS WITH LICENSE\n')

    f.write('\n\n\n*** ')
    f.write('CC BY     -     CC BY-NC-SA\n\n')
    try:
        f.write(yang.getJournalsWithLicense({'CC BY', 'CC BY-NC-SA'}).to_string())
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n*** ')
    f.write('CC BY-NC\n\n')
    try:
        f.write(yang.getJournalsWithLicense({'CC BY-NC'}).to_string())
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n*** ')
    f.write('ChiChi\n\n')
    try:
        f.write(yang.getJournalsWithLicense({'ChiChi'}).to_string())
    except:
        f.write('!!! ERROR IN EXECUTION')

    f.write('\n\n\n*** ')
    f.write('no license specified\n\n')
    try:
        f.write(yang.getJournalsWithLicense({}).to_string())
    except:
        f.write('!!! ERROR IN EXECUTION')



    f.write('\n\n\nGET JOURNALS WITH APC\n')
    try:
        f.write(yang.getJournalsWithAPC().to_string())
    except:
        f.write('!!! ERROR IN EXECUTION')



    f.write('\n\n\nGET JOURNALS WITH DOAJ SEAL\n')
    try:
        f.write(yang.getJournalsWithDOAJSeal().to_string())
    except:
        f.write('!!! ERROR IN EXECUTION')



f.close()

print('Done - Send immediately the results.txt file to Daniele ;P')