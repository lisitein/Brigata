from impl import *
from li import *
sophie=JournalUploadHandler()
sophie.setDbPathOrUrl('http://127.0.0.1:9999/blazegraph/sparql')
sophie.pushDataToDb('data/doaj.csv')
sophie.pushDataToDb('data/doaj2.csv')
print('Done')

#from daniele import *
#daniele=CategoryUploadHandler()
#daniele.setDbPathOrUrl('data/relational_database_doubled.db')
#daniele.pushDataToDb('data/scimago.json')
#daniele.pushDataToDb('data/scimago2.json')
#print('Done')