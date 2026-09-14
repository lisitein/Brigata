from impl import *
from li import *
sophie=JournalUploadHandler()
sophie.setDbPathOrUrl('http://127.0.0.1:9999/blazegraph/sparql')
sophie.pushDataToDb('data/small_doaj1.csv')
print('Done')

from daniele import *
daniele=CategoryUploadHandler()
daniele.setDbPathOrUrl('data/relational_database.db')
daniele.pushDataToDb('data/small_scimago1.json')
print('Done')