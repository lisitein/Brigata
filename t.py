from impl import *
from li import *
sophie=JournalUploadHandler()
sophie.setDbPathOrUrl('http://127.0.0.1:9999/blazegraph/sparql')
sophie.pushDataToDb('data/d.csv')
print('Done')

from daniele import *
daniele=CategoryUploadHandler()
daniele.setDbPathOrUrl('data/relational_database.db')
daniele.pushDataToDb('data/s.json')
print('Done')