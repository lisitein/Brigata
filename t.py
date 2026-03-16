from impl import *
sophie=JournalUploadHandler()
sophie.setDbPathOrUrl('http://127.0.0.1:9999/blazegraph/sparql')
sophie.pushDataToDb('data/doaj.csv')
print('Done')