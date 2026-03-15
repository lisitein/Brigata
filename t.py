from impl import *
sophie=JournalUploadHandler()
sophie.setDbPathOrUrl('http://192.168.1.115:9999/blazegraph/')
sophie.pushDataToDb('data/doaj.csv')
print('Done')