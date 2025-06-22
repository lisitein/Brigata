import pandas as pd
import json
import csv
from daniele import Handler, CategoryUploadHandler

#goal of UploadHandler is to recognize the format of the file  
class UploadHandler(Handler): 
    def __init__(self):
        super().__init__()   
        
    def pushDataToDb(self, path):
        db_path = self.getDbPathOrUrl()
        if not db_path:
            print("Error: No database path or URL provided. Please call setDbPathOrUrl() first.")
            return False
        if path.endswith('.csv'):
            journal_handler = JournalUploadHandler()
            journal_handler.setDbPathOrUrl(db_path)
            result = journal_handler.pushDataToDb(path)
            return result
        elif path.endswith('.json'):
            category_handler = CategoryUploadHandler()
            category_handler.setDbPathOrUrl(db_path)  
            result = category_handler.pushDataToDb(path)
            return result  
        else:
            print("Error: Unsupported file format: {path}")  
            return False
        
    
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF

#implements the method of the superclass to handle the specific scenario
#JournalUploadHandler to handle CSV files in input and to store their data in a graph database
class JournalUploadHandler(UploadHandler):
    def __init__(self):  
        super().__init__()

    def pushDataToDb(self, path):
        base_url = Namespace("http://Brigata.github.org/journal/")  
        self.graph = Graph()  # Create a new RDF graph
        self.graph.bind("base_url", base_url)  # Bind the base URL to the graph

        journal = pd.read_csv(path, sep=',', encoding='utf-8',
                              keep_default_na=False,
                              names=['title', 'issn', 'eissn', 
                                    #  'categories', 'areas',
                                     'languages', 'publisher', 'seal', 'licence','apc'],
                              header=0,
                              dtype={
                                  "Journal title":str,
                                  "Journal ISSN (print version)":str,  
                                  "Journal EISSN (online version)":str,
                                  "Languages in which the journal accepts manuscripts":str,
                                  "Publisher":str,
                                  "DOAJ Seal":bool,
                                  "Journal license":str,  
                                  "APC":bool
                                  }) #Read the CSV file into a pandas DataFrame and change the columns' name
        # print(journal)                       
        # subj wil be
        # id string [1..*]->Journal ISSN (print version),Journal EISSN (online version)
        # each will be id of the journal(if both ISSN and EISSN), and there will be two subj - one entity
        # journal_id = {} 
        id_cols = ['issn','eissn']
        attribute_cols = ['title', 'languages', 'publisher', 'seal', 'licence', 'apc']
        for idx, row in journal.iterrows():
            local_id = "journal_" + str(idx)  # local_id = journal_0, journal_1, etc.
            subject = URIRef(base_url[local_id]) # can automatically deal with the URL
            self.graph.add((subject, RDF.type, URIRef(base_url["Journal"]))) # add type
        # attributes will be:
        # Journal title, title: string (1)  -title = URIRef("https://schema.org/name")
        # Languages in which the journal accepts manuscripts, languages : string [1..*]
        # Publisher, publisher : string [0.1] 
        # DOAJ Seal, seal: boolean [1]
        # Journal license, licence : string [1]
        # APC, apc : boolean [1]
            for column in attribute_cols:
                attribute = str(row[column])
                if attribute:
                    predicate = URIRef(base_url[column])
                    if column in ['seal', 'apc']:
                        booleanvalue = attribute.lower() in ['true', '1', 't', 'y', 'yes']  
                        object = Literal(booleanvalue)
                    else:
                        object = Literal(attribute)
                    self.graph.add((subject, predicate, object))
                    
            for column in id_cols:
                id_value = str(row[column])
                if id_value:
                    predicate = URIRef(base_url["id"])
                    self.graph.add((subject, predicate, Literal(id_value)))  
# do i need to add relations between Journal and Category\ Journal and Area?  
# Jounal - hasCategory -> Category (but Journal has Area?? what is area?)
        # return self.graph

        # Save the graph to a file or database - I don't know if it is necessary to do it
        # self.graph.serialize(destination="journal_data.rdf", format="xml")

        # store and populate a graph database
        from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

        store = SPARQLUpdateStore()
        endpoint = "http://127.0.0.1:9999/blazegraph/sparql"  # SPARQL endpoint URL
        store.open((endpoint, endpoint))  # Open the SPARQL store

        # Add each triple to the store
        for triple in self.graph.triples((None, None, None)):
            store.add( triple )

        # Close the store connection
        store.close()

        return True

# SOPHIE!! Try like this
import csv
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF
from baseHandler import UploadHandler, QueryHandler

class JournalUploadHandler(UploadHandler):
    def __init__(self):
        self.graph = Graph()
        self.endpoint = None
        self.base = "https://example.org/"

    def setDbPathOrUrl(self, url):
        self.endpoint = url

    def pushDataToDb(self, path: str) -> bool:
        DOAJ = Namespace(self.base)
        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                title = row["Journal title"]
                identifiers = []
                if row["Journal ISSN (print version)"]:
                    identifiers.append(row["Journal ISSN (print version)"])
                if row["Journal EISSN (online version)"]:
                    identifiers.append(row["Journal EISSN (online version)"])

                journal_uri = URIRef(f"{self.base}journal/{title.replace(' ', '_')}")
                self.graph.add((journal_uri, RDF.type, DOAJ.Journal))
                self.graph.add((journal_uri, DOAJ.title, Literal(title)))

                for id_value in identifiers:
                    self.graph.add((journal_uri, DOAJ.identifier, Literal(id_value)))

                langs = row["Languages in which the journal accepts manuscripts"]
                if langs:
                    for lang in [l.strip() for l in langs.split(",")]:
                        self.graph.add((journal_uri, DOAJ.language, Literal(lang)))

                publisher = row["Publisher"]
                if publisher:
                    self.graph.add((journal_uri, DOAJ.publisher, Literal(publisher)))

                license = row["Journal license"]
                if license:
                    self.graph.add((journal_uri, DOAJ.license, Literal(license)))

                seal = row["DOAJ Seal"]
                self.graph.add((journal_uri, DOAJ.seal, Literal(seal.strip().lower() == "yes")))

                apc = row["APC"]
                self.graph.add((journal_uri, DOAJ.apc, Literal(apc.strip().lower() == "yes")))

        self.graph.serialize(destination=self.endpoint, format="nt")
        return True
