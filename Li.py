#Li: Ciao a tutti, I put the annotations of class above that class
import pandas as pd
import json
import csv
from Daniele.py import Handler, CategoryUploadHandler


#goal of UploadHandler is to recognize the format of the file
class UploadHandler(Handler): 
    def __init__(self, path):
        super().__init__(path)  # I am not sure about params here
        # something about setDbPathOrUrl(path)
        
    def pushDataToDb(self, path):
        if dbPathOrUrl.endswith('.csv'):
            result = JournalUploadHandler.graph(path)  # Call the graph method of JournalUploadHandler
        elif dbPathOrUrl.endswith('.json'):
            result = CategoryUploadHandler(path)

        return result

from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, RDFS

#implements the method of the superclass to handle the specific scenario
#JournalUploadHandler to handle CSV files in input and to store their data in a graph database
class JournalUploadHandler(UploadHandler):
    def __init__(self， path):
        super().__init__()

        base_url = "http://example.org/journal/"  # Base URL?
        self.graph = Graph()  # Create a new RDF graph

# Journal title,Journal ISSN (print version),Journal EISSN (online version),Languages in which the journal accepts manuscripts,Publisher,DOAJ Seal,Journal license,APC
        journal = pd.read_csv(path, sep=',', encoding='utf-8',
                              keep_default_na=False)
        
        journal_id = {} # to create a internal id for each journal
        for idx, row in journal.iterrows():
            local_id = "journal_" + str(idx)  # local_id = journal_0, journal_1, etc.

            subj = URIRef(base_url + local_id) # the subject of the RDFtriple
            journal_id[row["id"]] = subj  # store the mapping of local_id to subj

            for column in journal.columns:
                # Create a predicate for each column in the CSV file
                predicate = URIRef(base_url + column)
                # Create a literal value（object) for the column value
                value = row[column]
                if pd.isna(value): # if the value is NaN, skip it
                    continue

        # Save the graph to a file or database - I don't know if it is necessary to do it
        # self.graph.serialize(destination="journal_data.rdf", format="xml")

        # store and populate a graph database
        from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

        store = SPARQLUpdateStore()
        endpoint = "http://localhost:9999/blazegraph/sparql"  # SPARQL endpoint URL
        store.open((endpoint, endpoint))  # Open the SPARQL store

        # Add each triple to the store
        for triple in self.graph.triples((None, None, None)):
            store.add( triple )

        # Close the store connection
        store.close()