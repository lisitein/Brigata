from baseHandler import  UploadHandler 
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, XSD
import pandas as pd

#implements the method of the superclass to handle the specific scenario
#JournalUploadHandler to handle CSV files in input and to store their data in a graph database
class JournalUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path):
        base_url = Namespace("https://brigata.github.org/")  
        self.graph = Graph()  # Create a new RDF graph
        self.graph.bind("base_url", base_url)  # Bind the base URL to the graph

        journal = pd.read_csv(path, sep=',', encoding='utf-8',
                              keep_default_na=False,
                              names=['title', 'issn', 'eissn', 
                                    #  'categories', 'areas',
                                     'languages', 'publisher', 'seal', 'license','apc'],
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
        attribute_cols = ['title', 'languages', 'publisher', 'seal', 'license', 'apc']
        
        endpoint =self.getDbPathOrUrl()
        last_index = self.get_last_journal_index(endpoint)
        
        for idx, row in journal.iterrows():
            local_id = "journal_" + str(idx+last_index)  # internalId??
            subject = URIRef(base_url[local_id]) # can automatically deal with the URL
            self.graph.add((subject, RDF.type, URIRef(base_url["Journal"]))) # add type
        
        # attributes will be:
        # Journal title, title: string (1)  -title = URIRef("https://schema.org/name")
        # Languages in which the journal accepts manuscripts, languages : string [1..*]
        # Publisher, publisher : string [0.1] 
        # DOAJ Seal, seal: boolean [1]
        # Journal licence, licence : string [1]
        # APC, apc : boolean [1]
            for column in attribute_cols:
                attribute = str(row[column])
                if attribute:
                    predicate = URIRef(base_url[column])
                    if column in ['seal', 'apc']:
                        booleanvalue = attribute.lower() in ['true','yes']
                        obj = Literal(booleanvalue, datatype=XSD.boolean)
                        self.graph.add((subject, predicate, obj ))
                    elif column == 'languages':
                        languages = attribute.split(',')
                        for language in languages:
                            obje = Literal(language.strip())
                            self.graph.add((subject, predicate, obje))
                    else:
                        objec = Literal(attribute)
                        self.graph.add((subject, predicate, objec))
                    
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
        endpoint =self.getDbPathOrUrl()
        # endpoint = "http://127.0.0.1:9999/blazegraph/sparql"  # SPARQL endpoint URL
        if not endpoint:
            print("Error: No database URL set. Call setDbPathOrUrl() first.")
            return False
        
        store.open((endpoint, endpoint))  # Open the SPARQL store

        # instead of committing one by one (so slow), use SPARQL
        insert_query = "INSERT DATA {\n" 
        # Graph <urn:sofia:import:22-02-v2> {\n"

        for triple in self.graph.triples((None, None, None)):
            subject = triple[0]
            predicate = triple[1] 
            object_value = triple[2]
            

            text_value = str(object_value)
            
            text_value = text_value.replace('\\', '\\\\')  # \
            text_value = text_value.replace('"', '\\"')    # ""
            text_value = text_value.replace('\n', '\\n')   # \n
            text_value = text_value.replace('\r', '\\r')   # \r
            
            line = f"{subject.n3()} {predicate.n3()} {object_value.n3()} .\n"
            # line = "<" + str(subject) + "> <" + str(predicate) + "> \"" + text_value + "\" .\n"
           

            insert_query = insert_query + line

        # end the query
        insert_query = insert_query + "}"

        # store every triples once
        if len(self.graph) > 0:
            store.update(insert_query)

        # Close the store connection
        store.close()

        return True


    def get_last_journal_index(self, endpoint: str) -> int:
        from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
        store = SPARQLUpdateStore()
        try:
            store.open((endpoint, endpoint))
            query = """
                SELECT (COUNT(DISTINCT ?s) AS ?count)
                WHERE { ?s a <https://brigata.github.org/Journal> . }
            """
            results = list(store.query(query))
            return int(results[0][0]) if results else 0
        except Exception:
            return 0
        finally:
            store.close()
