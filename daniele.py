from baseHandler import UploadHandler
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, XSD
import pandas as pd

#I created an image of the relational database and I uploaded on GitHub: yangish_database.png

class JournalUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path):
        base_url = Namespace("https://brigata.github.org/")
        self.graph = Graph()
        self.graph.bind("base_url", base_url)

        journal = pd.read_csv(
            path,
            sep=',',
            encoding='utf-8',
            keep_default_na=False,
            names=['title', 'issn', 'eissn', 'languages', 'publisher', 'seal', 'license', 'apc'],
            header=0,
            dtype=str
        )

        id_cols = ['issn', 'eissn']
        attribute_cols = ['title', 'languages', 'publisher', 'seal', 'license', 'apc']

        for idx, row in journal.iterrows():
            local_id = f"journal-{idx}"
            subject = URIRef(base_url[local_id])
            self.graph.add((subject, RDF.type, URIRef(base_url["Journal"])))

            for column in attribute_cols:
                attribute = (row.get(column) or "")
                if attribute is None:
                    attribute = ""
                attribute = str(attribute).strip()
                if not attribute:
                    continue

                predicate = URIRef(base_url[column])

                if column in ['seal', 'apc']:
                    booleanvalue = attribute.lower() in ['true', 'yes', '1', 'y', 't']
                    obj = Literal(booleanvalue, datatype=XSD.boolean)
                    self.graph.add((subject, predicate, obj))
                elif column == 'languages':
                    languages = [lang.strip() for lang in attribute.split(',') if lang.strip()]
                    for language in languages:
                        obje = Literal(language)
                        self.graph.add((subject, predicate, obje))
                else:
                    objec = Literal(attribute)
                    self.graph.add((subject, predicate, objec))

            for column in id_cols:
                id_value = (row.get(column) or "").strip()
                if id_value:
                    predicate = URIRef(base_url["id"])
                    self.graph.add((subject, predicate, Literal(id_value)))

            if 'categories' in journal.columns:
                cats = (row.get('categories') or "").strip()
                if cats:
                    for cat in [c.strip() for c in cats.split(',') if c.strip()]:
                        self.graph.add((subject, URIRef(base_url["hasCategory"]), Literal(cat)))

            if 'areas' in journal.columns:
                ars = (row.get('areas') or "").strip()
                if ars:
                    for ar in [a.strip() for a in ars.split(',') if a.strip()]:
                        self.graph.add((subject, URIRef(base_url["hasArea"]), Literal(ar)))

        from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

        store = SPARQLUpdateStore()
        endpoint = self.getDbPathOrUrl()
        if not endpoint:
            print("Error: No database URL set. Call setDbPathOrUrl() first.")
            return False

        store.open((endpoint, endpoint))

        insert_query = "INSERT DATA {\n"
        for triple in self.graph.triples((None, None, None)):
            s = triple[0].n3()
            p = triple[1].n3()
            o = triple[2].n3()
            insert_query += f"{s} {p} {o} .\n"
        insert_query += "}"

        if len(self.graph) > 0:
            store.update(insert_query)

        store.close()
        return True
