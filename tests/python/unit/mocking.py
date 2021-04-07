from dku_neo4j import Neo4jHandle
from commons import ImportFileHandler


class MockNeo4jHandle(Neo4jHandle):
    def __init__(self):
        self.queries = []
        pass

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        pass

    def run(self, query, data=None, log_results=False):
        self.queries.append(query)


class MockImportFileHandler(ImportFileHandler):
    def __init__(self):
        pass

    def write(self, df, path):
        return path

    def delete(self, path):
        pass
