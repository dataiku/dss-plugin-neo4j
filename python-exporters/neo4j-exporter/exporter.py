# This file is the actual code for the custom Python exporter neo4j-exporter

from dataiku.exporter import Exporter
from dataiku.exporter import SchemaHelper
import os

class CustomExporter(Exporter):
    """
    The methods will be called like this:
       __init__
       open
       write_row
       write_row
       write_row
       ...
       write_row
       close
    """

    def __init__(self, config, plugin_config):
        """
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.config = config
        self.plugin_config = plugin_config
        self.f = None
        self.row_index = 0

    def open(self, schema):
        """
        Start exporting. Only called for exporters with behavior MANAGES_OUTPUT
        :param schema: the column names and types of the data that will be streamed
                       in the write_row() calls
        """
        raise Exception("unimplemented")

    def open_to_file(self, schema, destination_file_path):
        """
        Start exporting. Only called for exporters with behavior OUTPUT_TO_FILE
        :param schema: the column names and types of the data that will be streamed
                       in the write_row() calls
        :param destination_file_path: the path where the exported data should be put
        """
        self.f = open(destination_file_path, 'w')
        self.f.write('_index_')
        for column in schema['columns']:
            self.f.write('\t')
            self.f.write(column['name'].encode())
        self.f.write('\n')

    def write_row(self, row):
        """
        Handle one row of data to export
        :param row: a tuple with N strings matching the schema passed to open.
        """
        self.f.write(str(self.row_index))
        for v in row:
            self.f.write('\t')
            self.f.write(('%s' % v).encode() if v is not None else '')
        self.f.write('\n')
        self.row_index += 1
        
    def close(self):
        """
        Perform any necessary cleanup
        """
        self.f.close()

