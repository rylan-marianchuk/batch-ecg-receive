import sqlite3
import numpy as np

class SqliteDBWrap:

    def __init__(self, db_name):
        """

        :param db_name:
        """
        sqlite3.register_adapter(np.float32, float)
        sqlite3.register_adapter(np.int32, int)
        self.db_name = db_name
        self.conx = sqlite3.connect(db_name)
        self.table_entry_headers = {}   # Includes col_name and dtype
        self.table_column_mappings = {}


    def exit(self):
        self.conx.close()


    def create_table(self, tb_name, column_mapping):
        """

        :param tb_name:
        :param column_mapping: (dict) key: (str) column header -> val: (str) sqlite type
        :return:
        """
        header = "("
        for col_name in column_mapping.keys():
            dtype = column_mapping[col_name]
            header += col_name + " " + dtype + ", "
        header = header[:-2] + ")"
        self.table_entry_headers[tb_name] = header
        self.table_column_mappings[tb_name] = column_mapping
        self.conx.execute("CREATE TABLE IF NOT EXISTS " + tb_name + header + ";")
        self.conx.commit()



    def batch_insert(self, tb_name, iterable):
        """
        Complete a batch insert given a table and iterable object to write from

        :param tb_name: (str) table name, must exist in DB
        :param iterable: (iter) where .__next__() returns the next row to insert
        :return: None
        """
        col_headers = self.table_column_mappings[tb_name].keys()
        n_cols = len(col_headers)
        self.conx.executemany("INSERT OR IGNORE INTO " + tb_name + "(" + ", ".join(col_headers) + ") VALUES (" + " ,".join(['?'] * n_cols) + ")", iterable)
        self.conx.commit()
