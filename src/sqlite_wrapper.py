import sqlite3
import numpy as np
import io
import zlib

# Numpy array handling taken from http://stackoverflow.com/a/31312102/
def adapt_array(arr):
    out = io.BytesIO()
    np.save(out, arr)
    out.seek(0)
    return sqlite3.Binary(zlib.compress(out.read()))

def convert_array(text):
    out = io.BytesIO(text)
    out.seek(0)
    out = io.BytesIO(zlib.decompress(out.read()))
    return np.load(out)


class ColumnIterator:

    def __init__(self, column_containers=None):
        """
        Create an iterable object for SQLbatch insertion, instead of data given as rows, give data as columns containers
        :param column_containers: (ordered tuple) PRIMARY KEY container, next column container, ... last column container
        """
        self.i = 0
        self.container_size = 0
        if column_containers is not None:
            self.refill_containers(column_containers)


    def refill_containers(self, column_containers):
        """
        Reset the columns for a new iteration pass through

        :param column_containers: (tuple of container objects) first element is first column (Primary Key), second
                                  element holds data for the second column
        Must assume all columns have at least the length of first element (primary key container), excess elements ignored
        """
        self.column_containers = column_containers
        self.i = 0
        self.container_size = len(column_containers[0])
        self.columns = len(column_containers)


    def __iter__(self):
        return self


    def __next__(self):
        if self.i >= self.container_size:
            raise StopIteration
        L = list(self.column_containers[x][self.i] for x in range(self.columns))
        self.i += 1
        return L



class SqliteDBWrap:

    def __init__(self, db_name):
        """

        :param db_name:
        """
        sqlite3.register_adapter(np.float32, float)
        sqlite3.register_adapter(np.int32, int)
        # Converts np.array to TEXT when inserting
        sqlite3.register_adapter(np.ndarray, adapt_array)
        # Converts TEXT to np.array when selecting
        sqlite3.register_converter("NDARRAY", convert_array)

        self.db_name = db_name
        self.conx = sqlite3.connect(db_name)
        self.table_entry_headers = {}
        self.table_column_mappings = {}

        tbl_query = self.conx.execute("SELECT name FROM sqlite_master WHERE type='table';")
        for table in sum(tbl_query.fetchall(), ()):
            header_query = self.conx.execute("PRAGMA table_info(" + table + ");").fetchall()
            column_mapping = {col[1]:col[2] for col in header_query}
            column_mapping[header_query[0][1]] += " PRIMARY KEY"
            self.populate_metadata(table, column_mapping)


    def exit(self):
        self.conx.close()


    def populate_metadata(self, tb_name, column_mapping):
        """
        Populate class variables according to the just read database

        :param tb_name:  (str) table name
        :param column_mapping: (dict) key: (str) column header -> val: (str) valid sqlite type
        :return: None
        """
        # Generate a string header from the column mappings for create and insert statement
        header = "("
        for col_name in column_mapping.keys():
            dtype = column_mapping[col_name]
            header += col_name + " " + dtype + ", "
        header = header[:-2] + ")"
        self.table_entry_headers[tb_name] = header
        self.table_column_mappings[tb_name] = column_mapping


    def create_table(self, tb_name, column_mapping):
        """
        Establish a table, do nothing if table already exists

        :param tb_name: (str) table name
        :param column_mapping: (dict) key: (str) column header -> val: (str) valid sqlite type
        :return: None
        """
        if tb_name in self.table_column_mappings: return
        self.populate_metadata(tb_name, column_mapping)
        self.conx.execute("CREATE TABLE IF NOT EXISTS " + tb_name + self.table_entry_headers[tb_name] + ";")
        self.conx.commit()



    def batch_insert(self, iterable, tb_name=None):
        """
        Complete a batch insert given a table and iterable object to write from

        :param tb_name: (str) table name, must exist in DB
        :param iterable: (iter) where .__next__() returns the next row to insert
        :return: None
        """
        if tb_name is None:
            # Default to first table inserted
            tb_name = list(self.table_column_mappings.keys())[0]

        col_headers = self.table_column_mappings[tb_name].keys()
        n_cols = len(col_headers)
        self.conx.executemany("INSERT OR IGNORE INTO " + tb_name + "(" + ", ".join(col_headers) + ") VALUES (" + " ,".join(['?'] * n_cols) + ")", iterable)
        self.conx.commit()


    def entries_in_tbl(self, tb_name=None):
        """
        Obtain the number of rows (entries) in a table

        :param tb_name: (str) or (None)
        :return: (int) rows
        """
        if tb_name is None:
            # Default to first table inserted
            tb_name = list(self.table_column_mappings.keys())[0]

        res = self.conx.execute("SELECT COUNT(*) FROM " + tb_name)
        return res.fetchall()[0][0]


    def batch_primarykey_query(self, primary_keys, specify_cols=None, tb_name=None):
        """
        Perform a batch query when given a list of primary keys

        :param primary_keys: (list) the primary keys to query
        :param specify_cols: (tuple or str)
        :param tb_name: (str) table name, if None defaults to first inserted table
        :return: (list of tuples) result     if specify_cols is None, result[i] is an entry row as a tuple
        """
        if tb_name is None:
            # Default to first table inserted
            tb_name = list(self.table_column_mappings.keys())[0]

        # Primary key name
        pk_name = self.table_column_mappings[tb_name].keys()[0]

        q_size = len(primary_keys)
        sql = "SELECT"
        if specify_cols is None:
            sql += " * "
        else:
            sql += ", ".join(specify_cols)

        sql += "FROM " + tb_name + " WHERE " + pk_name + " IN ({0})".format(", ".join(["?"] * q_size))
        return self.conx.execute(sql, primary_keys).fetchall()



