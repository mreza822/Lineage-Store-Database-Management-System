from lstore.table import Table, Record
from lstore.index import Index
from lstore.db import Database
import pandas as pd
import os
import csv


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        self.db = Database()
        self.path = ""

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        try:
            page_name = self.table.index.locate(0, primary_key)[0] // 513 + 1
            self.db.pin_page(page_name)
            if self.table.is_locked(primary_key):
                return False
            
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False
            
            for rid in rids:
                record = self.table.page_directory.get(rid)
                if record and isinstance(record, Record) and record.is_valid:
                    record.invalidate()  
                    del self.table.page_directory[rid]  # remove record

            self.db.unpin_page(page_name)
            return True  # deletion successful

        except Exception as e:
            print(f"Deletion error: {e}")
            return False
            
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        try:
            self.path = self.db.get_path()
            self.table.path = self.path
            key = columns[0]
            rid = self.table.generate_base_rid()
            record = Record(rid, key, columns)
            self.table.page_directory[rid] = record # add to page directory
            for i in range(0,len(columns)):
                self.table.index.add_to_index(i, columns[i], rid) # add to indices
            time = self.table.get_time()
            columns = columns + (rid, rid, '0000', time)
            self.table.Bpage_insert(*columns) # create base pages
            return True
        
        except Exception as e:
            # for debugging
            print(f"Insertion error: {e}")
            return False
        
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        try:        
            page_name = self.table.index.locate(0, search_key)[0] // 513 + 1
            self.db.pin_page(page_name)
            
            rids = self.table.index.locate(search_key_index, search_key)
            records = []

            # pull rid and corresponding values from page directory
            for rid in rids:
                full_record = self.table.page_directory.get(rid) 
                if full_record:
                    projected_columns = []
                    for i in range(len(projected_columns_index)):
                        if projected_columns_index[i]:
                            projected_columns.append(full_record.columns[i])
                        else:
                            projected_columns.append(None)

                    # append projected record to records
                    projected_record = Record(full_record.rid, full_record.key, projected_columns)
                    records.append(projected_record)

            self.db.unpin_page(page_name)
            return records
        except Exception as e:
            print(f"Select error: {e}")
            return False

    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        pass

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        try:
            page_name = self.table.index.locate(0, primary_key)[0] // 513 + 1
            self.db.mark_page_dirty(page_name)
            self.db.pin_page(page_name)
            
            # time, schema
            time = self.table.get_time()
            schema_encoding = ''.join(['1' if value is not None else '0' for value in columns[1:]])
            
            # create rids and BaseID for each tail page range
            baseid = self.table.index.locate(0, primary_key)[0]
            page_range = (baseid - 1) // 8192
            
            current_tail_page = len(self.table.page_ranges[page_range]['tail_pages']) - 1
            if len(self.table.page_ranges[page_range]['tail_pages'][current_tail_page]['Column_6']) == 0:
                rid = -1
            else:
                rid = -(len(self.table.page_ranges[page_range]['tail_pages'][len(self.table.page_ranges[page_range]['tail_pages']) - 1]['Column_6'])+1)

            # indirection
            indirection = self.table.get_indirection(current_tail_page, page_range, primary_key)


            # update indices and page directory
            new_columns = []
            for i in range(0, len(columns)):
                if columns[i] is not None:
                    self.table.index.remove_from_index(i, self.table.page_directory[baseid].columns[i], baseid)
                    self.table.index.add_to_index(i, columns[i], baseid)
                    new_columns.append(columns[i])
                else:
                    new_columns.append(self.table.page_directory[baseid].columns[i])
            del self.table.page_directory[baseid]
            
            record = Record(baseid, primary_key, new_columns)
            self.table.page_directory[baseid] = record # add to page directory

            # update disk
            self.path = self.db.get_path()
            file_path = os.path.join(self.path, self.table.name + '.csv')
            df = pd.read_csv(file_path)
            df.loc[baseid-1, [f'Column_{i + 1}' for i in range(self.table.num_columns)]] = new_columns
            df.to_csv(file_path, index=False)

            
            # pack columns and insert to tail pages
            columns = columns + (rid, indirection, schema_encoding, time, baseid)
            self.table.Tpage_insert(primary_key, page_range, *columns) # insert into tail pages
            self.db.unpin_page(page_name)
        except Exception as e:
            print(f"Select error: {e}")
            return False

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        try:
            
            total_sum = 0
            for key in range(start_range, end_range + 1):  # iterate over keys in the specified range
                records = self.select(key, self.table.key, [1] * self.table.num_columns)
                for record in records:
                    if record:  
                        total_sum += record.columns[aggregate_column_index]
            return total_sum
        except Exception as e:
            print(f"Sum error: {e}")
            return False

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        try:
            
            total_sum = 0
            for key in range(start_range, end_range + 1):  
                records = self.select_version(key, self.table.key, [1] * self.table.num_columns, relative_version)
                for record in records:
                    if record:  
                        total_sum += record.columns[aggregate_column_index]
            return total_sum
        except Exception as e:
            print(f"Sum error: {e}")
            return False
    
    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False


    def select_distinct(self, search_key, search_key_index, projected_columns_index):
        try:                
            
            rids = self.table.index.locate(search_key_index, search_key)
            records = []
            seen_keys = set()

            for rid in rids:
                full_record = self.table.page_directory.get(rid)
                if full_record and full_record.key not in seen_keys:
                    projected_columns = []
                    for i in range(len(projected_columns_index)):
                        if projected_columns_index[i]:
                            projected_columns.append(full_record.columns[i])
                        else:
                            projected_columns.append(None)
                
                    projected_record = Record(full_record.rid, full_record.key, projected_columns)
                    records.append(projected_record)
                    seen_keys.add(full_record.key)

            return records
        except Exception as e:
            print(f"Select DISTINCT error: {e}")
            return False

    def avg(self, start_range, end_range, aggregate_column_index):
        try:
            total_sum = 0
            count = 0
            for key in range(start_range, end_range + 1):  # Iterate over keys in the specified range
                records = self.select(key, self.table.key, [1] * self.table.num_columns)
                for record in records:
                    if record:  
                        count += 1
                        total_sum += record.columns[aggregate_column_index]
            return total_sum/count
        except Exception as e:
            print(f"Avg error: {e}")
            return False
    
    def min(self, start_range, end_range, aggregate_column_index):
        try:
            min = 0
            count = 0
            for key in range(start_range, end_range + 1):  # Iterate over keys in the specified range
                records = self.select(key, self.table.key, [1] * self.table.num_columns)
                for record in records:
                    if record and record.columns[aggregate_column_index] is not None: 
                        if (count == 0):
                            min = record.columns[aggregate_column_index]
                        count += 1
                        
                        if (min > record.columns[aggregate_column_index]): 
                            min = record.columns[aggregate_column_index]
            return min
        except Exception as e:
            print(f"Min error: {e}")
            return False
    
    def max(self, start_range, end_range, aggregate_column_index):
        try:
            max = 0
            count = 0
            for key in range(start_range, end_range + 1):  # Iterate over keys in the specified range
                records = self.select(key, self.table.key, [1] * self.table.num_columns)
                for record in records:
                    if record: 
                        if (count == 0):
                            max = record.columns[aggregate_column_index]
                        count += 1
                        
                        if (max < record.columns[aggregate_column_index]): 
                            max = record.columns[aggregate_column_index]
            return max
        except Exception as e:
            print(f"Max error: {e}")
            return False
    
    def count(self, start_range, end_range, aggregate_column_index):
        try:
            count = 0
            for key in range(start_range, end_range + 1):  # Iterate over keys in the specified range
                records = self.select(key, self.table.key, [1] * self.table.num_columns)
                for record in records:
                    if record: 
                        count += 1
            return count
        except Exception as e:
            print(f"Count error: {e}")
            return False
    def order_by(self, column_index, ascending=True):
        try:
            # Get all records from the table
            records = list(self.table.page_directory.values())

            # Sort the records based on the specified column index
            records.sort(key=lambda record: record.columns[column_index], reverse=not ascending)

            return records
        except Exception as e:
            print(f"ORDER BY error: {e}")
            return None
