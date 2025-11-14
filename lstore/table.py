from lstore.index import Index
from lstore.time import time
from datetime import datetime
import threading
import os
import pandas as pd


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
        self.is_valid = True

    def invalidate(self):
        self.is_valid = False

    def __repr__(self):
        return f"Record(rid={self.rid}, key={self.key}, columns={self.columns})"

    def __str__(self):
        return f"RID: {self.rid}, Key: {self.key}, Columns: {self.columns}"

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.next_base_rid = 1  # base positive
        self.next_tail_rid = -1 # tail negative
        self.lock_table = {}
        self.page_ranges = []
        
        self.path = ""
        self.Bpages = {f'Column_{i + 1}': [] for i in range(self.num_columns + 4)}
        self.Tpages = {f'Column_{i + 1}': [] for i in range(self.num_columns + 5)}
        self.max_records_per_page = 512 # max records per base page
        self.records_inserted = 0
        self.current_page_range = 0 # current page range
        self.current_base_page = 0 # current base page
        self.merge_counter = 0 # merge every n updates

        self.TPS = 0
        self.merge_thread = threading.Thread(target=self.__background_merge)
        self.merge_thread.daemon = True

    def lock_record(self, primary_key, transaction_id):
        if primary_key in self.lock_table:
            # Record is already locked
            return False
        else:
            self.lock_table[primary_key] = transaction_id
            return True
        
    def unlock_record(self, primary_key):
        if primary_key in self.lock_table:
            del self.lock_table[primary_key]

    def is_locked(self, primary_key):
        return primary_key in self.lock_table
    
    def add_record(self, columns):
        try:      
            key = columns[0]
            rid = self.generate_base_rid()
            record = Record(rid, key, columns)
            self.page_directory[rid] = record # add to page directory
            for i in range(0,len(columns)):
                self.index.add_to_index(i, columns[i], rid) # add to indices
            return True
        
        except Exception as e:
            # for debugging
            print(f"Insertion error: {e}")
            return False
        
    def generate_base_rid(self):
        current_rid = self.next_base_rid
        self.next_base_rid += 1
        return current_rid

    def generate_tail_rid(self):
        current_rid = self.next_tail_rid
        self.next_tail_rid -= 1 
        return current_rid

    def get_time(self):
        return datetime.now().strftime("%H:%M:%S")

    def Bpage_insert(self, *columns):
        if len(columns) != (self.num_columns + 4):
            return

        # Initialize first page range
        if len(self.page_ranges) == 0:
            self.page_ranges.append({'base_pages': [{f'Column_{i + 1}': [] for i in range(self.num_columns + 4)}], 'tail_pages': [{f'Column_{i + 1}': [] for i in range(self.num_columns + 5)}]})
        
        # Create page range if previous was full, and the last base page is full
        if len(self.page_ranges[self.current_page_range]['base_pages']) >= 16:
            if len(self.page_ranges[self.current_page_range]['base_pages'][self.current_base_page]['Column_1']) >= self.max_records_per_page:
                self.page_ranges.append({'base_pages': [{f'Column_{i + 1}': [] for i in range(self.num_columns + 4)}], 'tail_pages': [{f'Column_{i + 1}': [] for i in range(self.num_columns + 5)}]})
                self.current_page_range += 1
                self.current_base_page = 0

        # Create base page if previous was full
        # The first column is always the primary key, the last 4 pages are always RID, indirection, schema_encoding, and time
        if len(self.page_ranges[self.current_page_range]['base_pages'][self.current_base_page]['Column_1']) >= self.max_records_per_page:
            self.page_ranges[self.current_page_range]['base_pages'].append({f'Column_{i + 1}': [] for i in range(self.num_columns + 4)})
            self.current_base_page += 1

        try:
            # Add to disk
            file_path = os.path.join(self.path, self.name + '.csv')
            df = pd.read_csv(file_path)
            df.loc[len(df)] = list(columns[0:self.num_columns])
            df.to_csv(file_path, index=False)
        except:
            pass
        
        # Insert records into the base pages
        for i, value in enumerate(columns):
            column_name = f'Column_{i + 1}'
            self.page_ranges[self.current_page_range]['base_pages'][self.current_base_page][column_name].append(value)
        
    def Tpage_insert(self, primary_key, page_range, *columns):
        # insert records into tail pages based on page_range
        current_tail_page = len(self.page_ranges[page_range]['tail_pages']) - 1
        if len(columns) != (self.num_columns + 5):
            return

        # Create tail page if previous was full
        # The first column is always the primary key, the last 5 pages are always RID, indirection, schema_encoding, time, and BaseID
        if len(self.page_ranges[page_range]['tail_pages'][current_tail_page]['Column_1']) >= self.max_records_per_page:
            self.page_ranges[page_range]['tail_pages'].append({f'Column_{i + 1}': [] for i in range(self.num_columns + 5)})
            current_tail_page = len(self.page_ranges[page_range]['tail_pages']) - 1
      
        # insert update records into the tail pages
        self.page_ranges[page_range]['tail_pages'][current_tail_page]['Column_1'].append(primary_key)
        for i, value in enumerate(columns[1:10]):
            column_name = f'Column_{i + 2}'
            self.page_ranges[page_range]['tail_pages'][current_tail_page][column_name].append(value)
        # increment merge counter and merge at specified number
        
        self.merge_counter += 1
        if self.merge_counter == 100:
            self.__merge()
            self.merge_counter = 0
            print("Merged")

            
            
    def get_indirection(self, current_tail_page, page_range, primary_key):
        # first check if indirection is in tail pages
        # if not, return rid of the record in the base pages
        for i in range(0, len(self.page_ranges[page_range]['tail_pages'])):
            for j in reversed(self.page_ranges[page_range]['tail_pages'][i]['Column_1']):
                if j == primary_key:
                    primary_key_index = self.page_ranges[page_range]['tail_pages'][i]['Column_1'].index(primary_key)
                    return self.page_ranges[page_range]['tail_pages'][i]['Column_6'][primary_key_index]
        return self.index.locate(0, primary_key)[0]

    def insert_page_directory(self, columns, rid):
        # insert new page_directory record during merge
        key = columns[0]
        rid = rid
        record = Record(rid, key, columns)
        self.page_directory[rid] = record

    
    def __background_merge(self):
        try:
            self.__merge()
        except Exception as e:
            print(f"Merge error: {e}")
    
    def __merge(self):
        
        # Retaining the original data
        original_base_pages = [self.page_ranges[i]['base_pages'] for i in range(0, len(self.page_ranges))]
        # iterate through records from last TPS
        for k in range(self.TPS, self.TPS + 100):
            try:
                if k >= 512:
                    k = k % 512
                # get page range and page number
                page_range = self.TPS // 8192
                page = self.TPS // 512
                if page > 15: 
                    page = page % 16
                # check if the TPS actually corresponds with a record
                try:
                    base_id = self.page_ranges[page_range]['tail_pages'][page]['Column_10'][k]
                except:
                    return False
                # get base id and the page range the base id is in
                base_page = base_id // 512
                if base_id % 512 == 0:
                    base_page = base_id // 513
                base_page_range = base_page // 16
                if base_page > 15: 
                    base_page = base_page % 16
                # get the index of the base id
                rid_index = self.page_ranges[base_page_range]['base_pages'][base_page]['Column_6'].index(base_id)
                # get the schema
                schema = self.page_ranges[page_range]['tail_pages'][page]['Column_8'][k]
                # merge with copied pages
                for element in schema:
                    if element == '1':
                        column_name = 'Column_' + str(schema.index('1') + 2)
                        self.page_ranges[base_page_range]['base_pages'][base_page][column_name][rid_index] = self.page_ranges[page_range]['tail_pages'][page][column_name][k] 
                        
            except Exception as e:
                print(f"Merge error: {e}")
            
            self.TPS = self.TPS + 1
