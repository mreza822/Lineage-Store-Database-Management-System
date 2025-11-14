from lstore.table import Table, Record
import pandas as pd
import csv
import os
import logging
import random
import struct
import pickle

pathway = ""
class Database():

    def __init__(self, bufferpool_size = 10):
        self.tables = {}
        self.dirty_pages = set() # Set of page names that have been modified
        self.pinned_pages = set() # Dictionary of page names to pin count
        '''
        self.pinned_values = {}
        '''
        self.bufferpool = []   # Fixed-size list of pages in memory for simplicity
        self.bufferpool_size = bufferpool_size
        self.path = ""
        self.num_columns = 0
        self.page_directory_file_path = ""
        self.index_file_path = ""
        self.pages_file_path = ""
        self.table_info_path = ""
        self.name = ""

    def get_path(self):
        return pathway

    # Not required for milestone1
    def open(self, path):
        self.path = path
        pass


    def close(self):
        try:
            open(self.page_directory_file_path, 'w').close()
            with open(self.page_directory_file_path, 'wb') as file:
                # Serialize and write page directory
                self.serialize_page_directory(file)
            
            open(self.index_file_path, 'w').close()
            with open(self.index_file_path, 'wb') as file:
                # Serialize and write index
                self.serialize_index(file)
            
            # Serialize and write pages
            self.serialize_page_ranges()

            logging.info(f"Successfully closed database and wrote changes to path: {self.path}")
        except Exception as e:
            logging.error(f"Error closing database at path: {self.path}: {e}")

    def serialize_page_directory(self, file):
        # Serialize page directory
        for rid, record in self.tables[self.name].page_directory.items():
            # Serialize record data
            serialized_record = self.serialize_record(record)
            # Write the length of the serialized record
            file.write(struct.pack('<I', len(serialized_record)))
            # Write the serialized record
            file.write(serialized_record)

    def serialize_record(self, record):
        """
        Serialize a record to bytes.
        Modify this function according to the structure of the Record class.
        """
        # Encode record attributes to bytes
        serialized_data = b''
        serialized_data += struct.pack('<I', record.rid)
        serialized_data += struct.pack('<I', record.key)
        # Assuming columns is a list of integers
        serialized_data += b''.join(struct.pack('<I', column) for column in record.columns)
        # Encode is_valid flag (assuming it's boolean)
        serialized_data += struct.pack('<?', record.is_valid)
        return serialized_data

    def serialize_index(self, file):
        # Serialize index using pickle
        serialized_index = pickle.dumps(self.tables[self.name].index)
        file.write(struct.pack('<I', len(serialized_index)))
        file.write(serialized_index)

    def serialize_page_ranges(self):
    # Serialize page ranges manually
        for page_range in self.tables[self.name].page_ranges:
        # Serialize base pages
            for base_page in page_range['base_pages']:
                for column_name, column_data in base_page.items():
                    bpage_path = os.path.join(self.path, 'Base' + 'Column_' + column_name[7:] + '.txt')
                    check = column_data[0]

                    if column_name == "Column_8" or column_name == "Column_9":
                        with open(bpage_path, 'w') as file:
                            file.write(','.join(str(item) for item in column_data))
                    else:
                        # Encode column data for base pages
                        serialized_column_data = self.encode_column_data(column_data)
                        # Write the length of the encoded data
                        with open(bpage_path, 'wb') as file:
                            file.write(struct.pack('<I', len(serialized_column_data)))
                            # Write the encoded data
                            file.write(serialized_column_data)

        # Serialize tail pages
            for tail_page in page_range['tail_pages']:
                for column_name, column_data in tail_page.items():
                    tpage_path = os.path.join(self.path, 'Tail' + 'Column_' + column_name[7:] + '.txt')
                    check = column_data[0]

                    if column_name == "Column_8" or column_name == "Column_9":
                        with open(tpage_path, 'w') as file:
                            file.write(','.join(str(item) for item in column_data))
                    else:
                        # Encode column data for tail pages
                        serialized_column_data = self.encode_column_data(column_data)
                        # Write the length of the encoded data
                        with open(tpage_path, 'wb') as file:
                            file.write(struct.pack('<I', len(serialized_column_data)))
                            # Write the encoded data
                            file.write(serialized_column_data)

    def encode_column_data(self, column_data):
        encoded_data = b''
        for item in column_data:
            if item is None:
                # Encode None as zero
                encoded_data += struct.pack('<I', 0)
            elif item < 0:
                # Handle negative integers differently
                encoded_data += struct.pack('<q', item)  # Encode as signed long long integer
            else:
                encoded_data += struct.pack('<I', item)
        
        return encoded_data

        
    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        if name in self.tables:
            raise Exception(f"Table {name} already exists")
        self.tables[name] = Table(name, num_columns, key)
        self.name = name
        self.num_columns = num_columns
        if self.path == "":
            self.path = './ECS165'
            global pathway
            pathway = self.path
        self.page_directory_file_path = os.path.join(self.path, 'page_directory.txt')
        self.index_file_path = os.path.join(self.path, 'index.txt')
        self.table_info_path = os.path.join(self.path, 'table_info.txt')

        # Write table information to table_info.txt
        with open(self.table_info_path, 'w') as file:
            table_info = [name, num_columns, key]
            file.write(','.join(map(str, table_info)))
        '''
        add to self.pinned_values {name : pin value}, initialize to 0
        self.pinned_values[name] = 0
        '''
        return self.tables[name]

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if name in self.tables:
            del self.tables[name] # delete the table from the database tables list

        raise Exception(f"Table {name} doesn't exist")

    """
    # Returns table with the passed name
    """
    
    def get_table(self, name):
        self.table_info_path = os.path.join(self.path, 'table_info.txt')
        with open(self.table_info_path, 'r') as file:
            table_info_str = file.read().strip()  # Read the content and remove leading/trailing whitespace
            name, num_columns, key = table_info_str.split(',')  # Split the string by comma to get individual components
            num_columns = int(num_columns)  # Convert num_columns and key to integers
            self.num_columns = num_columns
            key = int(key)
        
        if name in self.tables:
            raise Exception(f"Table {name} already exists")
        self.tables[name] = Table(name, num_columns, key)
        self.name = name

        self.page_directory_file_path = os.path.join(self.path, 'page_directory.txt')
        self.index_file_path = os.path.join(self.path, 'index.txt')

        try:
            with open(self.page_directory_file_path, 'rb') as file:
                # Deserialize and write page directory
                self.deserialize_page_directory(file)

            with open(self.index_file_path, 'rb') as file:
                # Deserialize and write index
                self.deserialize_index(file)

            self.deserialize_base_pages()
            self.deserialize_tail_pages()

            logging.info(f"Successfully opened database and wrote changes to path: {self.path}")
        except Exception as e:
            logging.error(f"Error opening database at path: {self.path}: {e}")

        # Return the table object
        return self.tables[self.name]
    
    def deserialize_page_directory(self, file):
        # Deserialize page directory
        while True:
            # Read the length of the serialized record
            length_bytes = file.read(4)
            if not length_bytes:
                break  # No more data to read
            length = struct.unpack('<I', length_bytes)[0]
            # Read the serialized record
            serialized_record = file.read(length)
            # Deserialize the record
            record = self.deserialize_record(serialized_record)
            # Add the record to the page directory
            self.tables[self.name].page_directory[record.rid] = record

    def deserialize_record(self, serialized_record):
        """
        Deserialize a record from bytes.
        Modify this function according to the structure of the Record class.
        """
        # Decode record attributes from bytes
        # Assuming columns is a list of integers
        rid, key, col1, col2, col3, col4, col5, is_valid = struct.unpack('<IIIIIII?', serialized_record)
        columns = (col1, col2, col3, col4, col5)
        return Record(rid, key, columns)
    
    def deserialize_index(self, file):
        # Deserialize index
        length_bytes = file.read(4)
        if length_bytes:
            length = struct.unpack('<I', length_bytes)[0]
            serialized_index = file.read(length)
            self.tables[self.name].index = pickle.loads(serialized_index)

    def deserialize_base_pages(self):
        file_paths = []
        for i in range(0, self.num_columns + 4):
            file_path = os.path.join(self.path, 'Base' + f'Column_{i + 1}' + '.txt')
            file_paths.append(file_path)
        # Deserialize pages
        all_records = []
        for file_path in file_paths:
            column_number = file_paths.index(file_path) + 1
            if column_number != self.num_columns + 3 and column_number != self.num_columns + 4:
                with open(file_path, 'rb') as file:
                    serialized_column_data = file.read()
                    column_data = self.decode_column_data(serialized_column_data)
                    all_records.append(column_data)
            else:
                with open(file_path, 'r') as file:
                    column_data = file.read().strip().split(',')
                    all_records.append(column_data)
                
        # Transpose the list of records to group them by index
        grouped_records = list(zip(*all_records))
        for i in grouped_records:
            self.tables[self.name].Bpage_insert(i)
    
    def deserialize_tail_pages(self):
        file_paths = []
        for i in range(0, self.num_columns + 4):
            file_path = os.path.join(self.path, 'Tail' + f'Column_{i + 1}' + '.txt')
            file_paths.append(file_path)
        # Deserialize pages
        all_records = []
        for file_path in file_paths:
            column_number = file_paths.index(file_path) + 1
            if column_number != self.num_columns + 3 and column_number != self.num_columns + 4:
                with open(file_path, 'rb') as file:
                    serialized_column_data = file.read()
                    column_data = self.decode_column_data(serialized_column_data)
                    all_records.append(column_data)
            else:
                with open(file_path, 'r') as file:
                    column_data = file.read().strip().split(',')
                    all_records.append(column_data)
                
        # Transpose the list of records to group them by index
        grouped_records = list(zip(*all_records))

    def decode_column_data(self, serialized_column_data):
        """
        Decode column data from bytes.
        Modify this function according to the data types and encoding scheme used.
        """
        decoded_data = []
        # Assuming all items are integers
        num_items = len(serialized_column_data) // 4  # Assuming each item is 4 bytes
        for i in range(num_items):
            item = struct.unpack('<I', serialized_column_data[i * 4:(i + 1) * 4])[0]
            decoded_data.append(item)
        return decoded_data

    def pin_page(self, table_name):
        # Mark a page as pinned to prevent it from being evicted
        if table_name in self.bufferpool:
            self.pinned_pages.add(table_name)

    def unpin_page(self, table_name):
        self.pinned_pages.discard(table_name)

    def mark_page_dirty(self, page_name):
        if page_name in self.bufferpool:
            self.dirty_pages.add(self.bufferpool[page_name])

    def flush_dirty_pages(self):
        for page_name in self.dirty_pages:
            # Write the page back to disk
            self.write_page_to_disk(page_name)
        # Clear the set of dirty pages after flushing
        self.dirty_pages.clear()
    
    def access_table(self, table_name):
        if table_name not in self.bufferpool:
            if len(self.bufferpool) >= self.bufferpool_size:
                self.evict_page()  # Evict a page if the bufferpool is full
            self.bufferpool.append(table_name)
            self.dirty_pages.add(table_name)  # Mark the page as dirty

    def evict_page(self):
    # Check if there's any page to evict
        if not self.bufferpool:
            return  # No pages to evict
    
    # Randomly select a page to evict
        evicted_page_name = random.choice(list(self.bufferpool.keys()))
        evicted_page_content = self.bufferpool.pop(evicted_page_name)

        if evicted_page_name in self.dirty_pages:
            self.write_back_to_disk(evicted_page_content)
            self.dirty_pages.remove(evicted_page_name)


    def read_from_disk(self, file_path):
        try:
        # Check if the file exists
            if not os.path.isfile(file_path):
                raise FileNotFoundError(f"File {file_path} not found")

        # Open the file for reading
            with open(file_path, mode='r') as file:
                reader = csv.reader(file, delimiter='\t')

            # Initialize variables to store page name and content
                page_name = None
                page_content = []

            # Iterate through each row in the file
                for row in reader:
                # Check if the row contains the marker for page break
                    if row == ['#PAGE_BREAK']:
                    # Add the page content to the bufferpool
                        self.bufferpool[page_name] = page_content

                    # Reset variables for the next page
                        page_name = None
                        page_content = []
                    else:
                    # If not a page break marker, add the row to page content
                        if page_name is None:
                            page_name = row[0]  # Set the page name
                        else:
                            page_content.append(row)  # Add row to page content

            # print("Data read from disk successfully.")

        except Exception as e:
            print(f"Error reading data from disk: {e}")
            
    
    def save_table_to_disk(self, table_name):
        if table_name in self.tables:
            table = self.tables[table_name]
            file_path = os.path.join(self.path, table_name + '.csv')
            table.save_to_csv(file_path)

    
    def write_page_to_disk(self, file_path, page):
        try:
        # Check if the bufferpool is empty
            self.path = os.path.join(self.path, file_path + '.csv')
            with open(self.path, 'w', newline='') as file:
                writer = csv.writer(file)
                for row in page:
                    writer.writerow(row)
                    # Add page delimiter after each page
                    writer.writerow(['#PAGE_BREAK'])

        except Exception as e:
            print(f"Error writing data to disk: {e}")
