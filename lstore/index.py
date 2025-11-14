"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # self.indices will save the indices for each column of table
        # This will put an empty dictionary by # of columns 
        # If there are 3 it would be self.indices = [ {}, {} ,{}]
        self.indices = [{} for _ in range(table.num_columns)]

    """
    # Returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        return self.indices[column].get(value, [])

    '''
    # Adds a rid to the index of a column based on value
    '''
    
    def add_to_index(self, column_number, value, rid):
        # check if a key already exists in the index for that column
        # if the key already exists, append the RID to the existing list associated with that key
        if value not in self.indices[column_number]:
            self.indices[column_number][value] = []
        self.indices[column_number][value].append(rid)

    '''
    # Removes a rid of the index of a column based on value
    '''

    def remove_from_index(self, column_number, value, rid):
        self.indices[column_number][value].remove(rid)
    
    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        if column >= len(self.indices):
            print("Column is out of range.")
            return None
        
        rids = []
        index = self.indices[column]

        for rid in range(begin, end + 1):
            if rid in index:
                rids.extend(index[rid])
        return rids

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        self.indices.insert(column_number, {})

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        del self.indices[column_number]
