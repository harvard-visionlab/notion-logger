import os
from notion_client import Client

from . import notion_functional as F

__all__ = ['NotionLogger']

class NotionLogger(object):
    def __init__(self, database_name, auth_token=None, unique_property=None):
        if auth_token is None: 
            auth_token = os.environ.get("NOTION_TOKEN", None)
        assert auth_token is not None, "You must set env variable 'NOTION_TOKEN' or pass auth_token"        
        self.client = Client(auth=auth_token)
        self.database_name = database_name
        self.database_id = F.get_database_id(self.client, self.database_name)
        self.schema = F.get_database_schema(self.client, self.database_id)
        self.unique_property = unique_property
        
    def get_rows(self, filters=None, sorts=None, page_size=100, as_dataframe=True, order="ascending"):
        if sorts is None:
            sorts = [{ "timestamp": "created_time", "direction": order }]
        
        rows = F.get_database_rows(self.client, self.database_id, filters=filters, sorts=sorts, page_size=page_size)
        if as_dataframe:
            return F.notion_rows_to_dataframe(rows)
        return rows
    
    def insert(self, row_data, unique_property=None):
        if unique_property is None:
            unique_property = self.unique_property
            
        if unique_property and unique_property not in row_data:
            raise ValueError(f"A value for '{unique_property}' must be provided to enforce the unique_property constraint.")
            
        if unique_property and unique_property in row_data:
            is_unique = F.is_property_unique(self.client, self.database_id, self.schema, unique_property, row_data[unique_property])
            if not is_unique:
                raise ValueError(f"Value for '{unique_property}' must be unique. The provided value '{row_data[unique_property]}' already exists.")
        
        response = F.insert_row(self.client, self.database_id, self.schema, row_data)
        return response
    
    def update_row(self, row_data, unique_property=None):
        if unique_property is None:
            unique_property = self.unique_property
            
        # Find the row by the unique property
        if unique_property not in row_data:
            raise ValueError(f"Unique property '{unique_property}' must be provided in row_data.")
        
        row_value = row_data[unique_property]
        row = F.find_row_by_unique_property(self.client, self.database_id, self.schema, unique_property, row_value)
        row_id = row['id']
        
        # Update the row
        response = F.update_row(self.client, row_id, self.schema, row_data)
        return response
    