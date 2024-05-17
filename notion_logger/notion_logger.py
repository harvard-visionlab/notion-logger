import os
from notion_client import Client

from . import notion_functional as F

__all__ = ['NotionLogger']

class NotionLogger(object):
    def __init__(self, database_name, auth_token=None):
        if auth_token is None: 
            auth_token = os.environ.get("NOTION_TOKEN", None)
        assert auth_token is not None, "You must set env variable 'NOTION_TOKEN' or pass auth_token"        
        self.client = Client(auth=auth_token)
        self.database_name = database_name
        self.database_id = F.get_database_id(self.client, self.database_name)
        self.schemas = F.get_database_schemas(self.client, self.database_id)
    
    def get_rows(self, filters=None, sorts=None, page_size=100, as_dataframe=True, order="ascending"):
        if sorts is None:
            sorts = [{ "timestamp": "created_time", "direction": order }]
        
        rows = F.get_database_rows(self.client, self.database_id, filters=filters, sorts=sorts, page_size=page_size)
        if as_dataframe:
            return F.notion_rows_to_dataframe(rows)
        return rows
    