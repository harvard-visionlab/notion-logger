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
    
    def list_databases(self):
        """
        List all databases accessible with the provided API token.
        """
        response = self.client.search(filter={"property": "object", "value": "database"})
        databases = [dict(title=db['title'][0]['plain_text'], id=db['id']) for db in response['results']]
        return databases

    def get_rows(self, filters=None, sorts=None, page_size=100, as_dataframe=True, order="ascending"):
        if sorts is None:
            sorts = [{ "timestamp": "created_time", "direction": order }]
        
        rows = F.get_database_rows(self.client, self.database_id, filters=filters, sorts=sorts, page_size=page_size)
        if as_dataframe:
            return F.notion_rows_to_dataframe(rows)
        return rows
    
    def get_row_by_id(self, row_id):
        """
        Retrieve a specific row by its Notion ID.
        """
        response = self.client.pages.retrieve(page_id=row_id)
        return response
    
    def find_row(self, filter_dict, plain_text=False):
        rows = F.get_filtered_rows(self.client, self.database_id, self.schema, filter_dict)
        if len(rows) == 0:
            raise ValueError(f"No row found matching filter criteria: {filter_dict}")
        if len(rows) > 1:
            raise ValueError(f"Multiple rows found matching filter criteria: {filter_dict}")
        
        if plain_text:
            return F.row_to_plain_text(rows[0], self.schema)
        
        return rows[0]
    
    def find_rows(self, filter_dict, plain_text=False):
        rows = F.get_filtered_rows(self.client, self.database_id, self.schema, filter_dict)
        if len(rows) == 0:
            raise ValueError(f"No row found matching filter criteria: {filter_dict}")
        
        if plain_text:
            return [F.row_to_plain_text(row, self.schema) for row in rows]
        
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
    
    def insert_or_update(self, row_data, unique_property=None):
        if unique_property is None:
            unique_property = self.unique_property
            
        if unique_property and unique_property not in row_data:
            raise ValueError(f"A value for '{unique_property}' must be provided to enforce the unique_property constraint.")
        
        response = None
        if unique_property:
            is_unique = F.is_property_unique(self.client, self.database_id, self.schema, unique_property, row_data[unique_property])
            if is_unique:
                response = F.insert_row(self.client, self.database_id, self.schema, row_data)                
            else:
                response = self.update_row(row_data, unique_property=unique_property)
        else:
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
    
    def delete_row(self, row_id):
        """
        Delete a row from the Notion database by its ID.
        """
        response = self.client.pages.update(
            page_id=row_id,
            archived=True
        )
        return response
        
    def list_blocks(self, page_id):
        """
        List all blocks in a page.
        """
        blocks = F.get_page_blocks(self.client, page_id)
        return blocks
    
    def append_code_block(self, page_id, toggle_text, code_text):
        """
        Append a new toggle header 3 block with a code block inside it to a page.
        """
        response = F.append_heading_with_code(self.client, page_id, toggle_text, code_text)
        return response
    
    def append_callout_block(self, page_id, callout_text, emoji='💡', text_color='default', background_color='gray_background'):
        """
        Append a new toggle header 3 block with a code block inside it to a page.
        """
        block = dict(
            block_type='callout',
            content=callout_text, 
            emoji=emoji,
            text_color=text_color,
            background_color=background_color
        )
        response = F.append_block(self.client, page_id, block)        
        return response
    
    def append_figure_block(self, page_id, toggle_text, fig, caption=None):
        raise NotImplementedError("Appending figures is not yet implemented (not officially supported by notion).")
        #image_url = F.upload_figure(self.client, page_id, fig)        
        #block = dict(block_type="image", toggle_text=toggle_text, content=image_url, caption=caption)
        #response = F.append_block(self.client, page_id, block)        
        #return response
    
    def append_image_block(self, page_id, toggle_text, url, caption=None):
        """
        Append a new toggle header 3 block with a figure inside it to a page.
        """   
        raise NotImplementedError("Appending images is not yet implemented (not officially supported by notion).")
        # block = dict(block_type="image", toggle_text=toggle_text, content=url, caption=caption)
        # response = F.append_block(self.client, page_id, block)        
        # return response        
    
    def append_block(self, page_id, block_type, content, color='default'):
        """
        Append a block to a Notion page.
        """
        response = F.append_block(self.client, page_id, block)
        return response

    def append_nested_blocks(self, page_id, toggle_block_content, toggle_block_type, *blocks):
        """
        Append a toggle block with nested blocks to a Notion page.
        """
        response = F.append_nested_blocks(self.client, page_id, toggle_block_content, toggle_block_type, *blocks)
        return response
    