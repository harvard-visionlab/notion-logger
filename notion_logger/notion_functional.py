import pandas as pd
from pdb import set_trace

def get_database_id(client, database_name):
    """
    Query the Notion API to find the database ID for the given database name.
    """
    response = client.search(query=database_name, filter={"property": "object", "value": "database"})
    for result in response.get("results", []):
        if result.get("title", [{}])[0].get("text", {}).get("content") == database_name:
            return result.get("id")
    raise ValueError(f"Database with name '{database_name}' not found")

def get_database_schema(client, database_id):
    """
    Get information about each table within the database (their names, ids, field properties, etc.).
    """
    database_info = client.databases.retrieve(database_id=database_id)
    schema = {}
    for prop_name, prop_info in database_info["properties"].items():
        schema[prop_name] = {
            "id": prop_info["id"],
            "type": prop_info["type"],
            "details": prop_info
        }
    return schema

def get_database_rows(client, database_id, filters=None, sorts=None, page_size=100):
    """
    Retrieve all rows from a Notion database with optional filtering and sorting.
    """
    all_rows = []
    payload = {
        "database_id": database_id,
        "page_size": page_size
    }
    
    if filters:
        payload['filter'] = filters
    
    if sorts:
        payload['sorts'] = sorts
    
    while True:
        try:
            response = client.databases.query(**payload)
        except Exception as e:
            raise RuntimeError(f"Failed to query database: {e}")

        all_rows.extend(response['results'])

        if response.get('next_cursor'):
            payload['start_cursor'] = response['next_cursor']
        else:
            break

    return all_rows

def notion_rows_to_dataframe(rows):
    """
    Convert Notion database rows to a pandas DataFrame.
    """
    data = []

    for row in rows:
        row_data = {}
        properties = row['properties']

        for key, value in properties.items():
            if value['type'] == 'title':
                row_data[key] = value['title'][0]['text']['content'] if value['title'] else ""
            elif value['type'] == 'rich_text':
                row_data[key] = value['rich_text'][0]['text']['content'] if value['rich_text'] else ""
            elif value['type'] == 'number':
                row_data[key] = value['number']
            elif value['type'] == 'select':
                row_data[key] = value['select']['name'] if value['select'] else None
            elif value['type'] == 'multi_select':
                row_data[key] = [option['name'] for option in value['multi_select']]
            elif value['type'] == 'date':
                row_data[key] = value['date']['start'] if value['date'] else None
            elif value['type'] == 'checkbox':
                row_data[key] = value['checkbox']
            elif value['type'] == 'url':
                row_data[key] = value['url']
            elif value['type'] == 'email':
                row_data[key] = value['email']
            elif value['type'] == 'phone_number':
                row_data[key] = value['phone_number']
            elif value['type'] == 'created_time':
                row_data[key] = value['created_time']
            elif value['type'] == 'last_edited_time':
                row_data[key] = value['last_edited_time']
            # Add more property types as needed

        data.append(row_data)

    df = pd.DataFrame(data)
    return df

def format_properties(schema, row_data):
    formatted_properties = {}

    for key, value in row_data.items():
        if key not in schema:
            raise ValueError(f"Property '{key}' does not exist in the database schema.")

        prop_schema = schema[key]
        prop_type = prop_schema['type']

        if prop_type == 'title':
            formatted_properties[key] = {
                "title": [
                    {
                        "text": {
                            "content": value
                        }
                    }
                ]
            }
        elif prop_type == 'rich_text':
            formatted_properties[key] = {
                "rich_text": [
                    {
                        "text": {
                            "content": value
                        }
                    }
                ]
            }
        elif prop_type == 'number':
            formatted_properties[key] = {"number": value}
        elif prop_type == 'select':
            formatted_properties[key] = {"select": {"name": value}}
        elif prop_type == 'multi_select':
            formatted_properties[key] = {"multi_select": [{"name": v} for v in value]}
        elif prop_type == 'date':
            formatted_properties[key] = {"date": {"start": value}}
        elif prop_type == 'checkbox':
            formatted_properties[key] = {"checkbox": value}
        elif prop_type == 'url':
            formatted_properties[key] = {"url": value}
        elif prop_type == 'email':
            formatted_properties[key] = {"email": value}
        elif prop_type == 'phone_number':
            formatted_properties[key] = {"phone_number": value}
        elif prop_type == 'created_time' or prop_type == 'last_edited_time':
            # These are automatically managed by Notion, so no need to set them
            continue
        else:
            raise ValueError(f"Unsupported property type '{prop_type}' for property '{key}'.")

    return formatted_properties

def insert_row(client, database_id, schema, row_data):
    """
    Insert a new row into the Notion database.
    """
    formatted_properties = format_properties(schema, row_data)
    response = client.pages.create(
        parent={"database_id": database_id},
        properties=formatted_properties
    )
    return response

def is_property_unique(client, database_id, schema, property_name, value):
    """
    Check if a given value for a property is unique in the database.
    """
    if property_name not in schema:
        raise ValueError(f"Property '{property_name}' does not exist in the database schema.")

    prop_type = schema[property_name]['type']
    
    filters = {
        "property": property_name
    }

    if prop_type == 'title':
        filters['title'] = {"equals": value}
    elif prop_type == 'rich_text':
        filters['rich_text'] = {"equals": value}
    elif prop_type == 'number':
        filters['number'] = {"equals": value}
    elif prop_type == 'select':
        filters['select'] = {"equals": value}
    elif prop_type == 'multi_select':
        filters['multi_select'] = {"contains": value}
    elif prop_type == 'date':
        filters['date'] = {"equals": value}
    elif prop_type == 'checkbox':
        filters['checkbox'] = {"equals": value}
    elif prop_type == 'url':
        filters['url'] = {"equals": value}
    elif prop_type == 'email':
        filters['email'] = {"equals": value}
    elif prop_type == 'phone_number':
        filters['phone_number'] = {"equals": value}
    else:
        raise ValueError(f"Unsupported property type '{prop_type}' for property '{property_name}'.")

    response = client.databases.query(database_id=database_id, filter=filters)
    return len(response['results']) == 0

def find_row_by_unique_property(client, database_id, schema, property_name, value):
    """
    Find a row by a unique property in the Notion database.
    """
    if property_name not in schema:
        raise ValueError(f"Property '{property_name}' does not exist in the database schema.")

    prop_type = schema[property_name]['type']
    
    filters = {
        "property": property_name
    }

    if prop_type == 'title':
        filters['title'] = {"equals": value}
    elif prop_type == 'rich_text':
        filters['rich_text'] = {"equals": value}
    elif prop_type == 'number':
        filters['number'] = {"equals": value}
    elif prop_type == 'select':
        filters['select'] = {"equals": value}
    elif prop_type == 'multi_select':
        filters['multi_select'] = {"contains": value}
    elif prop_type == 'date':
        filters['date'] = {"equals": value}
    elif prop_type == 'checkbox':
        filters['checkbox'] = {"equals": value}
    elif prop_type == 'url':
        filters['url'] = {"equals": value}
    elif prop_type == 'email':
        filters['email'] = {"equals": value}
    elif prop_type == 'phone_number':
        filters['phone_number'] = {"equals": value}
    else:
        raise ValueError(f"Unsupported property type '{prop_type}' for property '{property_name}'.")

    response = client.databases.query(database_id=database_id, filter=filters)
    if len(response['results']) == 0:
        raise ValueError(f"No row found with {property_name} = {value}")
    elif len(response['results']) > 1:
        raise ValueError(f"Multiple rows found with {property_name} = {value}")

    return response['results'][0]

def update_row(client, row_id, schema, row_data):
    """
    Update a row in the Notion database.
    """
    formatted_properties = format_properties(schema, row_data)
    response = client.pages.update(
        page_id=row_id,
        properties=formatted_properties
    )
    return response

def build_filter(schema, filter_dict):
    """
    Build a Notion filter from a dictionary of property names and values.
    """
    filters = []
    
    for key, value in filter_dict.items():
        if key not in schema:
            raise ValueError(f"Property '{key}' does not exist in the database schema.")

        prop_type = schema[key]['type']
        filter_condition = {"property": key}
        
        if prop_type == 'title':
            filter_condition['title'] = {"equals": value}
        elif prop_type == 'rich_text':
            filter_condition['rich_text'] = {"equals": value}
        elif prop_type == 'number':
            filter_condition['number'] = {"equals": value}
        elif prop_type == 'select':
            filter_condition['select'] = {"equals": value}
        elif prop_type == 'multi_select':
            filter_condition['multi_select'] = {"contains": value}
        elif prop_type == 'date':
            filter_condition['date'] = {"equals": value}
        elif prop_type == 'checkbox':
            filter_condition['checkbox'] = {"equals": value}
        elif prop_type == 'url':
            filter_condition['url'] = {"equals": value}
        elif prop_type == 'email':
            filter_condition['email'] = {"equals": value}
        elif prop_type == 'phone_number':
            filter_condition['phone_number'] = {"equals": value}
        else:
            raise ValueError(f"Unsupported property type '{prop_type}' for property '{key}'.")

        filters.append(filter_condition)
    
    return {"and": filters}

def get_filtered_rows(client, database_id, schema, filter_dict):
    """
    Get rows from the Notion database based on a filter dictionary.
    """
    notion_filter = build_filter(schema, filter_dict)
    response = client.databases.query(database_id=database_id, filter=notion_filter)
    return response['results']

def row_to_plain_text(row, schema):
    """
    Convert a Notion row to a plain text dictionary.
    """
    properties = row['properties']
    plain_text_row = {"id": row['id']}
    
    for key, value in properties.items():
        if key not in schema:
            continue

        prop_type = schema[key]['type']

        if prop_type in ['title', 'rich_text']:
            plain_text_row[key] = value[prop_type][0]['plain_text'] if value[prop_type] else ""
        elif prop_type == 'number':
            plain_text_row[key] = value['number']
        elif prop_type == 'select':
            plain_text_row[key] = value['select']['name'] if value['select'] else None
        elif prop_type == 'multi_select':
            plain_text_row[key] = [option['name'] for option in value['multi_select']]
        elif prop_type == 'date':
            plain_text_row[key] = value['date']['start'] if value['date'] else None
        elif prop_type == 'checkbox':
            plain_text_row[key] = value['checkbox']
        elif prop_type == 'url':
            plain_text_row[key] = value['url']
        elif prop_type == 'email':
            plain_text_row[key] = value['email']
        elif prop_type == 'phone_number':
            plain_text_row[key] = value['phone_number']
        elif prop_type == 'created_time':
            plain_text_row[key] = value['created_time']
        elif prop_type == 'last_edited_time':
            plain_text_row[key] = value['last_edited_time']
        else:
            plain_text_row[key] = None  # Unsupported property types can be handled as needed

    return plain_text_row