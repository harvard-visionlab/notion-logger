import pandas as pd

def get_database_id(client, database_name):
    """
    Query the Notion API to find the database ID for the given database name.
    """
    response = client.search(query=database_name, filter={"property": "object", "value": "database"})
    for result in response.get("results", []):
        if result.get("title", [{}])[0].get("text", {}).get("content") == database_name:
            return result.get("id")
    raise ValueError(f"Database with name '{database_name}' not found")

def get_database_schemas(client, database_id):
    """
    Get information about each table within the database (their names, ids, field properties, etc.).
    """
    database_info = client.databases.retrieve(database_id=database_id)
    schemas = {}
    for prop_name, prop_info in database_info["properties"].items():
        schemas[prop_name] = {
            "id": prop_info["id"],
            "type": prop_info["type"],
            "details": prop_info
        }
    return schemas

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