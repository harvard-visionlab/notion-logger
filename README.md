# Notion Logger

A Notion logger for model training records, implemented as a wrapper around [notion_client](https://github.com/ramnes/notion-sdk-py)

## Installation

```bash
pip install git+https://github.com/harvard-visionlab/notion-logger.git
```

## Basic Usage

The main NotionLogger class supports several basic CRUD type operations.

**init logger:**
```
from notion_logger import NotionLogger

# setup logger for the TrainLog database
notion_logger = NotionLogger('TrainLog', unique_property="uuid")
notion_logger.schema
```

**Get all rows as pandas dataframe:**
```
df = notion_logger.get_rows(as_dataframe=True)
df
```

**Insert a new row:**
```
new_row = {
    "Name": "testing5678",
    "epoch": 0,
    "arch": "resnet18",
    "loss": 6.1234,
    "Tags": ["tag2", "tag4"],
    "uuid": "20240502_1215"
}
try:
    response = notion_logger.insert(new_row)
except Exception as e:
    print(e)
```

**Update a row:**
```

# add hash_id value to the row with unique_property uuid = '20240502_1215'
update_row = {
    "hash_id": "abcd2346",
    "uuid": "20240502_1215"
}
try:
    response = notion_logger.update_row(update_row, unique_property="uuid")
except Exception as e:
    print(e)
```    

**Find all rows matching filter:**
```
filter_dict = {
    "arch": "resnet18"
}
rows = notion_logger.find_rows(filter_dict, plain_text=True)
rows
```

**Find one row:**
```
filter_dict = {
    "uuid": "20240302_1121"
}
try:
    row = notion_logger.find_row(filter_dict, plain_text=True)
    print(row)
except Exception as e:
    print(e)
```    
