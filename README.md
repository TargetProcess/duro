# Duro
Framework for data processing and transformation.

## How Duro works

### Concept of materialization
https://github.com/TargetProcess/materialized-views

This repository has following structure:

```
schema_name/
    table_name - INTERVAL.sql
    table_name_ddl.sql
    table_name.py
    table_name.conf
```

**Where:**
`table_name - INTERVAL.sql` file name should have following format:
**table_name** - name of your table inside scheme
**interval** - time interval for update (e.g `changes — 24h.sql` means your table `changes` will be updated every 24 hour)

`table_name_ddl.sql` - file with structure og destination table. It's common sql *create table* query

`table_name.py` can process data that you get after running `table_name - INTERVAL.sql` and before data will be inserted in destination table.

`table_name.conf` set redshift specific settings on your table

### Concept of transformation

`table_name.py` is used for process your data before pack it to csv and upload into Redshift.

Transformation process in simple. You need to create at least on method:

```
def process(data):
    #...
    # some transformation here
    #...
    return data
```

where `data` param is list of dictionaries. Each dictionary is row of your destination table. Dictionaries have structure `{'table_column_name': 'data'}`.
So your table will be represented as following list:
```
[
    {'table_column_name_1': 'some_important_data', 'table_column_name_2': 'some_other_data'}, # it's row 1
    {'table_column_name_1': 'some_important_data_2', 'table_column_name_2': 'super_value'}, # it's row 2
    ...
]
```


# How to add your materialization view 

1. Clone repository:
https://github.com/TargetProcess/materialized-views
2. Create directory (schema name) or use existed derictory.
3. Create file `destination_table - Interval.sql` inside your scheme directory (mandatory)
4. Create file `destination_table.sql` with structure of your destination table (mandatory)
5. Create file `destination_table.py` (optional)
6. Commit files to repository

