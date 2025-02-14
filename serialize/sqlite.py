#!/usr/env python3

from dataclasses import *
from typing import *
import sqlite3
import os
import copy


type_map = {
    int: 'integer',
    str: 'text',
    float: 'real',
    bool: 'integer'
}


def create_table(cursor: sqlite3.Cursor, table_name: str, column_names: List[str]):
    columns_string = ', '.join(column_names)
    cursor.execute(f'create table if not exists {table_name} ({columns_string})')


def create_type_table(cursor: sqlite3.Cursor, table_name: str, Type, parent_key_columns=[]):
    table_id = f'{table_name}_id'
    key_column = table_id + ' integer primary key'

    column_names: List[str] = [key_column] + parent_key_columns

    # a) simple datatypes (in type_map)
    if Type in type_map:
        SQLType = type_map[Type]
        column_names.append(f'value {SQLType}')
        create_table(cursor, table_name, column_names)
        return

    # b) Lists
    OriginType = get_origin(Type)
    if OriginType == list:
        # Table for the list
        ItemType = get_args(Type)[0]
        index_number = len(parent_key_columns)
        item_parent_key_columns = parent_key_columns + ['index' + str(index_number)]
        create_type_table(cursor, table_name, ItemType, item_parent_key_columns)
        return
    
    # c) Dicts
    if OriginType == dict:
        # Table for the dict items
        ValueType = get_args(Type)[1]
        index_number = len(parent_key_columns)
        item_parent_key_columns = parent_key_columns + ['key' + str(index_number)]
        create_type_table(cursor, table_name, ValueType, item_parent_key_columns)
        return

    # d) python objects
    if OriginType == None:
        for var_name, type_hint in get_type_hints(Type).items():
            if type_hint in type_map:
                column_names.append(var_name + ' ' + type_map[type_hint])
            else:
                child_table_name = table_name + '$' + var_name
                create_type_table(cursor, child_table_name, type_hint, parent_key_columns=[table_id + ' integer'])

        create_table(cursor, table_name, column_names)
        return

    raise Exception(f'Error!  Unknown OriginType = {OriginType}')


def insert_into_table(cursor: sqlite3.Cursor, table_name: str, values: Dict[str, any]):
    column_names_string = ', '.join(values.keys())
    value_placeholders_string = ', '.join(['?'] * len(values))
    values_list = values.values()

    cursor.execute(f'insert into {table_name} ({column_names_string}) values ({value_placeholders_string})', list(values_list))


def get_next_index(cursor: sqlite3.Cursor, table_name: str, parent_keys: Dict[str, any], index_key: str) -> int:
    conditional_string = ' and '.join([f'{key} is ?' for key in parent_keys.keys()])
    row = cursor.execute(f'select max({index_key}) from {table_name} where {conditional_string}', list(parent_keys.values())).fetchone()
    if row[0] is None:
        return 0
    else:
        return row[0] + 1


def insert(cursor: sqlite3.Cursor, table_name: str, Type, obj: any, parent_keys: Dict[str, any]={}):
    table_id = f'{table_name}_id'
    values: Dict[str, any] = dict(parent_keys)

    # a) simple datatypes (in type_map)
    if Type in type_map:
        values['value'] = obj
        insert_into_table(cursor, table_name, values)
        return

    # b) Lists
    OriginType = get_origin(Type)
    if OriginType == list:
        # Table for the list (no data columns)
        ItemType = get_args(Type)[0]
        index_number = len(parent_keys)
        index_key = 'index' + str(index_number)
        index_value = get_next_index(cursor, table_name, parent_keys, index_key)
        for item in obj:
            item_parent_key_values = copy.deepcopy(parent_keys)
            item_parent_key_values[index_key] = index_value
            insert(cursor, table_name, ItemType, item, item_parent_key_values)
            index_value += 1
        return
    
    # c) Dicts
    if OriginType == dict:
        # Table for the dict items
        ValueType = get_args(Type)[1]
        index_number = len(parent_keys)
        index_key = 'key' + str(index_number)
        for key, value in obj.items():
            item_parent_key_values = copy.deepcopy(parent_keys)
            item_parent_key_values[index_key] = key
            insert(cursor, table_name, ValueType, value, item_parent_key_values)
        return

    # d) python objects
    if OriginType is None:
        for var_name, type_hint in get_type_hints(Type).items():
            if type_hint in type_map:
                values[var_name] = getattr(obj, var_name)

        insert_into_table(cursor, table_name, values)
        lastrowid = cursor.lastrowid

        for var_name, type_hint in get_type_hints(Type).items():
            if type_hint not in type_map:
                child_table_name = table_name + '$' + var_name
                insert(cursor, child_table_name, type_hint, getattr(obj, var_name), {table_id: lastrowid})
        return

    raise Exception(f'Error!  Unknown OriginType = {OriginType}')
    

def main():
    db_file = 'test.sqlite'

    if os.path.exists(db_file):
        os.remove(db_file)

    db = sqlite3.connect(db_file)
    cursor = db.cursor()

    @dataclass
    class AnotherClass:
        baz: int
        bar: int

    @dataclass
    class Class:
        a: int = 37
        b: float = 3.14159
        c: Dict[str, List[AnotherClass]] = field(default_factory=dict)

    obj = Class(a=934, b=3.4, c={
        'test':[AnotherClass(baz=2, bar=3), AnotherClass(baz=1, bar=42)],
        'sanville':[AnotherClass(baz=23, bar=98)]
    })
    print(obj)

    create_type_table(cursor, 'objects', Class)
    insert(cursor, 'objects', Class, obj)

    db.commit()

if __name__ == '__main__':
    main()
