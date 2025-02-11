#!/usr/env python3

from dataclasses import *
from typing import *
import sqlite3
import os


type_map = {
    int: 'integer',
    str: 'text',
    float: 'real',
    bool: 'integer'
}


def normalize(value: any):
    if type(value) == str:
        return '"' + value + '"'
    if type(value) == bool:
        return str(int(value))
    else:
        return str(value)


def create_table(db: sqlite3.Connection, table_name: str, Class, key_columns=[]):
    table_id = f'{table_name}_id'
    key_column = table_id + ' integer primary key'

    columns = [key_column] + key_columns
    for var_name, type_hint in get_type_hints(Class).items():

        if type_hint in type_map:
            columns.append(var_name + ' ' + type_map[type_hint])
            continue

        origin = get_origin(type_hint)
        args = get_args(type_hint)

        subtable_name = table_name + '$' + var_name

        if origin == list:
            assert(len(args) == 1)

            create_table(db, subtable_name, args[0], key_columns=[table_id + ' integer'])
    
        elif origin == dict:
            assert(len(args) == 2)
            assert(args[0] == str)

            create_table(db, subtable_name, args[1], key_columns=[table_id + ' integer', 'key text'])

        else:
            # python class
            create_table(db, subtable_name, type_hint, key_columns=[table_id + ' integer'])
        
    
    if Class in type_map:
        columns.append(f'value {type_map[Class]}')


    columns_string = ', '.join(columns)

    db.execute(f'create table if not exists {table_name} ({columns_string})')


def insert(db: sqlite3.Connection, table_name: str, obj, other_columns={}):
    # obj is python class, List[Class], or Dict[str, Class]
    cursor = db.cursor()
    Class = type(obj)

    columns: dict = other_columns

    if Class in type_map:
        columns['value'] = obj

        column_names = ', '.join(columns.keys())
        value_placeholders = ', '.join(['?'] * len(columns))
        cursor.execute(f'insert into {table_name} ({column_names}) values ({value_placeholders})', list(columns.values()))
    else:
        type_hints = get_type_hints(Class)

        for var_name, type_hint in type_hints.items():
            value = getattr(obj, var_name)

            if type_hint in type_map:
                columns[var_name] = value
                continue
        
        print(obj)

        if len(columns) > 0:
            column_names = ', '.join(columns.keys())
            value_placeholders = ', '.join(['?'] * len(columns))
            cursor.execute(f'insert into {table_name} ({column_names}) values ({value_placeholders})', list(columns.values()))
        else:
            cursor.execute(f'insert into {table_name} default values')

        id = cursor.lastrowid

        for var_name, type_hint in type_hints.items():
            value = getattr(obj, var_name)
            origin = get_origin(type_hint)
            args = get_args(type_hint)

            if origin == list:
                assert(len(args) == 1)
                for item in value:
                    insert(db, table_name + '$' + var_name, item, other_columns={table_name + '_id': id})

            if origin == dict:
                assert(len(args) == 2)
                for key, item in value.items():
                    insert(db, table_name + '$' + var_name, item, other_columns={table_name + '_id': id, 'key': key})
    



def main():
    db_file = 'test.sqlite'

    if os.path.exists(db_file):
        os.remove(db_file)

    db = sqlite3.connect(db_file)

    @dataclass
    class AnotherClass:
        baz: int
        foo: List[int]
        bar: Dict[str, float]

    @dataclass
    class Class:
        a: List[int]

    obj = Class(a=[1, 2, 3, 4])

    create_table(db, 'objects', Class)
    insert(db, 'objects', obj)

    db.commit()

if __name__ == '__main__':
    main()
