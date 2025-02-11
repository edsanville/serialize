#!/usr/env python3

from dataclasses import *
from typing import *
import sqlite3

type_map = {
    int: 'integer',
    str: 'text',
    float: 'real',
    bool: 'integer'
}

def create_table(db: sqlite3.Connection, table_name: str, Class, key_columns=[]):
    table_id = f'{table_name}$id'
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

    db.execute(f'create table {table_name} ({columns_string})')


def create_tables(db: sqlite3.Connection, Class):
    for table_name, type_hint in get_type_hints(Class).items():
        assert(get_origin(type_hint) == list)
        create_table(db, table_name, get_args(type_hint)[0])


def main():
    db = sqlite3.connect('test.sqlite')

    @dataclass
    class AnotherClass:
        baz: int
        foo: List[int]
        bar: Dict[str, float]

    @dataclass
    class Class:
        c: List[AnotherClass]

    create_tables(db, Class)


if __name__ == '__main__':
    main()
