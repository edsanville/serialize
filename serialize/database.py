import sqlite3
from serialize import json
from typing import *
from dataclasses import *
import logging
import collections.abc

# 1. insert rows

T = TypeVar("T")
l = logging.getLogger(__file__)


def get_values(obj: any, key_path: str):
    key_path_components = key_path.split('.')
    for key_path_component in key_path_components:
        try:
            obj = getattr(obj, key_path_component)
        except AttributeError:
            return None

    if isinstance(obj, list) or isinstance(obj, set):
        return obj
    else:
        return [obj]


class Database(Generic[T]):
    datatype: Callable[..., T]
    conn: sqlite3.Connection


    def __init__(self, datatype: Callable[..., T], filename: str='data.db'):
        self.conn = sqlite3.connect(filename)
        self.datatype = datatype
        self.init_db()


    def init_db(self):
        self.conn.execute('pragma foreign_keys = on')
        self.conn.execute('create table if not exists objects (id integer primary key, json text)')

        self.conn.execute('create table if not exists indexes (table_name text, key_path text, unique(table_name), unique(key_path))')

        self.conn.commit()


    def insert(self, new_item: T):
        cur = self.conn.cursor()
        cur.execute('insert into objects (json) values (?)', (json.dumps(new_item), ))
        self.conn.commit()
        id = cur.lastrowid

        # Add index entries
        for row in self.conn.execute('select table_name, key_path from indexes'):
            table_name, key_path = row
            values = get_values(new_item, key_path)
            if values is None:
                continue

            for value in get_values(new_item, key_path):
                cur.execute(f'insert into {table_name} (id, value) values (?, ?)', (id, value))
        self.conn.commit()

    
    def fetchall(self):
        rows = self.conn.execute('select json from objects')
        return [json.loads(row[0], self.datatype) for row in rows]


    def fetchall_with_ids(self):
        rows = self.conn.execute('select id, json from objects')
        return [(row[0], json.loads(row[1], self.datatype)) for row in rows]


    def create_index(self, key_path: str):
        if self.conn.execute(f'select key_path from indexes where key_path is ?', (key_path,)).fetchone() is not None:
            l.warning(f'Index already exists for {key_path}')
            return

        key_path_components = key_path.split('.')

        # Get the type of this key path
        type_hints = get_type_hints(self.datatype)
        for index, key_path_component in enumerate(key_path_components):
            datatype = type_hints[key_path_component]
            if datatype in [int, str, float]:
                break
            
            origin_type = get_origin(datatype)
            if origin_type in [list, set]:
                datatype = get_args(datatype)[0]

            type_hints = get_type_hints(datatype)

        sql_type_strings = {
            str: 'text',
            int: 'integer',
            float: 'real'
        }

        sql_type_string = sql_type_strings[datatype]


        table_name = key_path.replace('.', '__')

        # Create the index table
        self.conn.execute(f'''
            create table if not exists {table_name} 
            (id integer, value {sql_type_string}, foreign key(id) references objects(id) on delete cascade, unique(id, value))''')
        self.conn.execute(f'create index if not exists {table_name}_index on {table_name}(value)')

        # Add entries for existing  items
        for id, item in self.fetchall_with_ids():
            for value in get_values(item, key_path):
                self.conn.execute(f'insert into {table_name} (id, value) values (?, ?)', (id, value))

        # Create the entry in the indexes table, referencing this index table
        self.conn.execute('insert into indexes (table_name, key_path) values (?, ?)', (table_name, key_path))

        self.conn.commit()


    def select(self, key_path: str, conditional: str):
        table_name = key_path.replace('.', '__')

        self.create_index(key_path)

        cmd = f'select json from objects natural join {table_name} where value {conditional}'

        for row in self.conn.execute(cmd):
            yield json.loads(row[0], self.datatype)


    def delete(self, key_path: str, conditional: str):
        table_name = key_path.replace('.', '__')

        self.create_index(key_path)

        cmd = f'delete from objects where id in (select id from {table_name} where value {conditional})'

        self.conn.execute(cmd)
        self.conn.commit()


if __name__ == '__main__':
    
    @dataclass
    class Address:
        number: int
        street: str
        town: str
        state: str

    @dataclass
    class Person:
        name: str
        age: int
        address: Address
        hobbies: list[str]


    test_db = Database(Person)

    test_db.insert(Person("Ed", 45, Address(5, "Allens Drive", "Grantham", "NH"), hobbies=['programming', 'hiking']))
    print(list(test_db.select('address.street', 'like "Allen%"')))
    test_db.delete('address.town', 'like "Gran%"')

    print(list(test_db.fetchall()))
