import sqlite3
from serialize import json
from typing import *
from dataclasses import *

# 1. insert rows

T = TypeVar("T")

class Database(Generic[T]):
    datetype: Callable[..., T]
    conn: sqlite3.Connection


    def __init__(self, datatype: Callable[..., T], filename: str='data.db'):
        self.conn = sqlite3.connect(filename)
        self.datatype = datatype
        self.init_db()


    def init_db(self):
        self.conn.execute('create table if not exists objects (id integer primary key, json text)')
        self.conn.commit()


    def insert(self, new_item: T):
        self.conn.execute('insert into objects (json) values (?)', (json.dumps(new_item), ))
        self.conn.commit()

    
    def fetchall(self):
        rows = self.conn.execute('select json from objects')
        return [json.loads(row[0], self.datatype) for row in rows]
    

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

    print(test_db.fetchall())
