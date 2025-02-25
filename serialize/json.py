#!/usr/env python3

from dataclasses import *
from typing import *
import json

normal_types = set([int, float, str, bool, type(None)])

T = TypeVar("T")

def normalize(obj: any):
    t = type(obj)

    if t in normal_types:
        return obj
    
    if t == list or t == set:
        return [normalize(item) for item in obj]

    if t == dict:
        return {key: normalize(obj[key]) for key in obj}
    
    # python objects
    return {key: normalize(getattr(obj, key)) for key in vars(obj)}



def denormalize(obj: any, Class: Callable[[], T]) -> T:
    if Class is None or Class is Any or obj is None:
        return obj

    t = type(obj)

    if Class == t:
        return obj
    
    # We can convert integers to floats without los of data
    if Class == float and t == int:
        return float(obj)

    if Class == list:
        return [item for item in obj]
    
    if Class == set:
        return set([item for item in obj])

    if Class == dict:
        return {key: obj[key] for key in obj}
    
    if get_origin(Class) == Literal:
        return obj

    if get_origin(Class) == list:
        assert(t == list)
        type_args = get_args(Class)
        
        if len(type_args) == 0:
            # No type arguments
            return [item for item in obj]

        return [denormalize(item, type_args[0]) for item in obj]

    if get_origin(Class) == set:
        assert(t == list)
        type_args = get_args(Class)
        
        if len(type_args) == 0:
            # No type arguments
            return set([item for item in obj])

        return set([denormalize(item, type_args[0]) for item in obj])

    if get_origin(Class) == dict:
        assert(t == dict)
        type_args = get_args(Class)
        
        if len(type_args) == 0:
            # No type arguments
            return {key: obj[key] for key in obj}
        
        key_type, value_type = type_args
        assert(key_type == str)
        value_type = type_args[1]
        return {key: denormalize(obj[key], value_type) for key in obj}

    # python objects
    if t != dict:
        raise Exception(f"Need a dict when denormalizing to class '{Class}', got '{t}': {obj}")
    
    kwargs = {}
    for var_name, type_hint in get_type_hints(Class).items():
        if var_name in obj:
            kwargs[var_name] = denormalize(obj[var_name], type_hint)
    return Class(**kwargs)


def dumps(obj: any):
    return json.dumps(normalize(obj))


def dump(obj: any, fp):
    json.dump(normalize(obj), fp)


def loads(s: Union[str, bytes, bytearray], Class: Callable[[], T]) -> T:
    return denormalize(json.loads(s), Class)


def load(fp, Class: Callable[[], T]) -> T:
    return denormalize(json.load(fp), Class)


class JSONFile(Generic[T]):
    filename: str
    contents: T

    def __init__(self, filename: str, Class: Callable[[], T]):
        self.filename = filename
        
        try:
            self.contents = load(open(filename, 'r'), Class)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            self.contents = Class()

    def save(self):
        dump(self.contents, open(self.filename, 'w'))


def main():
    @dataclass
    class E:
        c: bool = False
        d: int = None
        e: List[int] = field(default_factory=list)
        f: Dict[str, int] = field(default_factory=dict)

    @dataclass
    class Test:
        a: int = 5
        b: str = 'test'
        c: Dict[str, E] = field(default_factory=dict)
        d: Set[str] = field(default_factory=set)

    obj = Test(a=7, b='asdf', c={'test1': E(c=False, d=999, e=[1, 3, 8, 0], f={'a': 99})}, d=set(['test', 'test2']))

    dump([obj], open('test.json', 'w'))

    obj2 = load(open('test.json'), List[Test])

    print(obj)
    print(obj2)

if __name__ == '__main__':
    main()
