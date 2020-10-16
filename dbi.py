
from collections import defaultdict
import db_api
import shelve
import os
from typing import Any, Dict, List, Type


class DBField(db_api.DBField):
    name: str
    type: Type

    def __init__(self, name, type):
        self.name = name
        self.type = type


class SelectionCriteria(db_api.SelectionCriteria):
    field_name: str
    operator: str
    value: Any

    def __init__(self, field_name, operator, value):
        self.field_name = field_name
        self.operator = operator
        self.value = value


class DBTable(db_api.DBTable):
    name: str
    fields: List[DBField]
    key_field_name: str
    indexes: Dict[str, str]

    def __init__(self, name, fields, key_field_name):
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name
        self.indexes = defaultdict()

    def count(self) -> int:
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=False) as db:
            return len(db)

    def insert_record(self, values: Dict[str, Any]) -> None:
        try:
            for field in self.fields:
                if field.name not in values.keys():
                    raise ValueError("invalid fields")
        except ValueError as exp:
            print(exp.args)
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            if db.get(str(values[self.key_field_name])):
                raise ValueError
            db[str(values[self.key_field_name])] = values
        for key in values.keys():
            if self.indexes.get(key):
                with shelve.open(os.path.join(db_api.DB_ROOT, key + ".db"), writeback=True) as index:
                    if not index.get(str(values[key])):
                        index[str(values[key])] = list(values[self.key_field_name])
                    else:
                        index[str(values[key])].append(values[self.key_field_name])

    def delete_record(self, key: Any) -> None:
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            key_str = str(key)
            if not db.get(key_str):
                raise ValueError
            for field in db[key_str].keys():
                if self.indexes.get(field):
                    with shelve.open(os.path.join(db_api.DB_ROOT, field + ".db"), writeback=True) as index:
                        ids_of_value = index[str(db[key_str][field])]
                        if len(ids_of_value) > 1:
                            ids_of_value.remove(key)
                        else:
                            del ids_of_value

            del db[key_str]

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        flag = 0
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            for key in db.keys():
                for item in criteria:
                    if item.operator == '=':
                        item.operator = '=='
                    if item.field_name == self.key_field_name:
                        field_value = key
                        item_value = item.value
                    elif not db[key].get(item.field_name):
                        raise NameError
                    elif isinstance(db[key][item.field_name], str):
                        field_value = "'" + db[key][item.field_name] + "'"
                        item_value = "'" + item.value + "'"
                    else:
                        field_value = db[key][item.field_name]
                        item_value = item.value
                    if not eval(str(field_value) + item.operator + str(item_value)):
                        flag = 1
                        break
                if not flag:
                    for field in db[key].keys():
                        if self.indexes.get(field):
                            with shelve.open(os.path.join(db_api.DB_ROOT, field + ".db"), writeback=True) as index:
                                ids_of_value = index[str(db[key][field])]
                                if len(ids_of_value) > 1:
                                    ids_of_value.remove(db[key][self.key_field_name])
                                else:
                                    del ids_of_value
                    del db[key]
                flag = 0

    def get_record(self, key: Any) -> Dict[str, Any]:
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=False) as db:
            if not db.get(str(key)):
                raise ValueError
            return db[str(key)]

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            key = str(key)
            if db.get(key):
                for field in db[key].keys():
                    if field not in values.keys() or db[key][field] != values[field]:
                        if self.indexes.get(field):
                            with shelve.open(os.path.join(db_api.DB_ROOT, field + ".db"), writeback=True) as index:
                                ids_of_value = index[str(db[key][field])]
                                if len(ids_of_value) > 1:
                                    ids_of_value.remove(db[key][self.key_field_name])
                                else:
                                    del ids_of_value

                for field in values.keys():
                    if field not in db[key].keys():
                        if self.indexes.get(key):
                            with shelve.open(os.path.join(db_api.DB_ROOT, key + ".db"), writeback=True) as index:
                                if not index.get(str(values[key])):
                                    index[str(values[key])] = list(values[self.key_field_name])
                                else:
                                    index[str(values[key])].append(values[self.key_field_name])

                db[str(key)] = values

    def query_table(self, criteria: List[SelectionCriteria]) -> List[Dict[str, Any]]:
        query_list = []
        flag = 0
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=False) as db:
            for key, value in db.items():
                for item in criteria:
                    if item.operator == '=':
                        item.operator = '=='
                    if not value.get(item.field_name):
                        raise NameError
                    if item.field_name == self.key_field_name:
                        field_value = key
                        item_value = item.value
                    elif not db[key].get(item.field_name):
                        raise NameError
                    elif isinstance(value[item.field_name], str):
                        field_value = "'" + value[item.field_name] + "'"
                        item_value = "'" + item.value + "'"
                    else:
                        field_value = value[item.field_name]
                        item_value = item.value
                    if not eval(str(field_value) + item.operator + str(item_value)):
                        flag = 1
                        break
                if not flag:
                    query_list.append(value)
                flag = 0
        return query_list

    def create_index(self, field_to_index: str) -> None:
        db = shelve.open(os.path.join(db_api.DB_ROOT, field_to_index + '.db'))
        db.close()
        self.indexes[field_to_index] = field_to_index + '.db'


class DataBase(db_api.DataBase):
    tabels: Dict[str, DBTable]
    __TABLES__ = defaultdict()

    def __init__(self):
        with shelve.open(os.path.join(db_api.DB_ROOT, "DB.db"), writeback=True) as db:
            for table_name in db:
                DataBase.__TABLES__[table_name] = DBTable(table_name, db[table_name][0],
                                                          db[table_name][1])

    def create_table(self, table_name: str, fields: List[DBField], key_field_name: str) -> DBTable:
        db = shelve.open(os.path.join(db_api.DB_ROOT, table_name))
        db.close()
        if DataBase.__TABLES__.get(table_name):
            raise ValueError
        flag = 0
        for field_name in fields:
            if key_field_name == field_name.name:
                flag = 1
                break
        if not flag:
            raise ValueError
        with shelve.open(os.path.join(db_api.DB_ROOT, "DB.db"), writeback=True) as db:
            db[table_name] = [fields, key_field_name]
        table = DBTable(table_name, fields, key_field_name)
        DataBase.__TABLES__[table_name] = table
        return table

    def num_tables(self) -> int:
        return len(DataBase.__TABLES__)

    def get_table(self, table_name: str) -> DBTable:
        if not DataBase.__TABLES__.get(table_name):
            raise ValueError
        return DataBase.__TABLES__[table_name]

    def delete_table(self, table_name: str) -> None:
        db = (os.path.join('db_files', table_name + ".bak"))
        os.remove(db)
        db = (os.path.join('db_files', table_name + ".dat"))
        os.remove(db)
        db = (os.path.join('db_files', table_name + ".dir"))
        os.remove(db)
        with shelve.open(os.path.join(db_api.DB_ROOT, "DB.db"), writeback=True) as db:
            del db[table_name]
        del DataBase.__TABLES__[table_name]

    def get_tables_names(self) -> List[Any]:
        return list(DataBase.__TABLES__.keys())

    def query_multiple_tables(self, tables: List[str], fields_and_values_list: List[List[SelectionCriteria]],
                              fields_to_join_by: List[str]) -> List[Dict[str, Any]]:
        pass


