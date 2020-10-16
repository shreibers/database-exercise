from collections import defaultdict
from typing import Any, Dict, List, Type
import db_api
import os
import shelve


class DBField(db_api.DBField):
    name: str
    type: Type

    def __init__(self, name, type_):
        self.name = name
        self.type = type_


class SelectionCriteria(db_api.SelectionCriteria):
    field_name: str
    operator: str
    value: Any

    def __init__(self, field_name, operator, value):
        self.field_name = field_name
        self.operator = operator
        self.value = value


def set_compare_operator(item):
    if item.operator == '=':
        item.operator = '=='

    return item


def set_val(val):
    return "'" + val + "'"


class DBTable(db_api.DBTable):
    name: str
    fields: List[DBField]
    key_field_name: str

    def __init__(self, name, fields, key_field_name):
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name

    def count(self) -> int:
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=False) as db:
            return len(db)

    def is_meets_criterion(self, criteria: List[SelectionCriteria], key: Any, value: Any, flag: bool) -> bool:
        for item in criteria:
            item = set_compare_operator(item)

            if item.field_name == self.key_field_name:
                field_value = key
                item_value = item.value

            elif not value.get(item.field_name):
                raise NameError

            elif isinstance(value[item.field_name], str):
                field_value = set_val(value[item.field_name])
                item_value = set_val(item.value)

            else:
                field_value = value[item.field_name]
                item_value = item.value

            if not eval(str(field_value) + item.operator + str(item_value)):
                return True

        return flag

    def insert_record(self, values: Dict[str, Any]) -> None:
        try:
            for field in self.fields:
                if field.name not in values.keys():
                    raise ValueError("invalid field")

        except ValueError as exp:
            print(exp.args)

        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            if db.get(str(values[self.key_field_name])):
                raise ValueError

            db[str(values[self.key_field_name])] = values

    def delete_record(self, key: Any) -> None:
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            if not db.get(str(key)):
                raise ValueError

            del db[str(key)]

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        flag = False
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            for key, value in db.items():

                flag = self.is_meets_criterion(criteria, key, value, flag)
                if not flag:
                    del db[key]

                flag = False

    def get_record(self, key: Any) -> Dict[str, Any]:
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=False) as db:

            if not db.get(str(key)):
                raise ValueError

            return db[str(key)]

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            db[str(key)] = values

    def query_table(self, criteria: List[SelectionCriteria]) -> List[Dict[str, Any]]:
        query_list = []
        flag = False
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=False) as db:
            for key, value in db.items():
                flag = self.is_meets_criterion(criteria, key, value, flag)

                if not flag:
                    query_list.append(value)
                flag = False

        return query_list

    def create_index(self, field_to_index: str) -> None:
        pass


def is_valid_fields(fields, key_field_name):
    for field_name in fields:
        if key_field_name == field_name.name:
                return True
    return False


class DataBase(db_api.DataBase):

    tables: Dict[str, DBTable]
    __tables__ = defaultdict(DBTable)

    def __init__(self):
        with shelve.open(os.path.join(db_api.DB_ROOT, "DB.db"), writeback=True) as db:
            for key in db:
                DataBase.__tables__[key] = DBTable(key, db[key][0], db[key][1])

    def create_table(self, table_name: str, fields: List[DBField], key_field_name: str) -> DBTable:
        db = shelve.open(os.path.join(db_api.DB_ROOT, table_name))
        db.close()

        if DataBase.__tables__.get(table_name):
            raise ValueError

        if not is_valid_fields(fields, key_field_name):
            raise ValueError

        with shelve.open(os.path.join(db_api.DB_ROOT, "DB.db"), writeback=True) as db:
            db[table_name] = [fields, key_field_name]

        table = DBTable(table_name, fields, key_field_name)
        DataBase.__tables__[table_name] = table
        return table

    def num_tables(self) -> int:
        return len(DataBase.__tables__)

    def get_table(self, table_name: str) -> DBTable:
        if not DataBase.__tables__.get(table_name):
            raise ValueError
        return DataBase.__tables__[table_name]

    def delete_table(self, table_name: str) -> None:
        db = (os.path.join('db_files', table_name + ".bak"))
        os.remove(db)
        db = (os.path.join('db_files', table_name + ".dat"))
        os.remove(db)
        db = (os.path.join('db_files', table_name + ".dir"))
        os.remove(db)
        with shelve.open(os.path.join(db_api.DB_ROOT, "DB.db"), writeback=True) as db:
            del db[table_name]

        del DataBase.__tables__[table_name]

    def get_tables_names(self) -> List[Any]:
        return list(DataBase.__tables__.keys())

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        pass
