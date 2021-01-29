#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2020-08-19 10:57:25
# @Author  : gwentmaster(1950251906@qq.com)
# I regret in my life


from typing import Optional

from marshmallow import fields
from sqlalchemy.sql.type_api import TypeEngine


class BaseDataStructure(object):

    dialect = None  # type: Optional[str]
    raw_type = None  # type: TypeEngine

    def _to_mysql(self) -> str:
        raise NotImplementedError()

    def to_mysql(self) -> str:
        if self.dialect == "mysql":
            return self.raw_type.compile()
        return self._to_mysql()

    def _to_sqlite(self) -> str:
        raise NotImplementedError()

    def to_sqlite(self) -> str:
        if self.dialect == "sqlite":
            return self.raw_type.compile()
        return self._to_sqlite()

    def to_marshmallow(self) -> fields.Field:
        raise NotImplementedError()

    def to_marshmallow_str(self) -> str:
        raise NotImplementedError()


class Boolean(BaseDataStructure):

    def __init__(self, type_: TypeEngine, dialect: str = None):
        self.raw_type = type_
        self.dialect = dialect

    def _to_mysql(self) -> str:
        return "TINYINT"

    def _to_sqlite(self) -> str:
        return "INTEGER"

    def to_marshmallow(self) -> fields.Field:
        return fields.Boolean()

    def to_marshmallow_str(self) -> str:
        return "fields.Boolean()"


class Date(BaseDataStructure):

    def __init__(self, type_: TypeEngine, dialect: str = None):
        self.raw_type = type_
        self.dialect = dialect

    def _to_mysql(self) -> str:
        return "DATE"

    def _to_sqlite(self) -> str:
        return "DATE"

    def to_marshmallow(self) -> fields.Field:
        return fields.Date()

    def to_marshmallow_str(self) -> str:
        return "fields.Date()"


class DateTime(BaseDataStructure):

    def __init__(self, type_: TypeEngine, dialect: str = None):
        self.raw_type = type_
        self.dialect = dialect

    def _to_mysql(self) -> str:
        return "DATETIME"

    def _to_sqlite(self) -> str:
        return "DATETIME"

    def to_marshmallow(self) -> fields.Field:
        return fields.DateTime()

    def to_marshmallow_str(self) -> str:
        return "fields.DateTime()"


class Decimal(BaseDataStructure):

    def __init__(self, type_: TypeEngine, dialect: str = None):
        self.raw_type = type_
        self.dialect = dialect
        self.precision = getattr(type_, "precision", None)
        self.scale = getattr(type_, "scale", None)

    def _to_mysql(self) -> str:
        if (self.precision is not None) and (self.scale is not None):
            return f"DECIMAL({self.precision},{self.scale})"
        return "DECIMAL"

    def _to_sqlite(self) -> str:
        return "TEXT"

    def to_marshmallow(self) -> fields.Field:
        return fields.Decimal(places=self.scale)

    def to_marshmallow_str(self) -> str:
        return f"fields.Decimal(places={self.scale})"


class Float(BaseDataStructure):

    def __init__(self, type_: TypeEngine, dialect: str = None):
        self.raw_type = type_
        self.dialect = dialect

    def to_mysql(self) -> str:
        return "FLOAT"

    def to_sqlite(self) -> str:
        return "REAL"

    def to_marshmallow(self) -> fields.Field:
        return fields.Float()

    def to_marshmallow_str(self) -> str:
        return "fields.Float()"


class Integer(BaseDataStructure):

    def __init__(self, type_: TypeEngine, dialect: str = None):
        self.raw_type = type_
        self.dialect = dialect
        self.length = getattr(type_, "display_length", None)

    def _to_mysql(self) -> str:
        if self.length is not None:
            return f"INTEGER({self.length})"
        return "INTEGER"

    def _to_sqlite(self) -> str:
        return "INTEGER"

    def to_marshmallow(self) -> fields.Field:
        return fields.Integer()

    def to_marshmallow_str(self) -> str:
        return "fields.Integer()"


class String(BaseDataStructure):

    def __init__(self, type_: TypeEngine, dialect: str = None):

        self.raw_type = type_
        self.dialect = dialect
        self.length = getattr(type_, "length", None)

    def _to_mysql(self) -> str:
        if self.length is not None:
            return f"VARCHAR({self.length})"
        return "VARCHAR"

    def _to_sqlite(self) -> str:
        return "TEXT"

    def to_marshmallow(self) -> fields.Field:
        return fields.String()

    def to_marshmallow_str(self) -> str:
        return "fields.String()"
