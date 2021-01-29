#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2020-08-25 09:30:08
# @Author  : gwentmaster(1950251906@qq.com)
# I regret in my life


from typing import Any, Dict, List, Tuple, Union

from sqlalchemy.schema import Table
from sqlalchemy.dialects.mysql import (
    BIT as mysql_bit,
    DATE as mysql_date,
    DATETIME as mysql_datetime,
    DECIMAL as mysql_decimal,
    ENUM as mysql_enum,
    JSON as mysql_json,
    TIMESTAMP as mysql_timestamp,
    TINYINT as mysql_tinyint
)
from sqlalchemy.dialects.mysql.types import (
    _FloatType as mysql_float,
    _IntegerType as mysql_int,
    _StringType as mysql_string
)
from sqlalchemy.sql.sqltypes import (
    Date as sql_date,
    Float as sql_float,
    Integer as sql_int,
    String as sql_string,
    TIMESTAMP as sql_timestamp
)

from datastructures import (
    BaseDataStructure,
    Boolean,
    Date,
    DateTime,
    Decimal,
    Float,
    Integer,
    String
)


def _analyse_mysql_type(type_) -> BaseDataStructure:
    """推断MySQL字段的类型

    Args:
        type_: sqlalchemy通过反射获取的字段类型

    Raises:
        TypeError: 不支持的类型
    """

    if isinstance(type_, mysql_decimal):
        return Decimal(type_, dialect="mysql")

    elif isinstance(type_, mysql_float):
        return Float(type_, dialect="mysql")

    elif isinstance(type_, mysql_int):
        return Integer(type_, dialect="mysql")

    elif isinstance(type_, mysql_tinyint):
        if type_.display_width == 1:
            return Boolean(type_, dialect="mysql")
        else:
            return Integer(type_, dialect="mysql")

    elif isinstance(type_, mysql_string):
        return String(type_, dialect="mysql")

    elif isinstance(type_, mysql_date):
        return Date(type_, dialect="mysql")

    elif isinstance(type_, mysql_timestamp):
        return DateTime(type_, dialect="mysql")

    else:
        mysql_structure = BaseDataStructure()
        mysql_structure.raw_type = type_
        mysql_structure.dialect = "mysql"
        return mysql_structure


def _analyse_sqlite_type(type_) -> BaseDataStructure:
    """推断SQLite字段类型

    Args:
        type_: sqlalchemy通过反射获取的字段类型

    Raises:
        TypeError: 不支持的类型
    """

    if isinstance(type_, sql_float):
        return Float(type_, dialect="sqlite")

    elif isinstance(type_, sql_int):
        return Integer(type_, dialect="sqlite")

    elif isinstance(type_, sql_string):
        return String(type_, dialect="sqlite")

    elif isinstance(type_, sql_date):
        return Date(type_, dialect="sqlite")

    else:
        sqlite_structure = BaseDataStructure()
        sqlite_structure.raw_type = type_
        sqlite_structure.dialect = "sqlite"
        return sqlite_structure


def analyse_table(table: Table, dialect: str) -> Dict[str, Any]:
    """分析表结构

    Args:
        table: sqlalchemy通过反射获取的表
        dialect: 数据库类型

    Returns:
        字典,各项含义如下
        table: 表名
        columns: 字段类型列表,每一项是如下字典
            name: 字段名
            type: 字段类型,BaseDataStructure类
            nullable: 是否能为NULL
            primary: 是否主键
        foreitn_keys: 代表外键的列表,每一项是(字段名, 外键表.外键字段)的元组
        indexes: 代表索引的列表,每一项是(索引名, 字段名)的元组

    """

    if dialect == "mysql":
        _analyse_type = _analyse_mysql_type
    elif dialect == "sqlite":
        _analyse_type = _analyse_sqlite_type
    else:
        raise TypeError(f"no such dialect: {dialect}")

    columns = []  # type: List[Dict[str, Union[bool, str, BaseDataStructure]]]
    foreign_keys = []  # type: List[Tuple[str, str]]
    indexes = []  # type: List[Tuple[str, str]]
    for column in table.columns:
        name = column.name
        type_ = _analyse_type(column.type)
        nullable = column.nullable
        primary = column.primary_key
        for foreign_key in column.foreign_keys:
            foreign_keys.append((name, str(foreign_key.column)))
        columns.append({
            "name": name,
            "type": type_,
            "nullable": nullable,
            "primary": primary
        })

    for index in table.indexes:
        indexes.append((index.name, list(index.columns)[0].name))

    return {
        "table": table.name,
        "columns": columns,
        "foreign_keys": foreign_keys,
        "indexes": indexes
    }
