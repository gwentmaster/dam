#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2020-08-18 16:34:05
# @Author  : gwentmaster(1950251906@qq.com)
# I regret in my life


import decimal
import json
from collections import defaultdict
from operator import itemgetter
from typing import Dict, List, Set

from marshmallow import fields, Schema
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql.base import MySQLIdentifierPreparer
from sqlalchemy.dialects.sqlite.base import SQLiteIdentifierPreparer
from sqlalchemy.orm import mapper
from sqlalchemy.orm.session import Session
from sqlalchemy.schema import MetaData, Table


from analyser import analyse_table
from datastructures import Decimal

# TODO  unique index


MYSQL_RESERVED_WORDS = MySQLIdentifierPreparer.reserved_words
SQLITE_RESERVED_WORDS = SQLiteIdentifierPreparer.reserved_words


class MyJsonEncoder(json.JSONEncoder):
    """自定义json编码器,将Decimal类在序列化时转化为字符串
    """
    def default(self, field):
        if isinstance(field, decimal.Decimal):
            if field == 0:  # 防止出现0E-8
                field = 0.0
            return str(field)
        else:  # pragma: no cover
            return json.JSONEncoder.default(self, field)


def gen_mysql_sql(tables: List[Table], dialect: str) -> None:
    """生成MySQL建表语句

    Args:
        tables: sqlalchemy通过反射获取的表
    """

    f = open("mysql_table.sql", "wb")
    for table in tables:
        result = analyse_table(table, dialect)
        f.write((
            f"DROP TABLE IF EXISTS {result['table']};\n"
            + f"CREATE TABLE {result['table']}\n"
            + "    (\n"
        ).encode("utf-8"))

        first_column = True
        primary_keys = []  # type: List[str]
        name_length = max([len(c["name"]) for c in result["columns"]])
        for column in sorted(result["columns"], key=itemgetter("name")):
            name = column["name"]
            if name in MYSQL_RESERVED_WORDS:
                name = f"`{name}`"
            if column["primary"] is True:
                primary_keys.append(name)
            column_string = name.ljust(name_length)
            column_string += (" " + column["type"].to_mysql())
            if column["nullable"] is False:
                column_string += " NOT NULL\n"
            else:
                column_string += "\n"
            if first_column is True:
                column_string = " "*6 + column_string
                first_column = False
            else:
                column_string = " "*4 + ", " + column_string
            f.write(column_string.encode("utf-8"))

        # 主键
        if primary_keys:
            f.write((
                " " * 4
                + ", PRIMARY KEY "
                + f"({', '.join(primary_keys)})\n"
            ).encode("utf-8"))

        # 索引
        for index_name, col_name in result["indexes"]:
            f.write((
                " "*4
                + ", KEY "
                + f"{index_name} ({col_name})\n"
            ).encode("utf-8"))

        # 外键
        for col_name, reference in result["foreign_keys"]:
            reference = reference.split(".")
            f.write((
                " " * 4
                + ", FOREIGN KEY "
                + f"({col_name})"
                + " REFERENCES "
                + f"{reference[0]} ({reference[1]})\n"
            ).encode("utf-8"))

        f.write((" "*4 + ");" + "\n\n").encode("utf-8"))

    f.close()


def gen_sqlite_sql(
    tables: List[Table],
    dialect: str,
    decimal_as_real: bool = False
) -> None:
    """生成SQLite建表语句

    Args:
        tables: sqlalchemy通过反射获取的表
        decimal_as_real: 是否将DICIMAL字段用REAL表示,默认用TEXT表示
    """

    results = []
    for table in tables:
        results.append(analyse_table(table, dialect))

    # 找出被外键绑定的字段,之后为其添加唯一索引
    unique_indexs = defaultdict(set)  # type: Dict[str, Set[str]]
    for result in results:
        for _, foreign_key in result["foreign_keys"]:
            table_name, col_name = foreign_key.split(".")
            unique_indexs[table_name].add(col_name)

    f = open("sqlite_table.sql", "wb")
    for result in results:

        # 表开始
        f.write((
            f"DROP TABLE IF EXISTS {result['table']};\n"
            + f"CREATE TABLE {result['table']}\n"
            + "    (\n"
        ).encode("utf-8"))

        first_column = True
        name_length = max([len(c["name"]) for c in result["columns"]])
        primaries = set()  # type: Set[str]
        for column in sorted(result["columns"], key=itemgetter("name")):

            name = column["name"]

            # 键名,保留字则加中括号
            if name in SQLITE_RESERVED_WORDS:
                name = f"[{name}]"
            column_string = name.ljust(name_length)

            # 字段类型
            if (
                    isinstance(column["type"], Decimal)
                    and (decimal_as_real is True)
            ):
                column_string += (" REAL")
            else:
                column_string += (" " + column["type"].to_sqlite())

            # 是否主键
            if column["primary"] is True:
                # column_string = column_string + " PRIMARY KEY"
                primaries.add(name)

            # 是否唯一索引
            if (
                    (name in unique_indexs[result["table"]])
                    and (column["primary"] is False)
            ):
                column_string += " UNIQUE"

            # 能否为空
            if column["nullable"] is False:
                column_string += " NOT NULL\n"
            else:
                column_string += "\n"

            # 第一个字段开头无逗号
            if first_column is True:
                column_string = " "*6 + column_string
                first_column = False
            else:
                column_string = " "*4 + ", " + column_string

            f.write(column_string.encode("utf-8"))

        # 主键
        f.write((
            " " * 4
            + f", PRIMARY KEY ({', '.join(primaries)})\n"
        ).encode("utf-8"))

        # 外键
        for col_name, reference in result["foreign_keys"]:
            _reference = reference.split(".")
            f.write((
                " " * 4
                + ", FOREIGN KEY "
                + f"({col_name})"
                + " REFERENCES "
                + f"{_reference[0]} ({_reference[1]})\n"
            ).encode("utf-8"))

        # 表结束
        f.write((" "*4 + ");" + "\n\n").encode("utf-8"))

    f.close()


def gen_schemas(
    tables: List[Table],
    dialect: str,
    to_file: bool = True
) -> Dict[str, Schema]:
    """生成各表对应的marshmallow的Schema及定义这些Schema的py文件

    Args:
        tables: sqlalchemy通过反射获取的表
        to_file: 是否生成py文件

    Returns:
        表名为键,相应Schema为值的字典
    """

    if to_file is True:
        f = open("schemas.py", "wb")
        f.write((
            "# -*- coding: utf-8 -*-\n\n\n"
            + "from marshmallow import fields, Schema\n\n\n"
        ).encode("utf-8"))

    schemas = {}  # type: Dict[str, Schema]

    last_table_num = len(tables) - 1
    for i, table in enumerate(tables):
        schema_dic = {}  # type: Dict[str, fields.Field]
        result = analyse_table(table, dialect)
        if to_file is True:
            schema_name = "".join(
                [x.capitalize() for x in result["table"].split("_")]
            )

            f.write((
                f"class {schema_name}Schema(Schema):\n\n"
            ).encode("utf-8"))
        for column in result["columns"]:
            if to_file is True:
                f.write((
                    " " * 4
                    + column["name"]
                    + f" = {column['type'].to_marshmallow_str()}\n"
                ).encode("utf-8"))

            schema_dic[column["name"]] = column["type"].to_marshmallow()

        if (to_file is True) and (i != last_table_num):
            f.write("\n\n".encode("utf-8"))

        schemas[result["table"]] = (
            Schema.from_dict(schema_dic)()  # type: ignore[arg-type]
        )

    if to_file is True:
        f.close()
    return schemas


def _gen_models(metadata) -> None:
    """使用sqlacodegen生成sqlalchemy模型
    """

    from sqlacodegen.codegen import CodeGenerator
    generator = CodeGenerator(metadata)
    f = open("sqlalchemy_models.py", "w", encoding="utf-8")
    generator.render(f)
    f.close()


def gen_models(tables: List[Table]) -> None:
    """生成sqlalchemy模型文件

    Args:
        tables: 通过sqlalchemy反射获取的表
    """

    template = (
        "# -*- coding: utf-8 -*-\n\n\n{imports}\n\n\n"
        + "{declarative}\n\n\n{models}"
    )

    imports_dic = defaultdict(set)  # 需要导入的模块
    imports_dic["sqlalchemy"].add("Column")
    import_foreign_key_flag = False

    # declarative语句
    imports_dic["sqlalchemy.ext.declarative"].add("declarative_base")
    imports_dic["sqlalchemy.ext.declarative.api"].add("DeclarativeMeta")
    imports_dic["sqlalchemy"].add("MetaData")
    declarative = (
        "Base = declarative_base()  # type: DeclarativeMeta\n"
        + "metadata = Base.metadata  # type: MetaData"
    )

    table_strings = []  # type: List[str]
    for table in tables:
        table_string = ""
        class_name = "".join(x.capitalize() for x in table.name.split("_"))
        table_string += (
            f"class {class_name}:\n\n"
            + " "*4 + f"__tablename__ = \"{table.name}\"\n\n"
        )

        # 解析字段信息
        column_dics = {}  # type: Dict[str, Dict]
        for column in table.columns:

            if isinstance(column.type, type):
                type_name = column.type.__name__
            else:
                type_name = type(column.type).__name__
            type_module = column.type.__module__
            if type_module.startswith('sqlalchemy.dialects.'):
                type_module = ".".join(type_module.split(".")[:3])
            imports_dic[type_module].add(type_name)

            column_dics[column.name] = {
                "type": repr(column.type),
                "primary": column.primary_key,
                "nullable": column.nullable,
                "foreign_key": [
                    str(foreign_key.column)
                    for foreign_key in column.foreign_keys
                ]
            }

        # 判断字段是否需要添加索引
        for index in table.indexes:
            for column in index.columns:
                column_dics[column.name]["index"] = True

        # 定入字段信息
        for k, v in column_dics.items():
            table_string += (
                " " * 4
                + f"{k} = Column({v['type']}"
            )
            if v["primary"] is True:
                table_string += ", primary_key=True"
            for foreign_key in v["foreign_key"]:
                import_foreign_key_flag = True
                table_string += f", ForeignKey(\"{foreign_key}\")"
            if (v["primary"] is not True) and (v.get("index", False) is True):
                table_string += ", index=True"
            if v["nullable"] is False:
                table_string += ", nullable=False"
            table_string += ")\n"

        table_strings.append(table_string)

    models = "\n\n".join(table_strings)

    # import语句整理
    imports = ""
    if import_foreign_key_flag is True:
        imports_dic["sqlalchemy"].add("ForeignKey")
    for key in sorted(imports_dic.keys()):
        imports += (
            f"from {key} import "
            + ", ".join(sorted(imports_dic[key]))
            + "\n"
        )
    imports = imports[:-1]

    # 写入py文件
    with open("models.py", "wb") as f:
        f.write(template.format(
            imports=imports,
            declarative=declarative,
            models=models
        ).encode("utf-8"))


def gen_db(
    tables: List[Table],
    engine,
    dialect: str,
    decimal_as_real: bool = False
):
    """生成db文件并将数据导入,会先生成SQLite建表语句

    Args:
        tables: sqlalchemy通过反射获取的表
        engine: 数据库连接
        decimal_as_real: 是否将原本为DECIMAL的字段在
                         db文件中设为REAL,默认为TEXT
    """

    gen_sqlite_sql(tables, dialect, decimal_as_real)

    with open("sqlite_table.sql", encoding="utf-8") as f:
        sqls = f.read()

    sqlite_engine = create_engine("sqlite:///data.db")
    sqlite_session = Session(bind=sqlite_engine)
    for sql in sqls.split(";"):
        sqlite_session.execute(sql + ";")

    session = Session(bind=engine)
    for i, table in enumerate(tables):
        objs = session.query(table).all()
        class_ = type(f"table_{i}", (), {})
        mapper(class_, table)
        objs = session.query(class_).all()
        for obj in objs:
            new_obj = class_()
            for k, v in obj.__dict__.items():
                if k == "_sa_instance_state":
                    continue
                setattr(new_obj, k, v)
            sqlite_session.add(new_obj)

    session.close()
    sqlite_session.commit()
    sqlite_session.close()


def gen_json(tables: List[Table], engine, dialect: str) -> None:
    """生成json文件

    Args:
        tables: sqlalchemy通过反射获取的表
        engine: 数据库连接
    """

    result = {}
    schemas = gen_schemas(tables, to_file=False, dialect=dialect)
    session = Session(bind=engine)
    for table in tables:
        name = table.name
        objs = session.query(table).all()
        result[name] = schemas[name].dump(objs, many=True)
    session.close()

    with open("data.json", "wb") as f:
        f.write(json.dumps(
            result,
            ensure_ascii=False,
            cls=MyJsonEncoder
        ).encode("utf-8"))


if __name__ == "__main__":

    """============================数据库连接================================"""
    engine = create_engine(
        "sqlite:///"
    )
    dialect = "sqlite"
    metadata = MetaData(bind=engine)
    metadata.reflect()
    tables = [
        table for table in metadata.sorted_tables
        if table.name not in ('alembic_version', 'migrate_version', "apscheduler_job")
    ]

    """=================================schema=============================="""
    # gen_schemas(tables, dialect=dialect)

    """=================================model=============================="""
    # gen_models(tables)

    """=============================json数据=================================="""
    gen_json(tables, engine, dialect=dialect)

    """==============================mysql建表语句============================="""
    gen_mysql_sql(tables, dialect=dialect)

    """==============================sqlite建表语句============================="""
    gen_sqlite_sql(tables, decimal_as_real=True, dialect=dialect)

    """===============================db文件=================================="""
    gen_db(tables, engine=engine, dialect=dialect, decimal_as_real=True)
