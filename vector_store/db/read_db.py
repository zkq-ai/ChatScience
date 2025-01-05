#!/usr/bin/python
# -*- coding: UTF-8 -*-

import psycopg2
import pandas as pd
from arg_helper import args
from logging_utils import logger

conn = psycopg2.connect(
    host = args.pg_host,
    database = args.pg_database,
    user = args.pg_user,
    password = args.pg_password,
    port = args.pg_port
)

def get_schema_tables(schema):
    # 获取一个schema下的所有table name
    table_df = pd.read_sql_query(f"""SELECT tb.table_name, d.description
            FROM information_schema.tables tb
                     JOIN pg_class c ON c.relname = tb.table_name
                     LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = '0'
            WHERE tb.table_schema = '{schema}';
                """, conn)
    return table_df


def get_comment_df(table_name, schema):
    # 获取表、字段的comment, 以及 数据类型
    table_comment_df = pd.read_sql_query(f"""SELECT
    obj_description(c.oid) AS table_comment
FROM
    pg_class c
JOIN
    pg_namespace n ON n.oid = c.relnamespace    
where
	n.nspname = '{schema}' and
    c.relname = '{table_name}';""", conn)

    columns_comment_df = pd.read_sql_query(f"""SELECT
    column_name,
    data_type,
    col_description('{schema}.{table_name}'::regclass::oid, ordinal_position) AS description
FROM
    information_schema.columns
WHERE
    table_schema = '{schema}'
    AND table_name = '{table_name}'
ORDER BY
    ordinal_position;
    """, conn)

    # feature_type_df = pd.read_sql_query(
    #     f"select distinct column_name,data_type from information_schema.columns where table_name = '{table_name}'", conn)

    if len(table_comment_df) == 0:
        table_comment_str = table_name
    else:
        table_comment_str = table_comment_df["table_comment"].values[0]

    # map_type = {feature_type_df["column_name"][i]: feature_type_df["data_type"][i] for i in range(len(feature_type_df))}
    # columns_comment_df["data_type"] = columns_comment_df["column_name"].map(map_type)

    # 将数据类型合并至comment表中
    return table_comment_str, columns_comment_df

def get_comment_df_bak(table_name, schema):
    # 获取表、字段的comment, 以及 数据类型
    table_comment_df = pd.read_sql_query(f"""SELECT distinct tb.table_name, d.description
    FROM information_schema.tables tb
             JOIN pg_class c ON c.relname = tb.table_name
             LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = '0'
    WHERE tb.table_schema = '{schema}' and tb.table_name = '{table_name}';
        """, conn)

    columns_comment_df = pd.read_sql_query(f"""
    SELECT distinct col.table_name, col.column_name, col.ordinal_position AS o, d.description
    FROM information_schema.columns col
             JOIN pg_class c ON c.relname = col.table_name
             LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = col.ordinal_position
    WHERE col.table_schema = '{schema}' and col.table_name = '{table_name}'
    ORDER BY col.table_name, col.ordinal_position;

    """, conn)

    feature_type_df = pd.read_sql_query(
        f"select distinct column_name,data_type from information_schema.columns where table_name = '{table_name}'", conn)

    if len(table_comment_df) == 0:
        table_comment_str = table_name
    else:
        table_comment_str = table_comment_df[table_comment_df["table_name"] == table_name]["description"].values[0]

    map_type = {feature_type_df["column_name"][i]: feature_type_df["data_type"][i] for i in range(len(feature_type_df))}
    columns_comment_df["data_type"] = columns_comment_df["column_name"].map(map_type)

    # 将数据类型合并至comment表中
    return table_comment_str, columns_comment_df


def insert_sql(sqls: object) -> object:
    # 创建游标对象
    cur = conn.cursor()

    try:
        # cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
        # 开始事务
        # conn.autocommit = False
        # 遍历 SQL 语句列表并执行每个语句
        for sql in sqls:
            try:
                cur.execute(sql)
                # 提交事务
                conn.commit()
            except Exception as e:
                # 如果发生错误，回滚事务
                conn.rollback()
                logger.error(f"An error occurred: {e}")


    except Exception as e:
        # 如果发生错误，回滚事务
        conn.rollback()
        logger.error(f"An error occurred: {e}")
        raise Exception(e)

    finally:
        # 关闭游标和连接
        cur.close()
