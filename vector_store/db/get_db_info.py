#!/usr/bin/python
# -*- coding: UTF-8 -*-
import copy
import json

import numpy as np
import pandas as pd
from .read_db import get_schema_tables, get_comment_df, conn, insert_sql
from .tools import save_json
from arg_helper import args
from logging_utils import logger

def get_data_infos_dsl_with_comment():
    # 给拼音 加上COMMENT 遵循mysql语法
    # 用于str dsl 推理

    comment_format = "{pinyin_str}"
    res = []

    schema_list = args.schema_list
    # schema_list = ["public"]
    table_name_dict=args.table_name_dict
    int2str_col_dict = args.int2str_col_dict

    d_type_set= set()

    for schema in schema_list:
        single_res = {"db_name": "",
                      "db_info": {},
                      "foreign_keys": [],
                      "comment_info": {}
                      }
        if schema in table_name_dict:
            table_name_list=table_name_dict[schema]
        else:
            table_df = get_schema_tables(schema)
            table_name_list = table_df["table_name"]
        for table_name in table_name_list:
            single_table_info = {
                "numeric_info": {},
                "categorical_info": {},
                "date_cols_info": {},
                "other_cols_info": {},
            }
            single_comment_info = {
                "table_name_comment": "",
                "column_name_comment": {}
            }

            single_table_info['other_cols_info']['list_cols']=[]

            int2str_col_list=int2str_col_dict.get(table_name,[])
            table_comment_str, columns_comment_df = get_comment_df(table_name, schema=schema)
            single_comment_info["table_name_comment"] = table_comment_str

            df_data = pd.read_sql_query(f"SELECT * FROM {schema}.{table_name}", conn)


            for i in range(len(columns_comment_df)):
                d_type = columns_comment_df["data_type"][i]
                column = columns_comment_df["column_name"][i]
                comment = columns_comment_df["description"][i]

                d_type_set.add(d_type)
                single_comment_info["column_name_comment"][column] = comment

                column_with_comment = comment_format.format_map({"pinyin_str": column})

                if hasattr(args,'table_col_for_sel_dict') and f'{schema}.{table_name}' in args.table_col_for_sel_dict and \
                        column not in args.table_col_for_sel_dict[f'{schema}.{table_name}']:
                    continue
                if "time" in d_type:
                    # df_data[column] = pd.to_datetime(df_data[column])
                    # st = df_data[column].min()
                    # et = df_data[column].max()
                    # st = st.strftime("%Y-%m-%d")
                    # et = et.strftime("%Y-%m-%d")
                    st='2010-01-01'
                    et='2030-12-31'

                    single_table_info["date_cols_info"][column_with_comment] = [st, et]

                elif d_type in ["text", "character varying"] or column in int2str_col_list:
                    if d_type in ["integer", "bigint", "smallint"]:
                        not_null_unique = [str(int(x)) for x in list(df_data[df_data[column].notnull()][column].unique())][:args.max_col_enumerate_num]
                    else:
                        try:
                            not_null_unique = [str(x) for x in list(df_data[df_data[column].notnull()][column].unique())]
                            if any([args.sep_string in x for x in not_null_unique]):
                                single_table_info['other_cols_info']['list_cols'].append(column)
                                not_null_unique_=[]
                                for e in not_null_unique:
                                    es = e.split(args.sep_string)
                                    not_null_unique_.extend(es)
                                not_null_unique = not_null_unique_
                            not_null_unique = [x[:args.str_max_len] for x in not_null_unique[:args.max_col_enumerate_num]]
                        except:
                            not_null_unique=None
                    # if column=='wlbm':
                    #     print('===type: ', d_type)
                    if not_null_unique:
                        single_table_info["categorical_info"][column_with_comment] = not_null_unique

                    # if len(not_null_unique) == len(df_data) or len(not_null_unique) >100:
                    #     single_table_info["categorical_info"][column_with_comment] = not_null_unique#[:5]
                    # else:
                    #     if len(not_null_unique) >30:
                    #         single_table_info["categorical_info"][column_with_comment] = not_null_unique#[:5]
                    #     else:
                    #         single_table_info["categorical_info"][column_with_comment] = copy.deepcopy(not_null_unique)


                elif d_type in ["numeric", "integer", "double precision", "bigint", "real", "smallint"]:
                    values=df_data[column].dropna()
                    if not values.empty:
                        single_table_info["numeric_info"][column_with_comment] = [
                            round(min(values)), round(float(np.median(values))), round(max(values))]
                    else:
                        single_table_info["numeric_info"][column_with_comment] = [0,0,0]

                elif d_type in ["bytea"]:
                    single_table_info["numeric_info"][column_with_comment] = []
                else:
                    print(f"d_type: {d_type} is not define")

            single_res["db_name"] = schema
            table_with_comment = comment_format.format_map(
                {"pinyin_str": table_name})

            single_res["db_info"][table_with_comment] = single_table_info
            single_res["comment_info"][table_with_comment] = single_comment_info
        res.append(single_res)
        print()
        print(d_type_set)

    return res

def single_table_():
    # 以数仓为一个单位
    all_table_infos = get_data_infos_dsl_with_comment()
    res = []
    for db in all_table_infos:
        for table_name, table_info in db["db_info"].items():
            single_res = {"db_name": table_name,
                          "db_info" : {table_name: table_info},
                          "foreign_keys": [],
                          "comment_info": {table_name: db["comment_info"][table_name]}
                          }
            res.append(single_res)
    return res

def insert_dbinfo_into_table(all_table_infos):
    """
    将解析的表枚举值等信息插入到数据库中，用于后续的模型服务读取
    :param all_table_infos:
    :return:
    """
    logger.info("开始插入表信息至数据库。。。")

    table_name='gpt.t_table_infos'
    sqls=[]
    for table_info in all_table_infos:
        db_name=table_info['db_name']
        db_info=table_info['db_info']
        comment_info=table_info['comment_info']
        for k,v in db_info.items():
            numeric_info=v['numeric_info']
            categorical_info=v['categorical_info']
            date_cols_info=v['date_cols_info']
            other_cols_info = v['other_cols_info']

            for info in [numeric_info, categorical_info, date_cols_info]:
                for k2,v2 in info.items():
                    info[k2]=[]
            date_cols=','.join(set(date_cols_info.keys()))
            sql = f"""insert into {table_name}(table_name_cn, table_name_en,table_schema,numeric_info,categorical_info,
                  date_cols_info,col_comments, other_cols_info) values(
                  '{comment_info[k]["table_name_comment"]}', '{k}','{db_name}','{json.dumps(numeric_info,ensure_ascii=False)}', 
                  '{json.dumps(categorical_info,ensure_ascii=False)}','{json.dumps(date_cols,ensure_ascii=False)}',
                  '{json.dumps(comment_info[k],ensure_ascii=False)}','{json.dumps(other_cols_info,ensure_ascii=False)}')"""

            sqls.append(sql)
    try:
        insert_sql(sqls)
        logger.info("完成插入表信息至数据库！")
    except Exception as e:
        raise Exception(e)



def run(outfile):
    all_table_infos = get_data_infos_dsl_with_comment()
    save_json(all_table_infos, outfile)
    insert_dbinfo_into_table(all_table_infos)



if __name__ == "__main__":
    #cd /home/zkq/2024/01gpt/01data_gen/zxsk_model_and_data
    #/home/zkq/.conda/envs/zkq_py310_gpt/bin/python -m zxsk.gen_data_xingyu.db.get_db_info
    all_table_infos = get_data_infos_dsl_with_comment()
    save_json(all_table_infos, f"./{args.project_name}_data/table_infos.json")
