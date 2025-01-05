#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os
import copy
from collections import Counter
import json
import numpy as np


def load_data_info(file_path):
    datas_info = load_json(file_path)
    all_table_infos = []
    for db in datas_info:
        for table_name, table_info in db["db_info"].items():
            if table_name == "dm_xmqsmzq_qttzjl":
                continue
            table_info["comment_info"] = db["comment_info"][table_name]
            all_table_infos.append(table_info)

    return all_table_infos


def _filter_data_info(datas_info, table_name):
    for data in datas_info:
        if data["db_name"] == table_name:
            data_info = copy.deepcopy(data)
            return data_info
    raise ValueError(f"No data_info of table: {table_name}")


def _get_numeric_categorical_cols(data_info):
    c_columns = list(data_info["numeric_info"].keys())
    d_columns = list(data_info["categorical_info"].keys())
    return c_columns, d_columns


def _random_choice_category_col(categories_info, num_least=1):
    # 选择符合条件的 类别 字段 和 值
    all_values = []
    for col, values in categories_info.items():
        all_values.extend(values)
    value_count = dict(Counter(all_values))

    res_dict = {}
    for col, values in categories_info.items():
        for value in values:
            if value_count[value] != 1:
                continue
            try:
                float(value) or float(value.replace(",", ""))
                continue
            except:
                if col not in res_dict:
                    res_dict[col] = [value]
                else:
                    res_dict[col].append(value)

    res_dict_least = {k: v for k, v in res_dict.items() if len(v) >= num_least}
    return res_dict_least


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)
    
def load_json(input_path: str):
    with open(input_path, encoding="utf-8") as f:
        data = json.load(f)
    return data


def save_json(data, output_path):
    abs_path = os.path.abspath(output_path)
    if not os.path.exists(os.path.dirname(abs_path)):
        os.mkdir(os.path.dirname(abs_path))

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, cls=NumpyEncoder, indent=4)


def save_jsonl(data, output_path):
    abs_path = os.path.abspath(output_path)
    if not os.path.exists(os.path.dirname(abs_path)):
        os.mkdir(os.path.dirname(abs_path))

    with open(output_path, "w", encoding="utf-8") as f:
        for d in data:
            json.dump(d, f, ensure_ascii=False)
            f.write("\n")


def load_jsonl(path):
    datas = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            data = json.loads(line)
            datas.append(data)

    return datas


class ColumnsNumError(Exception):
    def __init__(self, func_name, cate, num):
        super().__init__(func_name, cate, num)
        self.func_name = func_name
        self.num = num
        self.cate = cate
        self.status = 101
        print(f"Error function: {func_name}, 该操作至少需要{num}个{cate}特征")
