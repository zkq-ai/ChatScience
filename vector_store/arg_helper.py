# coding: utf-8
"""
功能：

Author：zhuokeqiu
Date ：2024/9/26 9:37
"""

class args:
    # project_name='test' #项目名
    sep_string='|||' #字符串分隔符，当遇到该分隔符，需要拆分后再入库
    str_max_len=512 #字符串最大长度

    # vectorstore_uri = "http://10.45.139.201:19531" # milvus向量库地址
    vectorstore_uri = "http://127.0.0.1:19530" # milvus向量库地址

    improved_embedding_url = "127.0.0.1:28083" # 训练后的向量模型地址
    original_embedding_url = "127.0.0.1:18084" # 通用的未训练的向量模型地址


    ###以下为数仓pg库相关配置
    pg_host = "127.0.0.1"
    pg_database = "postgres" #为数仓构建的标准层数据库名
    pg_user = "postgres"
    pg_password = "postgres"
    pg_port = 5432


    schema_list = ["test"]   #需要同步向量化的相关schema

    #需要同步向量化相关schema下的表名，如果某个schema未指定具体表名，则默认同步全部表
    table_name_dict = {
        "test": ['view_1', 'view_2']
    }

    #需要同步的表字段，未指定时，默认同步该表全部字段
    table_col_for_sel_dict={
        "test.info1": ["name","source","create_time"],
        "test.info2": ["name", "create_time"],
    
    }

    #表中的个别字段需要从int型转为string型
    int2str_col_dict = {
        "公共域.dim_pub_wl": ['wlbm', 'wlflbm', 'wlyjflbm', 'wlejflbm', 'wlsjflbm']
    }
    max_col_enumerate_num=500 #单个字段下最大枚举值数量

    ###每天执行数据库同步的时间
    sync_time='03:10' #每天3点10分
