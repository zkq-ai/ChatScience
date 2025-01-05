# coding: utf-8
"""
功能：入向量库

Author：zhuokeqiu
Date ：2024/9/3 15:38
"""


import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Generator

import grpc
from langchain.indexes import SQLRecordManager, index
from langchain_community.document_loaders import JSONLoader
from langchain_community.vectorstores.milvus import Milvus
from langchain_core.embeddings import Embeddings
from logging_utils import logger
from pydantic import BaseModel
from proto import tei_pb2, tei_pb2_grpc
from arg_helper import args
from urllib.parse import quote_plus

class HuggingfaceTEIGrpcEmbeddings(BaseModel, Embeddings):
    """See <https://huggingface.github.io/text-embeddings-inference/>"""

    base_url: str
    normalize: bool = True
    truncate: bool = False
    query_instruction: str
    """Instruction to use for embedding query."""

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        with grpc.insecure_channel(self.base_url) as channel:
            stub = tei_pb2_grpc.EmbedStub(channel)
            responses = stub.EmbedStream(self._embed_request(texts))
            embeddings: list[list[float]] = [
                list(response.embeddings) for response in responses
            ]
            return embeddings

    def embed_query(self, text: str) -> list[float]:
        instructed_query = self.query_instruction + text
        return self.embed_documents([instructed_query])[0]

    def _embed_request(
        self, texts: list[str]
    ) -> Generator[tei_pb2.EmbedRequest, Any, None]:
        for inputs in texts:
            yield tei_pb2.EmbedRequest(
                inputs=inputs,
                normalize=self.normalize,
                truncate=self.truncate,
            )



def init_table_metrics() -> None:
    """Initialize the table metrics
    See <https://python.langchain.com/v0.1/docs/modules/data_connection/indexing/>
    """
    vectorstore = Milvus(
        improved_embedding,
        collection_name=f"gpt_metrics_ip_{project_name}",
        connection_args={"uri": vectorstore_uri},
        index_params={
            "metric_type": "IP",
            "index_type": "HNSW",
            "params": {"M": 8, "efConstruction": 64},
        },
        auto_id=False,
        drop_old=False,
        primary_field="key",
    )

    def metadata_func(record: dict, metadata: dict) -> dict:
        metadata["origin_metrics"] = record.get("ori_index")
        return metadata

    loader = JSONLoader(
        file_path=table_metrics_file_path,
        jq_schema=".[]",
        content_key="aug_index",
        metadata_func=metadata_func,
    )

    load_result = index(
        loader,
        record_manager,
        vectorstore,
        cleanup="incremental",
        source_id_key="source",
        batch_size=32,
    )

    logger.info(f"finished metric document loading: {load_result}")


def init_table_enumeration():
    """Initialize the table enumeration
    See <https://python.langchain.com/v0.1/docs/modules/data_connection/indexing/>
    """
    vectorstore = Milvus(
        original_embedding,
        collection_name=f"gpt_enumeration_ip_{project_name}",
        connection_args={"uri": vectorstore_uri},
        index_params={
            "metric_type": "IP",
            "index_type": "HNSW",
            "params": {"M": 8, "efConstruction": 64},
        },
        auto_id=False,
        drop_old=False,
        primary_field="key",
    )

    def metadata_func(record: dict, metadata: dict) -> dict:
        metadata["db_name"] = record.get("db_name")
        metadata["table_name"] = record.get("table_name")
        metadata["column_name"] = record.get("column_name")
        metadata["value"] = record.get("value")
        return metadata

    loader = JSONLoader(
        file_path=table_enumeration_file_path,
        jq_schema=".[] | .db_name as $db_name | .db_info | to_entries[] | .key as $table_name | .value.categorical_info | to_entries[] | .key as $column_name | .value[] | {db_name: $db_name, table_name: $table_name, column_name: $column_name, value: .}",
        content_key="value",
        metadata_func=metadata_func,
    )

    load_result = index(
        loader,
        record_manager,
        vectorstore,
        cleanup="incremental",
        source_id_key="source",
        batch_size=32,
    )

    logger.info(f"finished enumeration document loading: {load_result}")


def init_knowledge():
    """Initialize the knowledge base
    See <https://python.langchain.com/v0.1/docs/modules/data_connection/indexing/>
    """
    vectorstore = Milvus(
        original_embedding,
        collection_name=f"gpt_knowledge_ip_{project_name}",
        connection_args={"uri": vectorstore_uri},
        index_params={
            "metric_type": "IP",
            "index_type": "HNSW",
            "params": {"M": 8, "efConstruction": 64},
        },
        auto_id=False,
        drop_old=False,
        primary_field="key",
    )

    def metadata_func(record: dict, metadata: dict) -> dict:
        metadata["prompt"] = record.get("prompt", "")
        metadata["domain"] = record.get("domain", "")
        metadata["metrics"] = ",".join(record.get("metrics", []))
        metadata["type"] = record.get("type", "")
        return metadata

    loader = JSONLoader(
        file_path=knowledge_file_path,
        jq_schema=".[]",
        content_key="match",
        metadata_func=metadata_func,
    )

    load_result = index(
        loader,
        record_manager,
        vectorstore,
        cleanup="incremental",
        source_id_key="source",
        batch_size=32,
    )
    logger.info(f"finished knowledge document loading: {load_result}")

class TableEnumeration:
    project_name = args.project_name  # 项目名

    vectorstore_uri = args.vectorstore_uri  # milvus向量库地址
    # vectorstore_uri = "http://127.0.0.1:19530" # milvus向量库地址

    improved_embedding_url = args.improved_embedding_url  # 训练后的向量模型地址
    original_embedding_url = args.original_embedding_url  # 通用的未训练的向量模型地址
    pg_user=quote_plus(args.pg_user)
    pg_password=quote_plus(args.pg_password)
    postgres_url = f'{pg_user}:{pg_password}@{args.pg_host}:{args.pg_port}'  # 格式：用户名:密码@ip:port
    start = time.perf_counter()

    # Embedding table metrics
    improved_embedding = HuggingfaceTEIGrpcEmbeddings(
        base_url=improved_embedding_url, query_instruction=""
    )

    # Embedding other data
    original_embedding = HuggingfaceTEIGrpcEmbeddings(
        base_url=original_embedding_url, query_instruction=""
    )

    record_manager = SQLRecordManager(
        f"milvus/gpt-snvt-{project_name}",
        db_url=f"postgresql+psycopg://{postgres_url}/postgres",
    )
    record_manager.create_schema()

    def init_table_enumeration(self, table_enumeration_file_path):
        """Initialize the table enumeration
        See <https://python.langchain.com/v0.1/docs/modules/data_connection/indexing/>
        """
        vectorstore = Milvus(
            self.original_embedding,
            collection_name=f"gpt_enumeration_ip_{self.project_name}",
            connection_args={"uri": self.vectorstore_uri},
            index_params={
                "metric_type": "IP",
                "index_type": "HNSW",
                "params": {"M": 8, "efConstruction": 64},
            },
            auto_id=False,
            drop_old=False,
            primary_field="key",
        )

        def metadata_func(record: dict, metadata: dict) -> dict:
            metadata["db_name"] = record.get("db_name")
            metadata["table_name"] = record.get("table_name")
            metadata["column_name"] = record.get("column_name")
            metadata["value"] = record.get("value")
            return metadata

        loader = JSONLoader(
            file_path=table_enumeration_file_path,
            jq_schema=".[] | .db_name as $db_name | .db_info | to_entries[] | .key as $table_name | .value.categorical_info | to_entries[] | .key as $column_name | .value[] | select(. != null and . != \"\") | {db_name: $db_name, table_name: $table_name, column_name: $column_name, value: .}",
            content_key="value",
            metadata_func=metadata_func,
        )

        load_result = index(
            loader,
            self.record_manager,
            vectorstore,
            cleanup="incremental",
            source_id_key="source",
            batch_size=32
        )

        logger.info(f"finished enumeration document loading: {load_result}")

    def init_table_columns(self, table_enumeration_file_path) -> None:
        """Initialize the table metrics
        用于表召回
        """
        vectorstore = Milvus(
            self.improved_embedding,
            collection_name=f"gpt_column_ip_{self.project_name}",
            connection_args={"uri": self.vectorstore_uri},
            index_params={
                "metric_type": "IP",
                "index_type": "HNSW",
                "params": {"M": 8, "efConstruction": 64},
            },
            auto_id=False,
            drop_old=False,
            primary_field="key",
        )

        def metadata_func(record: dict, metadata: dict) -> dict:
            metadata["db_name"] = record.get("db_name")
            metadata["table_name"] = record.get("table_name")
            metadata["column_name"] = record.get("column_name")
            metadata["value"] = record.get("value")
            return metadata

        loader = JSONLoader(
            file_path=table_enumeration_file_path,
            jq_schema=".[] | .db_name as $db_name | .comment_info | to_entries[] | .key as $table_name | .value.column_name_comment | to_entries[] | .key as $column_name |.value as $value | {db_name: $db_name, table_name: $table_name, column_name: $column_name, value: $value}",
            content_key="value",
            metadata_func=metadata_func,
        )

        load_result = index(
            loader,
            self.record_manager,
            vectorstore,
            cleanup="incremental",
            source_id_key="source",
            batch_size=32
        )
        logger.info(f"finished metric document loading: {load_result}")

if __name__ == "__main__":
    #python环境：209: /home/zkq/anaconda3/envs/zkq_py310_gpt/bin/python
    project_name=args.project_name #项目名

    vectorstore_uri = args.vectorstore_uri # milvus向量库地址
    # vectorstore_uri = "http://127.0.0.1:19530" # milvus向量库地址

    improved_embedding_url = args.improved_embedding_url # 训练后的向量模型地址
    original_embedding_url = args.original_embedding_url # 通用的未训练的向量模型地址
    postgres_url = f'{args.pg_user}:{args.pg_password}@{args.pg_host}:{args.pg_port}'  # 格式：用户名:密码@ip:port
    start = time.perf_counter()

    # Embedding table metrics
    improved_embedding = HuggingfaceTEIGrpcEmbeddings(
        base_url=improved_embedding_url, query_instruction=""
    )

    # Embedding other data
    original_embedding = HuggingfaceTEIGrpcEmbeddings(
        base_url=original_embedding_url, query_instruction=""
    )

    record_manager = SQLRecordManager(
        f"milvus/gpt-snvt-{project_name}",
        db_url=f"postgresql+psycopg://{postgres_url}/postgres",
    )
    record_manager.create_schema()

    # knowledge_file_path = f"{project_name}_data/knowledge.json"
    # init_knowledge()
    # table_metrics_file_path = f"{project_name}_data/index_aug.json"
    # init_table_metrics()
    table_enumeration_file_path = f"{project_name}_data/table_infos_test1.json"
    init_table_enumeration()

    end = time.perf_counter()
    logger.info(f"Initialized in {(end - start):.4}s")
