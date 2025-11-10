"""Schema and metadata definitions for all TPCH tables."""

from __future__ import annotations

from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    DoubleType,
    StringType,
    DateType,
)


def _schema(fields):
    return StructType([StructField(name, dtype, nullable) for name, dtype, nullable in fields])


TPCH_TABLES = {
    "customer": {
        "filename": "customer.csv",
        "schema": _schema([
            ("c_custkey", IntegerType(), False),
            ("c_name", StringType(), False),
            ("c_address", StringType(), False),
            ("c_nationkey", IntegerType(), False),
            ("c_phone", StringType(), False),
            ("c_acctbal", DoubleType(), False),
            ("c_mktsegment", StringType(), False),
            ("c_comment", StringType(), False),
            ("_dummy", StringType(), True),
        ]),
        "date_cols": [],
        "hudi": {
            "record_key": ["c_custkey"],
            "partition_field": "",
            "precombine_field": "c_custkey",
        },
    },
    "orders": {
        "filename": "orders.csv",
        "schema": _schema([
            ("o_orderkey", IntegerType(), False),
            ("o_custkey", IntegerType(), False),
            ("o_orderstatus", StringType(), False),
            ("o_totalprice", DoubleType(), False),
            ("o_orderdate", DateType(), False),
            ("o_orderpriority", StringType(), False),
            ("o_clerk", StringType(), False),
            ("o_shippriority", IntegerType(), False),
            ("o_comment", StringType(), False),
            ("_dummy", StringType(), True),
        ]),
        "date_cols": ["o_orderdate"],
        "hudi": {
            "record_key": ["o_orderkey"],
            "partition_field": "",
            "precombine_field": "o_orderdate",
        },
    },
    "lineitem": {
        "filename": "lineitem.csv",
        "schema": _schema([
            ("l_orderkey", IntegerType(), False),
            ("l_partkey", IntegerType(), False),
            ("l_suppkey", IntegerType(), False),
            ("l_linenumber", IntegerType(), False),
            ("l_quantity", DoubleType(), False),
            ("l_extendedprice", DoubleType(), False),
            ("l_discount", DoubleType(), False),
            ("l_tax", DoubleType(), False),
            ("l_returnflag", StringType(), False),
            ("l_linestatus", StringType(), False),
            ("l_shipdate", DateType(), False),
            ("l_commitdate", DateType(), False),
            ("l_receiptdate", DateType(), False),
            ("l_shipinstruct", StringType(), False),
            ("l_shipmode", StringType(), False),
            ("l_comment", StringType(), False),
            ("_dummy", StringType(), True),
        ]),
        "date_cols": ["l_shipdate", "l_commitdate", "l_receiptdate"],
        "hudi": {
            "record_key": ["l_orderkey", "l_linenumber"],
            "partition_field": "",
            "precombine_field": "l_shipdate",
        },
    },
    "supplier": {
        "filename": "supplier.csv",
        "schema": _schema([
            ("s_suppkey", IntegerType(), False),
            ("s_name", StringType(), False),
            ("s_address", StringType(), False),
            ("s_nationkey", IntegerType(), False),
            ("s_phone", StringType(), False),
            ("s_acctbal", DoubleType(), False),
            ("s_comment", StringType(), False),
            ("_dummy", StringType(), True),
        ]),
        "date_cols": [],
        "hudi": {
            "record_key": ["s_suppkey"],
            "partition_field": "",
            "precombine_field": "s_suppkey",
        },
    },
    "nation": {
        "filename": "nation.csv",
        "schema": _schema([
            ("n_nationkey", IntegerType(), False),
            ("n_name", StringType(), False),
            ("n_regionkey", IntegerType(), False),
            ("n_comment", StringType(), False),
            ("_dummy", StringType(), True),
        ]),
        "date_cols": [],
        "hudi": {
            "record_key": ["n_nationkey"],
            "partition_field": "",
            "precombine_field": "n_nationkey",
        },
    },
    "region": {
        "filename": "region.csv",
        "schema": _schema([
            ("r_regionkey", IntegerType(), False),
            ("r_name", StringType(), False),
            ("r_comment", StringType(), False),
            ("_dummy", StringType(), True),
        ]),
        "date_cols": [],
        "hudi": {
            "record_key": ["r_regionkey"],
            "partition_field": "",
            "precombine_field": "r_regionkey",
        },
    },
    "part": {
        "filename": "part.csv",
        "schema": _schema([
            ("p_partkey", IntegerType(), False),
            ("p_name", StringType(), False),
            ("p_mfgr", StringType(), False),
            ("p_brand", StringType(), False),
            ("p_type", StringType(), False),
            ("p_size", IntegerType(), False),
            ("p_container", StringType(), False),
            ("p_retailprice", DoubleType(), False),
            ("p_comment", StringType(), False),
            ("_dummy", StringType(), True),
        ]),
        "date_cols": [],
        "hudi": {
            "record_key": ["p_partkey"],
            "partition_field": "",
            "precombine_field": "p_partkey",
        },
    },
    "partsupp": {
        "filename": "partsupp.csv",
        "schema": _schema([
            ("ps_partkey", IntegerType(), False),
            ("ps_suppkey", IntegerType(), False),
            ("ps_availqty", IntegerType(), False),
            ("ps_supplycost", DoubleType(), False),
            ("ps_comment", StringType(), False),
            ("_dummy", StringType(), True),
        ]),
        "date_cols": [],
        "hudi": {
            "record_key": ["ps_partkey", "ps_suppkey"],
            "partition_field": "",
            "precombine_field": "ps_partkey",
        },
    },
}

TABLE_LIST = list(TPCH_TABLES.keys())
