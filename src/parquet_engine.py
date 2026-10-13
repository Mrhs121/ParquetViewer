#!/usr/bin/env python3
"""
parquet_engine.py
Backend engine for Parquet & Iceberg Avro QuickView Mac App.
Powered by DuckDB, PyArrow, and FastAvro.
"""

import sys
import os
import glob
import json
import time
import datetime
import decimal
import traceback
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import fastavro


def safe_json_default(obj: Any) -> Any:
    """Serialize timestamps, dates, decimals, bytes, and sets to JSON friendly types."""
    if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
        return obj.isoformat()
    elif isinstance(obj, decimal.Decimal):
        return float(obj)
    elif isinstance(obj, (bytes, bytearray)):
        try:
            s = obj.decode('utf-8')
            if s.isprintable() and len(s) > 0:
                return s
        except Exception:
            pass
        return "0x" + obj.hex()
    elif isinstance(obj, set):
        return list(obj)
    return str(obj)


def human_size(nbytes: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if nbytes < 1024.0:
            return f"{nbytes:.2f} {unit}" if unit != 'B' else f"{nbytes} B"
        nbytes /= 1024.0
    return f"{nbytes:.2f} PB"


def resolve_source(raw_path: str) -> Tuple[str, str, int, str]:
    """
    Given a file or directory path:
    Returns (duckdb_sql_source, representative_file_path, file_count, format_type)
    format_type: 'avro' or 'parquet'
    """
    clean_path = os.path.abspath(os.path.expanduser(raw_path))
    if not os.path.exists(clean_path):
        raise FileNotFoundError(f"Path does not exist: {clean_path}")

    if os.path.isdir(clean_path):
        # 1. Look for avro files
        avro_pattern = os.path.join(clean_path, "**", "*.avro")
        avro_matches = glob.glob(avro_pattern, recursive=True)
        if avro_matches:
            escaped_dir = clean_path.replace("'", "''")
            duckdb_source = f"read_avro('{escaped_dir}/**/*.avro')"
            return duckdb_source, avro_matches[0], len(avro_matches), "avro"

        # 2. Look for parquet files
        pattern = os.path.join(clean_path, "**", "*.parquet")
        matches = glob.glob(pattern, recursive=True)
        if not matches:
            pattern_part = os.path.join(clean_path, "**", "part-*")
            matches = [m for m in glob.glob(pattern_part, recursive=True) if not os.path.isdir(m)]
        
        if not matches:
            raise ValueError(f"No parquet or avro files found in directory: {clean_path}")

        escaped_dir = clean_path.replace("'", "''")
        duckdb_source = f"read_parquet('{escaped_dir}/**/*.parquet', hive_partitioning=1)"
        return duckdb_source, matches[0], len(matches), "parquet"
    else:
        escaped_file = clean_path.replace("'", "''")
        if clean_path.lower().endswith((".avro", ".avsc")):
            duckdb_source = f"read_avro('{escaped_file}')"
            return duckdb_source, clean_path, 1, "avro"
        else:
            duckdb_source = f"read_parquet('{escaped_file}')"
            return duckdb_source, clean_path, 1, "parquet"


def arrow_type_to_spark_types(arrow_type: pa.DataType) -> Tuple[str, str, str]:
    """
    Convert PyArrow type to Spark DDL, PySpark type, Scala Spark type.
    """
    if pa.types.is_list(arrow_type) or isinstance(arrow_type, (pa.ListType, pa.LargeListType)):
        elem_ddl, elem_py, elem_scala = arrow_type_to_spark_types(arrow_type.value_type)
        return f"ARRAY<{elem_ddl}>", f"ArrayType({elem_py})", f"ArrayType({elem_scala})"
    elif pa.types.is_struct(arrow_type) or isinstance(arrow_type, pa.StructType):
        fields_ddl = [f"`{arrow_type.field(i).name}`: {arrow_type_to_spark_types(arrow_type.field(i).type)[0]}" for i in range(arrow_type.num_fields)]
        return f"STRUCT<{', '.join(fields_ddl)}>", "StructType(...)", "StructType(...)"
    elif pa.types.is_map(arrow_type) or isinstance(arrow_type, pa.MapType):
        k_ddl, _, _ = arrow_type_to_spark_types(arrow_type.key_type)
        v_ddl, _, _ = arrow_type_to_spark_types(arrow_type.item_type)
        return f"MAP<{k_ddl}, {v_ddl}>", "MapType(...)", "MapType(...)"

    t_str = str(arrow_type).lower()
    if "int8" in t_str or "tinyint" in t_str:
        return "BYTE", "ByteType()", "ByteType"
    elif "int16" in t_str or "smallint" in t_str:
        return "SHORT", "ShortType()", "ShortType"
    elif "int32" in t_str:
        return "INT", "IntegerType()", "IntegerType"
    elif "int64" in t_str:
        return "BIGINT", "LongType()", "LongType"
    elif "float" in t_str:
        return "FLOAT", "FloatType()", "FloatType"
    elif "double" in t_str:
        return "DOUBLE", "DoubleType()", "DoubleType"
    elif "string" in t_str or "utf8" in t_str or "large_string" in t_str:
        return "STRING", "StringType()", "StringType"
    elif "bool" in t_str:
        return "BOOLEAN", "BooleanType()", "BooleanType"
    elif "timestamp" in t_str:
        return "TIMESTAMP", "TimestampType()", "TimestampType"
    elif "date" in t_str:
        return "DATE", "DateType()", "DateType"
    elif "binary" in t_str:
        return "BINARY", "BinaryType()", "BinaryType"
    elif "decimal" in t_str:
        prec = getattr(arrow_type, 'precision', 38)
        scale = getattr(arrow_type, 'scale', 10)
        return f"DECIMAL({prec}, {scale})", f"DecimalType({prec}, {scale})", f"DecimalType({prec}, {scale})"
    else:
        return str(arrow_type).upper(), "StringType()", "StringType"


def get_info(path: str) -> Dict[str, Any]:
    t0 = time.time()
    duckdb_source, sample_file, file_count, format_type = resolve_source(path)
    con = duckdb.connect(database=":memory:")
    
    is_dir = os.path.isdir(os.path.abspath(os.path.expanduser(path)))
    part_files = []
    if is_dir:
        abs_dir = os.path.abspath(os.path.expanduser(path))
        total_size = 0
        exts = (".avro", ".parquet")
        for dirpath, _, filenames in os.walk(abs_dir):
            for f in sorted(filenames):
                if f.endswith(exts) or "part-" in f:
                    fp = os.path.join(dirpath, f)
                    sz = os.path.getsize(fp)
                    total_size += sz
                    part_files.append({
                        "name": f,
                        "rel_path": os.path.relpath(fp, abs_dir),
                        "size": sz,
                        "size_formatted": human_size(sz)
                    })
    else:
        total_size = os.path.getsize(sample_file)

    # 1. AVRO / ICEBERG AVRO HANDLING
    if format_type == "avro":
        avro_meta = {}
        codec = "deflate"
        writer_schema = {}
        with open(sample_file, "rb") as fo:
            reader = fastavro.reader(fo)
            avro_meta = {k: v for k, v in reader.metadata.items() if k != "avro.schema"}
            writer_schema = reader.writer_schema
            codec = reader.codec or "deflate"

        schema_name = writer_schema.get("name", "") if isinstance(writer_schema, dict) else ""
        iceberg_type = None
        iceberg_badge = "AVRO"
        iceberg_title = "Avro 数据文件"

        if schema_name == "manifest_entry" or "schema" in avro_meta or "partition-spec" in avro_meta:
            iceberg_type = "manifest"
            iceberg_badge = "🧊 MANIFEST"
            iceberg_title = "Iceberg Manifest (清单文件)"
        elif schema_name == "manifest_file" or "snapshot-id" in avro_meta:
            iceberg_type = "manifest_list"
            iceberg_badge = "🧊 SNAPSHOT"
            iceberg_title = "Iceberg Manifest List (快照清单)"

        table_schema_json = None
        partition_spec_json = None
        if "schema" in avro_meta:
            try:
                table_schema_json = json.loads(avro_meta["schema"])
            except Exception:
                table_schema_json = avro_meta["schema"]

        if "partition-spec" in avro_meta:
            try:
                partition_spec_json = json.loads(avro_meta["partition-spec"])
            except Exception:
                partition_spec_json = avro_meta["partition-spec"]

        iceberg_info = {
            "is_iceberg": bool(iceberg_type),
            "type": iceberg_type,
            "badge": iceberg_badge,
            "title": iceberg_title,
            "format_version": avro_meta.get("format-version", "2"),
            "snapshot_id": avro_meta.get("snapshot-id"),
            "parent_snapshot_id": avro_meta.get("parent-snapshot-id"),
            "sequence_number": avro_meta.get("sequence-number"),
            "partition_spec_id": avro_meta.get("partition-spec-id"),
            "content": avro_meta.get("content"),
            "table_schema": table_schema_json,
            "partition_spec": partition_spec_json
        }

        # Query Avro table schema using DuckDB
        arrow_sample = con.execute(f"SELECT * FROM {duckdb_source} LIMIT 1").fetch_arrow_table()
        schema_arrow = arrow_sample.schema

        try:
            total_rows = con.execute(f"SELECT count(*) FROM {duckdb_source}").fetchone()[0]
        except Exception:
            total_rows = 0

        columns_info = []
        spark_ddl_parts = []
        pyspark_fields = []

        for i in range(len(schema_arrow)):
            field = schema_arrow.field(i)
            name = field.name
            arrow_type = field.type
            spark_ddl, pyspark_t, scala_t = arrow_type_to_spark_types(arrow_type)

            columns_info.append({
                "index": i,
                "name": name,
                "type": str(arrow_type),
                "spark_ddl_type": spark_ddl,
                "pyspark_type": pyspark_t,
                "scala_type": scala_t,
                "nullable": field.nullable,
                "compression": codec.upper()
            })
            spark_ddl_parts.append(f"  `{name}` {spark_ddl}")
            pyspark_fields.append(f"    StructField('{name}', {pyspark_t}, {str(field.nullable)})")

        spark_ddl_sql = "CREATE TABLE avro_table (\n" + ",\n".join(spark_ddl_parts) + "\n) USING AVRO;"
        pyspark_code = "from pyspark.sql.types import *\n\nschema = StructType([\n" + ",\n".join(pyspark_fields) + "\n])"

        # If this is an Iceberg manifest with table_schema, generate Iceberg table DDL as well
        iceberg_table_ddl = None
        if table_schema_json and isinstance(table_schema_json, dict) and "fields" in table_schema_json:
            ice_cols = []
            for f in table_schema_json["fields"]:
                c_name = f.get("name", "col")
                c_type = str(f.get("type", "string")).upper()
                ice_cols.append(f"  `{c_name}` {c_type}")
            part_str = ""
            if partition_spec_json and isinstance(partition_spec_json, list) and len(partition_spec_json) > 0:
                parts = [p.get("name", "") for p in partition_spec_json if p.get("name")]
                if parts:
                    part_str = f"\nPARTITIONED BY ({', '.join(parts)})"
            iceberg_table_ddl = f"CREATE TABLE iceberg_table (\n" + ",\n".join(ice_cols) + f"\n) USING ICEBERG{part_str};"

        elapsed = round((time.time() - t0) * 1000, 2)

        return {
            "format_type": "avro",
            "path": os.path.abspath(os.path.expanduser(path)),
            "is_dir": is_dir,
            "file_count": file_count,
            "sample_file": sample_file,
            "file_name": os.path.basename(os.path.abspath(os.path.expanduser(path))),
            "file_size_bytes": total_size,
            "file_size_formatted": human_size(total_size),
            "total_rows": total_rows,
            "num_columns": len(columns_info),
            "num_row_groups": 1,
            "created_by": f"Apache Avro ({codec})",
            "format_version": avro_meta.get("format-version", "1.0"),
            "columns": columns_info,
            "spark_ddl": spark_ddl_sql,
            "pyspark_code": pyspark_code,
            "spark_metadata": None,
            "iceberg_info": iceberg_info,
            "iceberg_table_ddl": iceberg_table_ddl,
            "row_groups": [],
            "part_files": part_files,
            "elapsed_ms": elapsed
        }

    # 2. PARQUET HANDLING
    pf = pq.ParquetFile(sample_file)
    meta = pf.metadata
    schema_arrow = pf.schema_arrow

    try:
        count_res = con.execute(f"SELECT count(*) FROM {duckdb_source}").fetchone()
        total_rows = count_res[0] if count_res else meta.num_rows
    except Exception:
        total_rows = meta.num_rows

    columns_info = []
    spark_ddl_parts = []
    pyspark_fields = []
    
    for i in range(len(schema_arrow)):
        field = schema_arrow.field(i)
        name = field.name
        arrow_type = field.type
        spark_ddl, pyspark_t, scala_t = arrow_type_to_spark_types(arrow_type)
        
        compression = "UNKNOWN"
        if meta.num_row_groups > 0:
            try:
                col_chunk = meta.row_group(0).column(i)
                compression = col_chunk.compression
            except Exception:
                pass

        columns_info.append({
            "index": i,
            "name": name,
            "type": str(arrow_type),
            "spark_ddl_type": spark_ddl,
            "pyspark_type": pyspark_t,
            "scala_type": scala_t,
            "nullable": field.nullable,
            "compression": compression
        })
        spark_ddl_parts.append(f"  `{name}` {spark_ddl}")
        pyspark_fields.append(f"    StructField('{name}', {pyspark_t}, {str(field.nullable)})")

    spark_ddl_sql = "CREATE TABLE parquet_table (\n" + ",\n".join(spark_ddl_parts) + "\n) USING PARQUET;"
    pyspark_code = "from pyspark.sql.types import *\n\nschema = StructType([\n" + ",\n".join(pyspark_fields) + "\n])"

    spark_meta = None
    if meta.metadata:
        for k, v in meta.metadata.items():
            k_str = k.decode('utf-8', errors='ignore') if isinstance(k, bytes) else str(k)
            if "org.apache.spark.sql.parquet.row.metadata" in k_str:
                try:
                    val_str = v.decode('utf-8', errors='ignore') if isinstance(v, bytes) else str(v)
                    spark_meta = json.loads(val_str)
                except Exception:
                    spark_meta = str(v)

    row_groups_summary = []
    for rg_idx in range(min(meta.num_row_groups, 10)):
        rg = meta.row_group(rg_idx)
        row_groups_summary.append({
            "index": rg_idx,
            "num_rows": rg.num_rows,
            "total_byte_size": rg.total_byte_size,
            "total_compressed_size": rg.total_compressed_size if hasattr(rg, 'total_compressed_size') else None
        })

    elapsed = round((time.time() - t0) * 1000, 2)

    return {
        "format_type": "parquet",
        "path": os.path.abspath(os.path.expanduser(path)),
        "is_dir": is_dir,
        "file_count": file_count,
        "sample_file": sample_file,
        "file_name": os.path.basename(os.path.abspath(os.path.expanduser(path))),
        "file_size_bytes": total_size,
        "file_size_formatted": human_size(total_size),
        "total_rows": total_rows,
        "num_columns": len(columns_info),
        "num_row_groups": meta.num_row_groups,
        "created_by": meta.created_by or "Unknown",
        "format_version": meta.format_version or "2.6",
        "columns": columns_info,
        "spark_ddl": spark_ddl_sql,
        "pyspark_code": pyspark_code,
        "spark_metadata": spark_meta,
        "iceberg_info": {"is_iceberg": False},
        "row_groups": row_groups_summary,
        "part_files": part_files,
        "elapsed_ms": elapsed
    }


def get_data_page(path: str, page: int = 1, page_size: int = 100, 
                  search: Optional[str] = None, 
                  sort_col: Optional[str] = None, 
                  sort_order: str = "ASC") -> Dict[str, Any]:
    t0 = time.time()
    duckdb_source, _, _, _ = resolve_source(path)
    
    con = duckdb.connect(database=":memory:")
    con.execute(f"CREATE TEMPORARY VIEW table_view AS SELECT * FROM {duckdb_source}")
    
    where_clause = ""
    col_names = [col[0] for col in con.execute("DESCRIBE table_view").fetchall()]
    
    if search and search.strip():
        kw = search.strip().replace("'", "''")
        conditions = []
        for c in col_names:
            conditions.append(f'CAST("{c}" AS VARCHAR) ILIKE \'%{kw}%\'')
        where_clause = "WHERE " + " OR ".join(conditions)

    count_sql = f"SELECT count(*) FROM table_view {where_clause}"
    filtered_total = con.execute(count_sql).fetchone()[0]
    
    order_clause = ""
    if sort_col and sort_col in col_names:
        clean_order = "DESC" if str(sort_order).upper() == "DESC" else "ASC"
        order_clause = f'ORDER BY "{sort_col}" {clean_order} NULLS LAST'

    offset = max(0, (page - 1) * page_size)
    data_sql = f'SELECT * FROM table_view {where_clause} {order_clause} LIMIT {page_size} OFFSET {offset}'
    
    arrow_tbl = con.execute(data_sql).fetch_arrow_table()
    rows = arrow_tbl.to_pylist()
    
    total_pages = max(1, (filtered_total + page_size - 1) // page_size)
    elapsed = round((time.time() - t0) * 1000, 2)

    return {
        "page": page,
        "page_size": page_size,
        "total_rows": filtered_total,
        "total_pages": total_pages,
        "columns": col_names,
        "rows": rows,
        "elapsed_ms": elapsed
    }


def run_sql_query(path: str, sql: str, limit: int = 1000) -> Dict[str, Any]:
    t0 = time.time()
    duckdb_source, _, _, _ = resolve_source(path)
    
    con = duckdb.connect(database=":memory:")
    
    # Register aliases: data, parquet_data, avro_data, t, df
    con.execute(f"CREATE TEMPORARY VIEW data AS SELECT * FROM {duckdb_source}")
    con.execute("CREATE TEMPORARY VIEW parquet_data AS SELECT * FROM data")
    con.execute("CREATE TEMPORARY VIEW avro_data AS SELECT * FROM data")
    con.execute("CREATE TEMPORARY VIEW t AS SELECT * FROM data")
    con.execute("CREATE TEMPORARY VIEW df AS SELECT * FROM data")
    
    clean_sql = sql.strip().rstrip(";")
    upper_sql = clean_sql.upper()
    if upper_sql.startswith("SELECT") and "LIMIT" not in upper_sql:
        query_sql = f"{clean_sql} LIMIT {limit}"
    else:
        query_sql = clean_sql
        
    try:
        arrow_tbl = con.execute(query_sql).fetch_arrow_table()
        col_names = arrow_tbl.column_names
        rows = arrow_tbl.to_pylist()
        row_count = len(rows)
        col_types = [str(arrow_tbl.schema.field(name).type) for name in col_names]
        elapsed = round((time.time() - t0) * 1000, 2)
        
        return {
            "success": True,
            "columns": col_names,
            "column_types": col_types,
            "rows": rows,
            "row_count": row_count,
            "elapsed_ms": elapsed,
            "executed_sql": query_sql
        }
    except Exception as e:
        elapsed = round((time.time() - t0) * 1000, 2)
        return {
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "elapsed_ms": elapsed,
            "executed_sql": query_sql
        }


def export_data(path: str, format_type: str, dest_path: str, sql: Optional[str] = None) -> Dict[str, Any]:
    t0 = time.time()
    duckdb_source, _, _, _ = resolve_source(path)
    
    con = duckdb.connect(database=":memory:")
    con.execute(f"CREATE TEMPORARY VIEW data AS SELECT * FROM {duckdb_source}")
    con.execute("CREATE TEMPORARY VIEW parquet_data AS SELECT * FROM data")
    con.execute("CREATE TEMPORARY VIEW avro_data AS SELECT * FROM data")
    con.execute("CREATE TEMPORARY VIEW t AS SELECT * FROM data")
    con.execute("CREATE TEMPORARY VIEW df AS SELECT * FROM data")
    
    query_sql = sql.strip().rstrip(";") if sql and sql.strip() else "SELECT * FROM data"
    dest_path_escaped = dest_path.replace("'", "''")
    
    fmt = format_type.lower()
    if fmt == "csv":
        export_sql = f"COPY ({query_sql}) TO '{dest_path_escaped}' (HEADER, DELIMITER ',')"
    elif fmt == "json":
        export_sql = f"COPY ({query_sql}) TO '{dest_path_escaped}' (FORMAT JSON, ARRAY true)"
    elif fmt == "parquet":
        export_sql = f"COPY ({query_sql}) TO '{dest_path_escaped}' (FORMAT PARQUET)"
    else:
        raise ValueError(f"Unsupported export format: {format_type}")
        
    con.execute(export_sql)
    elapsed = round((time.time() - t0) * 1000, 2)
    file_size = os.path.getsize(dest_path) if os.path.exists(dest_path) else 0

    return {
        "success": True,
        "format": fmt,
        "dest_path": dest_path,
        "file_size": file_size,
        "elapsed_ms": elapsed
    }


def main():
    if len(sys.argv) < 3:
        print(json.dumps({"error": "Usage: parquet_engine.py <command> <path> [args...]"}), file=sys.stderr)
        sys.exit(1)

    cmd = sys.argv[1].lower()
    path = sys.argv[2]

    try:
        if cmd == "info":
            res = get_info(path)
            print(json.dumps(res, default=safe_json_default, ensure_ascii=False))

        elif cmd == "page":
            page = int(sys.argv[3]) if len(sys.argv) > 3 else 1
            page_size = int(sys.argv[4]) if len(sys.argv) > 4 else 100
            search = sys.argv[5] if len(sys.argv) > 5 and sys.argv[5] != "__NONE__" else None
            sort_col = sys.argv[6] if len(sys.argv) > 6 and sys.argv[6] != "__NONE__" else None
            sort_order = sys.argv[7] if len(sys.argv) > 7 else "ASC"
            
            res = get_data_page(path, page, page_size, search, sort_col, sort_order)
            print(json.dumps(res, default=safe_json_default, ensure_ascii=False))

        elif cmd == "query":
            sql = sys.argv[3] if len(sys.argv) > 3 else "SELECT * FROM data LIMIT 100"
            limit = int(sys.argv[4]) if len(sys.argv) > 4 else 1000
            res = run_sql_query(path, sql, limit)
            print(json.dumps(res, default=safe_json_default, ensure_ascii=False))

        elif cmd == "export":
            format_type = sys.argv[3] if len(sys.argv) > 3 else "csv"
            dest_path = sys.argv[4] if len(sys.argv) > 4 else "/tmp/export.csv"
            sql = sys.argv[5] if len(sys.argv) > 5 and sys.argv[5] != "__NONE__" else None
            res = export_data(path, format_type, dest_path, sql)
            print(json.dumps(res, default=safe_json_default, ensure_ascii=False))

        else:
            print(json.dumps({"error": f"Unknown command: {cmd}"}), file=sys.stderr)
            sys.exit(1)

    except Exception as e:
        err_res = {
            "error": str(e),
            "traceback": traceback.format_exc()
        }
        print(json.dumps(err_res, default=safe_json_default, ensure_ascii=False))
        sys.exit(1)


if __name__ == "__main__":
    main()
