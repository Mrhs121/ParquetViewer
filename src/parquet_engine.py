#!/usr/bin/env python3
"""
parquet_engine.py
Backend engine for Parquet, Iceberg Avro & Remote Iceberg REST Catalog QuickView Mac App.
Powered by DuckDB, PyArrow (with native S3FileSystem), and FastAvro.
"""

import sys
import os

# Support bundled python dependencies inside App Bundle
_bundle_dir = os.path.dirname(os.path.abspath(__file__))
_bundled_lib = os.path.join(_bundle_dir, "python_lib")
if os.path.isdir(_bundled_lib) and _bundled_lib not in sys.path:
    sys.path.insert(0, _bundled_lib)

import io
import glob
import json
import time
import hashlib
import tempfile
import datetime
import decimal
import traceback
import concurrent.futures
import urllib.request
import urllib.parse
import urllib.error
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.fs as pafs
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


# ==============================================================================
# Remote S3 & Iceberg REST Catalog Helpers
# ==============================================================================

def get_catalog_config_from_env() -> Dict[str, Any]:
    """Load catalog configuration from PQ_CATALOG_CONFIG environment variable or cache file."""
    raw = os.environ.get("PQ_CATALOG_CONFIG", "").strip()
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass
    cfg_file = os.path.join(tempfile.gettempdir(), "pv_catalog_config.json")
    if os.path.exists(cfg_file):
        try:
            with open(cfg_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_catalog_config_cache(cfg: Dict[str, Any]) -> None:
    if not cfg:
        return
    cfg_file = os.path.join(tempfile.gettempdir(), "pv_catalog_config.json")
    try:
        with open(cfg_file, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False)
    except Exception:
        pass


def is_s3_uri(path: str) -> bool:
    p = path.strip().lower()
    return p.startswith("s3://") or p.startswith("s3a://") or p.startswith("s3n://")


def is_iceberg_uri(path: str) -> bool:
    return path.strip().lower().startswith("iceberg://")


def strip_s3_scheme(uri: str) -> str:
    for prefix in ("s3a://", "s3n://", "s3://"):
        if uri.lower().startswith(prefix):
            return uri[len(prefix):].lstrip("/")
    return uri


def strip_file_scheme(uri: str) -> str:
    if uri.lower().startswith("file://"):
        parsed = urllib.parse.urlparse(uri)
        return urllib.parse.unquote(parsed.path)
    return os.path.abspath(os.path.expanduser(uri))


def get_s3_filesystem(catalog_config: Optional[Dict[str, Any]] = None,
                      table_config: Optional[Dict[str, Any]] = None) -> pafs.S3FileSystem:
    """Create a PyArrow S3FileSystem from user config and/or table config."""
    cfg = catalog_config or get_catalog_config_from_env()
    t_cfg = table_config or {}

    # User-configured S3 endpoint always takes priority over cluster-internal endpoints (e.g. minio.svc.cluster.local)
    endpoint = (
        cfg.get("s3_endpoint")
        or t_cfg.get("s3.endpoint")
        or os.environ.get("AWS_S3_ENDPOINT")
        or ""
    ).strip()
    ak = (
        cfg.get("s3_access_key")
        or cfg.get("s3_ak")
        or t_cfg.get("s3.access-key-id")
        or os.environ.get("AWS_ACCESS_KEY_ID")
        or ""
    ).strip()
    sk = (
        cfg.get("s3_secret_key")
        or cfg.get("s3_sk")
        or t_cfg.get("s3.secret-access-key")
        or os.environ.get("AWS_SECRET_ACCESS_KEY")
        or ""
    ).strip()
    session_token = (
        cfg.get("s3_session_token")
        or t_cfg.get("s3.session-token")
        or os.environ.get("AWS_SESSION_TOKEN")
        or ""
    ).strip()
    region = (
        cfg.get("s3_region")
        or t_cfg.get("s3.region")
        or t_cfg.get("client.region")
        or os.environ.get("AWS_REGION")
        or "us-east-1"
    ).strip()

    scheme = "https"
    endpoint_override = None
    if endpoint:
        if endpoint.lower().startswith("http://"):
            scheme = "http"
            endpoint_override = endpoint[len("http://"):].rstrip("/")
        elif endpoint.lower().startswith("https://"):
            scheme = "https"
            endpoint_override = endpoint[len("https://"):].rstrip("/")
        else:
            scheme = "http"
            endpoint_override = endpoint.rstrip("/")

    kwargs: Dict[str, Any] = {
        "scheme": scheme,
        "region": region or "us-east-1",
        "force_virtual_addressing": False,
        "connect_timeout": 8.0,
        "request_timeout": 30.0,
    }
    if endpoint_override:
        kwargs["endpoint_override"] = endpoint_override
    if ak:
        kwargs["access_key"] = ak
    if sk:
        kwargs["secret_key"] = sk
    if session_token:
        kwargs["session_token"] = session_token

    return pafs.S3FileSystem(**kwargs)


def open_uri_bytes(uri: str, s3_fs: Optional[pafs.S3FileSystem] = None) -> bytes:
    """Read raw bytes from either s3://, file://, http(s)://, or local path."""
    if is_s3_uri(uri):
        fs = s3_fs or get_s3_filesystem()
        s3_key = strip_s3_scheme(uri)
        with fs.open_input_stream(s3_key) as stream:
            return stream.read()
    elif uri.lower().startswith(("http://", "https://")):
        req = urllib.request.Request(uri, headers={"Accept": "*/*"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.read()
    else:
        local_path = strip_file_scheme(uri)
        with open(local_path, "rb") as f:
            return f.read()


def cache_s3_file_locally(uri: str, s3_fs: Optional[pafs.S3FileSystem] = None, ext: str = ".avro") -> str:
    """Download a remote S3 file (such as an Avro manifest) to local temp dir for DuckDB."""
    cache_dir = os.path.join(tempfile.gettempdir(), "pv_s3_cache")
    os.makedirs(cache_dir, exist_ok=True)
    digest = hashlib.md5(uri.encode("utf-8")).hexdigest()
    base_name = os.path.basename(uri.rstrip("/")) or f"file{ext}"
    safe_base = "".join(c for c in base_name if c.isalnum() or c in ("-", "_", "."))
    local_file = os.path.join(cache_dir, f"{digest}_{safe_base}")
    if not os.path.exists(local_file) or os.path.getsize(local_file) == 0:
        data = open_uri_bytes(uri, s3_fs)
        with open(local_file, "wb") as f:
            f.write(data)
    return local_file


def rest_api_get(url: str, headers: Optional[Dict[str, str]] = None, timeout: float = 10.0) -> Dict[str, Any]:
    req_headers = {"Accept": "application/json"}
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(url, headers=req_headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as e:
        err_body = ""
        try:
            err_body = e.read().decode("utf-8", errors="ignore")
        except Exception:
            pass
        raise RuntimeError(f"REST 请求失败 ({e.code} {e.reason}): {url}\n{err_body[:300]}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"无法连接到 Iceberg REST Catalog ({url}): {e.reason}")


def resolve_rest_base_and_prefix(catalog_config: Dict[str, Any], use_cache: bool = True) -> Tuple[str, str, Dict[str, str], Dict[str, Any]]:
    """
    Returns (base_v1_url, prefix_path, auth_headers, server_defaults)
    base_v1_url is e.g. 'http://localhost:8181/v1'
    prefix_path is either '' or '/<prefix>'
    """
    raw_uri = (catalog_config.get("rest_uri") or "").strip().rstrip("/")
    if not raw_uri:
        raise ValueError("未配置 Iceberg REST Catalog 地址 (rest_uri)")
    if not raw_uri.lower().startswith(("http://", "https://")):
        raw_uri = "http://" + raw_uri

    if raw_uri.endswith("/v1"):
        base_v1 = raw_uri
    else:
        base_v1 = raw_uri + "/v1"

    headers: Dict[str, str] = {}
    token = (catalog_config.get("rest_token") or "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"

    user_prefix = (catalog_config.get("warehouse") or catalog_config.get("prefix") or "").strip().strip("/")
    cache_key = hashlib.md5(f"{base_v1}|{user_prefix}".encode("utf-8")).hexdigest()
    cfg_cache_file = os.path.join(tempfile.gettempdir(), f"pv_rest_cfg_{cache_key}.json")

    if use_cache and os.path.exists(cfg_cache_file):
        try:
            if time.time() - os.path.getmtime(cfg_cache_file) < 300:
                with open(cfg_cache_file, "r", encoding="utf-8") as f:
                    cached = json.load(f)
                return base_v1, cached.get("prefix_path", ""), headers, cached.get("server_props", {})
        except Exception:
            pass

    discovered_prefix = user_prefix
    server_props: Dict[str, Any] = {}

    try:
        cfg_url = f"{base_v1}/config"
        if user_prefix:
            cfg_url += f"?warehouse={urllib.parse.quote(user_prefix)}"
        cfg_resp = rest_api_get(cfg_url, headers=headers, timeout=6.0)
        defaults = cfg_resp.get("defaults") or {}
        overrides = cfg_resp.get("overrides") or {}
        server_props.update(defaults)
        server_props.update(overrides)
        if "prefix" in overrides and overrides["prefix"]:
            discovered_prefix = str(overrides["prefix"]).strip("/")
        elif "prefix" in defaults and defaults["prefix"]:
            discovered_prefix = str(defaults["prefix"]).strip("/")
    except Exception:
        pass

    prefix_path = f"/{urllib.parse.quote(discovered_prefix)}" if discovered_prefix else ""
    try:
        with open(cfg_cache_file, "w", encoding="utf-8") as f:
            json.dump({"prefix_path": prefix_path, "server_props": server_props}, f)
    except Exception:
        pass

    return base_v1, prefix_path, headers, server_props


def catalog_list_namespaces_and_tables(catalog_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    t0 = time.time()
    cfg = catalog_config or get_catalog_config_from_env()
    save_catalog_config_cache(cfg)

    base_v1, prefix_path, headers, server_props = resolve_rest_base_and_prefix(cfg, use_cache=False)
    ns_url = f"{base_v1}{prefix_path}/namespaces"
    ns_resp = rest_api_get(ns_url, headers=headers)

    raw_namespaces = ns_resp.get("namespaces") or []

    def fetch_ns_tables(ns_item: Any) -> Dict[str, Any]:
        if isinstance(ns_item, list):
            ns_parts = [str(p) for p in ns_item]
        else:
            ns_parts = [str(ns_item)]
        ns_dot = ".".join(ns_parts)
        ns_encoded = urllib.parse.quote("\x1f".join(ns_parts))

        tables = []
        tbl_resp = {}
        try:
            tbl_url = f"{base_v1}{prefix_path}/namespaces/{ns_encoded}/tables"
            tbl_resp = rest_api_get(tbl_url, headers=headers, timeout=8.0)
        except Exception:
            if ns_encoded != urllib.parse.quote(ns_dot):
                try:
                    tbl_url = f"{base_v1}{prefix_path}/namespaces/{urllib.parse.quote(ns_dot)}/tables"
                    tbl_resp = rest_api_get(tbl_url, headers=headers, timeout=8.0)
                except Exception:
                    tbl_resp = {}

        for ident in (tbl_resp.get("identifiers") or []):
            t_name = ident.get("name", "")
            if t_name:
                tables.append({
                    "name": t_name,
                    "namespace": ns_dot,
                    "full_name": f"{ns_dot}.{t_name}",
                    "uri": f"iceberg://{ns_dot}/{t_name}"
                })

        tables.sort(key=lambda t: t["name"].lower())
        return {
            "namespace": ns_dot,
            "namespace_parts": ns_parts,
            "tables": tables
        }

    # Fetch tables for all namespaces concurrently
    max_workers = min(16, max(1, len(raw_namespaces)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        namespaces_list = list(executor.map(fetch_ns_tables, raw_namespaces))

    # Sort namespaces: 'default' first, then namespaces with tables descending by count, then alphabetically
    def ns_sort_key(item: Dict[str, Any]) -> Tuple[int, int, str]:
        ns_name = item["namespace"].lower()
        is_not_default = 0 if ns_name == "default" else 1
        has_no_tables = 0 if len(item["tables"]) > 0 else 1
        return (is_not_default, has_no_tables, ns_name)

    namespaces_list.sort(key=ns_sort_key)

    elapsed = round((time.time() - t0) * 1000, 2)
    return {
        "success": True,
        "rest_uri": cfg.get("rest_uri"),
        "prefix": prefix_path.lstrip("/"),
        "namespaces": namespaces_list,
        "server_properties": server_props,
        "elapsed_ms": elapsed
    }


def parse_iceberg_uri(uri: str) -> Tuple[str, str, Optional[str]]:
    """
    Parse 'iceberg://<namespace>/<table_name>[@<snapshot_id>]'
    Returns (namespace_str, table_name, snapshot_id_or_None)
    """
    body = uri.strip()[len("iceberg://"):]
    snap_id = None
    if "@" in body:
        body, snap_id = body.rsplit("@", 1)
    parts = [p for p in body.split("/") if p]
    if len(parts) < 2:
        raise ValueError(f"无效的 Iceberg 表 URI (格式应为 iceberg://namespace/table): {uri}")
    table_name = parts[-1]
    namespace = ".".join(parts[:-1])
    return namespace, table_name, snap_id


def iceberg_type_to_arrow(ice_type: Any) -> pa.DataType:
    """Convert an Iceberg JSON schema field type to a PyArrow DataType."""
    if isinstance(ice_type, dict):
        t_kind = ice_type.get("type", "")
        if t_kind == "struct":
            sub_fields = [
                pa.field(f.get("name", f"f_{idx}"), iceberg_type_to_arrow(f.get("type", "string")), nullable=not f.get("required", False))
                for idx, f in enumerate(ice_type.get("fields", []))
            ]
            return pa.struct(sub_fields)
        elif t_kind == "list":
            return pa.list_(iceberg_type_to_arrow(ice_type.get("element", "string")))
        elif t_kind == "map":
            return pa.map_(
                iceberg_type_to_arrow(ice_type.get("key", "string")),
                iceberg_type_to_arrow(ice_type.get("value", "string"))
            )
        return pa.string()

    t_str = str(ice_type).lower().strip()
    if t_str == "boolean":
        return pa.bool_()
    elif t_str == "int":
        return pa.int32()
    elif t_str == "long":
        return pa.int64()
    elif t_str == "float":
        return pa.float32()
    elif t_str == "double":
        return pa.float64()
    elif t_str == "date":
        return pa.date32()
    elif t_str in ("timestamp", "timestamptz"):
        return pa.timestamp("us")
    elif t_str in ("binary", "uuid") or t_str.startswith("fixed"):
        return pa.binary()
    elif t_str.startswith("decimal"):
        try:
            inner = t_str[t_str.index("(") + 1:t_str.index(")")]
            p, s = [int(x.strip()) for x in inner.split(",")]
            return pa.decimal128(p, s)
        except Exception:
            return pa.decimal128(38, 10)
    return pa.string()


def iceberg_schema_to_arrow_schema(table_schema: Dict[str, Any]) -> pa.Schema:
    fields = []
    for f in (table_schema.get("fields") or []):
        pa_t = iceberg_type_to_arrow(f.get("type", "string"))
        fields.append(pa.field(f.get("name", "col"), pa_t, nullable=not f.get("required", False)))
    return pa.schema(fields)


def resolve_iceberg_table_plan(uri: str, force_refresh: bool = False) -> Dict[str, Any]:
    """
    Fetch Iceberg table metadata from REST Catalog and resolve active data files & delete files
    from the snapshot's manifest-list and manifests.
    Uses a short-lived local cache (60s) so pagination/sorting doesn't re-read manifests repeatedly.
    """
    cfg = get_catalog_config_from_env()
    save_catalog_config_cache(cfg)

    cache_key = hashlib.md5((uri + "|" + json.dumps(cfg, sort_keys=True)).encode("utf-8")).hexdigest()
    cache_file = os.path.join(tempfile.gettempdir(), f"pv_ice_plan_{cache_key}.json")

    if not force_refresh and os.path.exists(cache_file):
        try:
            if time.time() - os.path.getmtime(cache_file) < 60:
                with open(cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception:
            pass

    namespace, table_name, requested_snap_id = parse_iceberg_uri(uri)
    base_v1, prefix_path, headers, server_props = resolve_rest_base_and_prefix(cfg, use_cache=True)

    ns_parts = namespace.split(".")
    ns_encoded = urllib.parse.quote("\x1f".join(ns_parts))
    tbl_encoded = urllib.parse.quote(table_name)

    try:
        tbl_url = f"{base_v1}{prefix_path}/namespaces/{ns_encoded}/tables/{tbl_encoded}"
        load_result = rest_api_get(tbl_url, headers=headers)
    except Exception:
        if ns_encoded != urllib.parse.quote(namespace):
            tbl_url = f"{base_v1}{prefix_path}/namespaces/{urllib.parse.quote(namespace)}/tables/{tbl_encoded}"
            load_result = rest_api_get(tbl_url, headers=headers)
        else:
            raise

    metadata = load_result.get("metadata") or load_result
    metadata_location = load_result.get("metadata-location") or ""
    table_config = {**server_props, **(load_result.get("config") or {})}

    s3_fs = get_s3_filesystem(cfg, table_config)

    # Current Schema
    current_schema_id = metadata.get("current-schema-id", 0)
    schemas = metadata.get("schemas") or []
    current_schema = None
    for s in schemas:
        if s.get("schema-id") == current_schema_id:
            current_schema = s
            break
    if not current_schema and schemas:
        current_schema = schemas[-1]
    if not current_schema and metadata.get("schema"):
        current_schema = metadata.get("schema")
    if not current_schema:
        current_schema = {"schema-id": 0, "fields": []}

    # Partition Spec
    default_spec_id = metadata.get("default-spec-id", 0)
    partition_specs = metadata.get("partition-specs") or []
    current_spec_fields = []
    for sp in partition_specs:
        if sp.get("spec-id") == default_spec_id:
            current_spec_fields = sp.get("fields") or []
            break
    if not current_spec_fields and metadata.get("partition-spec"):
        current_spec_fields = metadata.get("partition-spec") or []

    # Snapshots
    current_snap_id = metadata.get("current-snapshot-id")
    if current_snap_id is not None and int(current_snap_id) == -1:
        current_snap_id = None

    target_snap_id = requested_snap_id if requested_snap_id else (str(current_snap_id) if current_snap_id is not None else None)
    snapshots_raw = metadata.get("snapshots") or []
    target_snapshot = None
    snapshots_summary = []

    for snap in snapshots_raw:
        sid = str(snap.get("snapshot-id", ""))
        ts_ms = snap.get("timestamp-ms")
        ts_iso = datetime.datetime.fromtimestamp(ts_ms / 1000.0).strftime("%Y-%m-%d %H:%M:%S") if ts_ms else "-"
        summary = snap.get("summary") or {}
        is_cur = (str(current_snap_id) == sid)
        snapshots_summary.append({
            "snapshot_id": sid,
            "parent_snapshot_id": str(snap.get("parent-snapshot-id")) if snap.get("parent-snapshot-id") is not None else None,
            "sequence_number": snap.get("sequence-number"),
            "timestamp_ms": ts_ms,
            "timestamp_formatted": ts_iso,
            "operation": summary.get("operation", "append"),
            "total_records": summary.get("total-records"),
            "total_data_files": summary.get("total-data-files"),
            "manifest_list": snap.get("manifest-list", ""),
            "is_current": is_cur,
            "is_selected": (target_snap_id == sid)
        })
        if target_snap_id and sid == target_snap_id:
            target_snapshot = snap

    if not target_snapshot and snapshots_raw and current_snap_id is not None:
        target_snapshot = snapshots_raw[-1]

    manifests_info = []
    data_files_info = []
    delete_files_info = []

    if target_snapshot and target_snapshot.get("manifest-list"):
        ml_uri = target_snapshot["manifest-list"]
        ml_bytes = open_uri_bytes(ml_uri, s3_fs)
        with io.BytesIO(ml_bytes) as bio:
            ml_reader = fastavro.reader(bio)
            manifest_records = list(ml_reader)

        # Sort manifests by sequence_number descending so newest schema files come first
        manifest_records.sort(key=lambda m: m.get("sequence_number") or 0, reverse=True)

        def read_single_manifest(m_rec: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
            m_path = m_rec.get("manifest_path", "")
            m_len = m_rec.get("manifest_length", 0)
            m_content = m_rec.get("content", 0)  # 0=DATA, 1=DELETES
            m_info = {
                "manifest_path": m_path,
                "name": os.path.basename(m_path.rstrip("/")),
                "length": m_len,
                "size_formatted": human_size(m_len),
                "content": "DELETES" if m_content == 1 else "DATA",
                "sequence_number": m_rec.get("sequence_number"),
                "added_files_count": m_rec.get("added_files_count", 0),
                "existing_files_count": m_rec.get("existing_files_count", 0),
                "deleted_files_count": m_rec.get("deleted_files_count", 0)
            }
            d_files = []
            del_files = []
            if m_path:
                m_bytes = open_uri_bytes(m_path, s3_fs)
                with io.BytesIO(m_bytes) as mbio:
                    m_reader = fastavro.reader(mbio)
                    for entry in m_reader:
                        if entry.get("status", 1) == 2:
                            continue
                        df = entry.get("data_file") or {}
                        df_path = df.get("file_path", "")
                        if not df_path:
                            continue
                        df_content = df.get("content", m_content)
                        df_size = df.get("file_size_in_bytes", 0)
                        df_records = df.get("record_count", 0)
                        df_format = df.get("file_format", "PARQUET")
                        df_partition = df.get("partition") or {}
                        f_obj = {
                            "name": os.path.basename(df_path.rstrip("/")),
                            "rel_path": df_path,
                            "full_path": df_path,
                            "size": df_size,
                            "size_formatted": human_size(df_size),
                            "record_count": df_records,
                            "file_format": df_format,
                            "partition": df_partition,
                            "content": df_content
                        }
                        if df_content == 0:
                            d_files.append(f_obj)
                        else:
                            del_files.append(f_obj)
            return m_info, d_files, del_files

        if len(manifest_records) == 1:
            m_results = [read_single_manifest(manifest_records[0])]
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(manifest_records))) as ex:
                m_results = list(ex.map(read_single_manifest, manifest_records))

        for m_info, d_files, del_files in m_results:
            manifests_info.append(m_info)
            data_files_info.extend(d_files)
            delete_files_info.extend(del_files)

    plan = {
        "uri": uri,
        "namespace": namespace,
        "table_name": table_name,
        "full_name": f"{namespace}.{table_name}",
        "table_uuid": metadata.get("table-uuid", ""),
        "location": metadata.get("location", ""),
        "metadata_location": metadata_location,
        "format_version": metadata.get("format-version", 2),
        "current_snapshot_id": str(current_snap_id) if current_snap_id is not None else None,
        "selected_snapshot_id": str(target_snapshot.get("snapshot-id")) if target_snapshot else None,
        "parent_snapshot_id": str(target_snapshot.get("parent-snapshot-id")) if (target_snapshot and target_snapshot.get("parent-snapshot-id") is not None) else None,
        "sequence_number": target_snapshot.get("sequence-number") if target_snapshot else metadata.get("last-sequence-number"),
        "manifest_list": target_snapshot.get("manifest-list", "") if target_snapshot else "",
        "partition_spec_id": default_spec_id,
        "table_schema": current_schema,
        "partition_spec": current_spec_fields,
        "properties": metadata.get("properties") or {},
        "snapshots": snapshots_summary,
        "manifests": manifests_info,
        "data_files": data_files_info,
        "delete_files": delete_files_info,
        "table_config": table_config
    }

    try:
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(plan, f, default=safe_json_default, ensure_ascii=False)
    except Exception:
        pass

    return plan


def build_iceberg_arrow_source(plan: Dict[str, Any], subset_data_files: Optional[List[Dict[str, Any]]] = None) -> Any:
    """
    Build a PyArrow Dataset or Table from an Iceberg table plan.
    Supports both S3 and local files, empty tables, and Iceberg v2 Position Deletes.
    """
    cfg = get_catalog_config_from_env()
    s3_fs = get_s3_filesystem(cfg, plan.get("table_config"))
    data_files = subset_data_files if subset_data_files is not None else (plan.get("data_files") or [])
    delete_files = plan.get("delete_files") or []

    if not data_files:
        schema = iceberg_schema_to_arrow_schema(plan.get("table_schema") or {})
        return pa.Table.from_pylist([], schema=schema)

    first_path = data_files[0]["full_path"]
    use_s3 = is_s3_uri(first_path)
    fs: pafs.FileSystem = s3_fs if use_s3 else pafs.LocalFileSystem()

    def normalize_path(p: str) -> str:
        return strip_s3_scheme(p) if use_s3 else strip_file_scheme(p)

    # Check for Iceberg v2 Position Deletes (content == 1)
    pos_delete_files = [df for df in delete_files if df.get("content") == 1]
    deleted_positions: Dict[str, set] = {}

    if pos_delete_files:
        try:
            del_paths = [normalize_path(df["full_path"]) for df in pos_delete_files]
            del_ds = ds.dataset(del_paths, filesystem=fs, format="parquet")
            del_tbl = del_ds.to_table(columns=["file_path", "pos"])
            for row in del_tbl.to_pylist():
                fp = row.get("file_path")
                pos = row.get("pos")
                if fp is not None and pos is not None:
                    deleted_positions.setdefault(fp, set()).add(int(pos))
        except Exception:
            pass

    if not deleted_positions:
        norm_paths = [normalize_path(df["full_path"]) for df in data_files]
        return ds.dataset(norm_paths, filesystem=fs, format="parquet")

    # Apply position deletes per file
    tables = []
    for df in data_files:
        raw_fp = df["full_path"]
        norm_fp = normalize_path(raw_fp)
        single_ds = ds.dataset([norm_fp], filesystem=fs, format="parquet")
        tbl = single_ds.to_table()
        del_set = deleted_positions.get(raw_fp)
        if del_set:
            keep_indices = [i for i in range(tbl.num_rows) if i not in del_set]
            tbl = tbl.take(keep_indices)
        tables.append(tbl)

    if not tables:
        return pa.Table.from_pylist([])
    return pa.concat_tables(tables, promote_options="default")


def register_source_in_duckdb(con: duckdb.DuckDBPyConnection, raw_path: str, view_name: str = "data") -> None:
    """
    Register any supported source (local file/dir, s3:// file, or iceberg:// table)
    as a temporary view in DuckDB.
    """
    if is_iceberg_uri(raw_path):
        plan = resolve_iceberg_table_plan(raw_path)
        arrow_obj = build_iceberg_arrow_source(plan)
        con.register("_iceberg_source_obj", arrow_obj)
        con.execute(f"CREATE TEMPORARY VIEW {view_name} AS SELECT * FROM _iceberg_source_obj")
        return

    if is_s3_uri(raw_path):
        s3_fs = get_s3_filesystem()
        if raw_path.lower().endswith((".avro", ".avsc")):
            local_cached = cache_s3_file_locally(raw_path, s3_fs, ext=".avro")
            escaped = local_cached.replace("'", "''")
            con.execute(f"CREATE TEMPORARY VIEW {view_name} AS SELECT * FROM read_avro('{escaped}')")
        else:
            s3_key = strip_s3_scheme(raw_path)
            dataset = ds.dataset([s3_key], filesystem=s3_fs, format="parquet")
            con.register("_s3_parquet_obj", dataset)
            con.execute(f"CREATE TEMPORARY VIEW {view_name} AS SELECT * FROM _s3_parquet_obj")
        return

    duckdb_source, _, _, _ = resolve_source(raw_path)
    con.execute(f"CREATE TEMPORARY VIEW {view_name} AS SELECT * FROM {duckdb_source}")


def resolve_source(raw_path: str) -> Tuple[str, str, int, str]:
    """
    Given a local file or directory path:
    Returns (duckdb_sql_source, representative_file_path, file_count, format_type)
    format_type: 'avro' or 'parquet'
    """
    clean_path = strip_file_scheme(raw_path)
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


def get_iceberg_table_info(uri: str) -> Dict[str, Any]:
    """
    Get comprehensive metadata and schema info for a remote Iceberg REST Catalog table.
    Uses Iceberg metadata & manifest statistics directly so no full S3 Parquet scan is needed!
    """
    t0 = time.time()
    plan = resolve_iceberg_table_plan(uri, force_refresh=True)
    data_files = plan.get("data_files") or []
    delete_files = plan.get("delete_files") or []
    table_schema_json = plan.get("table_schema") or {}

    # Derive Arrow schema directly from Iceberg schema (0 extra S3 Parquet reads!)
    schema_arrow = iceberg_schema_to_arrow_schema(table_schema_json)
    if len(schema_arrow) == 0 and data_files:
        arrow_obj = build_iceberg_arrow_source(plan, subset_data_files=data_files[:1])
        schema_arrow = arrow_obj.schema

    if delete_files:
        con = duckdb.connect(database=":memory:")
        arrow_obj = build_iceberg_arrow_source(plan)
        con.register("_iceberg_source_obj", arrow_obj)
        total_rows = con.execute("SELECT count(*) FROM _iceberg_source_obj").fetchone()[0]
    else:
        total_rows = sum(df.get("record_count", 0) for df in data_files)

    total_size = sum(df.get("size", 0) for df in data_files)

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
            "compression": "PARQUET (ICEBERG)"
        })
        spark_ddl_parts.append(f"  `{name}` {spark_ddl}")
        pyspark_fields.append(f"    StructField('{name}', {pyspark_t}, {str(field.nullable)})")

    partition_spec_json = plan.get("partition_spec") or []
    full_name = plan.get("full_name", "iceberg_table")

    part_str = ""
    if partition_spec_json:
        parts = []
        for p in partition_spec_json:
            p_name = p.get("name", "")
            p_trans = p.get("transform", "identity")
            if p_name:
                parts.append(p_name if p_trans == "identity" else f"{p_trans}({p_name})")
        if parts:
            part_str = f"\nPARTITIONED BY ({', '.join(parts)})"

    loc_str = f"\nLOCATION '{plan.get('location')}'" if plan.get("location") else ""
    iceberg_table_ddl = (
        f"CREATE TABLE {full_name} (\n"
        + ",\n".join(spark_ddl_parts)
        + f"\n) USING ICEBERG{part_str}{loc_str};"
    )
    pyspark_code = "from pyspark.sql.types import *\n\nschema = StructType([\n" + ",\n".join(pyspark_fields) + "\n])"

    iceberg_info = {
        "is_iceberg": True,
        "type": "catalog_table",
        "badge": "🧊 ICEBERG TABLE",
        "title": "Iceberg 远程数据表 (REST Catalog)",
        "namespace": plan.get("namespace"),
        "table_name": plan.get("table_name"),
        "full_name": full_name,
        "table_uuid": plan.get("table_uuid"),
        "location": plan.get("location"),
        "metadata_location": plan.get("metadata_location"),
        "format_version": str(plan.get("format_version", "2")),
        "snapshot_id": plan.get("selected_snapshot_id"),
        "current_snapshot_id": plan.get("current_snapshot_id"),
        "parent_snapshot_id": plan.get("parent_snapshot_id"),
        "sequence_number": plan.get("sequence_number"),
        "manifest_list": plan.get("manifest_list"),
        "partition_spec_id": plan.get("partition_spec_id"),
        "table_schema": table_schema_json,
        "partition_spec": partition_spec_json,
        "snapshots": plan.get("snapshots") or [],
        "manifests": plan.get("manifests") or [],
        "properties": plan.get("properties") or {},
        "delete_files_count": len(delete_files)
    }

    elapsed = round((time.time() - t0) * 1000, 2)

    return {
        "format_type": "iceberg_table",
        "path": uri,
        "is_dir": len(data_files) > 1,
        "file_count": len(data_files),
        "sample_file": data_files[0]["full_path"] if data_files else plan.get("location", uri),
        "file_name": full_name,
        "file_size_bytes": total_size,
        "file_size_formatted": human_size(total_size),
        "total_rows": total_rows,
        "num_columns": len(columns_info),
        "num_row_groups": len(data_files),
        "created_by": f"Iceberg REST Catalog (v{plan.get('format_version', 2)})",
        "format_version": str(plan.get("format_version", "2")),
        "columns": columns_info,
        "spark_ddl": iceberg_table_ddl,
        "pyspark_code": pyspark_code,
        "spark_metadata": plan.get("properties") or None,
        "iceberg_info": iceberg_info,
        "iceberg_table_ddl": iceberg_table_ddl,
        "row_groups": [],
        "part_files": data_files,
        "elapsed_ms": elapsed
    }


def get_info(path: str) -> Dict[str, Any]:
    t0 = time.time()

    # 0. REMOTE ICEBERG CATALOG TABLE HANDLING
    if is_iceberg_uri(path):
        return get_iceberg_table_info(path)

    con = duckdb.connect(database=":memory:")

    # Handle remote S3 single file (.avro or .parquet) vs local file/dir
    if is_s3_uri(path):
        s3_fs = get_s3_filesystem()
        s3_key = strip_s3_scheme(path)
        try:
            f_info = s3_fs.get_file_info(s3_key)
            total_size = f_info.size or 0
        except Exception:
            total_size = 0
        is_dir = False
        file_count = 1
        part_files = []
        if path.lower().endswith((".avro", ".avsc")):
            format_type = "avro"
            sample_file = cache_s3_file_locally(path, s3_fs, ext=".avro")
            escaped = sample_file.replace("'", "''")
            duckdb_source = f"read_avro('{escaped}')"
        else:
            format_type = "parquet"
            sample_file = path
            dataset = ds.dataset([s3_key], filesystem=s3_fs, format="parquet")
            con.register("_s3_parquet_obj", dataset)
            duckdb_source = "_s3_parquet_obj"
        display_path = path
        display_name = os.path.basename(path.rstrip("/"))
    else:
        duckdb_source, sample_file, file_count, format_type = resolve_source(path)
        clean_local = strip_file_scheme(path)
        is_dir = os.path.isdir(clean_local)
        part_files = []
        if is_dir:
            total_size = 0
            exts = (".avro", ".parquet")
            for dirpath, _, filenames in os.walk(clean_local):
                for f in sorted(filenames):
                    if f.endswith(exts) or "part-" in f:
                        fp = os.path.join(dirpath, f)
                        sz = os.path.getsize(fp)
                        total_size += sz
                        part_files.append({
                            "name": f,
                            "rel_path": os.path.relpath(fp, clean_local),
                            "full_path": fp,
                            "size": sz,
                            "size_formatted": human_size(sz)
                        })
        else:
            total_size = os.path.getsize(sample_file)
        display_path = clean_local
        display_name = os.path.basename(clean_local)

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
            "path": display_path,
            "is_dir": is_dir,
            "file_count": file_count,
            "sample_file": sample_file,
            "file_name": display_name,
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

    # 2. PARQUET HANDLING (Local or S3)
    if is_s3_uri(sample_file):
        s3_fs = get_s3_filesystem()
        pf = pq.ParquetFile(strip_s3_scheme(sample_file), filesystem=s3_fs)
    else:
        pf = pq.ParquetFile(sample_file)
    meta = pf.metadata
    schema_arrow = pf.schema_arrow

    if not is_dir:
        total_rows = meta.num_rows
    else:
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
        "path": display_path,
        "is_dir": is_dir,
        "file_count": file_count,
        "sample_file": sample_file,
        "file_name": display_name,
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
    con = duckdb.connect(database=":memory:")

    has_search = bool(search and search.strip())
    has_sort = bool(sort_col and sort_col.strip())

    # Fast path for remote Iceberg tables when no search or sort is active:
    # Prune data files using manifest record_count so we only read the S3 Parquet file(s) needed for this page!
    if is_iceberg_uri(path) and not has_search and not has_sort:
        plan = resolve_iceberg_table_plan(path)
        data_files = plan.get("data_files") or []
        delete_files = plan.get("delete_files") or []
        if not delete_files:
            filtered_total = sum(df.get("record_count", 0) for df in data_files)
            total_pages = max(1, (filtered_total + page_size - 1) // page_size)
            offset = max(0, (page - 1) * page_size)

            if not data_files or offset >= filtered_total:
                schema = iceberg_schema_to_arrow_schema(plan.get("table_schema") or {})
                return {
                    "page": page,
                    "page_size": page_size,
                    "total_rows": filtered_total,
                    "total_pages": total_pages,
                    "columns": schema.names,
                    "rows": [],
                    "elapsed_ms": round((time.time() - t0) * 1000, 2)
                }

            # Select only the data files overlapping [offset, offset + page_size)
            selected_files = []
            cum_rows = 0
            local_offset = 0
            target_end = offset + page_size
            for df in data_files:
                rc = df.get("record_count", 0)
                if rc <= 0:
                    continue
                file_start = cum_rows
                file_end = cum_rows + rc
                if file_end > offset and file_start < target_end:
                    if not selected_files:
                        local_offset = offset - file_start
                    selected_files.append(df)
                cum_rows = file_end
                if cum_rows >= target_end:
                    break

            arrow_obj = build_iceberg_arrow_source(plan, subset_data_files=selected_files or data_files[:1])
            con.register("_page_source_obj", arrow_obj)
            arrow_tbl = con.execute(f"SELECT * FROM _page_source_obj LIMIT {page_size} OFFSET {local_offset}").fetch_arrow_table()
            return {
                "page": page,
                "page_size": page_size,
                "total_rows": filtered_total,
                "total_pages": total_pages,
                "columns": arrow_tbl.column_names,
                "rows": arrow_tbl.to_pylist(),
                "elapsed_ms": round((time.time() - t0) * 1000, 2)
            }

    register_source_in_duckdb(con, path, view_name="table_view")
    
    where_clause = ""
    col_names = [col[0] for col in con.execute("DESCRIBE table_view").fetchall()]
    
    if has_search and search:
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
    con = duckdb.connect(database=":memory:")
    
    # Register aliases: data, parquet_data, avro_data, iceberg_data, t, df
    register_source_in_duckdb(con, path, view_name="data")
    con.execute("CREATE TEMPORARY VIEW parquet_data AS SELECT * FROM data")
    con.execute("CREATE TEMPORARY VIEW avro_data AS SELECT * FROM data")
    con.execute("CREATE TEMPORARY VIEW iceberg_data AS SELECT * FROM data")
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
    con = duckdb.connect(database=":memory:")
    register_source_in_duckdb(con, path, view_name="data")
    con.execute("CREATE TEMPORARY VIEW parquet_data AS SELECT * FROM data")
    con.execute("CREATE TEMPORARY VIEW avro_data AS SELECT * FROM data")
    con.execute("CREATE TEMPORARY VIEW iceberg_data AS SELECT * FROM data")
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
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Usage: parquet_engine.py <command> [args...]"}), file=sys.stderr)
        sys.exit(1)

    cmd = sys.argv[1].lower()

    try:
        if cmd == "catalog_namespaces":
            cfg = None
            if len(sys.argv) > 2 and sys.argv[2] != "__NONE__":
                cfg = json.loads(sys.argv[2])
            res = catalog_list_namespaces_and_tables(cfg)
            print(json.dumps(res, default=safe_json_default, ensure_ascii=False))
            return

        if len(sys.argv) < 3:
            print(json.dumps({"error": "Missing path argument"}), file=sys.stderr)
            sys.exit(1)

        path = sys.argv[2]

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
