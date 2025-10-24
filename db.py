#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import datetime
import logging
import json
from typing import Any

logger = logging.getLogger(__name__)


def firestore_enabled() -> bool:
    v = os.getenv("USE_FIRESTORE", "").strip().lower()
    return v in ("1", "true", "yes", "on")


_client = None


def _get_client():
    """Lazy init Firestore client using default credentials in Cloud Run.
    Returns None if Firestore is disabled or client initialization fails.
    """
    global _client
    if _client is not None:
        return _client
    if not firestore_enabled():
        return None
    try:
        from google.cloud import firestore
        database_id = os.getenv("FIRESTORE_DATABASE_ID", "(default)").strip() or "(default)"
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCLOUD_PROJECT")
        try:
            if project_id:
                _client = firestore.Client(project=project_id, database=database_id)
            else:
                _client = firestore.Client(database=database_id)
            return _client
        except Exception as e:
            logger.error(f"初始化 Firestore 客户端失败: {e}", exc_info=True)
            # 如果设置了非默认数据库，尝试回退到默认数据库
            if database_id != "(default)":
                try:
                    logger.warning("尝试回退到默认数据库 '(default)' 以继续写入")
                    if project_id:
                        _client = firestore.Client(project=project_id, database="(default)")
                    else:
                        _client = firestore.Client(database="(default)")
                    return _client
                except Exception as e2:
                    logger.error(f"默认数据库回退也失败: {e2}", exc_info=True)
            return None
    except Exception as e:
        logger.error(f"初始化 Firestore 客户端失败: {e}", exc_info=True)
        return None


def _doc_ref(doc_name: str):
    client = _get_client()
    if not client:
        return None
    collection = os.getenv("FIRESTORE_COLLECTION", "bot_i1play")
    return client.collection(collection).document(doc_name)


def load_json(doc_name: str, default: Any):
    """Read document from Firestore, return `default` if not exists or errors.
    We store JSON-able content under key 'content'.
    """
    ref = _doc_ref(doc_name)
    if not ref:
        logger.debug(f"load_json: Firestore disabled or client unavailable for doc '{doc_name}'")
        return default
    try:
        snap = ref.get()
        if not snap.exists:
            logger.debug(f"load_json: document '{doc_name}' not found in Firestore")
            return default
        data = snap.to_dict() or {}
        content = data.get("content", default)
        logger.debug(f"load_json: loaded doc '{doc_name}' from Firestore, content_type={type(content)}")
        return content
    except Exception as e:
        logger.error(f"读取 Firestore 文档 '{doc_name}' 失败: {e}", exc_info=True)
        return default

# 规范化为可写入 Firestore 的内容
# 复杂类型统一转字符串，datetime 转 ISO8601

def _normalize_for_firestore(value: Any):
    try:
        if isinstance(value, dict):
            normalized = {}
            for sk, v in value.items():
                normalized[sk] = _normalize_for_firestore(v)
            return normalized
        if isinstance(value, list):
            return [_normalize_for_firestore(v) for v in value]
        # 常见原子类型直接返回
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        # datetime 转字符串（ISO8601）
        if isinstance(value, datetime.datetime):
            try:
                return value.isoformat()
            except Exception:
                return str(value)
        # 其他类型：字符串表示兜底
        return str(value)
    except Exception:
        # 任何异常情况下，回退为字符串
        try:
            return str(value)
        except Exception:
            return None


def save_json(doc_name: str, content: Any) -> bool:
    """Write JSON-able content into Firestore. Returns True on success.
    """
    ref = _doc_ref(doc_name)
    if not ref:
        return False
    try:
        normalized_content = _normalize_for_firestore(content)
        ref.set({
            "content": normalized_content,
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "schema": "json",
            "version": 1,
        })
        return True
    except Exception as e:
        logger.error(f"写入 Firestore 文档 '{doc_name}' 失败: {e}", exc_info=True)
        return False

# 列出集合中以指定前缀开头的文档ID

def list_document_ids_with_prefix(prefix: str) -> list:
    client = _get_client()
    if not client:
        logger.debug(f"list_document_ids_with_prefix: Firestore client unavailable for prefix='{prefix}'")
        return []
    try:
        collection = os.getenv("FIRESTORE_COLLECTION", "bot_i1play")
        ids = []
        for snap in client.collection(collection).stream():
            doc_id = getattr(snap, "id", None)
            if isinstance(doc_id, str) and doc_id.startswith(prefix):
                ids.append(doc_id)
        logger.debug(f"list_document_ids_with_prefix: found {len(ids)} docs with prefix '{prefix}' -> {ids}")
        return ids
    except Exception as e:
        logger.error(f"列出 Firestore 文档前缀 '{prefix}' 失败: {e}", exc_info=True)
        return []

# 读取以 base_name 开头的分片文档并聚合返回
# 例如 base_name='sent_messages'，会聚合 'sent_messages_<uid>' 与 'sent_messages_<uid>_part_*'

def load_json_sharded(base_name: str) -> Any:
    # 先尝试加载主文档（若存在且是 dict，则纳入聚合）
    combined = {}
    base_data = load_json(base_name, default=None)
    if isinstance(base_data, dict):
        combined.update(base_data)
    # 收集所有分片文档
    prefix = f"{base_name}_"
    ids = list_document_ids_with_prefix(prefix)
    # 将分片按 <key> 分组
    shards = {}
    for doc_id in ids:
        rest = doc_id[len(prefix):]
        key = rest.split("_part_", 1)[0]
        shards.setdefault(key, []).append(doc_id)
    # 逐个 key 聚合分片
    for key, doc_ids in shards.items():
        # 单文档（无 _part_ 后缀）直接读取
        if len(doc_ids) == 1 and "_part_" not in doc_ids[0]:
            val = load_json(doc_ids[0], default=None)
            if val is not None:
                combined[key] = val
            continue
        # 多分片：按文档ID排序并拼接为列表
        parts = []
        for doc_id in sorted(doc_ids):
            part = load_json(doc_id, default=None)
            if isinstance(part, list):
                parts.extend(part)
            elif isinstance(part, dict) and "__string__" in part:
                try:
                    decoded = json.loads(part["__string__"])
                    if isinstance(decoded, list):
                        parts.extend(decoded)
                except Exception:
                    pass
            elif part is not None:
                # 兜底：非列表数据直接附加
                parts.append(part)
        combined[key] = parts
    return combined
