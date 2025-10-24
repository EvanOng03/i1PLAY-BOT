#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import datetime
import logging
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
        if project_id:
            _client = firestore.Client(project=project_id, database=database_id)
        else:
            _client = firestore.Client(database=database_id)
        return _client
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
        return default
    try:
        snap = ref.get()
        if not snap.exists:
            return default
        data = snap.to_dict() or {}
        return data.get("content", default)
    except Exception as e:
        logger.error(f"读取 Firestore 文档 '{doc_name}' 失败: {e}", exc_info=True)
        return default


# 规范化待写入的数据：递归将 dict 的键转换为字符串
def _normalize_for_firestore(value: Any) -> Any:
    try:
        if isinstance(value, dict):
            normalized = {}
            for k, v in value.items():
                sk = str(k) if k is not None else ""
                # Firestore 字段名不能为空，必要时用占位符兜底
                if not sk:
                    sk = "_"
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
