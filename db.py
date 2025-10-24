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
        # Project ID will be detected from environment/metadata automatically
        _client = firestore.Client()
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


def save_json(doc_name: str, content: Any) -> bool:
    """Write JSON-able content into Firestore. Returns True on success.
    """
    ref = _doc_ref(doc_name)
    if not ref:
        return False
    try:
        ref.set({
            "content": content,
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "schema": "json",
            "version": 1,
        })
        return True
    except Exception as e:
        logger.error(f"写入 Firestore 文档 '{doc_name}' 失败: {e}", exc_info=True)
        return False
