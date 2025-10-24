#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import logging
import datetime
import pandas as pd
import time
import asyncio
import traceback
import uuid
import re
import html
from collections import defaultdict, deque
from dotenv import load_dotenv
from db import load_json, save_json, firestore_enabled, load_json_sharded
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, MessageHandler, filters, 
    ContextTypes, ConversationHandler, CallbackQueryHandler, ChatMemberHandler
)
from telegram.error import TimedOut

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# 获取机器人令牌和管理员ID
BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_IDS = [int(admin_id) for admin_id in os.getenv('ADMIN_IDS', '').split(',') if admin_id]

# 定义会话状态
SELECTING_TARGET, SENDING_MESSAGE, CONFIRMING, SCHEDULE_SET_TIME, TIMER_SET_COUNTDOWN, SCHEDULE_CONFIRM_CREATE = range(6)

# 存储用户已发送消息的全局字典 {user_id: [{'chat_id': xxx, 'message_id': xxx, 'timestamp': xxx}]}
user_sent_messages = {}

# 发送记录持久化的异步锁，避免并发写入冲突
sent_messages_lock = asyncio.Lock()

# 速率限制管理器
class RateLimiter:
    def __init__(self):
        # 每个群组的消息发送时间记录
        self.group_message_times = defaultdict(deque)
        # Telegram API 限制：每秒最多30条消息
        self.max_messages_per_second = 30
        # 时间窗口（秒）
        self.time_window = 1.0
        
    async def wait_if_needed(self, chat_id):
        """如果需要，等待以遵循速率限制"""
        current_time = time.time()
        message_times = self.group_message_times[chat_id]
        
        # 清理超过时间窗口的旧记录
        while message_times and current_time - message_times[0] > self.time_window:
            message_times.popleft()
        
        # 如果当前时间窗口内的消息数量已达到限制
        if len(message_times) >= self.max_messages_per_second:
            # 计算需要等待的时间
            oldest_time = message_times[0]
            wait_time = self.time_window - (current_time - oldest_time)
            if wait_time > 0:
                logger.info(f"Rate limit reached for chat {chat_id}, waiting {wait_time:.2f} seconds")
                await asyncio.sleep(wait_time)
                current_time = time.time()
        
        # 记录这次发送的时间
        self.group_message_times[chat_id].append(current_time)
    
    def get_optimal_delay(self, chat_id, messages_count=1):
        """获取最优延迟时间"""
        current_time = time.time()
        message_times = self.group_message_times[chat_id]
        
        # 清理旧记录
        while message_times and current_time - message_times[0] > self.time_window:
            message_times.popleft()
        
        # 计算当前窗口内的消息数量
        current_count = len(message_times)
        
        # 如果加上即将发送的消息数量会超过限制
        if current_count + messages_count > self.max_messages_per_second:
            # 返回需要等待的时间
            if message_times:
                oldest_time = message_times[0]
                return self.time_window - (current_time - oldest_time)
        
        # 返回最小延迟（避免过于频繁的请求）
        return 0.05  # 50ms 最小延迟

# 全局速率限制器实例
rate_limiter = RateLimiter()

# 判断异常是否表示消息不存在（被他人删除或找不到）
def is_message_not_found_error(err: Exception) -> bool:
    try:
        s = str(err).lower()
    except Exception:
        return False
    needles = [
        'message to delete not found',
        'message to edit not found',
        'message to copy not found',
        'message to forward not found',
        'message not found'
    ]
    return any(n in s for n in needles)

# 判断异常是否表示“消息未修改”（用于将同样的键盘视为存在性成功）
def is_message_not_modified_error(err: Exception) -> bool:
    try:
        return 'message is not modified' in str(err).lower()
    except Exception:
        return False

# 在进入删除菜单时，自动检测并清理已不存在的消息（被他人删除）
async def auto_prune_nonexistent_messages(update: Update, context: ContextTypes.DEFAULT_TYPE, messages: list, per_group_limit: int | None = None) -> int:
    if not messages:
        return 0
    # 按群组并发、群组内顺序探测（不发送任何新消息）
    groups: dict[int, list] = {}
    for m in messages:
        try:
            groups.setdefault(m['chat_id'], []).append(m)
        except Exception:
            pass

    externally_deleted = set()

    async def probe_group(chat_id: int, msgs: list):
        # 可选：限制每群探测数量，加速完成
        msgs_to_probe = msgs
        try:
            if per_group_limit is not None and per_group_limit > 0:
                msgs_sorted = sorted(msgs, key=lambda x: x.get('timestamp') or '', reverse=True)
                msgs_to_probe = msgs_sorted[:per_group_limit]
        except Exception:
            msgs_to_probe = msgs
        for msg in msgs_to_probe:
            try:
                await rate_limiter.wait_if_needed(chat_id)
                # 使用编辑回复标记作为“无副作用”的存在性探测
                await context.bot.edit_message_reply_markup(
                    chat_id=msg['chat_id'],
                    message_id=msg['message_id'],
                    reply_markup=None
                )
            except Exception as e:
                if is_message_not_found_error(e):
                    externally_deleted.add((msg['chat_id'], msg['message_id']))
                    try:
                        append_botlog(update, 'deleted_by_others', {
                            'chat_id': msg['chat_id'],
                            'message_id': msg['message_id'],
                            'origin': 'auto_prune_on_delete',
                            'reason': str(e)
                        })
                    except Exception:
                        pass
                elif is_message_not_modified_error(e):
                    # 视为存在（键盘未变化），忽略
                    pass
                else:
                    logger.error(f"存在性探测失败(chat={chat_id} mid={msg['message_id']}): {e}")

    try:
        tasks = [probe_group(cid, msgs) for cid, msgs in groups.items()]
        await asyncio.gather(*tasks)
    except Exception as e:
        logger.error(f"执行存在性探测任务失败: {e}")

    # 将不存在的消息从全局记录中移除并持久化
    if externally_deleted:
        for uid in list(user_sent_messages.keys()):
            user_sent_messages[uid] = [
                m for m in user_sent_messages[uid]
                if (m['chat_id'], m['message_id']) not in externally_deleted
            ]
        try:
            save_sent_messages(user_sent_messages)
        except Exception as e:
            logger.error(f"自动清理持久化失败: {e}")
    return len(externally_deleted)

# 数据文件路径
# 使用脚本所在目录的绝对路径
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA_DIR = os.path.join(SCRIPT_DIR, 'data')
TMP_ROOT = os.getenv('CLOUD_RUN_TMP_DIR', '/tmp')
TMP_DATA_DIR = os.path.join(TMP_ROOT, 'data')
DATA_DIR = DEFAULT_DATA_DIR
try:
    os.makedirs(DEFAULT_DATA_DIR, exist_ok=True)
    if not os.access(DEFAULT_DATA_DIR, os.W_OK):
        raise PermissionError("DATA_DIR not writable")
except Exception:
    DATA_DIR = TMP_DATA_DIR
    os.makedirs(DATA_DIR, exist_ok=True)
    try:
        logger.info(f"DATA_DIR 回退到临时目录: {DATA_DIR}")
    except Exception:
        pass

GROUPS_FILE = os.path.join(DATA_DIR, 'groups.json')
CATEGORIES_FILE = os.path.join(DATA_DIR, 'categories.json')
LOGS_FILE = os.path.join(DATA_DIR, 'logs.json')
BOTLOG_FILE = os.path.join(DATA_DIR, 'botlog.json')
WHITELIST_FILE = os.path.join(DATA_DIR, 'whitelist.json')
SENT_MESSAGES_FILE = os.path.join(DATA_DIR, 'sent_messages.json')
BLACKLIST_FILE = os.path.join(DATA_DIR, 'blacklist.json')
SCHEDULED_TASKS_FILE = os.path.join(DATA_DIR, 'scheduled_tasks.json')

# 导出文件名配置（可在此修改导出文档名称）
BOTLOG_EXPORT_PREFIX = 'botlog_export(i1PLAY)'
BOTLOG_JSON_EXPORT_NAME = 'botlog(i1PLAY).json'

# 初始化数据文件
def init_data_files():
    if not os.path.exists(GROUPS_FILE):
        with open(GROUPS_FILE, 'w', encoding='utf-8') as f:
            json.dump([], f, ensure_ascii=False)
    
    if not os.path.exists(CATEGORIES_FILE):
        with open(CATEGORIES_FILE, 'w', encoding='utf-8') as f:
            json.dump([], f, ensure_ascii=False)
    
    if not os.path.exists(LOGS_FILE):
        with open(LOGS_FILE, 'w', encoding='utf-8') as f:
            json.dump([], f, ensure_ascii=False)
    
    if not os.path.exists(WHITELIST_FILE):
        with open(WHITELIST_FILE, 'w', encoding='utf-8') as f:
            json.dump({
                "groups": [],
                "private_users": [7133527616]  # 默认作者ID
            }, f, ensure_ascii=False, indent=2)
    # 初始化已发送消息持久化文件（按用户ID存储列表）
    if not os.path.exists(SENT_MESSAGES_FILE):
        with open(SENT_MESSAGES_FILE, 'w', encoding='utf-8') as f:
            json.dump({}, f, ensure_ascii=False, indent=2)
    # 初始化操作日志文件
    if not os.path.exists(BOTLOG_FILE):
        with open(BOTLOG_FILE, 'w', encoding='utf-8') as f:
            json.dump([], f, ensure_ascii=False, indent=2)

# 耗时格式化工具，输出 00:00:00.00
def format_elapsed(elapsed_seconds: float) -> str:
    try:
        hours = int(elapsed_seconds // 3600)
        minutes = int((elapsed_seconds % 3600) // 60)
        seconds = int(elapsed_seconds % 60)
        centiseconds = int((elapsed_seconds - int(elapsed_seconds)) * 100)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{centiseconds:02d}"
    except Exception:
        # 兜底格式化
        return f"{elapsed_seconds:.2f}s"

# 加载群组数据
def load_groups():
    if firestore_enabled():
        data = load_json('groups', default=[])
        if isinstance(data, list):
            return data
    with open(GROUPS_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

# 保存群组数据
def save_groups(groups):
    if firestore_enabled():
        ok = save_json('groups', groups)
        if ok:
            return
    with open(GROUPS_FILE, 'w', encoding='utf-8') as f:
        json.dump(groups, f, ensure_ascii=False, indent=2)

# 加载分类数据
def load_categories():
    if firestore_enabled():
        data = load_json('categories', default=[])
        if isinstance(data, list):
            return data
    with open(CATEGORIES_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

# 保存分类数据
def save_categories(categories):
    if firestore_enabled():
        ok = save_json('categories', categories)
        if ok:
            return
    with open(CATEGORIES_FILE, 'w', encoding='utf-8') as f:
        json.dump(categories, f, ensure_ascii=False, indent=2)

# 加载日志数据
def load_logs():
    if firestore_enabled():
        data = load_json('logs', default=[])
        if isinstance(data, list):
            return data
    with open(LOGS_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

# 保存日志数据
def save_logs(logs):
    if firestore_enabled():
        ok = save_json('logs', logs)
        if ok:
            return
        logger.warning("Firestore 写入 logs 失败，回退到本地文件保存")
    else:
        logger.info("Firestore 未启用，logs 保存到本地文件")
    with open(LOGS_FILE, 'w', encoding='utf-8') as f:
        json.dump(logs, f, ensure_ascii=False, indent=2)

# 加载/保存 Bot 操作日志
def load_botlog():
    if firestore_enabled():
        data = load_json('botlog', default=[])
        if isinstance(data, list):
            return data
    with open(BOTLOG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_botlog(entries):
    if firestore_enabled():
        ok = save_json('botlog', entries)
        if ok:
            return
        logger.warning("Firestore 写入 botlog 失败，回退到本地文件保存")
    else:
        logger.info("Firestore 未启用，botlog 保存到本地文件")
    with open(BOTLOG_FILE, 'w', encoding='utf-8') as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)

def _get_display_username(user) -> str:
    try:
        if not user:
            return '未知用户'
        if user.username:
            return f"@{user.username}"
        name = f"{user.first_name or ''} {user.last_name or ''}".strip()
        return name or str(user.id)
    except Exception:
        return '未知用户'

def append_botlog(update: Update, action: str, payload: dict):
    try:
        entries = load_botlog()
    except Exception:
        entries = []
    # 安全获取用户信息，允许 update 为 None
    try:
        user = getattr(update, 'effective_user', None) if update is not None else None
    except Exception:
        user = None
    entry = {
        'action': action,
        'username': _get_display_username(user),
        'user_id': getattr(user, 'id', None),
        'time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    try:
        if isinstance(payload, dict):
            entry.update(payload)
            # 若未能从 update 获取用户ID，尝试从 payload 补齐
            if entry.get('user_id') is None:
                uid = payload.get('created_by_user_id') or payload.get('user_id')
                if uid is not None:
                    entry['user_id'] = uid
    except Exception:
        pass
    entries.append(entry)
    # 保留最近 1000 条
    entries = entries[-1000:]
    try:
        save_botlog(entries)
    except Exception as e:
        logger.error(f"保存操作日志失败: {e}")

def summarize_messages_for_log(messages: list) -> str:
    try:
        texts = [m.get('text') for m in messages if m.get('type') == 'text' and m.get('text')]
        captions = [m.get('caption') for m in messages if m.get('type') in ['photo', 'video', 'document'] and m.get('caption')]
        parts = []
        combined = texts + captions
        if combined:
            parts.append(' | '.join(combined))
        counts = {}
        for m in messages:
            t = m.get('type')
            if t in ['photo', 'video', 'document']:
                counts[t] = counts.get(t, 0) + 1
        if counts:
            media_desc = ', '.join([f"{k}:{counts[k]}" for k in counts])
            parts.append(f"[{media_desc}]")
        summary = ' '.join(parts).strip()
        return summary[:500] if summary else '(媒体/非文本消息)'
    except Exception:
        return '(无法总结内容)'

# 加载/保存已发送消息（持久化）
def load_sent_messages():
    if firestore_enabled():
        try:
            # 聚合读取分片文档：sent_messages_<uid> 与 sent_messages_<uid>_part_*
            data = load_json_sharded('sent_messages')
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    with open(SENT_MESSAGES_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

# 将用户消息列表分片写入 Firestore，避免单文档过大（返回写入是否成功）
def _save_user_messages_sharded(user_id: int, messages: list) -> bool:
    """Store per-user sent messages into Firestore using sharding when needed.
    Adds diagnostic logging of payload size and written document ids.
    Returns True when all writes succeeded.
    """
    try:
        import json as _json
        base_id = f"sent_messages_{user_id}"
        blob = _json.dumps(messages, ensure_ascii=False)
        payload_bytes = len(blob.encode('utf-8'))
        logger.debug(f"_save_user_messages_sharded: user={user_id} payload_bytes={payload_bytes} items={len(messages)}")
        # 约束：单文档控制在 ~900KB 以内
        if payload_bytes <= 900_000:
            ok = save_json(base_id, messages)
            logger.info(f"_save_user_messages_sharded: wrote single doc {base_id} -> {bool(ok)}")
            return bool(ok)
        # 分片写入（每块500条，可按需调整）
        chunk_size = 500
        all_ok = True
        written_parts = []
        for idx in range(0, len(messages), chunk_size):
            part = messages[idx: idx + chunk_size]
            part_id = f"{base_id}_part_{idx // chunk_size:04d}"
            ok = save_json(part_id, part)
            written_parts.append((part_id, bool(ok)))
            all_ok = all_ok and bool(ok)
        logger.info(f"_save_user_messages_sharded: user={user_id} parts_written={written_parts}")
        return all_ok
    except Exception as e:
        logger.error(f"_save_user_messages_sharded: exception for user={user_id}: {e}", exc_info=True)
        # 兜底：尽力写入主文档
        try:
            ok = save_json(f"sent_messages_{user_id}", messages)
            logger.info(f"_save_user_messages_sharded: fallback single doc sent_messages_{user_id} -> {bool(ok)}")
            return bool(ok)
        except Exception as e2:
            logger.error(f"_save_user_messages_sharded: fallback failed user={user_id}: {e2}", exc_info=True)
            return False


def save_sent_messages(data):
    """
    保存 sent_messages：优先尝试写入 Firestore；为保证在启用 Firestore 时也能在容器内保留本地副本，
    我们将始终把数据写到本地 `SENT_MESSAGES_FILE`（作为副本），除非明确禁用。
    增加诊断日志，记录尝试写入的用户及 Firestore 中示例文档列表。
    """
    wrote_to_firestore = False
    attempted_uids = []
    if firestore_enabled():
        try:
            # 按用户ID分片写入，解决单文档体积限制
            all_ok = True
            for uid_str, msgs in (data or {}).items():
                try:
                    uid = int(uid_str)
                except Exception:
                    uid = uid_str
                attempted_uids.append(uid)
                ok = _save_user_messages_sharded(uid, msgs)
                all_ok = all_ok and bool(ok)
            if all_ok:
                wrote_to_firestore = True
                logger.info(f"save_sent_messages: wrote all users to Firestore, users={attempted_uids}")
            else:
                logger.warning(f"save_sent_messages: some writes to Firestore failed, users={attempted_uids}")
            # 诊断：列出样例用户在 Firestore 中的文档 id
            try:
                if attempted_uids:
                    sample_uid = attempted_uids[0]
                    found = list_document_ids_with_prefix(f"sent_messages_{sample_uid}")
                    logger.info(f"save_sent_messages: Firestore docs for sample user {sample_uid}: {found}")
            except Exception:
                pass
        except Exception as e:
            logger.error(f"保存 sent_messages 到 Firestore 失败，回退到本地文件: {e}", exc_info=True)
    else:
        logger.info("Firestore 未启用，sent_messages 保存到本地文件")

    # 始终写本地副本，确保 send_messages.json 在容器或共享存储中可见（便于排查/备份）
    try:
        with open(SENT_MESSAGES_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"Wrote local SENT_MESSAGES_FILE ({SENT_MESSAGES_FILE}), firestore_ok={wrote_to_firestore}")
    except Exception as e:
        logger.error(f"写入本地 sent_messages 文件失败: {e}")

def prune_sent_messages(data: dict, minutes: int = None, max_per_user: int = None):
    """保留全部发送记录，不做时间或数量限制。
    data 结构: { user_id: [ {chat_id, message_id, timestamp}, ... ] }
    """
    try:
        for uid in list(data.keys()):
            msgs = data.get(uid, [])
            # 不进行时间或条数裁剪
            data[uid] = msgs
    except Exception as e:
        logger.error(f"清理已发送消息失败: {e}")

# 立即持久化已发送消息记录，保证异常或中断时也能删除
async def _persist_sent_messages(user_id: int, msgs: list):
    """
    msgs: 列表元素形如 {chat_id, message_id, user_id}
    将补充 timestamp 与 sender_user_id 字段，并写入 sent_messages.json。
    """
    try:
        # 统一持久化为 ISO8601 UTC（带偏移），例如 2025-10-24T02:07:02.487679+00:00；展示时再转换到 GMT+8
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        async with sent_messages_lock:
            if user_id not in user_sent_messages:
                user_sent_messages[user_id] = []
            # 同时记录 GMT+8 的时间，便于展示/诊断
            tz8 = datetime.timezone(datetime.timedelta(hours=8))
            timestamp_gmt8 = datetime.datetime.now(datetime.timezone.utc).astimezone(tz8).isoformat()
            for m in msgs:
                m['timestamp'] = m.get('timestamp', timestamp)
                m['timestamp_gmt8'] = m.get('timestamp_gmt8', timestamp_gmt8)
                m['sender_user_id'] = user_id
                user_sent_messages[user_id].append(m)
            # 诊断日志：记录尝试持久化的用户与条数，以及前两条示例，便于定位写入失败或未触发的问题
            try:
                sample = user_sent_messages[user_id][-2:]
            except Exception:
                sample = msgs[:2]
            try:
                logger.info(f"_persist_sent_messages user={user_id} total_after={len(user_sent_messages.get(user_id, []))} adding={len(msgs)} sample={sample}")
            except Exception:
                logger.info(f"_persist_sent_messages user={user_id} adding={len(msgs)}")
            save_sent_messages(user_sent_messages)
    except Exception as e:
        logger.error(f"持久化已发送消息失败: {e}")

async def _record_after_send(user_id: int, result, content_summary: str = None, username: str = None):
    """根据发送结果统一记录消息。
    支持 Message 或 [Message]。"""
    try:
        if result is None:
            return
        # 单条消息时，自动提取内容摘要（若未显式传入）
        def get_summary(m):
            try:
                if content_summary:
                    return content_summary[:200]
                txt = getattr(m, 'text', None)
                cap = getattr(m, 'caption', None)
                raw = txt or cap
                if raw:
                    s = raw.strip()
                    return s[:200]
            except Exception:
                pass
            return None

        # 列表：多个消息
        if isinstance(result, list):
            msgs = []
            for m in result:
                mid = getattr(m, 'message_id', None)
                if mid is None:
                    continue
                chat = getattr(m, 'chat', None)
                cid = getattr(chat, 'id', None) if chat else getattr(m, 'chat_id', None)
                title = getattr(chat, 'title', None) if chat else None
                ctype = getattr(chat, 'type', None) if chat else None
                chat_username = getattr(chat, 'username', None) if chat else None
                first_name = getattr(chat, 'first_name', None) if chat else None
                last_name = getattr(chat, 'last_name', None) if chat else None
                msgs.append({
                    'chat_id': cid,
                    'message_id': mid,
                    'user_id': user_id,
                    'content_summary': get_summary(m),
                    'chat_title': title,
                    'chat_type': ctype,
                    'username': username,
                    'chat_username': chat_username,
                    'chat_first_name': first_name,
                    'chat_last_name': last_name
                })
            if msgs:
                await _persist_sent_messages(user_id, msgs)
        # 单条消息
        else:
            mid = getattr(result, 'message_id', None)
            if mid is None:
                return
            chat = getattr(result, 'chat', None)
            cid = getattr(chat, 'id', None) if chat else getattr(result, 'chat_id', None)
            title = getattr(chat, 'title', None) if chat else None
            ctype = getattr(chat, 'type', None) if chat else None
            chat_username = getattr(chat, 'username', None) if chat else None
            first_name = getattr(chat, 'first_name', None) if chat else None
            last_name = getattr(chat, 'last_name', None) if chat else None
            await _persist_sent_messages(user_id, [{
                'chat_id': cid,
                'message_id': mid,
                'user_id': user_id,
                'content_summary': get_summary(result),
                'chat_title': title,
                'chat_type': ctype,
                'username': username,
                'chat_username': chat_username,
                'chat_first_name': first_name,
                'chat_last_name': last_name
            }])
    except Exception as e:
        logger.error(f"记录发送消息失败: {e}")

async def send_and_record(send_callable, *args, user_id: int, content_summary: str = None, username: str = None, **kwargs):
    """统一封装：发送后立即记录 message_id/chat_id。"""
    result = await send_callable(*args, **kwargs)
    await _record_after_send(user_id, result, content_summary=content_summary, username=username)
    return result

# 处理/bot命令
async def bot_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # 检查是否在群组中使用
    if update.effective_chat.type not in ['group', 'supergroup']:
        return
    # 黑名单拦截（无提示、但记录尝试到日志）
    try:
        uid = update.effective_user.id
        if is_blacklisted(uid):
            try:
                append_botlog(update, 'bot_blocked_command', {
                    'group_id': update.effective_chat.id,
                    'group_name': update.effective_chat.title,
                    'has_buttons': True
                })
            except Exception as e:
                logger.error(f"记录 黑名单 bot 命令尝试失败: {e}")
            return
    except Exception:
        pass
    # 记录使用 bot 命令的日志
    try:
        append_botlog(update, 'bot_command', {
            'group_id': update.effective_chat.id,
            'group_name': update.effective_chat.title,
            'has_buttons': True
        })
    except Exception as e:
        logger.error(f"记录 bot 命令日志失败: {e}")
    
    # 创建内联键盘
    keyboard = [
        [
            InlineKeyboardButton("Official Website 官方网址", callback_data="demo_link"),
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # 发送消息和按钮（记录）
    await send_and_record(
        context.bot.send_message,
        chat_id=update.effective_chat.id,
        text="We are pleased to serve you. You can select the function from the options provided.\n"
        "我们很高兴为您服务。您可以从以下提供的选项中选择所需的功能",
        reply_markup=reply_markup,
        user_id=update.effective_user.id,
        username=_get_display_username(update.effective_user),
        content_summary="We are pleased to serve you. You can select the function from the options provided.\n"
        "我们很高兴为您服务。您可以从以下提供的选项中选择所需的功能"
    )

async def handle_bot_mention(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # 仅在群/超群中响应
    if update.effective_chat.type not in ['group', 'supergroup']:
        return
    msg = update.effective_message or update.message
    if not msg or not msg.text:
        return
    text = msg.text.strip()
    # 获取机器人用户名
    try:
        me = await context.bot.get_me()
        bot_username = me.username
    except Exception:
        bot_username = getattr(context.bot, 'username', None)
    if not bot_username:
        return
    mention = f"@{bot_username}".lower()
    # 仅当文本恰好是 @机器人 时响应；有其他文字则忽略
    is_exact_mention = text.lower() == mention
    if not is_exact_mention:
        # 进一步检查是否只有一个 mention 实体且覆盖整条文本
        entities = getattr(msg, 'entities', []) or []
        if not entities or len(entities) != 1:
            return
        ent = entities[0]
        try:
            if ent.type != 'mention' or ent.offset != 0 or ent.length != len(msg.text):
                return
            if msg.text.lower() != mention:
                return
        except Exception:
            return

    # 黑名单拦截（无提示，但记录尝试到日志）
    try:
        uid = update.effective_user.id
        if is_blacklisted(uid):
            try:
                append_botlog(update, 'bot_blocked_command', {
                    'group_id': update.effective_chat.id,
                    'group_name': update.effective_chat.title,
                    'has_buttons': True
                })
            except Exception as e:
                logger.error(f"记录 黑名单 bot 命令尝试失败: {e}")
            return
    except Exception:
        pass

    # 记录使用 bot 的日志
    try:
        append_botlog(update, 'bot_command', {
            'group_id': update.effective_chat.id,
            'group_name': update.effective_chat.title,
            'has_buttons': True
        })
    except Exception as e:
        logger.error(f"记录 bot 命令日志失败: {e}")

    # 创建内联键盘并发送
    keyboard = [
        [InlineKeyboardButton("Official Website 官方网址", callback_data="demo_link")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await send_and_record(
        context.bot.send_message,
        chat_id=update.effective_chat.id,
        text=(
            "We are pleased to serve you. You can select the function from the options provided.\n"
            "我们很高兴为您服务。您可以从以下提供的选项中选择所需的功能"
        ),
        reply_markup=reply_markup,
        user_id=update.effective_user.id,
        username=_get_display_username(update.effective_user),
        content_summary=(
            "We are pleased to serve you. You can select the function from the options provided.\n"
            "我们很高兴为您服务。您可以从以下提供的选项中选择所需的功能"
        )
    )

# 处理按钮回调
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id
    # 黑名单拦截（无提示、但记录尝试到日志）
    try:
        uid = update.effective_user.id
        if is_blacklisted(uid):
            try:
                button_map = {
                    'demo_link': 'Official Website 官方网址'
                }
                append_botlog(update, 'bot_blocked_button', {
                    'group_id': chat_id,
                    'group_name': getattr(query.message.chat, 'title', None),
                    'button_key': query.data,
                    'button_label': button_map.get(query.data, query.data)
                })
            except Exception as e:
                logger.error(f"记录 黑名单 bot 按钮尝试失败: {e}")
            return
    except Exception:
        pass
    # 记录按钮点击日志
    try:
        button_map = {
            'demo_link': 'Official Website 官方网址'
        }
        append_botlog(update, 'bot_button', {
            'group_id': chat_id,
            'group_name': getattr(query.message.chat, 'title', None),
            'button_key': query.data,
            'button_label': button_map.get(query.data, query.data)
        })
    except Exception as e:
        logger.error(f"记录 bot 按钮日志失败: {e}")
    
    if query.data == "demo_link":
        await send_and_record(
            context.bot.send_message,
            chat_id=chat_id,
            text="Hi team, please refer to the following link:\n"
            "您好 请参考以下链接:\n"
            "https://demo1.i1play.com\nhttps://demo2.i1play.com",
            parse_mode=ParseMode.MARKDOWN,
            user_id=update.effective_user.id,
            username=_get_display_username(update.effective_user),
            content_summary="Hi team, please refer to the following link:\n"
            "您好 请参考以下链接:\n"
            "https://demo1.i1play.com\nhttps://demo2.i1play.com"
        )
    elif query.data == "api_doc":
        await send_and_record(
            context.bot.send_message,
            chat_id=chat_id,
            text=""
            ""
            "",
            parse_mode=ParseMode.MARKDOWN,
            user_id=update.effective_user.id,
            username=_get_display_username(update.effective_user),
            content_summary=""
        )
    elif query.data == "game_list":
        await send_and_record(
            context.bot.send_message,
            chat_id=chat_id,
            text=""
            ""
            "",
            parse_mode=ParseMode.MARKDOWN,
            user_id=update.effective_user.id,
            username=_get_display_username(update.effective_user),
            content_summary=""
        )
    elif query.data == "game_resource":
        await send_and_record(
            context.bot.send_message,
            chat_id=chat_id,
            text=""
            ""
            "",
            parse_mode=ParseMode.MARKDOWN,
            user_id=update.effective_user.id,
            username=_get_display_username(update.effective_user),
            content_summary=""
        )

def load_whitelist():
    # 优先从 Firestore 读取（如果启用），否则回退到本地文件
    if firestore_enabled():
        try:
            data = load_json('whitelist', default=None)
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    try:
        with open(WHITELIST_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"groups": [], "private_users": []}

# 保存白名单数据
def save_whitelist(whitelist):
    # 优先写入 Firestore（若启用），失败或未启用时写本地文件
    if firestore_enabled():
        try:
            ok = save_json('whitelist', whitelist)
            if ok:
                return
            logger.warning('Firestore 写入 whitelist 失败，回退到本地文件')
        except Exception as e:
            logger.error(f'写入 whitelist 到 Firestore 失败: {e}')
    with open(WHITELIST_FILE, 'w', encoding='utf-8') as f:
        json.dump(whitelist, f, ensure_ascii=False, indent=2)

# 黑名单数据
def load_blacklist():
    try:
        with open(BLACKLIST_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"users": []}

def save_blacklist(blacklist):
    with open(BLACKLIST_FILE, 'w', encoding='utf-8') as f:
        json.dump(blacklist, f, ensure_ascii=False, indent=2)

def is_blacklisted(user_id: int) -> bool:
    try:
        bl = load_blacklist()
        return any(u.get('id') == user_id for u in bl.get('users', []))
    except Exception:
        return False

# 检查是否为管理员
def is_admin(user_id):
    # 检查是否在管理员ID列表中
    if user_id in ADMIN_IDS:
        return True
    
    # 检查是否在白名单中
    try:
        whitelist = load_whitelist()
        return user_id in whitelist['private_users']
    except:
        return False

# 检查用户权限
def has_permission(update: Update) -> bool:
    user_id = update.effective_user.id
    chat_type = update.effective_chat.type
    
    # 管理员总是有权限
    if is_admin(user_id):
        return True
    
    whitelist = load_whitelist()
    
    if chat_type == 'private':
        return user_id in whitelist['private_users']
    else:
        return update.effective_chat.id in whitelist['groups']

# 权限检查装饰器
def require_permission(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not has_permission(update):
            # 构建详细的操作记录并提示
            try:
                user = update.effective_user
                chat = update.effective_chat
                username = _get_display_username(user)
                chat_name = getattr(chat, 'title', None) or ('私聊' if chat.type == 'private' else '未知群组')
                chat_id = chat.id
                raw_text = None
                command = None
                if getattr(update, 'message', None) and update.message.text:
                    raw_text = update.message.text.strip()
                elif getattr(update, 'callback_query', None) and update.callback_query.data:
                    raw_text = f"[callback:{update.callback_query.data}]"
                if raw_text:
                    m = re.match(r'^/[^ \n]+', raw_text)
                    command = m.group(0) if m else raw_text
                else:
                    command = '未知'
                tz8 = datetime.timezone(datetime.timedelta(hours=8))
                now_local = datetime.datetime.now(datetime.timezone.utc).astimezone(tz8).strftime('%Y/%m/%d %H:%M:%S')
                # 写入操作记录（不记录原始文本）
                try:
                    append_botlog(update, 'permission_denied', {
                        'user_id': user.id,
                        'username': username,
                        'chat_id': chat_id,
                        'chat_name': chat_name,
                        'chat_type': chat.type,
                        'command': command
                    })
                except Exception as e:
                    logger.error(f"记录权限不足操作失败: {e}")
                # 对未白名单用户不返回任何可见内容
                if getattr(update, 'callback_query', None):
                    try:
                        # 仅结束回调以避免前端卡住，不展示任何提示
                        await update.callback_query.answer()
                    except Exception:
                        pass
            except Exception:
                # 兜底：不向未白名单用户返回任何内容
                if getattr(update, 'callback_query', None):
                    try:
                        await update.callback_query.answer()
                    except Exception:
                        pass
            return
        return await func(update, context)
    return wrapper

# 记录群组信息
def record_group(chat_id, title):
    groups = load_groups()
    
    # 检查群组是否已存在
    for group in groups:
        if group['chat_id'] == chat_id:
            # 更新群组标题
            group['title'] = title
            group['last_seen'] = datetime.datetime.now().isoformat()
            save_groups(groups)
            return
    
    # 添加新群组
    new_group = {
        'chat_id': chat_id,
        'title': title,
        'note': '',
        'categories': [],  # 使用多分类格式
        'added_date': datetime.datetime.now().isoformat(),
        'last_seen': datetime.datetime.now().isoformat(),
        'last_user_activity': ''
    }
    groups.append(new_group)
    save_groups(groups)

# 开始命令
@require_permission
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome_message = """
🤖 欢迎使用Telegram群发机器人！

这个机器人可以帮助您管理多个群组并进行消息群发。

使用 /help 查看所有可用命令。
"""
    await send_and_record(
        context.bot.send_message,
        chat_id=update.effective_chat.id,
        text=welcome_message,
        user_id=update.effective_user.id,
        username=_get_display_username(update.effective_user),
        content_summary=welcome_message
    )

# 帮助命令
@require_permission
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = """
🔧 **可用命令：**

📌 **基础：**
`/start` - 启动机器人
`/help` - 显示帮助信息

📋 **群组管理：**
`/listgroups` - 查看所有群组
`/addgroup` `chat_id` - 添加群组到管理列表
`/rename` `chat_id` `备注` - 为群组设置备注

🏷️ **分类管理：**
`/listcategories` - 查看所有分类
`/addcategory` `名称` - 添加新分类
`/editcategory` `旧名称` `新名称` - 编辑分类名称
`/removecategory` `chat_id` `分类名` - 从群组中移除该分类
`/assign` `chat_id` `分类名` - 为群组分配分类

📤 **消息发送：**
`/send` - 开始群发消息（支持文本、图片、视频、文件，支持多条）
`/schedule_send` - 定时群发（管理员）
`/timer_send` - 倒计时群发（管理员）

🗑️ **删除消息：**
`/delete` - 打开删除菜单（快速/搜索/详细列表）
  - 搜索支持多关键词：用 `/`、`,`、`，` 或空格分隔，OR 逻辑

📊 **日志与导出：**
`/logs` `N` - 查看最近 N 次群发记录
`/export` - 导出群组清单为 CSV/Excel
`/botlog` - 查看操作记录
`/exportbotlog` - 导出操作记录为 CSV
`/exportbotlogjson` - 导出原始操作日志 JSON

🧰 **任务与状态：**
`/list_tasks` `all` 或 `全部` - 查看任务列表（管理员）
`/tasks` - `/list_tasks` 的别名
`/status` - `图` 查看系统状态

🔐 **权限管理：**
`/whitelist` - 白名单管理（管理员）
`/blacklist` - 黑名单管理（管理员）

❌ **其他：**
`/cancel` - 取消当前操作
`/bot` - 群内按钮入口
"""
    await send_and_record(
        context.bot.send_message,
        chat_id=update.effective_chat.id,
        text=help_text,
        parse_mode=ParseMode.MARKDOWN,
        user_id=update.effective_user.id,
        username=_get_display_username(update.effective_user),
        content_summary=help_text
    )

# 检测群组中的机器人存在
async def detect_group_presence(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type in ['group', 'supergroup']:
        # 确保群组记录存在并更新标题/last_seen
        record_group(chat.id, chat.title)
        
        # 如果是非机器人用户的消息，更新最后活跃时间
        user = update.effective_user
        if user and not user.is_bot:
            groups = load_groups()
            for g in groups:
                if g.get('chat_id') == chat.id:
                    g['last_user_activity'] = datetime.datetime.now().isoformat()
                    break
            save_groups(groups)

# 处理群组事件
async def group_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    
    # 检查是否有新成员加入
    if update.message.new_chat_members:
        for member in update.message.new_chat_members:
            if member.id == context.bot.id:
                # 机器人被添加到群组，检查邀请人是否为管理员或白名单用户
                inviter = getattr(update.message, 'from_user', None) or update.effective_user
                inviter_id = getattr(inviter, 'id', None)
                inviter_name = _get_display_username(inviter) if inviter else ''
                allowed = False
                try:
                    if inviter_id is not None and inviter_id in load_whitelist()['private_users']:
                        allowed = True
                except Exception:
                    allowed = False

                if allowed:
                    # 允许加入：记录群组并写日志
                    record_group(chat.id, chat.title)
                    try:
                        append_botlog(update, 'bot_added_to_group', {
                            'group_id': chat.id,
                            'group_name': chat.title,
                            'invited_by_user_id': inviter_id,
                            'invited_by_username': inviter_name
                        })
                    except Exception as e:
                        logger.error(f"记录‘机器人加入群组’日志失败: {e}")
                else:
                    # 不允许：记录并离开群组（静默）
                    try:
                        append_botlog(update, 'bot_blocked_group_add', {
                            'group_id': chat.id,
                            'group_name': chat.title,
                            'invited_by_user_id': inviter_id,
                            'invited_by_username': inviter_name,
                            'reason': 'inviter_not_whitelisted'
                        })
                    except Exception as e:
                        logger.error(f"记录‘阻止机器人加入群组’日志失败: {e}")
                    try:
                        await context.bot.leave_chat(chat.id)
                    except Exception as e:
                        logger.error(f"离开未授权群组失败: {e}")
                

# 处理机器人成员状态变更（被踢或离开）
async def handle_my_chat_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        cmu = getattr(update, 'my_chat_member', None)
        if not cmu:
            return
        chat = update.effective_chat
        new_status = getattr(cmu.new_chat_member, 'status', None)
        old_status = getattr(cmu.old_chat_member, 'status', None)
        # 在群组或超级群组中，且机器人状态变更为离开/被踢
        if chat and chat.type in ['group', 'supergroup'] and new_status in ('kicked', 'left'):
            groups = load_groups()
            before_count = len(groups)
            groups = [g for g in groups if g.get('chat_id') != chat.id]
            removed = before_count - len(groups)
            save_groups(groups)
            actor = getattr(cmu, 'from_user', None) or update.effective_user
            if removed > 0:
                append_botlog(update, 'bot_removed_from_group', {
                    'group_id': chat.id,
                    'group_name': getattr(chat, 'title', None) or '',
                    'old_status': old_status,
                    'new_status': new_status,
                    'removed_groups': removed,
                    'by_user_id': getattr(actor, 'id', None),
                    'by_username': _get_display_username(actor)
                })
            logger.info(f"机器人已从群组移除: {chat.id} - {chat.title} ({new_status})")
    except Exception as e:
        logger.error(f"处理机器人成员状态更新失败: {e}")

# 兜底处理服务消息中机器人离群
async def handle_bot_left_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        msg = getattr(update, 'message', None)
        if not msg:
            return
        left_member = getattr(msg, 'left_chat_member', None)
        if left_member and left_member.id == context.bot.id:
            chat = update.effective_chat
            groups = load_groups()
            before_count = len(groups)
            groups = [g for g in groups if g.get('chat_id') != chat.id]
            removed = before_count - len(groups)
            # 仅当确实移除了群（removed > 0）才记录，避免与 my_chat_member 重复
            if removed > 0:
                save_groups(groups)
                actor = update.effective_user
                append_botlog(update, 'bot_removed_from_group', {
                    'group_id': chat.id,
                    'group_name': getattr(chat, 'title', None) or '',
                    'old_status': 'member',
                    'new_status': 'left',
                    'removed_groups': removed,
                    'by_user_id': getattr(actor, 'id', None),
                    'by_username': _get_display_username(actor),
                    'reason': 'left_chat_member'
                })
                logger.info(f"机器人通过服务消息离群: {chat.id} - {chat.title}")
    except Exception as e:
        logger.error(f"处理机器人离群服务消息失败: {e}")

# 监听群组标题变更并实时更新数据库
async def handle_chat_title_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    try:
        new_title = update.message.new_chat_title
        if chat and new_title:
            # 在更新前读取旧群名（如果已存在于数据库）
            old_title = None
            try:
                groups_snapshot = load_groups()
                for g in groups_snapshot:
                    if g.get('chat_id') == chat.id:
                        old_title = g.get('title')
                        break
            except Exception as e:
                logger.warning(f"读取旧群名失败: {e}")

            record_group(chat.id, new_title)
            logger.info(f"群组标题更新: {chat.id} -> {new_title}")
            # 记录操作日志（群组标题更新），包含旧群名
            try:
                append_botlog(update, 'group_title_update', {
                    'group_id': chat.id,
                    'old_title': old_title if old_title is not None else '',
                    'new_title': new_title
                })
            except Exception as e:
                logger.error(f"记录‘群组标题更新’日志失败: {e}")
    except Exception as e:
        logger.error(f"处理群组标题更新失败: {str(e)}")


# 列出所有群组
@require_permission
async def list_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    groups = load_groups()
    
    if not groups:
        await update.message.reply_text('目前没有任何群组。')
        return
    
    message = "📋 <b>群组列表：</b>\n\n"
    for i, group in enumerate(groups, 1):
        categories_str = ', '.join(group.get('categories', [])) if group.get('categories') else '无分类'
        note = group.get('note', '')
        note_str = f" ({note})" if note else ""
        
        # 转义HTML特殊字符
        title = str(group['title']).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        note_escaped = note_str.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        categories_escaped = categories_str.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        
        message += f"{i}. <b>{title}</b>{note_escaped}\n"
        message += f"   ID: <code>{group['chat_id']}</code>\n"
        message += f"   分类: {categories_escaped}\n"
        # 格式化添加时间为 YYYY-MM-DD HH:MM:SS
        added_raw = group.get('added_date')
        try:
            added_dt = datetime.datetime.fromisoformat(added_raw) if added_raw else None
            added_str = added_dt.strftime('%Y-%m-%d %H:%M:%S') if added_dt else '未知'
        except Exception:
            added_str = added_raw.replace('T', ' ')[:19] if added_raw else '未知'
        message += f"   添加时间: {added_str}\n\n"
    
    # 如果消息太长，分段发送
    if len(message) > 4000:
        parts = message.split('\n\n')
        current_message = "📋 <b>群组列表：</b>\n\n"
        
        for part in parts[1:]:  # 跳过标题
            if len(current_message + part + '\n\n') > 4000:
                await update.message.reply_text(current_message, parse_mode=ParseMode.HTML)
                current_message = part + '\n\n'
            else:
                current_message += part + '\n\n'
        
        if current_message.strip():
            await update.message.reply_text(current_message, parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(message, parse_mode=ParseMode.HTML)

# 添加群组
@require_permission
async def add_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text('只有管理员可以添加群组。')
        return
    
    context.user_data['waiting_for'] = 'add_group_chat_id'
    await update.message.reply_text('请提供要添加的群组ID（数字）:')

# 处理添加群组的逻辑
async def process_add_group(update: Update, context: ContextTypes.DEFAULT_TYPE, chat_id_str: str):
    try:
        chat_id = int(chat_id_str)
        
        # 尝试获取群组信息
        try:
            chat = await context.bot.get_chat(chat_id)
            record_group(chat_id, chat.title)
            await update.message.reply_text(f'成功添加群组: {chat.title} (ID: {chat_id})')
            # 记录操作日志（添加群组）
            try:
                append_botlog(update, 'add_group', {
                    'group_id': chat_id,
                    'group_name': chat.title
                })
            except Exception as e:
                logger.error(f"记录‘添加群组’日志失败: {e}")
        except Exception as e:
            await update.message.reply_text(f'无法获取群组信息，但已添加ID: {chat_id}。错误: {str(e)}')
            record_group(chat_id, f'群组_{chat_id}')
            # 记录操作日志（添加群组-仅ID）
            try:
                append_botlog(update, 'add_group', {
                    'group_id': chat_id,
                    'group_name': f'群组_{chat_id}',
                    'error': str(e)
                })
            except Exception as e2:
                logger.error(f"记录‘添加群组(仅ID)’日志失败: {e2}")
    except ValueError:
        await update.message.reply_text('无效的群组ID。请提供有效的数字ID。')

# 列出所有分类
@require_permission
async def list_categories(update: Update, context: ContextTypes.DEFAULT_TYPE):
    categories = load_categories()
    
    if not categories:
        await update.message.reply_text('目前没有任何分类。')
        return
    
    message = "🏷️ **分类列表：**\n\n"
    for i, category in enumerate(categories, 1):
        # 统计使用该分类的群组数量
        groups = load_groups()
        count = sum(1 for group in groups if category in group.get('categories', []))
        message += f"{i}. {category} ({count} 个群组)\n"
    
    await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)

# 添加分类
@require_permission
async def add_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text('只有管理员可以添加分类。')
        return
    
    context.user_data['waiting_for'] = 'category_name'
    await update.message.reply_text('请输入新分类的名称:')

# 编辑分类
@require_permission
async def edit_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text('只有管理员可以编辑分类。')
        return
    
    categories = load_categories()
    
    if not categories:
        await update.message.reply_text('目前没有任何分类。')
        return
    
    message = "请选择要编辑的分类:\n\n"
    for i, category in enumerate(categories, 1):
        message += f"{i}. {category}\n"
    
    message += "\n请输入分类名称或序号:"
    
    context.user_data['waiting_for'] = 'edit_category_old'
    await update.message.reply_text(message)

# 移除分类
@require_permission
async def remove_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text('只有管理员可以移除分类。')
        return
    
    # 创建内联键盘
    keyboard = [
        [InlineKeyboardButton("从群组中移除分类", callback_data="remove_from_group")],
        [InlineKeyboardButton("完全删除分类", callback_data="delete_category")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "请选择操作类型:",
        reply_markup=reply_markup
    )

# 处理移除分类的回调
async def handle_remove_category_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == "remove_from_group":
        context.user_data['waiting_for'] = 'remove_category_chat_id'
        await query.edit_message_text('请输入要移除分类的群组ID:')
    elif query.data == "delete_category":
        categories = load_categories()
        
        if not categories:
            await query.edit_message_text('目前没有任何分类。')
            return
        
        message = "⚠️ **警告：这将完全删除分类并从所有群组中移除！**\n\n"
        message += "请选择要删除的分类:\n\n"
        for i, category in enumerate(categories, 1):
            message += f"{i}. {category}\n"
        
        message += "\n请输入分类名称或序号:"
        
        context.user_data['waiting_for'] = 'delete_category_name'
        await query.edit_message_text(message, parse_mode=ParseMode.MARKDOWN)

# 分配分类
@require_permission
async def assign_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text('只有管理员可以分配分类。')
        return
    
    context.user_data['waiting_for'] = 'assign_chat_id'
    await update.message.reply_text('请输入要分配分类的群组ID:\n(支持多个群组ID，可用逗号或分号分隔，如：123456789,987654321 或 123456789;987654321)')

# 重命名群组
@require_permission
async def rename_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text('只有管理员可以重命名群组。')
        return
    
    context.user_data['waiting_for'] = 'rename_chat_id'
    await update.message.reply_text('请输入要设置备注的群组ID:')

# 处理文本输入
async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return
    
    # 检查用户是否在等待输入
    waiting_for = context.user_data.get('waiting_for')
    if not waiting_for:
        return
    
    text = update.message.text.strip()
    
    # 处理不同类型的等待输入
    if waiting_for == 'category_name':
        # 处理添加分类
        categories = load_categories()
        
        if text in categories:
            await update.message.reply_text(f'分类 "{text}" 已存在。')
        else:
            categories.append(text)
            save_categories(categories)
            await update.message.reply_text(f'成功添加分类: {text}')
            # 记录操作日志（添加分类）
            try:
                append_botlog(update, 'category_add', {
                    'category_name': text
                })
            except Exception as e:
                logger.error(f"记录‘添加分类’日志失败: {e}")
        
        # 清除等待状态
        context.user_data.pop('waiting_for', None)
    
    elif waiting_for == 'search_groups':
        # 处理群组搜索输入（支持分页 + 批次多选 + 预览）
        search_term = text
        
        if not search_term:
            await update.message.reply_text("❌ 请输入有效的搜索关键词。")
            return
        
        try:
            # 获取用户的消息组
            message_groups = context.user_data.get('delete_message_groups', [])
            
            if not message_groups:
                await update.message.reply_text("❌ 没有找到可删除的消息。")
                context.user_data.pop('waiting_for', None)
                return
            
            # 规范化搜索词，支持多关键词与群ID匹配
            # 支持分隔符: '/', ',', '，', 空格
            raw = search_term.strip()
            tokens = [t.strip() for t in re.split(r"[\/，,\s]+", raw) if t.strip()]
            token_lowers = [t.lower() for t in tokens]
            token_is_numeric = [t.lstrip('-').isdigit() for t in tokens]
            
            # 搜索匹配的群组
            matched_results = []
            # 加载当前群组名称映射（优先alias，其次title）
            groups_data = load_groups()
            group_name_map = {}
            for g in groups_data:
                name = g.get('alias') or g.get('title') or f"群组 {g.get('chat_id')}"
                group_name_map[g.get('chat_id')] = name
            
            for group_index, (timestamp, msgs) in enumerate(message_groups):
                # 按群组ID分组消息
                groups_in_batch = {}
                for msg in msgs:
                    chat_id = msg['chat_id']
                    if chat_id not in groups_in_batch:
                        # 优先使用消息记录中的群组标题，其次 groups.json 的别名/标题，
                        # 最后回退为“私聊 {chat_id}”或“群组 {chat_id}”
                        recorded_title = msg.get('chat_title')
                        ctype = msg.get('chat_type')
                        chat_username = msg.get('chat_username')
                        first_name = msg.get('chat_first_name')
                        last_name = msg.get('chat_last_name')
                        is_private = (ctype == 'private') or (ctype is None and chat_id > 0)
                        display_title = recorded_title or group_name_map.get(chat_id) or (f'私聊 {chat_id}' if is_private else f'群组 {chat_id}')
                        groups_in_batch[chat_id] = {
                            'chat_id': chat_id,
                            'chat_title': display_title,
                            'message_count': 0,
                            'chat_type': 'private' if is_private else 'group',
                            'chat_username': chat_username,
                            'chat_first_name': first_name,
                            'chat_last_name': last_name
                        }
                    groups_in_batch[chat_id]['message_count'] += 1
                
                # 检查群组名称或群ID是否匹配任一关键词
                for chat_id, group_info in groups_in_batch.items():
                    title_lower = group_info['chat_title'].lower()
                    matches_any = False
                    for idx, tok in enumerate(tokens):
                        if not tok:
                            continue
                        # 名称包含匹配
                        if token_lowers[idx] in title_lower:
                            matches_any = True
                            break
                        # ID 部分匹配（支持负ID与部分匹配）
                        if token_is_numeric[idx] and tok in str(chat_id):
                            matches_any = True
                            break
                    if matches_any:
                        matched_results.append({
                            'group_index': group_index,
                            'timestamp': timestamp,
                            'chat_id': chat_id,
                            'chat_title': group_info['chat_title'],
                            'message_count': group_info['message_count'],
                            'chat_type': group_info.get('chat_type'),
                            'chat_username': group_info.get('chat_username'),
                            'chat_first_name': group_info.get('chat_first_name'),
                            'chat_last_name': group_info.get('chat_last_name')
                        })
            
            if not matched_results:
                text_msg = f"🔍 搜索结果\n\n"
                text_msg += f"关键词: {search_term}\n"
                text_msg += f"❌ 没有找到匹配的群组。"
                
                keyboard = [[InlineKeyboardButton("🔙 返回主菜单", callback_data="back_to_main")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await update.message.reply_text(text_msg, reply_markup=reply_markup)
                context.user_data.pop('waiting_for', None)
                return
            
            # 排序并存储搜索结果上下文
            try:
                matched_results.sort(key=lambda r: r['timestamp'], reverse=True)
            except Exception:
                pass
            context.user_data['search_results'] = matched_results
            context.user_data['search_term'] = search_term
            context.user_data['search_page'] = 0
            context.user_data['search_mode'] = True
            
            # 渲染第一页（分页+多选+预览）
            page = 0
            items_per_page = 5
            start_idx = page * items_per_page
            end_idx = start_idx + items_per_page
            page_items = matched_results[start_idx:end_idx]
            
            text_msg = f"🔍 搜索结果\n\n"
            text_msg += f"关键词: {search_term}\n"
            text_msg += f"找到 {len(matched_results)} 个匹配的群组:\n\n"
            
            keyboard = []
            selected_batches = context.user_data.get('selected_batches', set()) or set()
            try:
                selected_batches = set(selected_batches)
            except Exception:
                selected_batches = set()
            
            for i, result in enumerate(page_items):
                actual_index = start_idx + i
                try:
                    dt = datetime.datetime.fromisoformat(result['timestamp'])
                    time_str = dt.strftime("%m-%d %H:%M")
                except Exception:
                    time_str = str(result['timestamp'])[:16]
                ctype = result.get('chat_type')
                is_private = (ctype == 'private') or (ctype is None and int(result['chat_id']) > 0)
                if is_private:
                    fname = result.get('chat_first_name') or ''
                    lname = result.get('chat_last_name') or ''
                    uname = result.get('chat_username')
                    if not fname and not lname and not uname:
                        try:
                            chat = await context.bot.get_chat(result['chat_id'])
                            if chat:
                                uname = chat.username or None
                                fname = chat.first_name or ''
                                lname = chat.last_name or ''
                                result['chat_username'] = uname if uname else result.get('chat_username')
                                result['chat_first_name'] = fname if fname else result.get('chat_first_name')
                                result['chat_last_name'] = lname if lname else result.get('chat_last_name')
                                matched_results[actual_index] = result
                                context.user_data['search_results'] = matched_results
                        except Exception:
                            pass
                    name_str = (fname + (f" {lname}" if lname else '')).strip() or '-'
                    uname_str = f"@{uname}" if uname else '-'
                    title_line = f"{name_str} | {uname_str} | {result['chat_id']}"
                else:
                    group_name = result.get('chat_title', '') or f"群组 {result['chat_id']}"
                    title_line = f"{group_name} | {result['chat_id']}"
                text_msg += f"{actual_index+1}. {title_line}\n"
                text_msg += f"   时间: {time_str} | 消息数: {result['message_count']}\n\n"
                
                is_selected = result['group_index'] in selected_batches
                checkbox = '✅ 已选批次' if is_selected else '☐ 选择批次'
                keyboard.append([
                    InlineKeyboardButton(f"🗑️ 删除 #{actual_index+1}", callback_data=f"delete_search_{result['group_index']}_{result['chat_id']}"),
                    InlineKeyboardButton(f"👁️ 预览 #{actual_index+1}", callback_data=f"preview_search_{result['group_index']}_{result['chat_id']}"),
                    InlineKeyboardButton(f"{checkbox}", callback_data=f"toggle_batch_{result['group_index']}")
                ])
            
            if len(selected_batches) > 0:
                keyboard.append([
                    InlineKeyboardButton("🗑️ 全删选中", callback_data="delete_selected_batches"),
                    InlineKeyboardButton("👁️ 全预览选中", callback_data="preview_selected_batches")
                ])
            
            nav_buttons = []
            if page > 0:
                nav_buttons.append(InlineKeyboardButton("⬅️ 上一页", callback_data=f"search_page_{page - 1}"))
            if end_idx < len(matched_results):
                nav_buttons.append(InlineKeyboardButton("➡️ 下一页", callback_data=f"search_page_{page + 1}"))
            if nav_buttons:
                keyboard.append(nav_buttons)
            keyboard.append([InlineKeyboardButton("🔙 返回主菜单", callback_data="back_to_main")])
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(text_msg, reply_markup=reply_markup)
            
        except Exception as e:
            logger.error(f"处理群组搜索失败: {str(e)}")
            await update.message.reply_text("❌ 搜索失败，请重试。")
        
        # 清除等待状态
        context.user_data.pop('waiting_for', None)
    
    elif waiting_for == 'assign_chat_id':
        # 处理分配分类的群组ID输入
        try:
            # 支持多个群组ID，可用逗号或分号分隔
            chat_ids = []
            # 替换所有分号为逗号，然后按逗号分割
            id_texts = text.replace(';', ',').split(',')
            
            for id_text in id_texts:
                id_text = id_text.strip()
                if id_text:  # 确保不是空字符串
                    chat_id = int(id_text)
                    chat_ids.append(chat_id)
            
            if not chat_ids:
                await update.message.reply_text('未提供有效的群组ID。请重新输入。')
                return
                
            context.user_data['assign_chat_ids'] = chat_ids
            context.user_data['waiting_for'] = 'assign_category'
            
            # 显示分类列表供选择
            categories = load_categories()
            if not categories:
                await update.message.reply_text('目前没有任何分类。请先创建分类。')
                context.user_data.pop('waiting_for', None)
                context.user_data.pop('assign_chat_ids', None)
                return
                
            message = f"已识别 {len(chat_ids)} 个群组ID: {', '.join(str(id) for id in chat_ids)}\n\n请选择要分配的分类:\n\n"
            for i, category in enumerate(categories, 1):
                message += f"{i}. {category}\n"
                
            await update.message.reply_text(message)
        except ValueError:
            await update.message.reply_text('无效的群组ID。请提供有效的数字ID，多个ID用逗号或分号分隔。')
    
    elif waiting_for == 'assign_category':
        # 处理分配分类的分类名输入
        chat_ids = context.user_data.get('assign_chat_ids', [])
        if not chat_ids:
            # 兼容旧版本
            chat_id = context.user_data.get('assign_chat_id')
            if chat_id:
                chat_ids = [chat_id]
            else:
                await update.message.reply_text('出错了，请重新开始。')
                context.user_data.pop('waiting_for', None)
                return
        
        categories = load_categories()
        
        # 检查是否输入了分类序号
        try:
            index = int(text) - 1
            if 0 <= index < len(categories):
                category_name = categories[index]
            else:
                await update.message.reply_text('无效的分类序号。')
                return
        except ValueError:
            # 如果不是序号，则直接使用输入的文本作为分类名
            category_name = text
        
        if category_name not in categories:
            await update.message.reply_text(f'分类 "{category_name}" 不存在。')
        else:
            groups = load_groups()
            
            # 记录成功和失败的群组
            success_groups = []
            failed_groups = []
            already_in_category = []
            # 详细日志数据
            success_details = []
            already_details = []
            failed_details = []
            
            # 处理每个群组ID
            for chat_id in chat_ids:
                group_found = False
                for group in groups:
                    if group.get('chat_id') == chat_id:
                        # 支持多分类：将category改为categories列表
                        if 'categories' not in group:
                            # 迁移旧数据：将单个category转换为categories列表
                            old_category = group.get('category', '')
                            group['categories'] = [old_category] if old_category else []
                            if 'category' in group:
                                del group['category']
                        
                        # 添加新分类（如果不存在）
                        if category_name not in group['categories']:
                            group['categories'].append(category_name)
                            success_groups.append(f"{group.get('title', chat_id)} (ID: {chat_id})")
                            success_details.append({
                                'group_id': chat_id,
                                'group_name': (group.get('alias') or group.get('title') or f"群组 {chat_id}")
                            })
                        else:
                            already_in_category.append(f"{group.get('title', chat_id)} (ID: {chat_id})")
                            already_details.append({
                                'group_id': chat_id,
                                'group_name': (group.get('alias') or group.get('title') or f"群组 {chat_id}")
                            })
                        
                        group_found = True
                        break
                
                if not group_found:
                    failed_groups.append(f"群组ID {chat_id} 不存在")
                    failed_details.append({
                        'group_id': chat_id,
                        'reason': 'group_not_found'
                    })
            
            # 构建响应消息
            response_parts = []
            if success_groups:
                response_parts.append(f"✅ 已将以下群组分配到分类 \"{category_name}\"：\n- " + "\n- ".join(success_groups))
            
            if already_in_category:
                response_parts.append(f"ℹ️ 以下群组已经在分类 \"{category_name}\" 中：\n- " + "\n- ".join(already_in_category))
            
            if failed_groups:
                response_parts.append(f"❌ 以下群组分配失败：\n- " + "\n- ".join(failed_groups))
            
            # 保存更新
            if success_groups:
                save_groups(groups)
                
            await update.message.reply_text("\n\n".join(response_parts))

            # 记录操作日志（分配分类）
            try:
                append_botlog(update, 'category_assign', {
                    'category_name': category_name,
                    'assigned_count': len(success_details),
                    'already_count': len(already_details),
                    'failed_count': len(failed_details),
                    'assigned_groups': success_details,
                    'already_groups': already_details,
                    'failed_groups': failed_details
                })
            except Exception as e:
                logger.error(f"记录‘分配分类’日志失败: {e}")
            
            # 清除等待状态
            context.user_data.pop('waiting_for', None)
            context.user_data.pop('assign_chat_id', None)
            context.user_data.pop('assign_chat_ids', None)
            return
            
            if not group_found:
                await update.message.reply_text(f'找不到ID为 {chat_id} 的群组。')
            else:
                save_groups(groups)
                await update.message.reply_text(f'已将群组 {chat_id} 添加到分类 "{category_name}"。')
        
        # 清除等待状态
        context.user_data.pop('waiting_for', None)
        context.user_data.pop('assign_chat_id', None)
    
    elif waiting_for == 'rename_chat_id':
        # 处理重命名群组的群组ID输入
        try:
            chat_id = int(text)
            context.user_data['rename_chat_id'] = chat_id
            context.user_data['waiting_for'] = 'rename_note'
            
            await update.message.reply_text('请输入新的群组备注:')
        except ValueError:
            await update.message.reply_text('无效的群组ID。请提供有效的数字ID。')
    
    elif waiting_for == 'rename_note':
        # 处理重命名群组的备注输入
        chat_id = context.user_data.get('rename_chat_id')
        if not chat_id:
            await update.message.reply_text('出错了，请重新开始。')
            context.user_data.pop('waiting_for', None)
            return
        
        note = text
        groups = load_groups()
        
        group_found = False
        for group in groups:
            if group['chat_id'] == chat_id:
                group['note'] = note
                group_found = True
                break
        
        if not group_found:
            await update.message.reply_text(f'找不到ID为 {chat_id} 的群组。')
        else:
            save_groups(groups)
            await update.message.reply_text(f'已为群组 {chat_id} 设置备注: {note}')
            # 记录操作日志（设置备注）
            try:
                group_name = next((g.get('title') for g in groups if g.get('chat_id') == chat_id), f"群组 {chat_id}")
                append_botlog(update, 'rename', {
                    'group_name': group_name,
                    'group_id': chat_id,
                    'note': note
                })
            except Exception as e:
                logger.error(f"记录‘设置备注’日志失败: {e}")
        
        # 清除等待状态
        context.user_data.pop('waiting_for', None)
        context.user_data.pop('rename_chat_id', None)
    
    elif waiting_for == 'add_group_chat_id':
        # 处理添加群组的群组ID输入
        await process_add_group(update, context, text)
        # 清除等待状态
        context.user_data.pop('waiting_for', None)
    
    elif waiting_for == 'edit_category_old':
        # 处理编辑分类的旧分类名输入
        categories = load_categories()
        
        # 检查是否输入了分类序号
        try:
            index = int(text) - 1
            if 0 <= index < len(categories):
                old_category = categories[index]
            else:
                await update.message.reply_text('无效的分类序号。')
                return
        except ValueError:
            # 如果不是序号，则直接使用输入的文本作为分类名
            old_category = text
        
        if old_category not in categories:
            await update.message.reply_text(f'分类 "{old_category}" 不存在。')
            context.user_data.pop('waiting_for', None)
        else:
            context.user_data['edit_category_old'] = old_category
            context.user_data['waiting_for'] = 'edit_category_new'
            await update.message.reply_text(f'请输入分类 "{old_category}" 的新名称:')
    
    elif waiting_for == 'edit_category_new':
        # 处理编辑分类的新分类名输入
        old_category = context.user_data.get('edit_category_old')
        if not old_category:
            await update.message.reply_text('出错了，请重新开始。')
            context.user_data.pop('waiting_for', None)
            return
        
        new_category = text
        categories = load_categories()
        
        if new_category in categories:
            await update.message.reply_text(f'分类 "{new_category}" 已存在。')
            return
        
        # 更新分类列表
        category_index = categories.index(old_category)
        categories[category_index] = new_category
        save_categories(categories)
        
        # 更新所有群组中的分类引用
        groups = load_groups()
        updated_groups = 0
        for group in groups:
            # 支持新的多分类格式
            if 'categories' in group and old_category in group['categories']:
                category_index = group['categories'].index(old_category)
                group['categories'][category_index] = new_category
                updated_groups += 1
            # 兼容旧的单分类格式
            elif group.get('category') == old_category:
                group['category'] = new_category
                updated_groups += 1
        
        if updated_groups > 0:
            save_groups(groups)
        
        await update.message.reply_text(f'成功将分类 "{old_category}" 重命名为 "{new_category}"，已更新 {updated_groups} 个群组。')
        # 记录操作日志（编辑分类）
        try:
            append_botlog(update, 'category_edit', {
                'old_name': old_category,
                'new_name': new_category,
                'updated_groups': updated_groups
            })
        except Exception as e:
            logger.error(f"记录‘编辑分类’日志失败: {e}")
        
        # 清除等待状态
        context.user_data.pop('waiting_for', None)
        context.user_data.pop('edit_category_old', None)
    
    elif waiting_for == 'remove_category_chat_id':
        # 处理移除分类的群组ID输入
        try:
            chat_id = int(text)
            context.user_data['remove_category_chat_id'] = chat_id
            context.user_data['waiting_for'] = 'remove_category_name'
            
            # 显示该群组的分类
            groups = load_groups()
            group_found = False
            for group in groups:
                if group['chat_id'] == chat_id:
                    group_found = True
                    # 支持新的多分类格式
                    if 'categories' in group and group['categories']:
                        categories_str = ', '.join([f'{i+1}. {cat}' for i, cat in enumerate(group['categories'])])
                        await update.message.reply_text(f'群组 {chat_id} 的分类:\n{categories_str}\n\n请输入要移除的分类名称或序号:')
                    # 兼容旧的单分类格式
                    elif group.get('category'):
                        await update.message.reply_text(f'群组 {chat_id} 的分类: {group["category"]}\n\n请输入要移除的分类名称:')
                    else:
                        await update.message.reply_text(f'群组 {chat_id} 没有分类。')
                        context.user_data.pop('waiting_for', None)
                        context.user_data.pop('remove_category_chat_id', None)
                    break
            
            if not group_found:
                await update.message.reply_text(f'找不到ID为 {chat_id} 的群组。')
                context.user_data.pop('waiting_for', None)
                context.user_data.pop('remove_category_chat_id', None)
        except ValueError:
            await update.message.reply_text('无效的群组ID。请提供有效的数字ID。')
    
    elif waiting_for == 'remove_category_name':
        # 处理移除分类的分类名输入
        chat_id = context.user_data.get('remove_category_chat_id')
        if not chat_id:
            await update.message.reply_text('出错了，请重新开始。')
            context.user_data.pop('waiting_for', None)
            return
        
        groups = load_groups()
        group_found = False
        for group in groups:
            if group['chat_id'] == chat_id:
                group_found = True
                
                # 检查是否输入了分类序号
                category_name = text
                try:
                    index = int(text) - 1
                    if 'categories' in group and 0 <= index < len(group['categories']):
                        category_name = group['categories'][index]
                except ValueError:
                    # 如果不是序号，则直接使用输入的文本作为分类名
                    pass
                
                removed = False
                
                # 支持新的多分类格式
                if 'categories' in group and category_name in group['categories']:
                    group['categories'].remove(category_name)
                    removed = True
                # 兼容旧的单分类格式
                elif group.get('category') == category_name:
                    group['category'] = ''
                    removed = True
                
                if removed:
                    save_groups(groups)
                    await update.message.reply_text(f'已从群组 {chat_id} 中移除分类 "{category_name}"。')
                    # 记录操作日志（从群组移除分类）
                    try:
                        append_botlog(update, 'category_remove_from_group', {
                            'group_id': chat_id,
                            'group_name': group.get('title', f'群组 {chat_id}'),
                            'category_name': category_name
                        })
                    except Exception as e:
                        logger.error(f"记录‘移除分类’日志失败: {e}")
                else:
                    await update.message.reply_text(f'群组 {chat_id} 没有分类 "{category_name}"。')
                break
        
        if not group_found:
            await update.message.reply_text(f'找不到ID为 {chat_id} 的群组。')
        
        # 清除等待状态
        context.user_data.pop('waiting_for', None)
        context.user_data.pop('remove_category_chat_id', None)

    elif waiting_for == 'delete_category_name':
        # 完全删除分类（从分类列表以及所有群组中移除）
        categories = load_categories()
        if not categories:
            await update.message.reply_text('目前没有任何分类。')
            context.user_data.pop('waiting_for', None)
            return
        # 解析输入为名称或序号
        try:
            idx = int(text) - 1
            if 0 <= idx < len(categories):
                category_name = categories[idx]
            else:
                await update.message.reply_text('无效的分类序号。')
                return
        except ValueError:
            category_name = text
        if category_name not in categories:
            await update.message.reply_text(f'分类 "{category_name}" 不存在。')
            context.user_data.pop('waiting_for', None)
            return
        # 从分类列表删除
        categories.remove(category_name)
        save_categories(categories)
        # 从所有群组中移除该分类
        groups = load_groups()
        affected = 0
        affected_details = []
        for group in groups:
            # 多分类格式
            if 'categories' in group and category_name in group['categories']:
                group['categories'] = [c for c in group['categories'] if c != category_name]
                affected += 1
                affected_details.append({
                    'group_id': group.get('chat_id'),
                    'group_name': (group.get('alias') or group.get('title') or f"群组 {group.get('chat_id')}")
                })
            # 旧单分类格式
            elif group.get('category') == category_name:
                group['category'] = ''
                affected += 1
                affected_details.append({
                    'group_id': group.get('chat_id'),
                    'group_name': (group.get('alias') or group.get('title') or f"群组 {group.get('chat_id')}")
                })
        save_groups(groups)
        await update.message.reply_text(f'已完全删除分类 "{category_name}"，从 {affected} 个群组中移除。')
        # 记录操作日志（删除分类）
        try:
            append_botlog(update, 'category_delete', {
                'category_name': category_name,
                'affected_groups': affected,
                'affected_groups_detail': affected_details
            })
        except Exception as e:
            logger.error(f"记录‘删除分类’日志失败: {e}")
        # 清除等待状态
        context.user_data.pop('waiting_for', None)
    
    elif waiting_for == 'whitelist_add_group':
        # 处理添加群组到白名单
        try:
            group_id = int(text)
            whitelist = load_whitelist()
            
            if group_id in whitelist['groups']:
                await update.message.reply_text(f'群组 {group_id} 已在白名单中。')
            else:
                whitelist['groups'].append(group_id)
                save_whitelist(whitelist)
                await update.message.reply_text(f'已将群组 {group_id} 添加到白名单。')
        except ValueError:
            await update.message.reply_text('无效的群组ID。请提供有效的数字ID。')
        
        # 清除等待状态
        context.user_data.pop('waiting_for', None)
    
    elif waiting_for == 'whitelist_remove_group':
        # 处理从白名单移除群组
        try:
            group_id = int(text)
            whitelist = load_whitelist()
            
            if group_id not in whitelist['groups']:
                await update.message.reply_text(f'群组 {group_id} 不在白名单中。')
            else:
                whitelist['groups'].remove(group_id)
                save_whitelist(whitelist)
                await update.message.reply_text(f'已将群组 {group_id} 从白名单中移除。')
        except ValueError:
            await update.message.reply_text('无效的群组ID。请提供有效的数字ID。')
        
        # 清除等待状态
        context.user_data.pop('waiting_for', None)
    
    elif waiting_for == 'whitelist_add_user':
        # 处理添加用户到白名单
        try:
            user_id = int(text)
            whitelist = load_whitelist()
            
            if user_id in whitelist['private_users']:
                await update.message.reply_text(f'用户 {user_id} 已在白名单中。')
            else:
                whitelist['private_users'].append(user_id)
                save_whitelist(whitelist)
                await update.message.reply_text(f'已将用户 {user_id} 添加到白名单。')
        except ValueError:
            await update.message.reply_text('无效的用户ID。请提供有效的数字ID。')
        
        # 清除等待状态
        context.user_data.pop('waiting_for', None)
    
    elif waiting_for == 'whitelist_remove_user':
        # 处理从白名单移除用户
        try:
            user_id = int(text)
            whitelist = load_whitelist()
            
            if user_id not in whitelist['private_users']:
                await update.message.reply_text(f'用户 {user_id} 不在白名单中。')
            else:
                whitelist['private_users'].remove(user_id)
                save_whitelist(whitelist)
                await update.message.reply_text(f'已将用户 {user_id} 从白名单中移除。')
        except ValueError:
            await update.message.reply_text('无效的用户ID。请提供有效的数字ID。')
        
        # 清除等待状态
        context.user_data.pop('waiting_for', None)

    elif waiting_for == 'blacklist_add_user':
        # 处理加入黑名单（支持备注）
        try:
            parts = text.split()
            bl_user_id = int(parts[0])
            remark = ''
            if len(parts) > 1:
                remark = ' '.join(parts[1:]).strip()
            blacklist = load_blacklist()
            users = blacklist.get('users', [])
            existing = next((u for u in users if u.get('id') == bl_user_id), None)
            if existing:
                # 已存在则更新备注（如果提供）
                updated = False
                if remark:
                    existing['remark'] = remark
                    updated = True
                save_blacklist(blacklist)
                msg = f"用户 {bl_user_id} 已在黑名单中。"
                if updated:
                    msg += " 已更新备注。"
                await update.message.reply_text(msg)
            else:
                users.append({'id': bl_user_id, 'remark': remark} if remark else {'id': bl_user_id})
                blacklist['users'] = users
                save_blacklist(blacklist)
                await update.message.reply_text(f'已将用户 {bl_user_id} 加入黑名单。')
            # 记录操作日志
            try:
                append_botlog(update, 'blacklist_add', {
                    'blocked_user_id': bl_user_id,
                    'remark': remark
                })
            except Exception as e:
                logger.error(f"记录‘加入黑名单’日志失败: {e}")
        except ValueError:
            await update.message.reply_text('无效的用户ID。请提供有效的数字ID，示例：12345 滥用点击')
        # 清除等待状态
        context.user_data.pop('waiting_for', None)

    elif waiting_for == 'blacklist_remove_user':
        # 处理移除黑名单
        try:
            bl_user_id = int(text)
            blacklist = load_blacklist()
            users = blacklist.get('users', [])
            new_users = [u for u in users if u.get('id') != bl_user_id]
            if len(new_users) == len(users):
                await update.message.reply_text(f'用户 {bl_user_id} 不在黑名单中。')
            else:
                blacklist['users'] = new_users
                save_blacklist(blacklist)
                await update.message.reply_text(f'已将用户 {bl_user_id} 从黑名单移除。')
            # 记录操作日志
            try:
                append_botlog(update, 'blacklist_remove', {
                    'blocked_user_id': bl_user_id
                })
            except Exception as e:
                logger.error(f"记录‘移除黑名单’日志失败: {e}")
        except ValueError:
            await update.message.reply_text('无效的用户ID。请提供有效的数字ID。')
        # 清除等待状态
        context.user_data.pop('waiting_for', None)

# 导出群组数据
@require_permission
async def export_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text('只有管理员可以导出数据。')
        return
    
    groups = load_groups()
    
    if not groups:
        await update.message.reply_text('没有群组数据可导出。')
        return
    
    # 发送进行中提示并记录开始时间
    start_time = time.perf_counter()
    progress_msg = await update.message.reply_text('⏳ 正在导出数据，请稍候...')
    
    # 准备导出数据
    export_data = []
    for group in groups:
        categories_str = ', '.join(group.get('categories', [])) if group.get('categories') else '无分类'
        # 格式化添加时间与最后活跃时间
        added_raw = group.get('added_date')
        # 若没有用户活跃时间，智能回退到 last_seen（机器人可见的最近事件）
        last_active_raw = group.get('last_user_activity') or group.get('last_seen')
        def fmt(ts):
            if not ts:
                return ''
            try:
                dt = datetime.datetime.fromisoformat(ts)
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                return ts.replace('T', ' ')[:19]
        export_data.append({
            '群组名称': group['title'],
            '群组ID': group['chat_id'],
            '备注': group.get('note', ''),
            '分类': categories_str,
            '添加时间': fmt(added_raw),
            '最后活跃': fmt(last_active_raw)
        })
    
    # 创建DataFrame
    df = pd.DataFrame(export_data)
    
    # 导出为Excel文件
    excel_filename = os.path.join(DATA_DIR, 'groups_export(i1PLAY).xlsx')
    df.to_excel(excel_filename, index=False, engine='openpyxl')
    
    # 导出为CSV文件
    csv_filename = os.path.join(DATA_DIR, 'groups_export(i1PLAY).csv')
    df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
    
    # 发送文件与完成提示
    try:
        with open(excel_filename, 'rb') as f:
            await send_and_record(
                context.bot.send_document,
                chat_id=update.effective_chat.id,
                document=f,
                filename='groups_export(i1PLAY).xlsx',
                caption=f'📊 群组数据导出完成\n\n总计: {len(groups)} 个群组',
                user_id=update.effective_user.id,
                username=_get_display_username(update.effective_user),
                content_summary=f'📊 群组数据导出完成\n\n总计: {len(groups)} 个群组'
            )
        elapsed_seconds = time.perf_counter() - start_time
        elapsed_str = format_elapsed(elapsed_seconds)
        await progress_msg.edit_text(f"✅ 导出完成\n总计: {len(groups)} 个群组\n🕐 耗时: {elapsed_str}")
        # 记录导出操作到 botlog
        try:
            append_botlog(update, 'export', {
                'groups_count': len(groups),
                'excel_file': os.path.basename(excel_filename),
                'csv_file': os.path.basename(csv_filename),
                'elapsed': elapsed_str,
                'elapsed_seconds': round(elapsed_seconds, 3)
            })
        except Exception as e:
            logger.error(f"记录‘导出完成’日志失败: {e}")
    except Exception as e:
        await progress_msg.edit_text(f'导出失败: {str(e)}')

# 查看日志
@require_permission
async def view_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text('只有管理员可以查看日志。')
        return
    
    logs = load_logs()
    
    if not logs:
        await update.message.reply_text('暂无发送日志。')
        return
    
    # 显示最近10条日志
    recent_logs = logs[-10:]
    
    message = "📋 **最近发送日志：**\n\n"
    for log in recent_logs:
        message += f"🕐 {log.get('timestamp', '未知时间')}\n"
        message += f"📤 发送到: {log.get('target_count', 0)} 个目标\n"
        message += f"✅ 成功: {log.get('success_count', 0)} 个\n"
        message += f"❌ 失败: {log.get('failed_count', 0)} 个\n"
        message += f"📝 内容: {log.get('content_preview', '无预览')}\n\n"
    
    await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 非白名单用户静默返回（不提示）
    if not has_permission(update):
        return

    # 时间基准（使用本地 naive 时间，兼容现有 JSON 存储）
    now_naive = datetime.datetime.now()

    # 解析查询日期范围（默认 GMT+8 当天 00:00:00 ~ 23:59:59）
    tz8 = datetime.timezone(datetime.timedelta(hours=8))
    args = getattr(context, 'args', []) or []
    # 更稳健的范围解析：先移除 metrics=...，再剔除图片开关
    arg_str_all = ' '.join(args or [])
    try:
        arg_str_without_metrics = re.sub(r'(?:metrics|指标)\s*=\s*[^\s]+', ' ', arg_str_all, flags=re.I)
    except Exception:
        arg_str_without_metrics = arg_str_all
    tokens = [t for t in arg_str_without_metrics.split() if t.lower() not in ('img','image','图','图片')]
    range_text = ' '.join(tokens).strip()

    # 解析 metrics= 选项（支持中文别名）
    metrics_selected = None
    try:
        arg_str = ' '.join(args or [])
        m = re.search(r'(?:metrics|指标)\s*=\s*([^\s]+)', arg_str, flags=re.I)
        if m:
            raw = m.group(1).strip().strip('"').strip("'")
            parts = re.split(r'[\,\uFF0C\u3001;|]+', raw)
            alias = {
                'groups_total': ['群总数','群组总数','总群数','群总量','groups'],
                'category_total': ['分类总数','分类总量','categories'],
                'new_groups': ['新增群','新增群数','新群','newgroups'],
                'new_categories': ['新增分类','新增分类数','新分类','newcategories'],
                'send_success': ['发送成功','成功数','sendsuccess'],
                'send_failed': ['发送失败','失败数','sendfailed'],
                'send_rate': ['成功率','发送成功率','sendrate','success_rate','successrate'],
                'delete_success': ['删除成功','删除成功数','deletesuccess'],
                'delete_failed': ['删除失败','删除失败数','deletefailed'],
                'delete_rate': ['删除成功率','deleterate'],
                'restricted_groups': ['陌生人拉群','陌生人将bot拉进群','陌生人拉群群数','受限群组','权限受限群','restricted','拦截群组','未加白拉群','blockedgroups','stranger_groups','stranger_add','stranger_add_groups','stranger_adds'],
                'whitelist_users': ['白名单用户数','白名单用户','whitelist'],
                'blacklist_users': ['黑名单用户数','黑名单用户','blacklist'],
                'pending_tasks': ['待执行发送任务','待执行任务','tasks_pending','pendingtasks'],
                'bot_usage': ['公用命令使用数','bot使用数','/bot使用数','botusage']
            }
            inv = {}
            for k, vs in alias.items():
                inv[k] = k
                for v in vs:
                    inv[str(v).lower()] = k
            sel = set()
            for p in parts:
                key = inv.get(str(p).lower())
                if key:
                    sel.add(key)
            if sel:
                metrics_selected = sel
    except Exception:
        metrics_selected = None

    def _parse_range(txt: str):
        now_tz8 = datetime.datetime.now(tz8)
        default_start = now_tz8.replace(hour=0, minute=0, second=0, microsecond=0)
        default_end = now_tz8
        if not txt:
            return default_start.replace(tzinfo=None), default_end.replace(tzinfo=None), f"{default_start.strftime('%Y-%m-%d %H:%M')} ~ {default_end.strftime('%Y-%m-%d %H:%M')} GMT+8"
        try:
            low = txt.lower()
            if '至' in txt:
                parts = txt.split('至')
            elif '~' in txt:
                parts = txt.split('~')
            elif ' to ' in low:
                import re as _re
                parts = _re.split(r'\bto\b', txt)
            else:
                parts = txt.split()
            if len(parts) == 1:
                s = parts[0].strip()
                e = s
            else:
                s = parts[0].strip()
                e = parts[1].strip()
            fmt_date = '%Y-%m-%d'
            fmt_dt1 = '%Y-%m-%d %H:%M'
            fmt_dt2 = '%Y-%m-%d %H:%M:%S'
            def parse_one(x, is_end=False):
                x = x.strip()
                dt = None
                for fmt in (fmt_dt2, fmt_dt1):
                    try:
                        dt = datetime.datetime.strptime(x, fmt)
                        break
                    except Exception:
                        pass
                if dt is None:
                    try:
                        d = datetime.datetime.strptime(x, fmt_date)
                        if is_end:
                            dt = now_tz8 if d.date() == now_tz8.date() else d.replace(hour=23, minute=59, second=59)
                        else:
                            dt = d.replace(hour=0, minute=0, second=0)
                    except Exception:
                        return (default_end if is_end else default_start)
                return dt.replace(tzinfo=tz8).replace(tzinfo=None)
            sdt = parse_one(s, is_end=False)
            edt = parse_one(e, is_end=True)
            if sdt > edt:
                sdt, edt = edt, sdt
            return sdt, edt, f"{sdt.strftime('%Y-%m-%d %H:%M')} ~ {edt.strftime('%Y-%m-%d %H:%M')} GMT+8"
        except Exception:
            return default_start.replace(tzinfo=None), default_end.replace(tzinfo=None), f"{default_start.strftime('%Y-%m-%d %H:%M')} ~ {default_end.strftime('%Y-%m-%d %H:%M')} GMT+8"

    start_dt, end_dt, query_range_str = _parse_range(range_text)

    # 解析筛选：来源(origin)、群组(groups)、分类(categories)
    origin_filter = None
    group_filter_ids = set()
    category_filter_names = set()
    try:
        arg_str = ' '.join(args or [])
        # origin=send|schedule|timer 或 中文：来源=...
        m_origin = re.search(r'(?:origin|来源)\s*=\s*([^\s]+)', arg_str, flags=re.I)
        if m_origin:
            raw = m_origin.group(1).strip().strip('"').strip("'").lower()
            parts = re.split(r'[\,\uFF0C\u3001;|]+', raw)
            mapped = set()
            for p in parts:
                pl = p.strip().lower()
                if pl in ('all','所有','全部'):
                    mapped = set()
                    break
                elif pl in ('send','/send'):
                    mapped.add('send')
                elif pl in ('schedule','schedule_send','/schedule_send','scheduled'):
                    mapped.add('schedule_send')
                elif pl in ('timer','timer_send','/timer_send'):
                    mapped.add('timer_send')
            origin_filter = mapped or None
        # groups=123,456 或群名/别名
        m_groups = re.search(r'(?:groups|群组)\s*=\s*([^\n]+)', arg_str, flags=re.I)
        if m_groups:
            raw = m_groups.group(1).strip()
            parts = re.split(r'[\,\uFF0C\u3001;|]+', raw)
            groups_data = load_groups()
            name_to_id = {}
            for g in groups_data:
                if g.get('alias'):
                    name_to_id[str(g.get('alias')).strip().lower()] = g.get('chat_id')
                if g.get('title'):
                    name_to_id[str(g.get('title')).strip().lower()] = g.get('chat_id')
            for p in parts:
                token = p.strip()
                if not token:
                    continue
                try:
                    gid = int(token)
                    group_filter_ids.add(gid)
                except Exception:
                    gid = name_to_id.get(token.lower())
                    if gid is not None:
                        group_filter_ids.add(gid)
        # categories=名称1,名称2
        m_cats = re.search(r'(?:categories|分类)\s*=\s*([^\n]+)', arg_str, flags=re.I)
        if m_cats:
            raw = m_cats.group(1).strip()
            parts = re.split(r'[\,\uFF0C\u3001;|]+', raw)
            all_categories = load_categories() or []
            lower_map = {c.lower(): c for c in all_categories}
            for p in parts:
                token = p.strip().lower()
                if token in lower_map:
                    category_filter_names.add(lower_map[token])
    except Exception:
        pass

    def include_group_id(gid: int) -> bool:
        # OR 逻辑：若同时提供群组和分类筛选，只要命中其一即可
        cond_group = (not group_filter_ids) or (gid in group_filter_ids)
        if not category_filter_names:
            return cond_group
        # 检查群组是否属于指定分类
        cat_match = False
        try:
            for g in (load_groups() or []):
                if g.get('chat_id') == gid:
                    g_cats = g.get('categories') or []
                    if any(c in g_cats for c in category_filter_names):
                        cat_match = True
                    break
        except Exception:
            cat_match = False
        return cond_group or cat_match

    # Ping 延迟（简单调用 get_me），并获取机器人名称（Name）
    bot_name = None
    try:
        t0 = time.perf_counter()
        me = await context.bot.get_me()
        ping_ms = int((time.perf_counter() - t0) * 1000)
        try:
            bot_name = getattr(me, 'full_name', None) or getattr(me, 'first_name', None)
        except Exception:
            bot_name = None
    except Exception:
        ping_ms = None

    # 运行时间（在 main 中写入 application.bot.start_time）
    uptime_str = '未知'
    try:
        start_time = context.application.bot_data.get('start_time')
        if isinstance(start_time, datetime.datetime):
            now_aware = datetime.datetime.now(datetime.timezone.utc)
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=datetime.timezone.utc)
            uptime_seconds = (now_aware - start_time).total_seconds()
            uptime_str = format_elapsed(uptime_seconds)
    except Exception:
        pass

    # 群组统计
    groups = load_groups()
    groups_total = len(groups)
    active_7d = 0
    new_groups_range = 0
    for g in groups:
        last_seen = g.get('last_seen')
        last_user = g.get('last_user_activity')
        added_date = g.get('added_date')
        latest = None
        try:
            if last_seen:
                latest = datetime.datetime.fromisoformat(last_seen)
                if latest and getattr(latest, 'tzinfo', None) is not None:
                    latest = latest.astimezone(tz8).replace(tzinfo=None)
        except Exception:
            latest = None
        try:
            if last_user:
                dt_user = datetime.datetime.fromisoformat(last_user)
                if dt_user and getattr(dt_user, 'tzinfo', None) is not None:
                    dt_user = dt_user.astimezone(tz8).replace(tzinfo=None)
                if not latest or (dt_user and dt_user > latest):
                    latest = dt_user
        except Exception:
            pass
        if latest and (start_dt <= latest <= end_dt):
            active_7d += 1
        try:
            if added_date:
                dt_added = datetime.datetime.fromisoformat(added_date)
                if dt_added and getattr(dt_added, 'tzinfo', None) is not None:
                    dt_added = dt_added.astimezone(tz8).replace(tzinfo=None)
                if dt_added >= start_dt and dt_added <= end_dt:
                    if not group_filter_ids and not category_filter_names:
                        new_groups_range += 1
                    else:
                        gid = g.get('chat_id')
                        if gid is not None and include_group_id(gid):
                            new_groups_range += 1
        except Exception:
            pass

    # 指定日期范围内的发送成功/失败统计（改为从 botlog，并支持筛选）
    success_in_range = 0
    failed_in_range = 0
    try:
        botlog = load_botlog()
    except Exception:
        botlog = []

    def _parse_dt_any(tstr: str):
        dt = None
        if not tstr:
            return dt
        try:
            dt = datetime.datetime.fromisoformat(tstr)
        except Exception:
            try:
                dt = datetime.datetime.strptime(tstr, '%Y-%m-%d %H:%M:%S')
            except Exception:
                dt = None
        if dt and getattr(dt, 'tzinfo', None) is not None:
            dt = dt.astimezone(tz8).replace(tzinfo=None)
        return dt

    for e in botlog:
        act = e.get('action')
        if act not in ('send', 'schedule_send', 'timer_send'):
            continue
        if origin_filter and act not in origin_filter:
            continue
        dt = _parse_dt_any(e.get('batch_timestamp') or e.get('timestamp') or e.get('time'))
        if not dt or dt < start_dt or dt > end_dt:
            continue
        for gd in (e.get('groups_detail') or []):
            gid = gd.get('group_id') or gd.get('id')
            if gid is None or not include_group_id(gid):
                continue
            st = str(gd.get('status') or '').lower()
            if st == 'success':
                success_in_range += 1
            elif st == 'failed':
                failed_in_range += 1
    total = success_in_range + failed_in_range
    success_rate = (success_in_range / total * 100.0) if total > 0 else 0.0

    # 陌生人拉群（统计查询范围内未加白名单邀请机器人入群的群数量）
    restricted_group_ids = set()
    try:
        botlog = load_botlog()
    except Exception:
        botlog = []

    def _parse_dt_simple(tstr: str):
        dt = None
        if not tstr:
            return dt
        try:
            dt = datetime.datetime.fromisoformat(tstr)
        except Exception:
            try:
                dt = datetime.datetime.strptime(tstr, '%Y-%m-%d %H:%M:%S')
            except Exception:
                dt = None
        if dt and getattr(dt, 'tzinfo', None) is not None:
            dt = dt.astimezone(tz8).replace(tzinfo=None)
        return dt

    for e in botlog:
        action = e.get('action')
        if action != 'bot_blocked_group_add':
            continue
        dt = _parse_dt_any(e.get('timestamp') or e.get('time'))
        if not dt or dt < start_dt or dt > end_dt:
            continue
        gid = e.get('group_id')
        if gid is None or not include_group_id(gid):
            continue
        restricted_group_ids.add(gid)
    restricted_count = len(restricted_group_ids)

    # 白/黑名单用户数量
    try:
        bl = load_blacklist()
        blacklist_users_count = len(bl.get('users', []))
    except Exception:
        blacklist_users_count = 0
    try:
        wl = load_whitelist()
        whitelist_users_count = len(wl.get('private_users', []))
    except Exception:
        whitelist_users_count = 0

    # 待执行定时/计时任务数量
    pending_tasks_count = 0
    try:
        tasks = load_scheduled_tasks()
        pending_tasks_count = sum(1 for t in tasks if t.get('status') == 'scheduled')
    except Exception:
        pending_tasks_count = 0

    # /bot 命令使用次数（范围内，支持群组筛选）
    bot_usage_count = 0
    try:
        for e in botlog:
            act = e.get('action')
            if act != 'bot_command':
                continue
            dt = _parse_dt_any(e.get('timestamp') or e.get('time'))
            if not dt or dt < start_dt or dt > end_dt:
                continue
            gid = e.get('group_id')
            if gid is None or not include_group_id(gid):
                continue
            bot_usage_count += 1
    except Exception:
        pass

    # 删除与新增分类统计（在查询范围内）
    delete_success_in_range = 0
    delete_failed_in_range = 0
    new_categories_in_range = 0
    try:
        for e in botlog:
            dt = _parse_dt_any(e.get('batch_timestamp') or e.get('timestamp') or e.get('time'))
            if not dt or dt < start_dt or dt > end_dt:
                continue
            act = e.get('action')
            if act == 'delete':
                try:
                    delete_success_in_range += int(e.get('deleted_count', 0) or 0)
                except Exception:
                    pass
                try:
                    delete_failed_in_range += int(e.get('failed_count', 0) or 0)
                except Exception:
                    pass
            elif act == 'category_add':
                new_categories_in_range += 1
    except Exception:
        pass
    delete_total = delete_success_in_range + delete_failed_in_range
    delete_rate = (delete_success_in_range / delete_total * 100.0) if delete_total > 0 else 0.0

    # 分类数量
    try:
        categories = load_categories()
        category_count = len(categories)
    except Exception:
        category_count = 0

    # 输出（新版：分组+图标+项目符号，全部加粗；运行时间中文单位）
    # 重新格式化运行时间为“天/小时/分钟/秒”
    try:
        st = context.application.bot_data.get('start_time')
        if st:
            st2 = st.replace(tzinfo=datetime.timezone.utc) if getattr(st, 'tzinfo', None) is None else st
            now = datetime.datetime.now(datetime.timezone.utc)
            delta = now - st2
            total = int(delta.total_seconds())
            d = total // 86400
            h = (total % 86400) // 3600
            m = (total % 3600) // 60
            s = total % 60
            if d > 0:
                uptime_str = f"{d}天 {h}小时 {m}分钟 {s}秒"
            elif h > 0:
                uptime_str = f"{h}小时 {m}分钟 {s}秒"
            elif m > 0:
                uptime_str = f"{m}分钟 {s}秒"
            else:
                uptime_str = f"{s}秒"
        else:
            uptime_str = "未知"
    except Exception:
        # 保底：如果出现异常，保留原值
        pass

    lines = []
    lines.append("<b>🤖 机器人状态总览</b>")
    lines.append("")
    lines.append("<b>🕒 系统信息</b>")
    lines.append(f"<b>• 运行时间: {uptime_str}</b>")
    lines.append(f"<b>• 延迟 (Ping): {'无法测量' if ping_ms is None else str(ping_ms) + ' ms'}</b>")
    lines.append(f"<b>• 查询时间: {query_range_str}</b>")
    lines.append("")
    # 群组统计（根据 metrics 过滤）
    group_lines = []
    if metrics_selected is None or 'groups_total' in metrics_selected:
        group_lines.append(f"<b>• 群总数: {groups_total}</b>")
    if metrics_selected is None or 'category_total' in metrics_selected:
        group_lines.append(f"<b>• 分类总数: {category_count}</b>")
    if metrics_selected is None or 'new_groups' in metrics_selected:
        group_lines.append(f"<b>• 新增群数: {new_groups_range}</b>")
    if metrics_selected is None or 'new_categories' in metrics_selected:
        group_lines.append(f"<b>• 新增分类数: {new_categories_in_range}</b>")
    if metrics_selected is None or 'restricted_groups' in metrics_selected:
        group_lines.append(f"<b>• 陌生人拉群: {restricted_count}</b>")
    if group_lines:
        lines.append("<b>👥 群组统计</b>")
        lines.extend(group_lines)
        lines.append("")
    # 消息与删除统计（根据 metrics 过滤）
    # 消息统计
    msg_lines = []
    if metrics_selected is None or 'send_success' in metrics_selected:
        msg_lines.append(f"<b>• 发送成功: {success_in_range}</b>")
    if metrics_selected is None or 'send_failed' in metrics_selected:
        msg_lines.append(f"<b>• 发送失败: {failed_in_range}</b>")
    if metrics_selected is None or 'send_rate' in metrics_selected:
        msg_lines.append(f"<b>• 成功率: {success_rate:.1f}%</b>")
    if msg_lines:
        lines.append("<b>📬 消息统计</b>")
        lines.extend(msg_lines)
        lines.append("")

    # 删除统计
    delete_lines = []
    if metrics_selected is None or 'delete_success' in metrics_selected:
        delete_lines.append(f"<b>• 删除成功: {delete_success_in_range}</b>")
    if metrics_selected is None or 'delete_failed' in metrics_selected:
        delete_lines.append(f"<b>• 删除失败: {delete_failed_in_range}</b>")
    if metrics_selected is None or 'delete_rate' in metrics_selected:
        delete_lines.append(f"<b>• 删除成功率: {delete_rate:.1f}%</b>")
    if delete_lines:
        lines.append("<b>🗑️ 删除统计</b>")
        lines.extend(delete_lines)
        lines.append("")

    # 访问控制
    access_lines = []
    if metrics_selected is None or 'whitelist_users' in metrics_selected:
        access_lines.append(f"<b>• 白名单用户: {whitelist_users_count}</b>")
    if metrics_selected is None or 'blacklist_users' in metrics_selected:
        access_lines.append(f"<b>• 黑名单用户: {blacklist_users_count}</b>")
    if access_lines:
        lines.append("<b>🔐 访问控制</b>")
        lines.extend(access_lines)
        lines.append("")

    # 任务统计
    task_lines = []
    if metrics_selected is None or 'pending_tasks' in metrics_selected:
        task_lines.append(f"<b>• 待执行任务: {pending_tasks_count}</b>")
    if task_lines:
        lines.append("<b>📋 任务统计</b>")
        lines.extend(task_lines)
        lines.append("")

    # 交互统计
    interact_lines = []
    if metrics_selected is None or 'bot_usage' in metrics_selected:
        interact_lines.append(f"<b>• /bot 使用次数: {bot_usage_count}</b>")
    if interact_lines:
        lines.append("<b>💬 交互统计</b>")
        lines.extend(interact_lines)
        lines.append("")

    text = "\n".join(lines)

    # 是否需要图片输出：支持 /status img|image|图|图片
    args = getattr(context, 'args', [])
    want_image = any(str(a).lower() in ('img', 'image', '图', '图片') for a in (args or []))
    view_label = 'simple'

    if want_image:
        try:
            from PIL import Image, ImageDraw, ImageFont, ImageFilter  # 可选依赖
            import io

            width, height = 1300, 630
            # 轻主题配色（与示例卡片一致）
            BG = (242, 240, 240)            # 页面浅灰背景
            CARD_BG = (255, 255, 255)       # 卡片纯白
            TEXT_MAIN = (33, 37, 41)        # 主要深色文字
            TEXT_SUB = (120, 126, 134)      # 次要灰色文字
            BLUE = (0, 153, 255)
            GREEN = (60, 179, 113)
            RED = (255, 85, 85)

            im = Image.new("RGB", (width, height), BG)
            draw = ImageDraw.Draw(im)

            def try_font(paths, size):
                for p in paths:
                    try:
                        return ImageFont.truetype(p, size)
                    except Exception:
                        continue
                return ImageFont.load_default()

            cn_fonts = [
                r"C:\\Windows\\Fonts\\msyh.ttc",
                r"C:\\Windows\\Fonts\\Microsoft YaHei UI.ttf",
                r"C:\\Windows\\Fonts\\SimHei.ttf",
                r"C:\\Windows\\Fonts\\Arial.ttf",
            ]
            title_font = try_font(cn_fonts, 40)
            section_font = try_font(cn_fonts, 28)
            label_font = try_font(cn_fonts, 22)
            value_font = try_font(cn_fonts, 44)

            emoji_fonts = [
                r"C:\\Windows\\Fonts\\seguiemj.ttf",
                r"C:\\Windows\\Fonts\\Segoe UI Emoji.ttf",
            ]
            emoji_font = try_font(emoji_fonts, 40)

            # 标题（表情与文字分开绘制，避免方块）
            title_x, title_y = 40, 30
            try:
                emoji_w = draw.textlength("🤖", font=emoji_font)
            except Exception:
                b = draw.textbbox((0, 0), "🤖", font=emoji_font)
                emoji_w = b[2] - b[0]
            if emoji_w and emoji_w > 0:
                draw.text((title_x, title_y + 12), "🤖", font=emoji_font, fill=TEXT_MAIN)
                title_x += int(emoji_w) + 12
            draw.text((title_x, title_y), "机器人状态总览", font=title_font, fill=TEXT_MAIN)
            # 右侧显示机器人名称（Name，不是 Username/ID）
            if bot_name:
                try:
                    name_w = draw.textlength(bot_name, font=section_font)
                except Exception:
                    bname = draw.textbbox((0, 0), bot_name, font=section_font)
                    name_w = bname[2] - bname[0]
                draw.text((width - 40 - int(name_w), title_y + 4), bot_name, font=section_font, fill=TEXT_MAIN)
                # 日期时间（置于名称下方，右对齐）
                dt_str = query_range_str
                try:
                    dt_w = draw.textlength(dt_str, font=label_font)
                except Exception:
                    bb = draw.textbbox((0, 0), dt_str, font=label_font)
                    dt_w = bb[2] - bb[0]
                draw.text((width - 40 - int(dt_w), title_y + 40), dt_str, font=label_font, fill=TEXT_MAIN)
             # 移除分隔线

            # 新卡片绘制：白卡 + 左侧竖向渐变强调条（与示例一致）
            def draw_card(x, y, w, h, gradient_top=(96, 80, 220), gradient_bottom=(0, 153, 255)):
                # 定向阴影（仅右下角），避免顶部与左侧出现灰影
                shadow = Image.new("RGBA", (w + 24, h + 24), (0, 0, 0, 0))
                shadow_draw = ImageDraw.Draw(shadow)
                shadow_draw.rounded_rectangle((0, 0, w, h), radius=20, fill=(0, 0, 0, 80))
                shadow = shadow.filter(ImageFilter.GaussianBlur(14))
          

                # 主卡片（白底，无边框）
                draw.rounded_rectangle((x, y, x + w, y + h), radius=22, fill=CARD_BG)

                # 左侧渐变色条（移到卡片外侧，圆角与卡片一致）
                bar_w = 5
                # 非独立圆边：统一尺寸的卡片形遮罩与渐变，避免不匹配
                edge_pad = 1
                accent_w, accent_h = bar_w + w + edge_pad, h
                accent = Image.new("RGBA", (accent_w, accent_h), (0, 0, 0, 0))
                mask = Image.new("L", (accent_w, accent_h), 0)
                mdraw = ImageDraw.Draw(mask)
                mdraw.rounded_rectangle((0, 0, accent_w, accent_h), radius=36, fill=255)

                grad = Image.new("RGBA", (accent_w, accent_h), (0, 0, 0, 0))
                gdraw = ImageDraw.Draw(grad)
                for i in range(accent_h):
                    t = i / accent_h
                    rr = int(gradient_top[0] * (1 - t) + gradient_bottom[0] * t)
                    gg = int(gradient_top[1] * (1 - t) + gradient_bottom[1] * t)
                    bb = int(gradient_top[2] * (1 - t) + gradient_bottom[2] * t)
                    gdraw.line([(0, i), (bar_w + edge_pad, i)], fill=(rr, gg, bb))

                accent = Image.composite(grad, accent, mask)
                im.paste(accent, (x - bar_w - edge_pad, y), accent)

            # 绘制指标
            def draw_metric(x, y, label, value, color=TEXT_MAIN):
                draw.text((x, y), label, font=label_font, fill=TEXT_SUB)
                draw.text((x, y + 30), value, font=value_font, fill=color)

            # 根据 metrics_selected 决定显示哪些卡片（动态压缩布局）
            group_keys_base = ['groups_total','category_total','new_groups','new_categories','restricted_groups','whitelist_users','blacklist_users','bot_usage']
            send_keys_base = ['send_success','send_failed','send_rate','pending_tasks']
            delete_keys_base = ['delete_success','delete_failed','delete_rate']

            if metrics_selected is None:
                show_group = True; show_send = True; show_delete = True
            else:
                show_group = any(k in metrics_selected for k in group_keys_base)
                show_send = any(k in metrics_selected for k in send_keys_base)
                show_delete = any(k in metrics_selected for k in delete_keys_base)

            visible = []
            if show_group: visible.append('group')
            if show_send: visible.append('send')
            if show_delete: visible.append('delete')
            if not visible:
                visible = ['group','send','delete']  # 安全兜底

            # 动态布局
            PADDING, GAP = 60, 40
            cols = len(visible)
            CARD_W = (width - 1 * PADDING - GAP * (cols - 1)) // cols
            CARD_H = 450
            TOP_Y = 110
            def card_x(idx):
                return PADDING + idx * (CARD_W + GAP)

            idx = 0
            # 左/第1张：群组统计
            if show_group:
                x = card_x(idx)
                draw_card(x, TOP_Y, CARD_W, CARD_H, (96, 80, 220), (0, 153, 255))
                draw.text((x + 40, TOP_Y + 20), "群组统计", font=section_font, fill=TEXT_MAIN)
                y = TOP_Y + 80
                left_order = group_keys_base
                left_keys = left_order if (metrics_selected is None) else [k for k in left_order if k in metrics_selected]
                if metrics_selected is None and not left_keys:
                    left_keys = ['groups_total','category_total','new_groups']
                def left_label_value_color(k):
                    if k == 'groups_total': return ("群总数", str(groups_total), TEXT_MAIN)
                    if k == 'category_total': return ("分类总数", str(category_count), TEXT_MAIN)
                    if k == 'new_groups': return ("新增群数", str(new_groups_range), TEXT_MAIN)
                    if k == 'new_categories': return ("新增分类数", str(new_categories_in_range), TEXT_MAIN)
                    if k == 'restricted_groups': return ("陌生人拉群", str(restricted_count), RED)
                    if k == 'whitelist_users': return ("白名单用户", str(whitelist_users_count), TEXT_MAIN)
                    if k == 'blacklist_users': return ("黑名单用户", str(blacklist_users_count), RED)
                    if k == 'bot_usage': return ("/bot 使用次数", str(bot_usage_count), TEXT_MAIN)
                    return (k, "-", TEXT_MAIN)
                # 支持超过3项时的两列布局
                if len(left_keys) <= 3:
                    for k in left_keys:
                        label, val, col = left_label_value_color(k)
                        draw_metric(x + 40, y, label, val, col); y += 90
                else:
                    import math
                    items_per_col = math.ceil(len(left_keys) / 2)
                    col_x = [x + 40, x + 40 + CARD_W // 2]
                    for i, k in enumerate(left_keys):
                        label, val, col = left_label_value_color(k)
                        col_idx = 0 if i < items_per_col else 1
                        row_idx = i if i < items_per_col else i - items_per_col
                        yy = TOP_Y + 80 + row_idx * 90
                        draw_metric(col_x[col_idx], yy, label, val, col)
                idx += 1

            # 中/下一张：消息统计
            if show_send:
                x = card_x(idx)
                draw_card(x, TOP_Y, CARD_W, CARD_H, (96, 80, 220), (0, 153, 255))
                draw.text((x + 40, TOP_Y + 20), "消息统计", font=section_font, fill=TEXT_MAIN)
                y_mid = TOP_Y + 80
                mid_order = send_keys_base
                mid_keys = mid_order if (metrics_selected is None) else [k for k in mid_order if k in metrics_selected]
                if metrics_selected is None and not mid_keys:
                    mid_keys = mid_order
                def mid_label_value_color(k):
                    if k == 'send_success': return ("发送成功", str(success_in_range), GREEN)
                    if k == 'send_failed': return ("发送失败", str(failed_in_range), RED)
                    if k == 'send_rate': return ("成功率", f"{success_rate:.1f}%", BLUE)
                    if k == 'pending_tasks': return ("待执行任务", str(pending_tasks_count), TEXT_MAIN)
                    return (k, "-", TEXT_MAIN)
                # 统一单列纵向布局（最多显示前4项）
                for k in mid_keys[:4]:
                    label, val, col = mid_label_value_color(k)
                    draw_metric(x + 40, y_mid, label, val, col); y_mid += 90
                # 进度环（成功率）
                if 'send_rate' in mid_keys:
                    pr_center_y = TOP_Y + CARD_H // 2
                    pr_x = x + CARD_W - 90
                    def draw_two_color_ring(cx, cy, radius, success_count, failed_count, label_text, line_w=14):
                        total = (success_count or 0) + (failed_count or 0)
                        # 无数据：整圈蓝色
                        if total <= 0:
                            draw.arc([cx - radius, cy - radius, cx + radius, cy + radius], start=0, end=360, fill=BLUE, width=line_w)
                        else:
                            # 灰色底环
                            draw.arc([cx - radius, cy - radius, cx + radius, cy + radius], start=0, end=360, fill=(220, 226, 235), width=line_w)
                            # 成功（蓝色）+ 失败（红色）分段显示
                            succ_pct = max(0.0, min(100.0, float(success_count) * 100.0 / float(total)))
                            fail_pct = 100.0 - succ_pct
                            start_ang = 270
                            succ_end = start_ang + int(360 * succ_pct / 100)
                            fail_end = start_ang + 360
                            if succ_pct > 0:
                                draw.arc([cx - radius, cy - radius, cx + radius, cy + radius], start=start_ang, end=succ_end, fill=BLUE, width=line_w)
                            if fail_pct > 0:
                                draw.arc([cx - radius, cy - radius, cx + radius, cy + radius], start=succ_end, end=fail_end, fill=RED, width=line_w)
                        lb = draw.textbbox((0,0), label_text, font=label_font)
                        lx = cx - (lb[2] - lb[0]) // 2
                        draw.text((lx, cy + radius + 16), label_text, font=label_font, fill=TEXT_SUB)
                    draw_two_color_ring(pr_x, pr_center_y, 60, success_in_range, failed_in_range, "成功率", line_w=12)
                idx += 1

            # 右/最后一张：删除统计
            if show_delete:
                x = card_x(idx)
                draw_card(x, TOP_Y, CARD_W, CARD_H, (96, 80, 220), (0, 153, 255))
                draw.text((x + 40, TOP_Y + 20), "删除统计", font=section_font, fill=TEXT_MAIN)
                y_right = TOP_Y + 80
                right_order = delete_keys_base
                right_keys = right_order if (metrics_selected is None) else [k for k in right_order if k in metrics_selected]
                if metrics_selected is None and not right_keys:
                    right_keys = right_order
                def right_label_value_color(k):
                    if k == 'delete_success': return ("删除成功", str(delete_success_in_range), GREEN)
                    if k == 'delete_failed': return ("删除失败", str(delete_failed_in_range), RED)
                    if k == 'delete_rate': return ("删除成功率", f"{delete_rate:.1f}%", BLUE)
                    return (k, "-", TEXT_MAIN)
                for k in right_keys[:3]:
                    label, val, col = right_label_value_color(k)
                    draw_metric(x + 40, y_right, label, val, col); y_right += 90
                # 进度环（删除成功率）
                if 'delete_rate' in right_keys:
                    pr_center_y = TOP_Y + CARD_H // 2
                    pr_x = x + CARD_W - 90
                    def draw_two_color_ring(cx, cy, radius, success_count, failed_count, label_text, line_w=14):
                        total = (success_count or 0) + (failed_count or 0)
                        if total <= 0:
                            draw.arc([cx - radius, cy - radius, cx + radius, cy + radius], start=0, end=360, fill=BLUE, width=line_w)
                        else:
                            draw.arc([cx - radius, cy - radius, cx + radius, cy + radius], start=0, end=360, fill=(220, 226, 235), width=line_w)
                            succ_pct = max(0.0, min(100.0, float(success_count) * 100.0 / float(total)))
                            fail_pct = 100.0 - succ_pct
                            start_ang = 270
                            succ_end = start_ang + int(360 * succ_pct / 100)
                            fail_end = start_ang + 360
                            if succ_pct > 0:
                                draw.arc([cx - radius, cy - radius, cx + radius, cy + radius], start=start_ang, end=succ_end, fill=BLUE, width=line_w)
                            if fail_pct > 0:
                                draw.arc([cx - radius, cy - radius, cx + radius, cy + radius], start=succ_end, end=fail_end, fill=RED, width=line_w)
                        lb = draw.textbbox((0,0), label_text, font=label_font)
                        lx = cx - (lb[2] - lb[0]) // 2
                        draw.text((lx, cy + radius + 16), label_text, font=label_font, fill=TEXT_SUB)
                    draw_two_color_ring(pr_x, pr_center_y, 60, delete_success_in_range, delete_failed_in_range, "删除成功率", line_w=12)


            # 底部系统信息
            ping_text = "无法测量" if ping_ms is None else f"{ping_ms} ms"
            sysinfo = f"系统信息 · 运行时间: {uptime_str} · 延迟: {ping_text}"
            draw.text((40, height - 50), sysinfo, font=label_font, fill=TEXT_SUB)

            buf = io.BytesIO()
            im.save(buf, format="PNG")
            buf.seek(0)
            await update.message.reply_photo(photo=buf)
            view_label = 'image'
        except Exception as e:
            # 发送图片可能出现网络超时，但图片仍可能稍后成功送达。
            # 针对超时不再回退发送文本，避免出现“图+文本”重复输出。
            try:
                from telegram.error import TimedOut, NetworkError  # 可用则使用
                is_timeout = isinstance(e, TimedOut) or isinstance(e, NetworkError)
            except Exception:
                # 兜底判断：通过异常名和信息匹配
                name = e.__class__.__name__
                is_timeout = (name == 'TimedOut' or name == 'NetworkError' or 'timeout' in str(e).lower())
            if is_timeout:
                logger.warning(f"状态图片发送超时，可能稍后自动送达: {e}")
                view_label = 'image_timeout'
                # 不发送文本，保持与图片一致的输出策略
            else:
                logger.error(f"状态图片生成失败: {e}")
                await update.message.reply_text(text, parse_mode=ParseMode.HTML)
                view_label = 'simple'
    else:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
        view_label = 'simple'

    try:
        append_botlog(update, 'status_view', {
            'ping_ms': ping_ms,
            'uptime': uptime_str,
            'query_start': start_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'query_end': end_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'query_range': query_range_str,
            'groups_total': groups_total,
            'category_count': category_count,
            'new_groups_range': new_groups_range,
            'new_categories_in_range': new_categories_in_range,
            'restricted_groups_in_range': restricted_count,
            'whitelist_users_count': whitelist_users_count,
            'blacklist_users_count': blacklist_users_count,
            'pending_tasks_count': pending_tasks_count,
            'bot_usage_count': bot_usage_count,
            'success_in_range': success_in_range,
            'failed_in_range': failed_in_range,
            'success_rate_percent': round(success_rate, 1),
            'delete_success_in_range': delete_success_in_range,
            'delete_failed_in_range': delete_failed_in_range,
            'delete_rate_percent': round(delete_rate, 1),
            'metrics_selected': sorted(list(metrics_selected)) if metrics_selected else None,
            'view': view_label
        })
    except Exception as e:
        logger.error(f"记录‘状态查看’日志失败: {e}")

# 查看操作日志
@require_permission
async def view_botlog(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        entries = load_botlog()
    except Exception:
        entries = []
    if not entries:
        await update.message.reply_text('暂无操作记录。')
        return
    # 显示最近20条
    recent = entries[-20:]
    lines = ["📋 最近操作记录:\n"]
    icon_map = {
        'bot_added_to_group': '🤖➕',
        'bot_removed_from_group': '🤖➖',
        'add_group': '🏢➕',
        'group_title_update': '🏷️✏️',
        'category_add': '🏷️➕',
        'category_edit': '🏷️✏️',
        'category_remove_from_group': '🏷️➖',
        'category_delete': '🏷️🗑️',
        'send': '📤',
        'delete': '🗑️',
        'export': '📦',
        'bot_command': '🧩',
        'bot_button': '🔘',
        'blacklist_add': '⛔➕',
        'blacklist_remove': '⛔➖',
        'bot_blocked_command': '⛔🧩',
        'bot_blocked_button': '⛔🔘',
        'bot_blocked_group_add': '⛔➕'
    }
    for e in recent:
        icon = icon_map.get(e.get('action'), '•')
        time_str = e.get('time') or e.get('timestamp') or '未知时间'
        user = e.get('username') or e.get('user_id')
        action = e.get('action')
        # 简要描述
        desc = ''
        if action == 'bot_added_to_group':
            desc = f"加入 {e.get('group_name')} ({e.get('group_id')})"
        elif action == 'bot_removed_from_group':
            desc = f"移出 {e.get('group_name')} ({e.get('group_id')})"
        elif action == 'add_group':
            desc = f"添加群组 {e.get('group_name')} ({e.get('group_id')})"
        elif action == 'group_title_update':
            desc = f"群组 {e.get('group_id')} 改名为 {e.get('new_title')}"
        elif action == 'category_add':
            desc = f"添加分类 {e.get('category_name')}"
        elif action == 'category_edit':
            desc = f"分类 {e.get('old_name')} → {e.get('new_name')} ({e.get('updated_groups')} 个群组)"
        elif action == 'category_remove_from_group':
            desc = f"群组 {e.get('group_name')} 移除分类 {e.get('category_name')}"
        elif action == 'category_delete':
            desc = f"删除分类 {e.get('category_name')} (影响 {e.get('affected_groups')} 群组)"
        elif action == 'bot_blocked_group_add':
            desc = f"阻止加入群组 {e.get('group_name')} ({e.get('group_id')})，邀请人：{e.get('invited_by_username') or e.get('invited_by_user_id')}"
        elif action == 'send':
            summary = e.get('summary')
            if summary:
                desc = f"群发 {e.get('message_count')} 条到 {e.get('target_count')} 目标 | 摘要: {summary[:80]}"
            else:
                desc = f"群发 {e.get('message_count')} 条到 {e.get('target_count')} 目标"
        elif action == 'delete':
            scope = e.get('scope') or {}
            scope_type = scope.get('type')
            scope_text = ''
            if scope_type == 'quick_all':
                scope_text = f"整批({e.get('groups_count')}群组)"
            elif scope_type == 'selected_groups':
                scope_text = "选中群组"
            elif scope_type == 'search':
                scope_text = f"按搜索群组 {scope.get('chat_id')}"
            else:
                scope_text = "未指定范围"
            # 展示部分群组名称
            groups_detail = e.get('groups_detail') or []
            sample_names = ', '.join([(gd.get('group_name') or str(gd.get('group_id')))[:15] for gd in groups_detail[:3]])
            extra = f" | 范围: {scope_text}"
            if sample_names:
                extra += f" | 群组: {sample_names}"
            desc = f"删除 {e.get('deleted_count')} 条 (失败 {e.get('failed_count')}){extra}"
        elif action == 'bot_command':
            desc = f"使用 bot 于 {e.get('group_name')} ({e.get('group_id')})"
        elif action == 'bot_button':
            btn = e.get('button_label') or e.get('button_key')
            desc = f"按钮 {btn} 于 {e.get('group_name')} ({e.get('group_id')})"
        elif action == 'bot_blocked_command':
            desc = f"黑名单用户尝试 bot 于 {e.get('group_name')} ({e.get('group_id')})"
        elif action == 'bot_blocked_button':
            btn = e.get('button_label') or e.get('button_key')
            desc = f"黑名单用户点击 按钮 {btn} 于 {e.get('group_name')} ({e.get('group_id')})"
        elif action == 'blacklist_add':
            desc = f"加入黑名单 {e.get('blocked_user_id')} | 备注: {e.get('remark')}"
        elif action == 'blacklist_remove':
            desc = f"移除黑名单 {e.get('blocked_user_id')}"
        else:
            desc = json.dumps(e, ensure_ascii=False)[:120]
        lines.append(f"{icon} {time_str} | {user}: {desc}")
    text = "\n".join(lines)
    await update.message.reply_text(text)

# 导出原始 botlog.json 文件
@require_permission
async def export_botlog_json(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text('只有管理员可以导出数据。')
        return

    # 尝试读取条数用于说明
    try:
        entries = load_botlog()
    except Exception:
        entries = []

    # 展示耗时
    start_time = time.perf_counter()
    progress_msg = await update.message.reply_text('⏳ 正在准备原始 BotLog 文件...')

    try:
        with open(BOTLOG_FILE, 'rb') as f:
            await send_and_record(
                context.bot.send_document,
                chat_id=update.effective_chat.id,
                document=f,
                filename=BOTLOG_JSON_EXPORT_NAME,
                caption=f'📄 原始 BotLog 文件（{len(entries)} 条记录）',
                user_id=update.effective_user.id,
                username=_get_display_username(update.effective_user),
                content_summary=f'原始 BotLog 文件，记录数 {len(entries)}'
            )
        elapsed_seconds = time.perf_counter() - start_time
        elapsed_str = format_elapsed(elapsed_seconds)
        try:
            await progress_msg.edit_text(f"✅ 导出完成\n总计: {len(entries)} 条记录\n🕐 耗时: {elapsed_str}")
        except TimedOut as te:
            # 编辑提示超时不影响导出成功
            try:
                append_botlog(update, 'export_botlog_json_timeout', {
                    'stage': 'edit_progress',
                    'error_message': str(te)
                })
            except Exception:
                pass
            await update.message.reply_text("✅ 导出完成（提示更新超时）")
        # 记录导出行为
        try:
            append_botlog(update, 'export_botlog_json', {
                'entries_count': len(entries),
                'file': BOTLOG_JSON_EXPORT_NAME,
                'elapsed': elapsed_str,
                'elapsed_seconds': round(elapsed_seconds, 3)
            })
        except Exception as e:
            logger.error(f"记录‘导出原始 BotLog’日志失败: {e}")
    except TimedOut as te:
        # 发送超时，可能仍成功
        try:
            append_botlog(update, 'export_botlog_json_timeout', {
                'stage': 'send_document',
                'error_message': str(te)
            })
        except Exception:
            pass
        await progress_msg.edit_text('⚠️ 发送超时，文件可能已成功发送，请稍后查看。')
    except Exception as e:
        try:
            append_botlog(update, 'export_botlog_json_error', {
                'error_message': str(e)
            })
        except Exception:
            pass
        await progress_msg.edit_text(f'导出失败: {str(e)}')

# 导出 BotLog 数据到 Excel/CSV
@require_permission
async def export_botlog(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text('只有管理员可以导出数据。')
        return

    try:
        entries = load_botlog()
    except Exception:
        entries = []

    if not entries:
        await update.message.reply_text('暂无可导出的操作记录。')
        return

    # 发送进行中提示并记录开始时间
    start_time = time.perf_counter()
    progress_msg = await update.message.reply_text('⏳ 正在导出 BotLog，请稍候...')

    # 规范列顺序：核心列在前，其余按字母序
    base_cols = ['time', 'action', 'user_id', 'username']
    all_keys = set(base_cols)
    for e in entries:
        try:
            all_keys.update(e.keys())
        except Exception:
            pass
    extra_cols = [k for k in sorted(all_keys) if k not in base_cols]
    columns = base_cols + extra_cols

    def normalize(v):
        try:
            if v is None:
                return ''
            if isinstance(v, (dict, list)):
                return json.dumps(v, ensure_ascii=False)
            return v
        except Exception:
            return str(v)

    # 构建导出数据行
    rows = []
    for entry in entries:
        row = {}
        for c in columns:
            row[c] = normalize(entry.get(c))
        rows.append(row)

    # 创建DataFrame
    df = pd.DataFrame(rows, columns=columns)

    # 生成文件名并导出
    ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    excel_filename = os.path.join(DATA_DIR, f'{BOTLOG_EXPORT_PREFIX}_{ts}.xlsx')
    csv_filename = os.path.join(DATA_DIR, f'{BOTLOG_EXPORT_PREFIX}_{ts}.csv')
    try:
        df.to_excel(excel_filename, index=False, engine='openpyxl')
        df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
    except Exception as e:
        try:
            append_botlog(update, 'export_botlog_error', {
                'stage': 'write_files',
                'error_message': str(e)
            })
        except Exception:
            pass
        await progress_msg.edit_text(f'导出失败: {str(e)}')
        return

    # 发送文件与完成提示
    try:
        with open(excel_filename, 'rb') as f:
            await send_and_record(
                context.bot.send_document,
                chat_id=update.effective_chat.id,
                document=f,
                filename=os.path.basename(excel_filename),
                caption=f'📊 BotLog 导出完成\n\n总计: {len(entries)} 条记录',
                user_id=update.effective_user.id,
                username=_get_display_username(update.effective_user),
                content_summary=f'BotLog 导出，记录数 {len(entries)}'
            )
        elapsed_seconds = time.perf_counter() - start_time
        elapsed_str = format_elapsed(elapsed_seconds)
        try:
            await progress_msg.edit_text(f"✅ 导出完成\n总计: {len(entries)} 条记录\n🕐 耗时: {elapsed_str}")
        except TimedOut as te:
            # 编辑提示超时不影响导出成功
            try:
                append_botlog(update, 'export_botlog_timeout', {
                    'stage': 'edit_progress',
                    'error_message': str(te)
                })
            except Exception:
                pass
            await update.message.reply_text("✅ 导出完成（提示更新超时）")
        # 记录导出操作到 botlog
        try:
            append_botlog(update, 'export_botlog', {
                'entries_count': len(entries),
                'excel_file': os.path.basename(excel_filename),
                'csv_file': os.path.basename(csv_filename),
                'elapsed': elapsed_str,
                'elapsed_seconds': round(elapsed_seconds, 3)
            })
        except Exception as e:
            logger.error(f"记录‘导出 BotLog’日志失败: {e}")
    except TimedOut as te:
        # 发送超时，可能仍成功
        try:
            append_botlog(update, 'export_botlog_timeout', {
                'stage': 'send_document',
                'error_message': str(te)
            })
        except Exception:
            pass
        await progress_msg.edit_text('⚠️ 发送超时，文件可能已成功发送，请稍后查看。')
    except Exception as e:
        try:
            append_botlog(update, 'export_botlog_error', {
                'stage': 'send_document',
                'error_message': str(e)
            })
        except Exception:
            pass
        await progress_msg.edit_text(f'导出失败: {str(e)}')

# 全局错误处理器：记录所有未捕获错误到 botlog
async def handle_global_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    try:
        err = getattr(context, 'error', None)
        tb = ''
        if err is not None:
            tb = ''.join(traceback.format_exception(None, err, err.__traceback__))
        payload = {
            'error_type': type(err).__name__ if err else 'Unknown',
            'error_message': str(err) if err else 'Unknown error',
            'traceback': tb[-2000:],  # 限制长度
            'source': 'global_error_handler',
        }
        # 尝试记录到 botlog
        if isinstance(update, Update):
            append_botlog(update, 'command_error', payload)
        logger.error(f"Global error captured: {payload['error_message']}")
    except Exception as e:
        logger.error(f"Failed to log global error: {e}")

# 显示发送目标选择菜单
async def start_send_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"[DEBUG] start_send_menu called")
    groups = load_groups()
    categories = load_categories()
    logger.info(f"[DEBUG] start_send_menu: loaded {len(groups)} groups, {len(categories)} categories")
    
    if not groups:
        if update.callback_query:
            await update.callback_query.edit_message_text('没有可发送的群组。请先添加群组。')
        else:
            await update.message.reply_text('没有可发送的群组。请先添加群组。')
        return ConversationHandler.END
    
    # 创建选择目标的键盘
    keyboard = []
    
    # 添加"所有群组"选项
    keyboard.append([InlineKeyboardButton("📢 所有群组", callback_data="target_all")])
    
    # 添加分类选项
    if categories:
        for category in categories:
            # 统计该分类下的群组数量
            count = sum(1 for group in groups if category in group.get('categories', []))
            if count > 0:
                keyboard.append([InlineKeyboardButton(f"🏷️ {category} ({count})", callback_data=f"target_category_{category}")])
    
    # 添加"特定群组"选项
    keyboard.append([InlineKeyboardButton("🎯 特定群组", callback_data="target_specific")])
    
    # 添加取消选项
    keyboard.append([InlineKeyboardButton("❌ 取消", callback_data="cancel")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            "📤 **选择发送目标：**",
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text(
            "📤 **选择发送目标：**",
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
    
    logger.info(f"[DEBUG] start_send_menu: returning SELECTING_TARGET ({SELECTING_TARGET})")
    return SELECTING_TARGET

# 开始发送消息
@require_permission
async def start_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"[DEBUG] start_send called by user {update.effective_user.id}")
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text('只有管理员可以发送消息。')
        return
    
    # 初始化消息列表
    context.user_data['messages'] = []
    logger.info(f"[DEBUG] start_send: initialized messages, calling start_send_menu")
    
    result = await start_send_menu(update, context)
    logger.info(f"[DEBUG] start_send: start_send_menu returned {result}")
    return result

# 选择目标
async def select_target(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    logger.info(f"[DEBUG] select_target called with callback_data: {query.data}")
    await query.answer()
    
    groups = load_groups()
    target_groups = []
    
    if query.data == "target_all":
        target_groups = groups
        target_description = "所有群组"
    elif query.data.startswith("target_category_"):
        category = query.data.replace("target_category_", "")
        target_groups = [group for group in groups if category in group.get('categories', [])]
        target_description = f"分类: {category}"
    elif query.data == "target_specific":
        # 显示所有群组供用户选择
        keyboard = []
        for group in groups:
            group_name = group.get('title', f"群组 {group['chat_id']}")
            if group.get('alias'):
                group_name = f"{group['alias']} ({group_name})"
            keyboard.append([InlineKeyboardButton(
                f"📱 {group_name}", 
                callback_data=f"select_group_{group['chat_id']}"
            )])
        
        # 添加返回和取消按钮
        keyboard.append([InlineKeyboardButton("🔙 返回", callback_data="back_to_target")])
        keyboard.append([InlineKeyboardButton("❌ 取消", callback_data="cancel")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            "🎯 **选择特定群组：**",
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
        return SELECTING_TARGET
    elif query.data.startswith("select_group_"):
        group_id = int(query.data.replace("select_group_", ""))
        target_groups = [group for group in groups if group['chat_id'] == group_id]
        if target_groups:
            group_name = target_groups[0].get('title', f"群组 {group_id}")
            if target_groups[0].get('alias'):
                group_name = f"{target_groups[0]['alias']} ({group_name})"
            target_description = f"特定群组: {group_name}"
        else:
            await query.edit_message_text("❌ 群组不存在。")
            return ConversationHandler.END
    elif query.data == "back_to_target":
        # 返回目标选择界面
        return await start_send_menu(update, context)
    elif query.data == "cancel":
        await query.edit_message_text("❌ 操作已取消。")
        return ConversationHandler.END
    
    if not target_groups:
        await query.edit_message_text("❌ 没有找到匹配的群组。")
        return ConversationHandler.END
    
    # 保存目标群组信息
    context.user_data['target_groups'] = target_groups
    context.user_data['target_description'] = target_description
    
    await query.edit_message_text(
        f"✅ 已选择目标: {target_description} ({len(target_groups)} 个群组)\n\n"
        "📝 请发送要群发的消息（支持文本、图片、视频、文档）："
    )
    
    return SENDING_MESSAGE

# 发送确认
async def send_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    messages = context.user_data.get('messages', [])
    target_groups = context.user_data.get('target_groups', [])
    target_description = context.user_data.get('target_description', '未知')
    
    if not messages:
        await update.message.reply_text("❌ 没有要发送的消息。")
        return ConversationHandler.END
    
    # 统计消息类型
    message_types = {}
    for msg in messages:
        msg_type = msg['type']
        message_types[msg_type] = message_types.get(msg_type, 0) + 1
    
    # 构建消息类型描述
    type_descriptions = []
    for msg_type, count in message_types.items():
        type_map = {
            'text': '文本',
            'photo': '图片',
            'video': '视频',
            'document': '文档'
        }
        type_descriptions.append(f"{type_map.get(msg_type, msg_type)}: {count}条")
    
    confirmation_text = f"""
📋 **发送确认**

🎯 **目标:** {target_description}
📊 **群组数量:** {len(target_groups)}
📝 **消息数量:** {len(messages)}
📄 **消息类型:** {', '.join(type_descriptions)}

⚠️ **确认要发送吗？**
"""
    
    keyboard = [
        [
            InlineKeyboardButton("✅ 合并发送", callback_data="confirm_send_merge"),
            InlineKeyboardButton("📄 逐条发送", callback_data="confirm_send_separate")
        ],
        [InlineKeyboardButton("➕ 继续添加消息", callback_data="add_more")],
        [InlineKeyboardButton("❌ 取消", callback_data="cancel")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # 处理callback query和普通消息两种情况
    if update.callback_query:
        await update.callback_query.edit_message_text(
            confirmation_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text(
            confirmation_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
    
    return CONFIRMING

# 接收消息
async def receive_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    import time
    current_time = time.time()
    logger.info(f"[DEBUG] ===== receive_message called at {current_time} =====")
    logger.info(f"[DEBUG] Message ID: {update.message.message_id}")
    media_group_id = getattr(update.message, 'media_group_id', None)
    logger.info(f"[DEBUG] Media group ID: {media_group_id}")
    logger.info(f"[DEBUG] Has photo: {bool(update.message.photo)}")
    logger.info(f"[DEBUG] Has video: {bool(update.message.video)}")
    logger.info(f"[DEBUG] Has document: {bool(update.message.document)}")
    logger.info(f"[DEBUG] Has text: {bool(update.message.text)}")
    logger.info(f"[DEBUG] Caption: {update.message.caption}")
    
    # 获取消息列表，如果不存在则初始化
    messages = context.user_data.get('messages', [])
    logger.info(f"[DEBUG] Current messages count: {len(messages)}")
    
    # 如果是媒体组消息，需要特殊处理
    if media_group_id:
        logger.info(f"[DEBUG] Processing media group message")
        # 初始化媒体组缓存
        if 'media_groups' not in context.user_data:
            context.user_data['media_groups'] = {}
            logger.info(f"[DEBUG] Initialized media_groups cache")
        
        # 如果这个媒体组还没有被处理过
        if media_group_id not in context.user_data['media_groups']:
            context.user_data['media_groups'][media_group_id] = {
                'messages': [],
                'first_received': current_time
            }
            logger.info(f"[DEBUG] Created new media group entry for {media_group_id}")
        
        # 构建消息数据
        message_data = {
            'message_id': update.message.message_id,
            'chat_id': update.effective_chat.id,
            'type': 'text',  # 默认类型
            'content': None,
            'caption': None
        }
        
        # 根据消息类型设置相应的数据
        if update.message.photo:
            message_data['type'] = 'photo'
            message_data['caption'] = update.message.caption
            message_data['file_id'] = update.message.photo[-1].file_id  # 获取最高质量的图片
        elif update.message.video:
            message_data['type'] = 'video'
            message_data['caption'] = update.message.caption
            message_data['file_id'] = update.message.video.file_id
        elif update.message.document:
            message_data['type'] = 'document'
            message_data['caption'] = update.message.caption
            message_data['file_id'] = update.message.document.file_id
        
        # 添加到媒体组缓存
        context.user_data['media_groups'][media_group_id]['messages'].append(message_data)
        logger.info(f"[DEBUG] Added to media group {media_group_id}: {message_data}")
        logger.info(f"[DEBUG] Media group {media_group_id} now has {len(context.user_data['media_groups'][media_group_id]['messages'])} messages")
        
        # 延迟处理媒体组（等待所有媒体项到达）
        import asyncio
        logger.info(f"[DEBUG] Waiting 2 seconds for more media group messages...")
        await asyncio.sleep(2)  # 增加等待时间到2秒
        
        # 检查是否有更多消息在等待期间到达
        media_group_data = context.user_data['media_groups'].get(media_group_id)
        if media_group_data:
            media_group_messages = media_group_data['messages']
            logger.info(f"[DEBUG] After waiting, media group {media_group_id} has {len(media_group_messages)} messages")
            
            # 将媒体组中的所有消息添加到主消息列表
            messages.extend(media_group_messages)
            context.user_data['messages'] = messages
            
            # 清理媒体组缓存
            del context.user_data['media_groups'][media_group_id]
            
            logger.info(f"[DEBUG] Added {len(media_group_messages)} messages from media group")
            logger.info(f"[DEBUG] Total messages now: {len(messages)}")
            
            # 显示当前消息数量并询问是否继续
            keyboard = [
                [InlineKeyboardButton("✅ 完成添加", callback_data="finish_adding")],
                [InlineKeyboardButton("➕ 继续添加", callback_data="add_more")],
                [InlineKeyboardButton("❌ 取消", callback_data="cancel")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                f"📝 已添加 {len(media_group_messages)} 条媒体消息（总计 {len(messages)} 条）\n\n"
                "💡 如需添加更多图片/视频，请点击「➕ 继续添加」\n"
                "✅ 如已添加完所有内容，请点击「✅ 完成添加」\n\n"
                "请选择下一步操作：",
                reply_markup=reply_markup
            )
            
            return CONFIRMING
        else:
            logger.error(f"[DEBUG] Media group {media_group_id} disappeared during processing!")
            return CONFIRMING
    
    else:
        # 处理单个消息（非媒体组）
        message_data = {
            'message_id': update.message.message_id,
            'chat_id': update.effective_chat.id,
            'type': 'text',  # 默认类型
            'content': None,
            'caption': None
        }
        
        # 根据消息类型设置相应的数据
        if update.message.text:
            message_data['type'] = 'text'
            message_data['content'] = update.message.text
            message_data['text'] = update.message.text  # 添加text字段以便后续处理
        elif update.message.photo:
            message_data['type'] = 'photo'
            message_data['caption'] = update.message.caption
            message_data['file_id'] = update.message.photo[-1].file_id  # 获取最高质量的图片
        elif update.message.video:
            message_data['type'] = 'video'
            message_data['caption'] = update.message.caption
            message_data['file_id'] = update.message.video.file_id
        elif update.message.document:
            message_data['type'] = 'document'
            message_data['caption'] = update.message.caption
            message_data['file_id'] = update.message.document.file_id
        
        # 添加到消息列表
        messages.append(message_data)
        context.user_data['messages'] = messages
        
        logger.info(f"[DEBUG] Added single message: {message_data}")
        logger.info(f"[DEBUG] Total messages now: {len(messages)}")
        
        # 显示当前消息数量并询问是否继续
        keyboard = [
            [InlineKeyboardButton("✅ 完成添加", callback_data="finish_adding")],
            [InlineKeyboardButton("➕ 继续添加", callback_data="add_more")],
            [InlineKeyboardButton("❌ 取消", callback_data="cancel")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            f"📝 已添加第 {len(messages)} 条消息\n\n"
            "💡 如需添加更多图片/视频/文本，请点击「➕ 继续添加」\n"
            "✅ 如已添加完所有内容，请点击「✅ 完成添加」\n\n"
            "请选择下一步操作：",
            reply_markup=reply_markup
        )
        
        return CONFIRMING

# 处理添加更多消息的回调
async def handle_add_more(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == "add_more":
        await query.edit_message_text(
            "📝 请继续发送要群发的消息（支持文本、图片、视频、文档）："
        )
        return SENDING_MESSAGE
    elif query.data == "finish_adding":
        return await send_confirmation(update, context)
    elif query.data == "cancel":
        # 清理消息列表
        context.user_data.pop('messages', None)
        await query.edit_message_text("❌ 操作已取消。")
        return ConversationHandler.END

# 确认发送
async def confirm_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data in ("confirm_send", "confirm_send_merge", "confirm_send_separate"):
        messages = context.user_data.get('messages', [])
        target_groups = context.user_data.get('target_groups', [])
        target_description = context.user_data.get('target_description', '未知')
        
        if not messages or not target_groups:
            await query.edit_message_text("❌ 发送数据不完整。")
            return ConversationHandler.END
        
        # 选择发送模式
        merge_mode = True if query.data == "confirm_send_merge" else False
        # 兼容旧按钮：confirm_send => 自动策略（若多媒体则合并，否则逐条）
        if query.data == "confirm_send":
            # 自动模式：存在两条及以上媒体则合并，否则逐条
            media_count = len([m for m in messages if m['type'] in ['photo', 'video', 'document']])
            merge_mode = media_count > 1

        await query.edit_message_text("📤 开始发送消息...（模式：" + ("合并" if merge_mode else "逐条") + ")")
        # 记录开始时间用于耗时统计
        start_time = time.perf_counter()
        # 为本次群发生成统一的批次时间戳，用于删除时整批识别
        batch_timestamp = datetime.datetime.now().isoformat()
        
        success_count = 0
        failed_count = 0
        total_messages_sent = 0
        
        # 存储已发送消息的ID，用于后续删除功能
        sent_messages = []
        user_id = update.effective_user.id
        
        # 统计消息类型
        message_types = {}
        for msg in messages:
            msg_type = msg['type']
            message_types[msg_type] = message_types.get(msg_type, 0) + 1
        
        # 分离文本消息和媒体消息
        text_messages = [msg for msg in messages if msg['type'] == 'text']
        media_messages = [msg for msg in messages if msg['type'] in ['photo', 'video', 'document']]
        
        # 创建并发发送任务
        async def send_to_group(group, messages, text_messages, media_messages, user_id, merge_mode: bool):
            """向单个群组发送消息的异步函数"""
            group_success = 0
            group_failed = 0
            group_sent_messages = []
            group_error = None
            
            try:
                # 使用速率限制器等待
                await rate_limiter.wait_if_needed(group['chat_id'])
                
                from telegram import InputMediaPhoto, InputMediaVideo, InputMediaDocument
                
                # 合并模式：多个媒体使用媒体组
                if merge_mode and len(media_messages) > 1:
                    media_group = []
                    
                    # 收集所有文本内容作为第一个媒体的caption
                    combined_text = ""
                    if text_messages:
                        combined_text = "\n\n".join([msg.get('text', '') for msg in text_messages if msg.get('text')])
                    
                    for i, message in enumerate(media_messages):
                        # 获取文件ID
                        if message['type'] == 'photo':
                            file_id = message['file_id']
                            if i == 0 and combined_text:  # 第一个媒体添加文本作为caption
                                media_item = InputMediaPhoto(media=file_id, caption=combined_text)
                            else:
                                media_item = InputMediaPhoto(media=file_id)
                        elif message['type'] == 'video':
                            file_id = message['file_id']
                            if i == 0 and combined_text:  # 第一个媒体添加文本作为caption
                                media_item = InputMediaVideo(media=file_id, caption=combined_text)
                            else:
                                media_item = InputMediaVideo(media=file_id)
                        elif message['type'] == 'document':
                            file_id = message['file_id']
                            if i == 0 and combined_text:  # 第一个媒体添加文本作为caption
                                media_item = InputMediaDocument(media=file_id, caption=combined_text)
                            else:
                                media_item = InputMediaDocument(media=file_id)
                        
                        media_group.append(media_item)
                    
                    # 发送媒体组
                    sent_msgs = await context.bot.send_media_group(
                        chat_id=group['chat_id'],
                        media=media_group
                    )
                    
                    # 存储发送的消息ID
                    for sent_msg in sent_msgs:
                        group_sent_messages.append({
                            'chat_id': group['chat_id'],
                            'message_id': sent_msg.message_id,
                            'user_id': user_id,
                            'username': _get_display_username(update.effective_user),
                            'content_summary': f"群发消息（{len(messages)}条）到 {group.get('alias') or group.get('title', '群组 ' + str(group['chat_id']))}",
                            'chat_title': group.get('alias') or group.get('title', f"群组 {group['chat_id']}"),
                            'chat_type': group.get('type', 'group')
                        })
                        # 统一批次时间戳，确保 /delete 将同批次聚为一个“全删”
                        group_sent_messages[-1]['timestamp'] = batch_timestamp
                        # 成功发送后立即持久化，保证中途中断也能删除
                        await _persist_sent_messages(user_id, [group_sent_messages[-1]])
                    
                    group_success += len(sent_msgs)
                
                else:
                    # 非合并模式：逐条按原消息发送（保留每条的caption与文本）
                    if not merge_mode:
                        for i, message in enumerate(messages):
                            if i > 0:
                                await rate_limiter.wait_if_needed(group['chat_id'])
                            sent_msg = await context.bot.copy_message(
                                chat_id=group['chat_id'],
                                from_chat_id=message['chat_id'],
                                message_id=message['message_id']
                            )
                            group_sent_messages.append({
                                'chat_id': group['chat_id'],
                                'message_id': sent_msg.message_id,
                                'user_id': user_id,
                                'username': _get_display_username(update.effective_user),
                                'content_summary': f"群发消息（{len(messages)}条）到 {group.get('alias') or group.get('title', '群组 ' + str(group['chat_id']))}",
                                'chat_title': group.get('alias') or group.get('title', f"群组 {group['chat_id']}"),
                                'chat_type': group.get('type', 'group')
                            })
                            # 统一批次时间戳，确保 /delete 将同批次聚为一个“全删”
                            group_sent_messages[-1]['timestamp'] = batch_timestamp
                            # 成功发送后立即持久化，保证中途中断也能删除
                            await _persist_sent_messages(user_id, [group_sent_messages[-1]])
                            group_success += 1
                    # 合并模式下：单媒体 + 文本合并到caption
                    elif len(media_messages) == 1:
                        msg = media_messages[0]
                        # 合并文本与原始caption
                        caption_parts = []
                        if msg.get('caption'):
                            caption_parts.append(msg.get('caption'))
                        if text_messages:
                            caption_parts.append("\n\n".join([t.get('text', '') for t in text_messages if t.get('text')]))
                        final_caption = "\n\n".join([p for p in caption_parts if p]) if caption_parts else None

                        # 为避免复制无法自定义caption，改用发送文件ID接口
                        if msg['type'] == 'photo':
                            sent_msg = await context.bot.send_photo(
                                chat_id=group['chat_id'],
                                photo=msg['file_id'],
                                caption=final_caption
                            )
                        elif msg['type'] == 'video':
                            sent_msg = await context.bot.send_video(
                                chat_id=group['chat_id'],
                                video=msg['file_id'],
                                caption=final_caption
                            )
                        elif msg['type'] == 'document':
                            sent_msg = await context.bot.send_document(
                                chat_id=group['chat_id'],
                                document=msg['file_id'],
                                caption=final_caption
                            )
                        else:
                            # 回退：未知类型按原方式复制
                            sent_msg = await context.bot.copy_message(
                                chat_id=group['chat_id'],
                                from_chat_id=msg['chat_id'],
                                message_id=msg['message_id']
                            )

                        group_sent_messages.append({
                            'chat_id': group['chat_id'],
                            'message_id': sent_msg.message_id,
                            'user_id': user_id,
                            'username': _get_display_username(update.effective_user),
                            'content_summary': f"群发消息（{len(messages)}条）到 {group.get('alias') or group.get('title', '群组 ' + str(group['chat_id']))}",
                            'chat_title': group.get('alias') or group.get('title', f"群组 {group['chat_id']}"),
                            'chat_type': group.get('type', 'group')
                        })
                        # 统一批次时间戳，确保 /delete 将同批次聚为一个“全删”
                        group_sent_messages[-1]['timestamp'] = batch_timestamp
                        # 成功发送后立即持久化，保证中途中断也能删除
                        await _persist_sent_messages(user_id, [group_sent_messages[-1]])
                        group_success += 1

                    else:
                        # 合并模式下且无媒体：逐条复制文本
                        for i, message in enumerate(messages):
                            if i > 0:
                                await rate_limiter.wait_if_needed(group['chat_id'])
                            sent_msg = await context.bot.copy_message(
                                chat_id=group['chat_id'],
                                from_chat_id=message['chat_id'],
                                message_id=message['message_id']
                            )
                            group_sent_messages.append({
                                'chat_id': group['chat_id'],
                                'message_id': sent_msg.message_id,
                                'user_id': user_id,
                                'username': _get_display_username(update.effective_user),
                                'content_summary': f"群发消息（{len(messages)}条）到 {group.get('alias') or group.get('title', '群组 ' + str(group['chat_id']))}",
                                'chat_title': group.get('alias') or group.get('title', f"群组 {group['chat_id']}"),
                                'chat_type': group.get('type', 'group')
                            })
                            # 统一批次时间戳，确保 /delete 将同批次聚为一个“全删”
                            group_sent_messages[-1]['timestamp'] = batch_timestamp
                            # 成功发送后立即持久化，保证中途中断也能删除
                            await _persist_sent_messages(user_id, [group_sent_messages[-1]])
                            group_success += 1
                
            except Exception as e:
                group_failed += len(messages)
                error_msg = str(e)
                logger.error(f"发送到群组 {group['chat_id']} 失败: {error_msg}")
                group_error = {
                    'name': group.get('title', f"群组 {group['chat_id']}"),
                    'id': group['chat_id'],
                    'reason': error_msg
                }
                if group.get('alias'):
                    group_error['name'] = f"{group['alias']} ({group_error['name']})"
            
            return {
                'success': group_success,
                'failed': group_failed,
                'sent_messages': group_sent_messages,
                'error': group_error
            }
        
        # 并发发送到所有群组
        tasks = [
            send_to_group(group, messages, text_messages, media_messages, user_id, merge_mode)
            for group in target_groups
        ]
        
        # 等待所有任务完成
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 汇总结果
        success_count = 0
        failed_count = 0
        total_messages_sent = 0
        failed_groups = []
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                # 处理异常情况
                failed_count += 1
                group = target_groups[i]
                group_name = group.get('title', f"群组 {group['chat_id']}")
                if group.get('alias'):
                    group_name = f"{group['alias']} ({group_name})"
                failed_groups.append({
                    'name': group_name,
                    'id': group['chat_id'],
                    'reason': str(result)
                })
                logger.error(f"发送到群组 {group['chat_id']} 时发生异常: {result}")
            else:
                # 处理正常结果
                if result['success'] > 0:
                    success_count += 1
                    total_messages_sent += result['success']
                    sent_messages.extend(result['sent_messages'])
                else:
                    failed_count += 1
                
                if result['error']:
                    failed_groups.append(result['error'])
        
        # 存储失败的群组信息
        if failed_groups:
            context.user_data['failed_groups'] = failed_groups

        # 构建每个群组的详细结果，用于操作日志
        groups_detail = []
        for i, group in enumerate(target_groups):
            group_name = group.get('title', f"群组 {group['chat_id']}")
            if group.get('alias'):
                group_name = f"{group['alias']} ({group_name})"
            detail = {
                'group_id': group['chat_id'],
                'group_name': group_name,
                'message_count': len(messages),
                'sent_count': 0,
                'failed_count': 0,
                'status': 'unknown'
            }
            try:
                result = results[i]
                if isinstance(result, Exception):
                    detail['error_reason'] = str(result)
                    detail['status'] = 'failed'
                    detail['failed_count'] = len(messages)
                else:
                    detail['sent_count'] = result.get('success', 0)
                    detail['failed_count'] = result.get('failed', 0)
                    detail['status'] = 'success' if detail['sent_count'] > 0 and not result.get('error') else 'failed'
                    if result.get('error'):
                        err = result['error']
                        detail['error_reason'] = err.get('reason') if isinstance(err, dict) else str(err)
            except Exception:
                # 保底，避免构建失败影响后续流程
                pass
            groups_detail.append(detail)

        # 记录日志
        log_entry = {
            'timestamp': datetime.datetime.now().isoformat(),
            'target_description': target_description,
            'target_count': len(target_groups),
            'message_count': len(messages),
            'message_types': message_types,
            'success_count': success_count,
            'failed_count': failed_count,
            'total_messages_sent': total_messages_sent,
            'content_preview': f"多消息群发 ({len(messages)}条消息)"
        }
        
        logs = load_logs()
        logs.append(log_entry)
        save_logs(logs)
        
        # 构建结果报告
        type_descriptions = []
        for msg_type, count in message_types.items():
            type_map = {
                'text': '文本',
                'photo': '图片', 
                'video': '视频',
                'document': '文档'
            }
            type_descriptions.append(f"{type_map.get(msg_type, msg_type)}: {count}条")
        
        result_text = f"""
✅ **多消息群发完成**

📊 **发送统计:**
• 目标群组: {len(target_groups)}
• 消息数量: {len(messages)}
• 消息类型: {', '.join(type_descriptions)}
• 总发送量: {total_messages_sent}条消息

📈 **群组结果:**
• 成功: {success_count} 个群组
• 失败: {failed_count} 个群组
"""

        # 添加失败群组详情
        failed_groups = context.user_data.get('failed_groups', [])
        if failed_groups:
            result_text += "\n❌ **失败群组详情:**\n"
            for i, group in enumerate(failed_groups, 1):
                result_text += f"• {group['name']}: {group['reason'][:100]}"
                if len(group['reason']) > 100:
                    result_text += "..."
                result_text += "\n"
            # 清除失败群组记录
            context.user_data.pop('failed_groups', None)
        
        # 添加耗时与完成时间
        elapsed_str = format_elapsed(time.perf_counter() - start_time)
        elapsed_seconds = time.perf_counter() - start_time
        result_text += f"\n🕐 耗时: {elapsed_str}"
        result_text += f"\n🕐 **完成时间:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        # 先记录操作日志（群发完成），再保存可删除的消息记录
        try:
            append_botlog(update, 'send', {
                'target_description': target_description,
                'target_count': len(target_groups),
                'message_count': len(messages),
                'success_count': success_count,
                'failed_count': failed_count,
                'total_messages_sent': total_messages_sent,
                'summary': summarize_messages_for_log(messages),
                'group_ids': [g['chat_id'] for g in target_groups],
                'groups_detail': groups_detail,
                'batch_timestamp': batch_timestamp,
                'elapsed': elapsed_str,
                'elapsed_seconds': round(elapsed_seconds, 3)
            })
        except Exception as e:
            logger.error(f"记录‘群发完成’日志失败: {e}")

        

        # 最后发送结果报告，若失败则降级为纯文本避免中断后续流程
        try:
            await send_and_record(
                context.bot.send_message,
                chat_id=update.effective_chat.id,
                text=result_text,
                parse_mode=ParseMode.MARKDOWN,
                user_id=update.effective_user.id,
                username=_get_display_username(update.effective_user),
                content_summary=f'群发完成报告：群组{len(target_groups)}，消息{len(messages)}，成功{success_count}，失败{failed_count}'
            )
        except Exception as e:
            logger.error(f"发送结果报告失败，降级为纯文本: {e}")
            try:
                await send_and_record(
                    context.bot.send_message,
                    chat_id=update.effective_chat.id,
                    text=result_text.replace("**", ""),
                    user_id=update.effective_user.id,
                    username=_get_display_username(update.effective_user),
                    content_summary=f'群发完成报告：群组{len(target_groups)}，消息{len(messages)}，成功{success_count}，失败{failed_count}'
                )
            except Exception as e2:
                logger.error(f"发送结果报告纯文本仍失败: {e2}")
        
        # 清理用户数据
        context.user_data.pop('messages', None)
        context.user_data.pop('target_groups', None)
        context.user_data.pop('target_description', None)
        
        return ConversationHandler.END
    
    elif query.data == "cancel":
        # 清理消息列表
        context.user_data.pop('messages', None)
        await query.edit_message_text("❌ 操作已取消。")
        return ConversationHandler.END

# 白名单管理
async def manage_whitelist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text('只有管理员可以管理白名单。')
        return
    
    whitelist = load_whitelist()
    
    keyboard = [
        [InlineKeyboardButton("➕ 添加群组", callback_data="whitelist_add_group")],
        [InlineKeyboardButton("➖ 移除群组", callback_data="whitelist_remove_group")],
        [InlineKeyboardButton("👤 添加用户", callback_data="whitelist_add_user")],
        [InlineKeyboardButton("👤 移除用户", callback_data="whitelist_remove_user")],
        [InlineKeyboardButton("📋 查看白名单", callback_data="whitelist_view")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "🔐 **白名单管理**\n\n请选择操作：",
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

# 黑名单管理
async def manage_blacklist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text('只有管理员可以管理黑名单。')
        return
    keyboard = [
        [InlineKeyboardButton("👤 加入黑名单", callback_data="blacklist_add_user")],
        [InlineKeyboardButton("👤 移除黑名单", callback_data="blacklist_remove_user")],
        [InlineKeyboardButton("📋 查看黑名单", callback_data="blacklist_view")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "⛔ **黑名单管理**\n\n请选择操作：",
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

# 处理白名单回调
async def handle_whitelist_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == "whitelist_add_group":
        context.user_data['waiting_for'] = 'whitelist_add_group'
        await query.edit_message_text('请输入要添加到白名单的群组ID:')
    
    elif query.data == "whitelist_remove_group":
        context.user_data['waiting_for'] = 'whitelist_remove_group'
        await query.edit_message_text('请输入要从白名单移除的群组ID:')
    
    elif query.data == "whitelist_add_user":
        context.user_data['waiting_for'] = 'whitelist_add_user'
        await query.edit_message_text('请输入要添加到白名单的用户ID:')
    
    elif query.data == "whitelist_remove_user":
        context.user_data['waiting_for'] = 'whitelist_remove_user'
        await query.edit_message_text('请输入要从白名单移除的用户ID:')
    
    elif query.data == "whitelist_view":
        whitelist = load_whitelist()
        
        message = "📋 **当前白名单状态:**\n\n"
        
        # 显示群组白名单
        if whitelist['groups']:
            message += "🏢 **群组白名单:**\n"
            for group_id in whitelist['groups']:
                message += f"• {group_id}\n"
        else:
            message += "🏢 **群组白名单:** 空\n"
        
        message += "\n"
        
        # 显示用户白名单
        if whitelist['private_users']:
            message += "👤 **用户白名单:**\n"
            for user_id in whitelist['private_users']:
                message += f"• {user_id}\n"
        else:
            message += "👤 **用户白名单:** 空\n"
        
        await query.edit_message_text(message, parse_mode=ParseMode.MARKDOWN)

# 处理黑名单回调
async def handle_blacklist_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "blacklist_add_user":
        context.user_data['waiting_for'] = 'blacklist_add_user'
        await query.edit_message_text('请输入要加入黑名单的用户ID 和备注(可选)：例如 12345 滥用点击')
    elif query.data == "blacklist_remove_user":
        context.user_data['waiting_for'] = 'blacklist_remove_user'
        await query.edit_message_text('请输入要从黑名单移除的用户ID:')
    elif query.data == "blacklist_view":
        bl = load_blacklist()
        users = bl.get('users', [])
        if not users:
            await query.edit_message_text('当前黑名单为空。')
            return
        message = "📋 **当前黑名单:**\n\n"
        for u in users:
            rid = u.get('id')
            remark = u.get('remark') or ''
            message += f"• {rid} {('(' + remark + ')') if remark else ''}\n"
        await query.edit_message_text(message, parse_mode=ParseMode.MARKDOWN)

# 取消操作
# 删除最近发送的消息
@require_permission
async def delete_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    logger.info(f"用户 {user_id} 请求删除消息")
    
    # 进入删除流程时清空上次多选的批次选择
    context.user_data.pop('selected_batches', None)
    
    # 尝试从已启用的持久化（可能为 Firestore 的分片文档）重新加载最新的 sent_messages
    all_messages = []
    try:
        if firestore_enabled():
            try:
                loaded = load_sent_messages() or {}
                logger.info(f"delete_messages: loaded sent_messages from Firestore, keys_count={len(loaded) if hasattr(loaded, 'keys') else 0}")
                # 规范键为 int
                temp = {}
                for k, v in loaded.items():
                    try:
                        ik = int(k)
                    except Exception:
                        ik = k
                    temp.setdefault(ik, []).extend(v or [])
                msgs_source = temp
                logger.info(f"delete_messages: msgs_source keys sample={list(msgs_source.keys())[:5]}")
            except Exception as e:
                logger.warning(f"加载远程 sent_messages 失败，回退到内存数据: {e}")
                msgs_source = user_sent_messages
        else:
            logger.info("delete_messages: Firestore 未启用，使用内存 user_sent_messages")
            msgs_source = user_sent_messages
    except Exception as e:
        logger.error(f"delete_messages: 检查持久化数据时发生异常: {e}")
        msgs_source = user_sent_messages

    for msgs in (msgs_source or {}).values():
        if msgs:
            all_messages.extend(msgs)

    logger.info(f"delete_messages: total aggregated messages={len(all_messages)} from source={'firestore' if firestore_enabled() else 'memory'}")

    if not all_messages:
        await update.message.reply_text('❌ 没有找到可删除的消息。')
        return

    # 不限制时间窗口，包含所有已发送记录
    recent_messages = all_messages[:]

    if not recent_messages:
        await update.message.reply_text('❌ 没有找到可删除的消息。')
        return

    # 自动检测并清理已被他人删除的消息（仅检测最近7天，后台快速进行）
    notice_msg = await update.message.reply_text('🔍 正在后台快速检测最近7天内的消息是否仍存在，请稍候...')
    try:
        context.user_data['auto_prune_notice'] = {
            'chat_id': notice_msg.chat.id,
            'message_id': notice_msg.message_id
        }
    except Exception:
        pass
    window_days = 7
    # 后台任务：限每群探测1条，加速完成
    try:
        asyncio.create_task(auto_prune_background(update, context, recent_messages, window_days))
    except Exception as e:
        logger.error(f"启动后台检测任务失败: {e}")

    # 按时间戳分组消息（同一次发送的消息），先展示主菜单
    message_groups = {}
    for msg in recent_messages:
        timestamp = msg.get('timestamp')
        if not timestamp:
            continue
        message_groups.setdefault(timestamp, []).append(msg)
    sorted_groups = sorted([(ts, msgs) for ts, msgs in message_groups.items() if msgs], reverse=True)
    context.user_data['delete_message_groups'] = sorted_groups
    context.user_data['delete_page'] = 0

    # 显示主菜单（检测完成后将自动刷新）
    await show_delete_main_menu(update, context)

# 后台自动检测并刷新菜单
async def auto_prune_background(update: Update, context: ContextTypes.DEFAULT_TYPE, recent_messages: list, window_days: int):
    try:
        now = datetime.datetime.now()
        threshold = now - datetime.timedelta(days=window_days)
        threshold_iso = threshold.isoformat()
        msgs_in_window = [m for m in recent_messages if m.get('timestamp') and m['timestamp'] >= threshold_iso]
        scanned = len(msgs_in_window)
        deleted = await auto_prune_nonexistent_messages(update, context, msgs_in_window, per_group_limit=1)
        try:
            context.user_data['auto_prune_scanned_count'] = scanned
            context.user_data['auto_prune_window_days'] = window_days
            context.user_data['auto_prune_deleted_count'] = deleted
        except Exception:
            pass
        # 更新后台提示消息
        notice = context.user_data.get('auto_prune_notice')
        if notice:
            try:
                await context.bot.edit_message_text(
                    chat_id=notice['chat_id'],
                    message_id=notice['message_id'],
                    text=f"✅ 自动检测完成：最近 {window_days} 天检查 {scanned} 条，清理 {deleted} 条已不存在的消息。"
                )
            except Exception as e:
                logger.error(f"更新后台检测提示失败: {e}")
        # 刷新菜单展示检测结果
        try:
            await show_delete_main_menu(update, context)
        except Exception as e:
            logger.error(f"刷新删除主菜单失败: {e}")
    except Exception as e:
        logger.error(f"后台自动检测失败: {e}")

# 显示删除主菜单
async def show_delete_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message_groups = context.user_data.get('delete_message_groups', [])
    
    # 当上下文中没有分组数据（例如返回后上下文丢失）时，回退到全局持久化数据重建分组
    if not message_groups:
        try:
            all_messages = []
            for msgs in user_sent_messages.values():
                if msgs:
                    all_messages.extend(msgs)
            if all_messages:
                groups_dict = {}
                for msg in all_messages:
                    ts = msg.get('timestamp')
                    if not ts:
                        continue
                    groups_dict.setdefault(ts, []).append(msg)
                sorted_groups = sorted(groups_dict.items(), reverse=True)
                context.user_data['delete_message_groups'] = sorted_groups
                context.user_data['delete_page'] = 0
                message_groups = sorted_groups
        except Exception as e:
            logger.error(f"重建删除分组失败: {e}")
    
    if not message_groups:
        text = '❌ 没有找到可删除的消息。'
        keyboard = []
    else:
        text = '🗑️ **删除消息**\n\n'
        text += 'ℹ️ 预览规则：跨群每个群预览1条；单群预览全部。\n'
        keyboard = []
        
        # 自动清理提示
        if 'auto_prune_scanned_count' in context.user_data:
            window_days = context.user_data.get('auto_prune_window_days') or 7
            scanned = context.user_data.get('auto_prune_scanned_count') or 0
            pruned = context.user_data.get('auto_prune_deleted_count') or 0
            text += f"🔍 已自动检测最近 {window_days} 天内 {scanned} 条消息"
            if pruned > 0:
                text += f"，并清理 {pruned} 条已被他人删除的消息"
            text += "\n"
        else:
            text += "🔍 正在后台快速检测最近7天内的消息，请稍候…\n"
        
        # 🚀 快速操作
        text += '🚀 **快速操作：**\n'
        quick_operations = message_groups[:3]  # 显示最近3次群发
        
        for i, (timestamp, msgs) in enumerate(quick_operations):
            try:
                tz8 = datetime.timezone(datetime.timedelta(hours=8))
                dt = datetime.datetime.fromisoformat(timestamp)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=tz8)
                dt8 = dt.astimezone(tz8)
                now8 = datetime.datetime.now(tz8)
                minutes_ago = max(0, int((now8 - dt8).total_seconds() / 60))
                time_str = f"{minutes_ago}分钟前"
            except Exception:
                time_str = "未知时间"
            
            # 统计群组数量
            group_count = len(set(msg['chat_id'] for msg in msgs))
            text += f'├── 删除最近群发 ({time_str}，{group_count}个群组)\n'
            
            # 添加快速删除按钮
            keyboard.append([
                InlineKeyboardButton(f"🗑️ 全删 ({time_str})", callback_data=f"quick_delete_all_{i}"),
                InlineKeyboardButton(f"🎯 选择群组", callback_data=f"quick_delete_select_{i}")
            ])
        
        text += '\n'
        
        # 🔍 智能筛选
        text += '🔍 **智能筛选：**\n'
        text += '└── 按群组名称搜索\n\n'
        keyboard.append([InlineKeyboardButton("🔍 按群组名称搜索", callback_data="search_by_group")])
        
        # 📋 详细列表
        text += '📋 **详细列表：**\n'
        text += f'├── 显示最近{min(len(message_groups), 10)}条群发记录\n'
        text += '└── 支持分页、预览、批量选择和删除'
        
        keyboard.append([InlineKeyboardButton("📋 查看详细列表", callback_data="show_detailed_list")])
    
    keyboard.append([InlineKeyboardButton("❌ 取消", callback_data="delete_cancel")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        sent = await update.message.reply_text(
            text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
        # 后台快扫完成后，自动删除首次秒开的菜单，保留刷新后的菜单
        try:
            scanned_done = 'auto_prune_scanned_count' in context.user_data
            if not scanned_done:
                context.user_data['initial_delete_menu'] = {
                    'chat_id': sent.chat.id,
                    'message_id': sent.message_id
                }
            else:
                initial = context.user_data.pop('initial_delete_menu', None)
                if initial:
                    async def _delete_initial_menu():
                        try:
                            await asyncio.sleep(3)
                            await context.bot.delete_message(
                                chat_id=initial['chat_id'],
                                message_id=initial['message_id']
                            )
                        except Exception as e:
                            logger.error(f"自动删除初始菜单失败: {e}")
                    asyncio.create_task(_delete_initial_menu())
        except Exception:
            pass

# 显示群组选择界面
async def show_group_selection(update: Update, context: ContextTypes.DEFAULT_TYPE, group_index: int):
    message_groups = context.user_data.get('delete_message_groups', [])
    
    if group_index >= len(message_groups):
        await update.callback_query.edit_message_text("❌ 无效的选择。")
        return
    
    timestamp, msgs = message_groups[group_index]
    
    # 按群组分组消息
    groups_dict = {}
    for msg in msgs:
        chat_id = msg['chat_id']
        if chat_id not in groups_dict:
            groups_dict[chat_id] = []
        groups_dict[chat_id].append(msg)
    
    # 获取群组信息
    groups_data = load_groups()
    group_names = {}
    for group in groups_data:
        group_names[group['chat_id']] = group.get('alias') or group.get('title') or f"群组 {group['chat_id']}"
    
    # 分页设置
    group_pages = context.user_data.get('group_selection_pages', {})
    page = int(group_pages.get(group_index, 0))
    items_per_page = 10
    
    # 排序并切片
    sorted_items = sorted(groups_dict.items(), key=lambda kv: group_names.get(kv[0], f"群组 {kv[0]}"))
    total_groups = len(sorted_items)
    start_idx = page * items_per_page
    end_idx = start_idx + items_per_page
    page_items = sorted_items[start_idx:end_idx]
    
    try:
        tz8 = datetime.timezone(datetime.timedelta(hours=8))
        dt = datetime.datetime.fromisoformat(timestamp)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz8)
        dt8 = dt.astimezone(tz8)
        now8 = datetime.datetime.now(tz8)
        minutes_ago = max(0, int((now8 - dt8).total_seconds() / 60))
        time_str = f"{minutes_ago}分钟前"
    except Exception:
        time_str = "未知时间"
    
    text = f'🎯 **选择要删除的群组** ({time_str})\n\n'
    text += f'共 {total_groups} 个群组（第{page + 1}页，每页{items_per_page}项）：\n\n'
    
    keyboard = []
    
    # 添加全选/全删按钮
    keyboard.append([
        InlineKeyboardButton("✅ 全选", callback_data=f"select_all_groups_{group_index}"),
        InlineKeyboardButton("🗑️ 全删", callback_data=f"quick_delete_all_{group_index}")
    ])
    
    # 为当前页的每个群组创建按钮
    for i, (chat_id, group_msgs) in enumerate(page_items):
        group_name = group_names.get(chat_id, f"群组 {chat_id}")
        msg_count = len(group_msgs)
        
        # 检查是否已选中
        selected_groups = context.user_data.get('selected_groups', set())
        is_selected = f"{group_index}_{chat_id}" in selected_groups
        
        button_text = f"{'✅' if is_selected else '☐'} {group_name} ({msg_count}条)"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"toggle_group_{group_index}_{chat_id}")])
    
    # 分页导航按钮
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ 上一页", callback_data=f"group_page_{group_index}_{page - 1}"))
    if end_idx < total_groups:
        nav_buttons.append(InlineKeyboardButton("➡️ 下一页", callback_data=f"group_page_{group_index}_{page + 1}"))
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    # 添加操作按钮（预览选中 + 删除选中）
    keyboard.append([
        InlineKeyboardButton("👀 预览选中", callback_data=f"preview_selected_{group_index}"),
        InlineKeyboardButton("🗑️ 删除选中", callback_data=f"delete_selected_{group_index}")
    ])
    keyboard.append([InlineKeyboardButton("🔙 返回", callback_data="back_to_main")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # 存储当前选择的群组索引
    context.user_data['current_group_index'] = group_index
    
    await update.callback_query.edit_message_text(
        text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

# 显示详细列表
async def show_detailed_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"show_detailed_list invoked; delete_message_groups_exists={ 'delete_message_groups' in context.user_data }")
    message_groups = context.user_data.get('delete_message_groups', [])
    page = context.user_data.get('delete_page', 0)

    # 当上下文中没有分组数据（例如机器人重启后点击旧按钮）时，回退到全局持久化数据重建分组
    if not message_groups:
        try:
            # 尝试从内存再次构建
            all_messages = []
            for msgs in user_sent_messages.values():
                if msgs:
                    all_messages.extend(msgs)
            # 如果内存为空，尝试从持久化(可能为 Firestore)加载
            if not all_messages:
                try:
                    loaded = load_sent_messages() or {}
                    logger.info(f"show_detailed_list: loaded sent_messages from persistence, keys_count={len(loaded) if hasattr(loaded, 'keys') else 0}")
                    for k, v in (loaded or {}).items():
                        if v:
                            # keys in loaded may be str; normalize
                            try:
                                ik = int(k)
                            except Exception:
                                ik = k
                            all_messages.extend(v)
                except Exception as le:
                    logger.warning(f"show_detailed_list: load_sent_messages failed: {le}")
            if all_messages:
                groups_dict = {}
                for msg in all_messages:
                    ts = msg.get('timestamp')
                    if not ts:
                        continue
                    groups_dict.setdefault(ts, []).append(msg)
                # 以时间排序分组，兼容旧数据无 tzinfo 的情况（视为 UTC）
                def _key_dt(kv):
                    ts = kv[0]
                    try:
                        dt = datetime.datetime.fromisoformat(ts)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=datetime.timezone.utc)
                        return dt
                    except Exception:
                        return ts
                sorted_groups = sorted(groups_dict.items(), key=_key_dt, reverse=True)
                context.user_data['delete_message_groups'] = sorted_groups
                # 重建后强制显示最新页
                context.user_data['delete_page'] = 0
                page = 0
                message_groups = sorted_groups
        except Exception as e:
            logger.error(f"重建删除分组失败: {e}")

    items_per_page = 5
    start_idx = page * items_per_page
    end_idx = start_idx + items_per_page
    page_groups = message_groups[start_idx:end_idx]

    # 批次多选集合（跨页持久）
    selected_batches = context.user_data.get('selected_batches', set())
    try:
        # 统一为集合类型，防止意外是列表
        selected_batches = set(selected_batches) if selected_batches else set()
    except Exception:
        selected_batches = set()

    text = f'📋 **详细列表** (第{page + 1}页)\n\n'

    if not page_groups:
        text += '没有更多记录。'
        keyboard = [[InlineKeyboardButton("🔙 返回", callback_data="back_to_main")]]
    else:
        keyboard = []
        # 预备映射：群名与分类
        try:
            groups_data = load_groups()
        except Exception:
            groups_data = []
        group_name_map = {}
        categories_map = {}
        try:
            for g in groups_data:
                gid = g.get('chat_id')
                title = g.get('alias') or g.get('title') or (f"群组 {gid}" if gid is not None else "群组")
                if gid is not None:
                    group_name_map[gid] = title
                for cat in g.get('categories', []):
                    categories_map.setdefault(cat, set()).add(gid)
        except Exception:
            pass
        # 用户信息缓存（私聊）
        user_cache = context.user_data.get('user_cache', {})
        if not isinstance(user_cache, dict):
            user_cache = {}
        context.user_data['user_cache'] = user_cache
        
        for i, (timestamp, msgs) in enumerate(page_groups):
            actual_index = start_idx + i
            
            try:
                tz8 = datetime.timezone(datetime.timedelta(hours=8))
                dt = datetime.datetime.fromisoformat(timestamp)
                # 若无 tzinfo（旧数据），视为 UTC 再转 GMT+8
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=datetime.timezone.utc)
                dt8 = dt.astimezone(tz8)
                time_str = dt8.strftime('%m-%d %H:%M')
                now8 = datetime.datetime.now(tz8)
                minutes_ago = max(0, int((now8 - dt8).total_seconds() / 60))
            except Exception:
                time_str = (timestamp[:16] if isinstance(timestamp, str) else '未知时间')
                minutes_ago = 0
            
            # 诊断：输出本批次的示例消息，便于确认 message 对象里是否包含 user_id / sender_user_id
            try:
                sample_msgs = []
                for m in (msgs or [])[:2]:
                    sample_msgs.append({
                        'chat_id': m.get('chat_id'),
                        'message_id': m.get('message_id'),
                        'user_id': m.get('user_id'),
                        'sender_user_id': m.get('sender_user_id'),
                    })
                logger.info(f"show_detailed_list: group_index={actual_index} timestamp={timestamp} sample_msgs={sample_msgs}")
            except Exception:
                pass
            
            # 汇总发起者id（优先 sender_user_id，再回退 user_id）
            initiator_ids = set()
            try:
                for m in (msgs or []):
                    sid = m.get('sender_user_id') or m.get('user_id')
                    if sid is not None:
                        initiator_ids.add(sid)
            except Exception:
                pass
            
            group_ids = list({msg['chat_id'] for msg in msgs})
            group_count = len(group_ids)
            msg_count = len(msgs)
            
            text += f'{actual_index + 1}. {time_str} ({minutes_ago}分钟前)\n'
            text += f'   📊 {group_count}个群组，{msg_count}条消息\n'
            
            # 标题行：群组/分类/私聊（优先显示发起者信息，如可解析）
            # 简化逻辑以避免复杂嵌套导致语法/逻辑问题
            display_initiator = None
            try:
                if initiator_ids:
                    rep = next(iter(initiator_ids))
                    # 尝试解析用户名
                    uinfo = user_cache.get(str(rep))
                    if not uinfo:
                        try:
                            uchat = await context.bot.get_chat(rep)
                            uinfo = {
                                'first_name': getattr(uchat, 'first_name', None),
                                'last_name': getattr(uchat, 'last_name', None),
                                'username': getattr(uchat, 'username', None),
                                'is_bot': getattr(uchat, 'is_bot', False)
                            }
                            user_cache[str(rep)] = uinfo
                        except Exception:
                            uinfo = None
                    if uinfo:
                        # 按“姓名 | @用户名 | 用户ID”格式显示（不再使用“由 @xxx 发起”）
                        first = (uinfo.get('first_name') or '').strip()
                        last = (uinfo.get('last_name') or '').strip()
                        full_name = (first + (' ' + last if last else '')).strip()
                        uname = uinfo.get('username')
                        uname_str = f'@{uname}' if uname else '无用户名'
                        uid_str = str(rep)
                        bot_marker = ' (机器人)' if uinfo.get('is_bot') else ''
                        display_initiator = f"{full_name or '用户'}{bot_marker} | {uname_str} | {uid_str}"
                    else:
                        # 无法解析 username，则以 id 显示（若为当前操作者则回退）
                        if rep != update.effective_user.id:
                            display_initiator = f"由 {rep} 发起"
            except Exception:
                display_initiator = None

            if display_initiator:
                text += f'    {display_initiator}\n'
            else:
                # 回退到群组/分类显示（原有行为）
                try:
                    if group_count == 1:
                        gid = group_ids[0]
                        if gid is not None and gid > 0:
                            # 私聊：补全用户信息
                            uinfo = user_cache.get(str(gid))
                            if not uinfo:
                                try:
                                    chat = await context.bot.get_chat(gid)
                                    uinfo = {
                                        'first_name': getattr(chat, 'first_name', None),
                                        'last_name': getattr(chat, 'last_name', None),
                                        'username': getattr(chat, 'username', None)
                                    }
                                    user_cache[str(gid)] = uinfo
                                except Exception:
                                    uinfo = {}
                            first = (uinfo.get('first_name') or '')
                            last = (uinfo.get('last_name') or '')
                            full_name = f"{first} {last}".strip() or '用户'
                            uname = uinfo.get('username')
                            uname_str = f'@{uname}' if uname else '无用户名'
                            text += f'    {full_name} | {uname_str} | {gid}\n'
                        else:
                            gname = group_name_map.get(gid) or (f"群组 {gid}" if gid is not None else "群组")
                            text += f'    {gname} | {gid}\n'
                    elif group_count > 1:
                        batch_set = set(group_ids)
                        exact = None
                        shared = []
                        for cat, ids in categories_map.items():
                            if ids == batch_set:
                                exact = cat
                                break
                            if batch_set.issubset(ids):
                                shared.append((cat, len(ids)))
                        if exact:
                            text += f'    {exact}\n'
                        elif shared:
                            shared.sort(key=lambda x: x[1])
                            text += f'    {shared[0][0]}\n'
                except Exception:
                    pass

            text += '\n'
            
            # 操作行：全删 / 选择群组 / 预览 / 勾选
            is_selected = actual_index in selected_batches
            checkbox = '✅ 已选' if is_selected else '☐ 选择'
            keyboard.append([
                InlineKeyboardButton(f"🗑️ 全删 #{actual_index + 1}", callback_data=f"quick_delete_all_{actual_index}"),
                InlineKeyboardButton(f"🎯 选择 #{actual_index + 1}", callback_data=f"quick_delete_select_{actual_index}"),
                InlineKeyboardButton(f"👁️ 预览 #{actual_index + 1}", callback_data=f"preview_batch_{actual_index}"),
                InlineKeyboardButton(f"{checkbox} 批次 #{actual_index + 1}", callback_data=f"toggle_batch_{actual_index}")
            ])
        
        # 已选批次数量提示与批量操作
        if len(selected_batches) > 0:
            text += f'已选批次：{len(selected_batches)} 个\n\n'
            keyboard.append([
                InlineKeyboardButton("🗑️ 全删选中", callback_data="delete_selected_batches"),
                InlineKeyboardButton("👁️ 全预览选中", callback_data="preview_selected_batches")
            ])
        
        # 分页按钮（诊断：记录分页状态以排查按钮不出现的原因）
        logger.info(f"detailed_list pagination: page={page} total_groups={len(message_groups)} start_idx={start_idx} end_idx={end_idx}")
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ 上一页", callback_data=f"page_{page - 1}"))
        if end_idx < len(message_groups):
            nav_buttons.append(InlineKeyboardButton("➡️ 下一页", callback_data=f"page_{page + 1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("🔙 返回", callback_data="back_to_main")])
    
    # 保存回写（保证跨页选择持久）
    context.user_data['selected_batches'] = selected_batches

    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.callback_query.edit_message_text(
        text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

# 按群组名称搜索
async def show_search_interface(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = '🔍 **按群组名称搜索**\n\n'
    text += '请输入群组名称或别名进行搜索：\n'
    text += '(支持模糊匹配；可用 / , ， 分隔多关键词)'
    
    keyboard = [[InlineKeyboardButton("🔙 返回", callback_data="back_to_main")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    context.user_data['waiting_for'] = 'search_groups'
    
    await update.callback_query.edit_message_text(
        text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

# 执行删除操作
async def execute_delete(update: Update, context: ContextTypes.DEFAULT_TYPE, messages_to_delete: list):
    # 显示进行中提示并记录开始时间
    await update.callback_query.edit_message_text("🗑️ 正在删除消息...")
    start_time = time.perf_counter()
    deleted_count = 0
    failed_count = 0
    externally_deleted_count = 0
    externally_deleted_msg_ids = set()
    
    # 按群组并发删除，群组内顺序并遵循速率限制
    groups: dict[int, list] = {}
    for msg in messages_to_delete:
        groups.setdefault(msg['chat_id'], []).append(msg)

    async def delete_for_group(chat_id: int, msgs: list):
        nonlocal deleted_count, failed_count, externally_deleted_count, externally_deleted_msg_ids
        for msg_info in msgs:
            try:
                await rate_limiter.wait_if_needed(chat_id)
                await context.bot.delete_message(
                    chat_id=msg_info['chat_id'],
                    message_id=msg_info['message_id']
                )
                deleted_count += 1
            except Exception as e:
                # 若错误表示消息不存在，认为该消息已被他人删除，记录并从后续列表中剔除
                if is_message_not_found_error(e):
                    externally_deleted_count += 1
                    externally_deleted_msg_ids.add((msg_info['chat_id'], msg_info['message_id']))
                    try:
                        append_botlog(update, 'deleted_by_others', {
                            'chat_id': msg_info['chat_id'],
                            'message_id': msg_info['message_id'],
                            'reason': str(e)
                        })
                    except Exception:
                        pass
                else:
                    logger.error(f"删除消息失败(chat={chat_id}): {str(e)}")
                    failed_count += 1

    tasks = [delete_for_group(chat_id, msgs) for chat_id, msgs in groups.items()]
    await asyncio.gather(*tasks)
    
    # 从全局存储中移除已删除的消息（跨所有用户），包括他人已删除的
    deleted_msg_ids = {(msg['chat_id'], msg['message_id']) for msg in messages_to_delete}
    deleted_msg_ids |= externally_deleted_msg_ids
    for uid in list(user_sent_messages.keys()):
        user_sent_messages[uid] = [
            msg for msg in user_sent_messages[uid]
            if (msg['chat_id'], msg['message_id']) not in deleted_msg_ids
        ]
    # 同步到持久化文件
    try:
        save_sent_messages(user_sent_messages)
    except Exception as e:
        logger.error(f"更新持久化删除记录失败: {e}")
    
    # 报告删除结果
    result_text = f"✅ 删除完成！\n\n"
    result_text += f"• 成功删除: {deleted_count} 条消息\n"
    if externally_deleted_count > 0:
        result_text += f"• 已被他人删除: {externally_deleted_count} 条\n"
    if failed_count > 0:
        result_text += f"• 删除失败: {failed_count} 条消息\n"
    elapsed_str = format_elapsed(time.perf_counter() - start_time)
    elapsed_seconds = time.perf_counter() - start_time
    result_text += f"🕐 耗时: {elapsed_str}\n"

    await update.callback_query.edit_message_text(result_text)
    # 记录操作日志（删除完成）
    try:
        # 计算涉及群组数量与批次时间
        groups_count = len(groups)
        ts_vals = [m.get('timestamp') for m in messages_to_delete if m.get('timestamp')]
        batch_ts = None
        if ts_vals:
            try:
                batch_ts = min(ts_vals)
            except Exception:
                batch_ts = ts_vals[0]
        # 汇总内容摘要（去重，最多5条）
        message_summary_items = [m.get('content_summary') for m in messages_to_delete if m.get('content_summary')]
        seen = set()
        message_summary = []
        for s in message_summary_items:
            if s and s not in seen:
                seen.add(s)
                message_summary.append(s)
        message_summary_str = "；".join(message_summary[:5])

        # 构建分组详情，正确区分私聊/群聊并携带内容摘要
        groups_detail_list = []
        for gid, msgs in groups.items():
            # 优先用消息记录中携带的标题/类型
            group_title = next((m.get('chat_title') for m in msgs if m.get('chat_title')), None)
            chat_type = next((m.get('chat_type') for m in msgs if m.get('chat_type')), None)

            # 回退到群组数据文件
            if not group_title or not chat_type:
                try:
                    ginfo = next((g for g in load_groups() if g.get('chat_id') == gid), None)
                except Exception:
                    ginfo = None
                if not group_title and ginfo:
                    group_title = (ginfo.get('alias') or ginfo.get('title'))
                if not chat_type and ginfo:
                    chat_type = ginfo.get('type')

            # 最后兜底：用ID判断类型，设置标题
            if not chat_type:
                chat_type = 'private' if gid > 0 else 'group'
            if not group_title:
                uname = next((m.get('username') for m in msgs if m.get('username')), None)
                if chat_type == 'private':
                    group_title = f"私聊 {uname}" if uname else f"私聊 {gid}"
                else:
                    group_title = f"群组 {gid}"

            content_summaries = [m.get('content_summary') for m in msgs if m.get('content_summary')]
            groups_detail_list.append({
                'group_id': gid,
                'group_name': group_title,
                'chat_type': chat_type,
                'message_count': len(msgs),
                'content_summaries': content_summaries
            })

        append_botlog(update, 'delete', {
            'deleted_count': deleted_count,
            'externally_deleted_count': externally_deleted_count,
            'failed_count': failed_count,
            'groups_count': groups_count,
            'batch_timestamp': batch_ts,
            'scope': context.user_data.get('delete_scope'),
            'elapsed': elapsed_str,
            'elapsed_seconds': round(elapsed_seconds, 3),
             'groups_detail': groups_detail_list
        })
    except Exception as e:
        logger.error(f"记录‘删除完成’日志失败: {e}")
    
    # 清理context数据
    context.user_data.pop('delete_message_groups', None)
    context.user_data.pop('selected_groups', None)
    context.user_data.pop('current_group_index', None)
    context.user_data.pop('delete_page', None)
    context.user_data.pop('delete_scope', None)
    context.user_data.pop('auto_prune_deleted_count', None)
    context.user_data.pop('auto_prune_scanned_count', None)
    context.user_data.pop('auto_prune_window_days', None)

# 处理删除消息的回调
@require_permission
async def handle_delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    logger.info(f"收到删除回调: {query.data}")
    
    try:
        # 取消操作
        if query.data == "delete_cancel":
            await query.edit_message_text("❌ 删除操作已取消。")
            context.user_data.pop('delete_message_groups', None)
            context.user_data.pop('selected_groups', None)
            context.user_data.pop('current_group_index', None)
            context.user_data.pop('delete_page', None)
            context.user_data.pop('selected_batches', None)
            context.user_data.pop('auto_prune_deleted_count', None)
            context.user_data.pop('auto_prune_scanned_count', None)
            context.user_data.pop('auto_prune_window_days', None)
            # 清理搜索相关状态
            context.user_data.pop('search_results', None)
            context.user_data.pop('search_term', None)
            context.user_data.pop('search_page', None)
            context.user_data.pop('search_mode', None)
            context.user_data.pop('waiting_for', None)
            return
        
        # 返回主菜单
        elif query.data == "back_to_main":
            # 清理搜索和批次选择状态
            context.user_data.pop('search_results', None)
            context.user_data.pop('search_term', None)
            context.user_data.pop('search_page', None)
            context.user_data.pop('search_mode', None)
            context.user_data.pop('waiting_for', None)
            context.user_data.pop('selected_batches', None)
            await show_delete_main_menu(update, context)
            return
        
        # 快速删除全部
        elif query.data.startswith("quick_delete_all_"):
            group_index = int(query.data.split("_")[-1])
            message_groups = context.user_data.get('delete_message_groups', [])
            
            if group_index >= len(message_groups):
                await query.edit_message_text("❌ 无效的选择。")
                return
            
            timestamp, messages_to_delete = message_groups[group_index]
            # 记录删除范围：整批删除
            try:
                scope_group_ids = list({m['chat_id'] for m in messages_to_delete})
                context.user_data['delete_scope'] = {
                    'type': 'quick_all',
                    'group_index': group_index,
                    'group_ids': scope_group_ids
                }
            except Exception:
                pass
            await execute_delete(update, context, messages_to_delete)
            return
        
        # 快速删除选择群组
        elif query.data.startswith("quick_delete_select_"):
            group_index = int(query.data.split("_")[-1])
            await show_group_selection(update, context, group_index)
            return
        
        # 切换群组选择状态
        elif query.data.startswith("toggle_group_"):
            parts = query.data.split("_")
            group_index = int(parts[2])
            chat_id = int(parts[3])
            
            selected_groups = context.user_data.get('selected_groups', set())
            group_key = f"{group_index}_{chat_id}"
            
            if group_key in selected_groups:
                selected_groups.remove(group_key)
            else:
                selected_groups.add(group_key)
            
            context.user_data['selected_groups'] = selected_groups
            await show_group_selection(update, context, group_index)
            return
        
        # 全选群组
        elif query.data.startswith("select_all_groups_"):
            group_index = int(query.data.split("_")[-1])
            message_groups = context.user_data.get('delete_message_groups', [])
            
            if group_index >= len(message_groups):
                await query.edit_message_text("❌ 无效的选择。")
                return
            
            timestamp, msgs = message_groups[group_index]
            group_ids = set(msg['chat_id'] for msg in msgs)
            
            selected_groups = context.user_data.get('selected_groups', set())
            for chat_id in group_ids:
                selected_groups.add(f"{group_index}_{chat_id}")
            
            context.user_data['selected_groups'] = selected_groups
            await show_group_selection(update, context, group_index)
            return
        
        # 删除选中的群组（严格匹配带数字后缀，避免误匹配 *_batches）
        elif query.data.startswith("delete_selected_") and query.data.split("_")[-1].isdigit():
            group_index = int(query.data.split("_")[-1])
            message_groups = context.user_data.get('delete_message_groups', [])
            selected_groups = context.user_data.get('selected_groups', set())
            
            if group_index >= len(message_groups):
                await query.edit_message_text("❌ 无效的选择。")
                return
            
            timestamp, all_msgs = message_groups[group_index]
            
            # 筛选出选中群组的消息
            messages_to_delete = []
            for msg in all_msgs:
                group_key = f"{group_index}_{msg['chat_id']}"
                if group_key in selected_groups:
                    messages_to_delete.append(msg)
            
            if not messages_to_delete:
                await query.edit_message_text("❌ 请先选择要删除的群组。")
                return
            # 记录删除范围：选中群组
            try:
                scope_group_ids = list({m['chat_id'] for m in messages_to_delete})
                context.user_data['delete_scope'] = {
                    'type': 'selected_groups',
                    'group_index': group_index,
                    'group_ids': scope_group_ids
                }
            except Exception:
                pass
            
            await execute_delete(update, context, messages_to_delete)
            return
        
        # 预览选中的群组（严格匹配带数字后缀，避免误匹配 *_batches）
        elif query.data.startswith("preview_selected_") and query.data.split("_")[-1].isdigit():
            group_index = int(query.data.split("_")[-1])
            message_groups = context.user_data.get('delete_message_groups', [])
            selected_groups = context.user_data.get('selected_groups', set())
            
            if group_index >= len(message_groups):
                await query.answer("❌ 无效的选择。", show_alert=True)
                return
            
            timestamp, all_msgs = message_groups[group_index]
            messages_to_preview = []
            for msg in all_msgs:
                group_key = f"{group_index}_{msg['chat_id']}"
                if group_key in selected_groups:
                    messages_to_preview.append(msg)
            
            if not messages_to_preview:
                await query.answer("❌ 请先选择要预览的群组。", show_alert=True)
                return
            
            admin_chat_id = update.effective_chat.id
            previewed_ids = []
            # 预览规则：若选择了多个群，仅选择一个群并预览该群全部消息；若只选中单群，则预览该群的全部消息
            unique_groups = {m['chat_id'] for m in messages_to_preview}
            if len(unique_groups) == 1:
                msgs_iter = sorted(messages_to_preview, key=lambda x: x.get('timestamp') or '', reverse=True)
            else:
                group_latest = {}
                for _m in messages_to_preview:
                    ts = _m.get('timestamp') or ''
                    prev = group_latest.get(_m['chat_id'])
                    if prev is None or ts > prev:
                        group_latest[_m['chat_id']] = ts
                target_group = max(group_latest.items(), key=lambda kv: kv[1])[0]
                msgs_iter = [m for m in sorted(messages_to_preview, key=lambda x: x.get('timestamp') or '', reverse=True) if m['chat_id'] == target_group]
            for msg in msgs_iter:
                try:
                    sent = await context.bot.copy_message(
                        chat_id=admin_chat_id,
                        from_chat_id=msg['chat_id'],
                        message_id=msg['message_id']
                    )
                    previewed_ids.append(sent.message_id)
                except Exception as e:
                    if is_message_not_found_error(e):
                        try:
                            for uid in list(user_sent_messages.keys()):
                                user_sent_messages[uid] = [
                                    m for m in user_sent_messages[uid]
                                    if not (m['chat_id'] == msg['chat_id'] and m['message_id'] == msg['message_id'])
                                ]
                            save_sent_messages(user_sent_messages)
                        except Exception as se:
                            logger.error(f"同步移除已不存在消息失败: {se}")
                        try:
                            ts, all_msgs = message_groups[group_index]
                            message_groups[group_index] = (ts, [m for m in all_msgs if not (m['chat_id'] == msg['chat_id'] and m['message_id'] == msg['message_id'])])
                            context.user_data['delete_message_groups'] = message_groups
                        except Exception as de:
                            logger.error(f"更新当前删除分组失败: {de}")
                        try:
                            append_botlog(update, 'deleted_by_others', {
                                'chat_id': msg['chat_id'],
                                'message_id': msg['message_id'],
                                'origin': 'preview_selected',
                                'reason': str(e)
                            })
                        except Exception:
                            pass
                    else:
                        logger.error(f"预览复制失败(chat={msg['chat_id']} mid={msg['message_id']}): {e}")
            
            async def _auto_delete(ids: list):
                try:
                    await asyncio.sleep(6)
                    for mid in ids:
                        try:
                            await context.bot.delete_message(chat_id=admin_chat_id, message_id=mid)
                        except Exception as e:
                            logger.error(f"删除预览消息失败(mid={mid}): {e}")
                except Exception:
                    pass
            if previewed_ids:
                asyncio.create_task(_auto_delete(previewed_ids))
            await query.answer("✅ 已发送预览，3秒后自动删除。", show_alert=False)
            return
        
        # 显示详细列表
        elif query.data == "show_detailed_list":
            await show_detailed_list(update, context)
            return

        # 切换选择某批次（不关闭菜单）
        elif query.data.startswith("toggle_batch_"):
            try:
                idx = int(query.data.split("_")[-1])
            except Exception:
                await query.answer("❌ 批次编号解析失败。", show_alert=True)
                return
            selected_batches = context.user_data.get('selected_batches', set())
            try:
                selected_batches = set(selected_batches) if selected_batches else set()
            except Exception:
                selected_batches = set()
            if idx in selected_batches:
                selected_batches.remove(idx)
            else:
                selected_batches.add(idx)
            context.user_data['selected_batches'] = selected_batches

            # 如果处于搜索模式，重新渲染搜索结果界面
            if context.user_data.get('search_mode'):
                matched_results = context.user_data.get('search_results', []) or []
                search_term = context.user_data.get('search_term', '')
                page = context.user_data.get('search_page', 0) or 0
                items_per_page = 5
                start_idx = page * items_per_page
                end_idx = start_idx + items_per_page
                page_items = matched_results[start_idx:end_idx]

                text_msg = f"🔍 搜索结果\n\n"
                text_msg += f"关键词: {search_term}\n"
                text_msg += f"找到 {len(matched_results)} 个匹配的群组:\n\n"

                keyboard = []
                selected_batches = context.user_data.get('selected_batches', set()) or set()
                try:
                    selected_batches = set(selected_batches)
                except Exception:
                    selected_batches = set()

                for i, result in enumerate(page_items):
                    actual_index = start_idx + i
                    try:
                        dt = datetime.datetime.fromisoformat(result['timestamp'])
                        time_str = dt.strftime("%m-%d %H:%M")
                    except Exception:
                        time_str = str(result['timestamp'])[:16]
                    ctype = result.get('chat_type')
                    is_private = (ctype == 'private') or (ctype is None and int(result['chat_id']) > 0)
                    if is_private:
                        fname = result.get('chat_first_name') or ''
                        lname = result.get('chat_last_name') or ''
                        uname = result.get('chat_username')
                        if not fname and not lname and not uname:
                            try:
                                chat = await context.bot.get_chat(result['chat_id'])
                                if chat:
                                    uname = chat.username or None
                                    fname = chat.first_name or ''
                                    lname = chat.last_name or ''
                                    result['chat_username'] = uname if uname else result.get('chat_username')
                                    result['chat_first_name'] = fname if fname else result.get('chat_first_name')
                                    result['chat_last_name'] = lname if lname else result.get('chat_last_name')
                                    matched_results[actual_index] = result
                                    context.user_data['search_results'] = matched_results
                            except Exception:
                                pass
                        name_str = (fname + (f" {lname}" if lname else '')).strip() or '-'
                        uname_str = f"@{uname}" if uname else '-'
                        title_line = f"{name_str} | {uname_str} | {result['chat_id']}"
                    else:
                        group_name = result.get('chat_title', '') or f"群组 {result['chat_id']}"
                        title_line = f"{group_name} | {result['chat_id']}"
                    text_msg += f"{actual_index+1}. {title_line}\n"
                    text_msg += f"   时间: {time_str} | 消息数: {result['message_count']}\n\n"

                    is_selected = result['group_index'] in selected_batches
                    checkbox = '✅ 已选批次' if is_selected else '☐ 选择批次'
                    keyboard.append([
                        InlineKeyboardButton(f"🗑️ 删除 #{actual_index+1}", callback_data=f"delete_search_{result['group_index']}_{result['chat_id']}"),
                        InlineKeyboardButton(f"👁️ 预览 #{actual_index+1}", callback_data=f"preview_search_{result['group_index']}_{result['chat_id']}"),
                        InlineKeyboardButton(f"{checkbox}", callback_data=f"toggle_batch_{result['group_index']}")
                    ])

                if len(selected_batches) > 0:
                    keyboard.append([
                        InlineKeyboardButton("🗑️ 全删选中", callback_data="delete_selected_batches"),
                        InlineKeyboardButton("👁️ 全预览选中", callback_data="preview_selected_batches")
                    ])

                nav_buttons = []
                if page > 0:
                    nav_buttons.append(InlineKeyboardButton("⬅️ 上一页", callback_data=f"search_page_{page - 1}"))
                if end_idx < len(matched_results):
                    nav_buttons.append(InlineKeyboardButton("➡️ 下一页", callback_data=f"search_page_{page + 1}"))
                if nav_buttons:
                    keyboard.append(nav_buttons)
                keyboard.append([InlineKeyboardButton("🔙 返回主菜单", callback_data="back_to_main")])
                reply_markup = InlineKeyboardMarkup(keyboard)

                await query.edit_message_text(text_msg, reply_markup=reply_markup)
                return

            await show_detailed_list(update, context)
            return

        # 删除选中的批次
        elif query.data == "delete_selected_batches":
            message_groups = context.user_data.get('delete_message_groups', [])
            selected_batches = context.user_data.get('selected_batches', set())
            try:
                selected_indices = sorted(list(selected_batches))
            except Exception:
                selected_indices = []
            if not selected_indices:
                await query.answer("❌ 请先选择要删除的批次。", show_alert=True)
                return
            messages_to_delete = []
            for idx in selected_indices:
                if 0 <= idx < len(message_groups):
                    _, msgs = message_groups[idx]
                    messages_to_delete.extend(msgs)
            if not messages_to_delete:
                await query.answer("❌ 没有找到要删除的消息。", show_alert=True)
                return
            # 记录删除范围（便于后续日志或统计）
            context.user_data['delete_scope'] = {
                'type': 'selected_batches',
                'indices': selected_indices
            }
            # 清空选择，避免残留
            context.user_data['selected_batches'] = set()
            await execute_delete(update, context, messages_to_delete)
            return

        # 预览选中的批次（不关闭菜单）
        elif query.data == "preview_selected_batches":
            message_groups = context.user_data.get('delete_message_groups', [])
            selected_batches = context.user_data.get('selected_batches', set())
            try:
                selected_indices = sorted(list(selected_batches))
            except Exception:
                selected_indices = []
            if not selected_indices:
                await query.answer("❌ 请先选择要预览的批次。", show_alert=True)
                return
            admin_chat_id = update.effective_chat.id
            previewed_ids = []
            for idx in selected_indices:
                if 0 <= idx < len(message_groups):
                    _, msgs = message_groups[idx]
                    # 预览规则：若该批次含多个群，仅选择一个群并预览该群全部消息；若仅单群，则预览该群的全部消息
                    unique_groups = {m['chat_id'] for m in msgs}
                    if len(unique_groups) == 1:
                        msgs_iter = sorted(msgs, key=lambda x: x.get('timestamp') or '', reverse=True)
                    else:
                        group_latest = {}
                        for _m in msgs:
                            ts = _m.get('timestamp') or ''
                            prev = group_latest.get(_m['chat_id'])
                            if prev is None or ts > prev:
                                group_latest[_m['chat_id']] = ts
                        target_group = max(group_latest.items(), key=lambda kv: kv[1])[0]
                        msgs_iter = [m for m in sorted(msgs, key=lambda x: x.get('timestamp') or '', reverse=True) if m['chat_id'] == target_group]
                    for msg in msgs_iter:
                        try:
                            sent = await context.bot.copy_message(
                                chat_id=admin_chat_id,
                                from_chat_id=msg['chat_id'],
                                message_id=msg['message_id']
                            )
                            previewed_ids.append(sent.message_id)
                        except Exception as e:
                            if is_message_not_found_error(e):
                                try:
                                    for uid in list(user_sent_messages.keys()):
                                        user_sent_messages[uid] = [
                                            m for m in user_sent_messages[uid]
                                            if not (m['chat_id'] == msg['chat_id'] and m['message_id'] == msg['message_id'])
                                        ]
                                    save_sent_messages(user_sent_messages)
                                except Exception as se:
                                    logger.error(f"同步移除已不存在消息失败: {se}")
                                try:
                                    ts, msgs_in_batch = message_groups[idx]
                                    message_groups[idx] = (ts, [m for m in msgs_in_batch if not (m['chat_id'] == msg['chat_id'] and m['message_id'] == msg['message_id'])])
                                    context.user_data['delete_message_groups'] = message_groups
                                except Exception as de:
                                    logger.error(f"更新删除批次失败: {de}")
                                try:
                                    append_botlog(update, 'deleted_by_others', {
                                        'chat_id': msg['chat_id'],
                                        'message_id': msg['message_id'],
                                        'origin': 'preview_selected_batches',
                                        'reason': str(e)
                                    })
                                except Exception:
                                    pass
                            else:
                                logger.error(f"批次预览复制失败(chat={msg['chat_id']} mid={msg['message_id']}): {e}")
            async def _auto_delete(ids: list):
                try:
                    await asyncio.sleep(3)
                    for mid in ids:
                        try:
                            await context.bot.delete_message(chat_id=admin_chat_id, message_id=mid)
                        except Exception as e:
                            logger.error(f"删除预览消息失败(mid={mid}): {e}")
                except Exception:
                    pass
            if previewed_ids:
                asyncio.create_task(_auto_delete(previewed_ids))
            await query.answer("✅ 已发送预览，3秒后自动删除。", show_alert=False)
            return
        
        # 预览某一批次（不关闭菜单）
        elif query.data.startswith("preview_batch_"):
            group_index = int(query.data.split("_")[-1])
            message_groups = context.user_data.get('delete_message_groups', [])
            
            if group_index >= len(message_groups):
                await query.answer("❌ 无效的选择。", show_alert=True)
                return
            
            timestamp, messages_to_preview = message_groups[group_index]
            # 预览规则：若该批次含多个群，仅选择一个群并预览该群全部消息；若仅单群，则预览该群的全部消息
            unique_groups = {m['chat_id'] for m in messages_to_preview}
            if len(unique_groups) == 1:
                msgs_iter = sorted(messages_to_preview, key=lambda x: x.get('timestamp') or '', reverse=True)
            else:
                group_latest = {}
                for _m in messages_to_preview:
                    ts = _m.get('timestamp') or ''
                    prev = group_latest.get(_m['chat_id'])
                    if prev is None or ts > prev:
                        group_latest[_m['chat_id']] = ts
                target_group = max(group_latest.items(), key=lambda kv: kv[1])[0]
                msgs_iter = [m for m in sorted(messages_to_preview, key=lambda x: x.get('timestamp') or '', reverse=True) if m['chat_id'] == target_group]
            admin_chat_id = update.effective_chat.id
            previewed_ids = []
            for msg in msgs_iter:
                try:
                    sent = await context.bot.copy_message(
                        chat_id=admin_chat_id,
                        from_chat_id=msg['chat_id'],
                        message_id=msg['message_id']
                    )
                    previewed_ids.append(sent.message_id)
                except Exception as e:
                    if is_message_not_found_error(e):
                        try:
                            for uid in list(user_sent_messages.keys()):
                                user_sent_messages[uid] = [
                                    m for m in user_sent_messages[uid]
                                    if not (m['chat_id'] == msg['chat_id'] and m['message_id'] == msg['message_id'])
                                ]
                            save_sent_messages(user_sent_messages)
                        except Exception as se:
                            logger.error(f"同步移除已不存在消息失败: {se}")
                        try:
                            ts, msgs_in_group = message_groups[group_index]
                            message_groups[group_index] = (ts, [m for m in msgs_in_group if not (m['chat_id'] == msg['chat_id'] and m['message_id'] == msg['message_id'])])
                            context.user_data['delete_message_groups'] = message_groups
                        except Exception as de:
                            logger.error(f"更新删除分组失败: {de}")
                        try:
                            append_botlog(update, 'deleted_by_others', {
                                'chat_id': msg['chat_id'],
                                'message_id': msg['message_id'],
                                'origin': 'preview_batch',
                                'reason': str(e)
                            })
                        except Exception:
                            pass
                    else:
                        logger.error(f"批次预览复制失败(chat={msg['chat_id']} mid={msg['message_id']}): {e}")
            
            async def _auto_delete(ids: list):
                try:
                    await asyncio.sleep(3)
                    for mid in ids:
                        try:
                            await context.bot.delete_message(chat_id=admin_chat_id, message_id=mid)
                        except Exception as e:
                            logger.error(f"删除预览消息失败(mid={mid}): {e}")
                except Exception:
                    pass
            if previewed_ids:
                asyncio.create_task(_auto_delete(previewed_ids))
            await query.answer("✅ 已发送预览，3秒后自动删除。", show_alert=False)
            return
        
        # 群组选择页分页
        elif query.data.startswith("group_page_"):
            parts = query.data.split("_")
            group_index = int(parts[2])
            page = int(parts[3])
            group_pages = context.user_data.get('group_selection_pages', {})
            group_pages[group_index] = page
            context.user_data['group_selection_pages'] = group_pages
            await show_group_selection(update, context, group_index)
            return
        
        # 分页操作
        elif query.data.startswith("page_"):
            page = int(query.data.split("_")[-1])
            context.user_data['delete_page'] = page
            await show_detailed_list(update, context)
            return
        
        # 搜索结果分页
        elif query.data.startswith("search_page_"):
            try:
                page = int(query.data.split("_")[-1])
            except Exception:
                await query.answer("❌ 页码解析失败。", show_alert=True)
                return
            context.user_data['search_page'] = page
            matched_results = context.user_data.get('search_results', []) or []
            search_term = context.user_data.get('search_term', '')
            items_per_page = 5
            start_idx = page * items_per_page
            end_idx = start_idx + items_per_page
            page_items = matched_results[start_idx:end_idx]
            
            text_msg = f"🔍 搜索结果\n\n"
            text_msg += f"关键词: {search_term}\n"
            text_msg += f"找到 {len(matched_results)} 个匹配的群组:\n\n"
            
            keyboard = []
            selected_batches = context.user_data.get('selected_batches', set()) or set()
            try:
                selected_batches = set(selected_batches)
            except Exception:
                selected_batches = set()
            
            for i, result in enumerate(page_items):
                actual_index = start_idx + i
                try:
                    dt = datetime.datetime.fromisoformat(result['timestamp'])
                    time_str = dt.strftime("%m-%d %H:%M")
                except Exception:
                    time_str = str(result['timestamp'])[:16]
                ctype = result.get('chat_type')
                is_private = (ctype == 'private') or (ctype is None and int(result['chat_id']) > 0)
                if is_private:
                    fname = result.get('chat_first_name') or ''
                    lname = result.get('chat_last_name') or ''
                    uname = result.get('chat_username')
                    if not fname and not lname and not uname:
                        try:
                            chat = await context.bot.get_chat(result['chat_id'])
                            if chat:
                                uname = chat.username or None
                                fname = chat.first_name or ''
                                lname = chat.last_name or ''
                                result['chat_username'] = uname if uname else result.get('chat_username')
                                result['chat_first_name'] = fname if fname else result.get('chat_first_name')
                                result['chat_last_name'] = lname if lname else result.get('chat_last_name')
                                matched_results[actual_index] = result
                                context.user_data['search_results'] = matched_results
                        except Exception:
                            pass
                    name_str = (fname + (f" {lname}" if lname else '')).strip() or '-'
                    uname_str = f"@{uname}" if uname else '-'
                    title_line = f"{name_str} | {uname_str} | {result['chat_id']}"
                else:
                    group_name = result.get('chat_title', '') or f"群组 {result['chat_id']}"
                    title_line = f"{group_name} | {result['chat_id']}"
                text_msg += f"{actual_index+1}. {title_line}\n"
                text_msg += f"   时间: {time_str} | 消息数: {result['message_count']}\n\n"
                
                is_selected = result['group_index'] in selected_batches
                checkbox = '✅ 已选批次' if is_selected else '☐ 选择批次'
                keyboard.append([
                    InlineKeyboardButton(f"🗑️ 删除 #{actual_index+1}", callback_data=f"delete_search_{result['group_index']}_{result['chat_id']}"),
                    InlineKeyboardButton(f"👁️ 预览 #{actual_index+1}", callback_data=f"preview_search_{result['group_index']}_{result['chat_id']}"),
                    InlineKeyboardButton(f"{checkbox}", callback_data=f"toggle_batch_{result['group_index']}")
                ])
            
            if len(selected_batches) > 0:
                keyboard.append([
                    InlineKeyboardButton("🗑️ 全删选中", callback_data="delete_selected_batches"),
                    InlineKeyboardButton("👁️ 全预览选中", callback_data="preview_selected_batches")
                ])
            
            nav_buttons = []
            if page > 0:
                nav_buttons.append(InlineKeyboardButton("⬅️ 上一页", callback_data=f"search_page_{page - 1}"))
            if end_idx < len(matched_results):
                nav_buttons.append(InlineKeyboardButton("➡️ 下一页", callback_data=f"search_page_{page + 1}"))
            if nav_buttons:
                keyboard.append(nav_buttons)
            keyboard.append([InlineKeyboardButton("🔙 返回主菜单", callback_data="back_to_main")])
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(text_msg, reply_markup=reply_markup)
            return
        
        # 按群组搜索
        elif query.data == "search_by_group":
            await show_search_interface(update, context)
            return
        
        # 搜索结果删除
        elif query.data.startswith("delete_search_"):
            parts = query.data.split("_")
            group_index = int(parts[2])
            chat_id = int(parts[3])
            
            message_groups = context.user_data.get('delete_message_groups', [])
            
            if group_index >= len(message_groups):
                await query.edit_message_text("❌ 无效的选择。")
                return
            
            timestamp, all_msgs = message_groups[group_index]
            
            # 筛选出指定群组的消息
            messages_to_delete = [msg for msg in all_msgs if msg['chat_id'] == chat_id]
            
            if not messages_to_delete:
                await query.edit_message_text("❌ 没有找到要删除的消息。")
                return
            # 记录删除范围：按搜索结果删除
            try:
                context.user_data['delete_scope'] = {
                    'type': 'search',
                    'group_index': group_index,
                    'chat_id': chat_id
                }
            except Exception:
                pass
            
            await execute_delete(update, context, messages_to_delete)
            return
        
        # 搜索结果预览
        elif query.data.startswith("preview_search_"):
            parts = query.data.split("_")
            group_index = int(parts[2])
            chat_id = int(parts[3])
            
            message_groups = context.user_data.get('delete_message_groups', [])
            
            if group_index >= len(message_groups):
                await query.answer("❌ 无效的选择。", show_alert=True)
                return
            
            timestamp, all_msgs = message_groups[group_index]
            messages_to_preview = [msg for msg in all_msgs if msg['chat_id'] == chat_id]
            
            if not messages_to_preview:
                await query.answer("❌ 没有找到要预览的消息。", show_alert=True)
                return
            
            admin_chat_id = update.effective_chat.id
            previewed_ids = []
            msgs_iter = sorted(messages_to_preview, key=lambda x: x.get('timestamp') or '', reverse=True)
            for msg in msgs_iter:
                try:
                    sent = await context.bot.copy_message(
                        chat_id=admin_chat_id,
                        from_chat_id=msg['chat_id'],
                        message_id=msg['message_id']
                    )
                    previewed_ids.append(sent.message_id)
                except Exception as e:
                    if is_message_not_found_error(e):
                        try:
                            for uid in list(user_sent_messages.keys()):
                                user_sent_messages[uid] = [
                                    m for m in user_sent_messages[uid]
                                    if not (m['chat_id'] == msg['chat_id'] and m['message_id'] == msg['message_id'])
                                ]
                            save_sent_messages(user_sent_messages)
                        except Exception as se:
                            logger.error(f"同步移除已不存在消息失败: {se}")
                        try:
                            ts, msgs_in_group = message_groups[group_index]
                            message_groups[group_index] = (ts, [m for m in msgs_in_group if not (m['chat_id'] == msg['chat_id'] and m['message_id'] == msg['message_id'])])
                            context.user_data['delete_message_groups'] = message_groups
                        except Exception as de:
                            logger.error(f"更新删除分组失败: {de}")
                        try:
                            append_botlog(update, 'deleted_by_others', {
                                'chat_id': msg['chat_id'],
                                'message_id': msg['message_id'],
                                'origin': 'preview_search',
                                'reason': str(e)
                            })
                        except Exception:
                            pass
                    else:
                        logger.error(f"搜索预览复制失败(chat={msg['chat_id']} mid={msg['message_id']}): {e}")
            
            async def _auto_delete(ids: list):
                try:
                    await asyncio.sleep(3)
                    for mid in ids:
                        try:
                            await context.bot.delete_message(chat_id=admin_chat_id, message_id=mid)
                        except Exception as e:
                            logger.error(f"删除预览消息失败(mid={mid}): {e}")
                except Exception:
                    pass
            if previewed_ids:
                asyncio.create_task(_auto_delete(previewed_ids))
            await query.answer("✅ 已发送预览，3秒后自动删除。", show_alert=False)
            return
        
        else:
            await query.edit_message_text("❌ 未知的操作。")
            
    except Exception as e:
        logger.error(f"处理删除回调失败: {str(e)}")
        await query.edit_message_text("❌ 删除操作失败。")

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 清理消息列表
    context.user_data.pop('messages', None)
    await update.message.reply_text('❌ 操作已取消。')
    return ConversationHandler.END

# =====================
# 定时/倒计时 群发功能
# =====================

def load_scheduled_tasks():
    if firestore_enabled():
        data = load_json('scheduled_tasks', default=[])
        if isinstance(data, list):
            return data
    try:
        if not os.path.exists(SCHEDULED_TASKS_FILE):
            with open(SCHEDULED_TASKS_FILE, 'w', encoding='utf-8') as f:
                json.dump([], f, ensure_ascii=False, indent=2)
        with open(SCHEDULED_TASKS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return []

def save_scheduled_tasks(tasks: list):
    if firestore_enabled():
        ok = save_json('scheduled_tasks', tasks)
        if ok:
            return
    try:
        with open(SCHEDULED_TASKS_FILE, 'w', encoding='utf-8') as f:
            json.dump(tasks, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"保存定时任务失败: {e}")

@require_permission
async def list_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tasks = []
    try:
        tasks = load_scheduled_tasks()
    except Exception as e:
        logger.error(f"加载定时/计时任务失败: {e}")
        await update.message.reply_text("加载任务失败，请稍后重试。")
        return

    # 默认仅显示待执行任务；提供参数 'all' 或 '全部' 显示全部
    show_all = False
    try:
        args = getattr(context, 'args', []) or []
        show_all = any(str(a).lower() in ("all", "全部") for a in args)
    except Exception:
        show_all = False

    display_tasks = tasks if show_all else [t for t in tasks if t.get('status') == 'scheduled']
    if not display_tasks:
        await update.message.reply_text("当前没有待执行的定时任务或计时任务。")
        return

    tz8 = datetime.timezone(datetime.timedelta(hours=8))

    # 统计
    total = len(display_tasks)
    sched_count = sum(1 for t in display_tasks if t.get('type') == 'schedule')
    timer_count = sum(1 for t in display_tasks if t.get('type') == 'timer')

    lines = []
    title = "📋 任务列表（全部）" if show_all else "📋 任务列表（待执行）"
    lines.append(title)
    lines.append(f"总数: {total} | 定时: {sched_count} | 倒计时: {timer_count}")

    for i, t in enumerate(display_tasks, 1):
        typ = "定时" if t.get('type') == 'schedule' else "倒计时"
        status = t.get('status', 'unknown')
        target_desc = t.get('target_description', '')
        creator = t.get('created_by_username') or t.get('created_by_user_id')
        created_at = t.get('created_at')

        time_part = ""
        if t.get('type') == 'schedule':
            st = t.get('schedule_time_utc')
            when_str = st
            if st:
                try:
                    dt = datetime.datetime.fromisoformat(st)
                    when_str = dt.astimezone(tz8).strftime('%Y-%m-%d %H:%M:%S')
                except Exception:
                    pass
            time_part = f"计划时间(GMT+8): {when_str}"
        else:
            cs = t.get('countdown_seconds')
            expected_str = None
            exec_at = t.get('execute_time_utc')
            if exec_at:
                try:
                    dt = datetime.datetime.fromisoformat(exec_at)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=datetime.timezone.utc)
                    expected_str = dt.astimezone(tz8).strftime('%Y-%m-%d %H:%M:%S')
                except Exception:
                    expected_str = None
            else:
                created = t.get('created_at_utc') or t.get('created_at')
                if created and cs is not None:
                    try:
                        cdt = datetime.datetime.fromisoformat(created)
                        if cdt.tzinfo is None:
                            # 兼容旧任务：若无 tzinfo，created_at_utc 视为 UTC，否则视为 GMT+8
                            if t.get('created_at_utc'):
                                cdt = cdt.replace(tzinfo=datetime.timezone.utc)
                            else:
                                cdt = cdt.replace(tzinfo=datetime.timezone(datetime.timedelta(hours=8)))
                        exp = cdt + datetime.timedelta(seconds=int(cs))
                        expected_str = exp.astimezone(tz8).strftime('%Y-%m-%d %H:%M:%S')
                    except Exception:
                        expected_str = None
            time_part = f"倒计时: {cs}s" + (f" | 预计执行(GMT+8): {expected_str}" if expected_str else "")

        # 创建时间格式 YYYY/MM/DD HH:MM:SS（GMT+8）
        created_str = created_at
        if created_at:
            try:
                cdt = datetime.datetime.fromisoformat(created_at)
                if cdt.tzinfo is None:
                    cdt = cdt.replace(tzinfo=datetime.timezone.utc)
                created_str = cdt.astimezone(tz8).strftime('%Y/%m/%d %H:%M:%S')
            except Exception:
                pass

        # HTML 转义，ID 使用 <code> 便于复制
        safe_target = html.escape(str(target_desc or ''))
        safe_creator = html.escape(str(creator or ''))
        id_str = html.escape(str(t.get('id')))

        lines.append(f"{i}. [{typ}/{status}] {safe_target}")
        lines.append(f"    ID: <code>{id_str}</code>")
        lines.append(f"    {time_part}")
        lines.append(f"    创建者: {safe_creator} | 创建时间: {created_str}")
        lines.append("")

    # 记录 /tasks 操作到 botlog
    try:
        append_botlog(update, 'tasks_view', {
            'show_all': show_all,
            'display_count': total,
            'scheduled_count': sched_count,
            'timer_count': timer_count,
            'task_ids': [t.get('id') for t in display_tasks]
        })
    except Exception:
        pass

    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, disable_web_page_preview=True)

@require_permission
async def cancel_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text('只有管理员可以取消任务。')
        return
    args = getattr(context, 'args', []) or []
    if not args:
        await update.message.reply_text('用法：/cancel_task <task_id>')
        return
    tid = str(args[0]).strip()
    tasks = []
    try:
        tasks = load_scheduled_tasks()
    except Exception as e:
        logger.error(f"加载任务失败：{e}")
        await update.message.reply_text('加载任务失败，请稍后重试。')
        return
    target = None
    for t in tasks:
        if t.get('id') == tid:
            target = t
            break
    if not target:
        await update.message.reply_text(f'未找到任务：{tid}')
        return
    status = target.get('status')
    if status in ('completed', 'failed', 'cancelled'):
        await update.message.reply_text(f'任务已结束，当前状态：{status}')
        return
    cancelled_running = False
    # 尝试中断正在执行的协程
    try:
        handles = context.application.bot_data.get('task_handles')
        h = handles.get(tid) if handles else None
        if h is not None:
            h.cancel()
            cancelled_running = True
            try:
                handles.pop(tid, None)
            except Exception:
                pass
    except Exception:
        pass
    # 更新任务状态为取消
    try:
        for t in tasks:
            if t.get('id') == tid:
                t['status'] = 'cancelled'
                t['error'] = 'cancelled_by_user'
                t['executed_at'] = datetime.datetime.now().isoformat()
                break
        save_scheduled_tasks(tasks)
    except Exception as e:
        logger.error(f'更新任务取消状态失败：{e}')
    # 从运行集合移除，避免继续调度
    try:
        started_ids = context.application.bot_data.get('running_task_ids')
        if started_ids is not None:
            started_ids.discard(tid)
    except Exception:
        pass
    # 记录到 botlog
    try:
        append_botlog(None, 'task_cancel', {
            'task_id': tid,
            'by_user_id': update.effective_user.id,
            'by_username': update.effective_user.username,
            'cancelled_running': cancelled_running
        })
    except Exception:
        pass
    await update.message.reply_text('✅ 已取消任务。' + ('（正在执行的任务已中断）' if cancelled_running else ''))

# 解析倒计时文本（支持如：1H 30M 15S、90M、3600S、HH:MM:SS）
def parse_countdown_seconds(text: str) -> int:
    s = text.strip().lower()
    # HH:MM:SS 格式
    m_hms = re.match(r"^(\d+):(\d{1,2}):(\d{1,2})$", s)
    if m_hms:
        h, m, s2 = int(m_hms.group(1)), int(m_hms.group(2)), int(m_hms.group(3))
        return h * 3600 + m * 60 + s2
    # 单位格式（支持任意顺序） 支持 d(天), h, m, s
    parts = re.findall(r"(\d+)\s*([dhms])", s)
    if parts:
        total = 0
        for num, unit in parts:
            val = int(num)
            if unit == 'd':
                total += val * 86400
            elif unit == 'h':
                total += val * 3600
            elif unit == 'm':
                total += val * 60
            elif unit == 's':
                total += val
        return total
    # 纯数字（视为秒）
    if s.isdigit():
        return int(s)
    return -1

# 启动 /schedule_send
@require_permission
async def start_schedule_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text('只有管理员可以使用定时群发。')
        return
    context.user_data['messages'] = []
    context.user_data['flow'] = 'schedule'
    return await start_send_menu(update, context)

# 启动 /timer_send
@require_permission
async def start_timer_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text('只有管理员可以使用倒计时群发。')
        return
    context.user_data['messages'] = []
    context.user_data['flow'] = 'timer'
    return await start_send_menu(update, context)

# 添加更多/完成添加（定时/倒计时流程专用）
async def handle_add_more_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    flow = context.user_data.get('flow')
    if query.data == 'add_more':
        await query.edit_message_text('📝 请继续发送要群发的消息（支持文本、图片、视频、文档）：')
        return SENDING_MESSAGE
    elif query.data == 'finish_adding':
        if flow == 'schedule':
            await query.edit_message_text('🕐 请输入执行时间（GMT+8），格式：YYYY/MM/DD HH:MM')
            return SCHEDULE_SET_TIME
        elif flow == 'timer':
            await query.edit_message_text('⏳ 请输入倒计时：例如 1H 30M 15S、90M、3600S 或 HH:MM:SS')
            return TIMER_SET_COUNTDOWN
        else:
            await query.edit_message_text('❌ 流程未识别。')
            return ConversationHandler.END
    elif query.data == 'cancel':
        context.user_data.pop('messages', None)
        await query.edit_message_text('❌ 操作已取消。')
        return ConversationHandler.END

# 在定时/倒计时流程中阻止“确认群发”按钮的误触
async def handle_block_confirm_send_in_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except Exception:
        pass
    try:
        await query.message.reply_text('当前为定时/倒计时流程，请点击“✅ 完成添加”进入时间设置。')
    except Exception:
        pass
    return CONFIRMING

# 输入定时时间
async def input_schedule_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or '').strip()
    try:
        dt_local = datetime.datetime.strptime(text, '%Y/%m/%d %H:%M')
        tz = datetime.timezone(datetime.timedelta(hours=8))
        dt_local = dt_local.replace(tzinfo=tz)
        dt_utc = dt_local.astimezone(datetime.timezone.utc)
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        delay = (dt_utc - now_utc).total_seconds()
        if delay <= 0:
            await update.message.reply_text('❌ 时间已过或格式错误，请重新输入：YYYY/MM/DD HH:MM（GMT+8）')
            return SCHEDULE_SET_TIME
        # 保存到上下文
        context.user_data['schedule_time_utc'] = dt_utc.isoformat()
        context.user_data['schedule_time_local'] = dt_local.strftime('%Y/%m/%d %H:%M:%S')
        # 显示确认信息
        target_groups = context.user_data.get('target_groups', [])
        target_description = context.user_data.get('target_description', '未知')
        messages = context.user_data.get('messages', [])
        keyboard = [
            [InlineKeyboardButton('✅ 确认创建', callback_data='confirm_create_schedule')],
            [InlineKeyboardButton('❌ 取消', callback_data='cancel')]
        ]
        # 计算当前时间（GMT+8）
        tz = datetime.timezone(datetime.timedelta(hours=8))
        now_local = datetime.datetime.now(datetime.timezone.utc).astimezone(tz)
        now_local_str = now_local.strftime('%Y/%m/%d %H:%M:%S')
        await update.message.reply_text(
            f"📋 定时任务确认\n\n"
            f"🎯 目标: {target_description}（{len(target_groups)} 个群组）\n"
            f"📝 消息数量: {len(messages)}\n"
            f"🕐 执行时间: {context.user_data['schedule_time_local']} (GMT+8)\n"
            f"⏰ 当前时间: {now_local_str} (GMT+8)",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return SCHEDULE_CONFIRM_CREATE
    except Exception:
        await update.message.reply_text('❌ 格式错误，请输入：YYYY/MM/DD HH:MM（GMT+8）')
        return SCHEDULE_SET_TIME

# 输入倒计时
async def input_timer_countdown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or '').strip()
    secs = parse_countdown_seconds(text)
    if secs <= 0:
        await update.message.reply_text('❌ 倒计时格式错误，请重新输入（示例：1H 30M 15S、90M、3600S 或 HH:MM:SS）')
        return TIMER_SET_COUNTDOWN
    context.user_data['countdown_seconds'] = secs
    # 显示确认信息
    target_groups = context.user_data.get('target_groups', [])
    target_description = context.user_data.get('target_description', '未知')
    messages = context.user_data.get('messages', [])
    keyboard = [
        [InlineKeyboardButton('✅ 确认创建', callback_data='confirm_create_timer')],
        [InlineKeyboardButton('❌ 取消', callback_data='cancel')]
    ]
    # 格式化友好显示
    h = secs // 3600
    m = (secs % 3600) // 60
    s = secs % 60
    friendly = f"{h}小时 {m}分钟 {s}秒"
    await update.message.reply_text(
        f"📋 倒计时任务确认\n\n"
        f"🎯 目标: {target_description}（{len(target_groups)} 个群组）\n"
        f"📝 消息数量: {len(messages)}\n"
        f"⏳ 倒计时: {friendly}",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return SCHEDULE_CONFIRM_CREATE

# 创建任务（定时/倒计时）
async def confirm_create_schedule_timer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    messages = context.user_data.get('messages', [])
    target_groups = context.user_data.get('target_groups', [])
    target_description = context.user_data.get('target_description', '未知')
    if not messages or not target_groups:
        await query.edit_message_text('❌ 数据不完整。')
        return ConversationHandler.END
    callback = query.data
    now_iso = datetime.datetime.now().isoformat()
    group_ids = [g['chat_id'] for g in target_groups]
    tasks = load_scheduled_tasks()
    task_id = str(uuid.uuid4())
    if callback == 'confirm_create_schedule':
        schedule_time_utc = context.user_data.get('schedule_time_utc')
        # 记录来源 chat 与消息 id，用于完成通知回复
        origin_chat_id = update.effective_chat.id
        origin_message_id = query.message.message_id
        task = {
            'id': task_id,
            'type': 'schedule',
            'created_by_user_id': user.id,
            'created_by_username': _get_display_username(user),
            'created_at': now_iso,
            'target_description': target_description,
            'group_ids': group_ids,
            'messages': messages,
            'merge_mode': False,
            'schedule_time_utc': schedule_time_utc,
            'status': 'scheduled',
            'origin_chat_id': origin_chat_id,
            'origin_message_id': origin_message_id
        }
        tasks.append(task)
        save_scheduled_tasks(tasks)
        # 组详情列表（含名称与每群消息条数）
        groups_detail = []
        for g in target_groups:
            title = g.get('title') or f"群组 {g.get('chat_id')}"
            if g.get('alias'):
                title = f"{g['alias']} ({title})"
            groups_detail.append({
                'group_id': g.get('chat_id'),
                'group_name': title,
                'message_count': len(messages)
            })
        # 记录创建日志（优化结构）
        append_botlog(update, 'schedule_create', {
            'target_description': target_description,
            'groups_count': len(target_groups),
            'messages_total': len(messages),
            'message_total': len(messages) * len(target_groups),
            'message_summary': summarize_messages_for_log(messages),
            'groups_detail': groups_detail,
            'scheduled_time_gmt8': context.user_data.get('schedule_time_local'),
            'task_id': task_id
        })
        # 启动后台执行
        try:
            started_ids = context.application.bot_data.setdefault('running_task_ids', set())
            started_ids.add(task_id)
        except Exception:
            pass
        # 保存任务句柄，支持中断
        try:
            handles = context.application.bot_data.setdefault('task_handles', {})
            handles[task_id] = asyncio.create_task(_execute_scheduled_task(context.application, task))
        except Exception:
            # 回退如果句柄存储失败，至少启动执行
            asyncio.create_task(_execute_scheduled_task(context.application, task))
        # 显示创建与执行时间
        tz = datetime.timezone(datetime.timedelta(hours=8))
        now_local = datetime.datetime.now(datetime.timezone.utc).astimezone(tz)
        now_local_str = now_local.strftime('%Y/%m/%d %H:%M:%S')
        exec_local_str = context.user_data.get('schedule_time_local')
        await query.edit_message_text(
            f"✅ 定时任务已创建！\n"
            f"📅 创建时间：{now_local_str} (GMT+8)\n"
            f"🕐 执行时间：{exec_local_str} (GMT+8)"
        )
        return ConversationHandler.END
    elif callback == 'confirm_create_timer':
        countdown_seconds = context.user_data.get('countdown_seconds')
        # 记录来源 chat 与消息 id，用于完成通知回复
        origin_chat_id = update.effective_chat.id
        origin_message_id = query.message.message_id
        # 使用 UTC 记录关键时间，确保断线重启后可准确恢复剩余倒计时
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        execute_time_utc = (now_utc + datetime.timedelta(seconds=int(countdown_seconds))).isoformat()
        task = {
            'id': task_id,
            'type': 'timer',
            'created_by_user_id': user.id,
            'created_by_username': _get_display_username(user),
            'created_at': now_iso,
            'created_at_utc': now_utc.isoformat(),
            'target_description': target_description,
            'group_ids': group_ids,
            'messages': messages,
            'merge_mode': False,
            'countdown_seconds': countdown_seconds,
            'execute_time_utc': execute_time_utc,
            'status': 'scheduled',
            'origin_chat_id': origin_chat_id,
            'origin_message_id': origin_message_id
        }
        tasks.append(task)
        save_scheduled_tasks(tasks)
        # 组详情列表（含名称与每群消息条数）
        groups_detail = []
        for g in target_groups:
            title = g.get('title') or f"群组 {g.get('chat_id')}"
            if g.get('alias'):
                title = f"{g['alias']} ({title})"
            groups_detail.append({
                'group_id': g.get('chat_id'),
                'group_name': title,
                'message_count': len(messages)
            })
        # 记录创建日志（优化结构）
        append_botlog(update, 'timer_create', {
            'target_description': target_description,
            'groups_count': len(target_groups),
            'messages_total': len(messages),
            'message_total': len(messages) * len(target_groups),
            'message_summary': summarize_messages_for_log(messages),
            'groups_detail': groups_detail,
            'countdown_seconds': countdown_seconds,
            'task_id': task_id
        })
        try:
            started_ids = context.application.bot_data.setdefault('running_task_ids', set())
            started_ids.add(task_id)
        except Exception:
            pass
        try:
            handles = context.application.bot_data.setdefault('task_handles', {})
            handles[task_id] = asyncio.create_task(_execute_scheduled_task(context.application, task))
        except Exception:
            asyncio.create_task(_execute_scheduled_task(context.application, task))
        h = countdown_seconds // 3600
        m = (countdown_seconds % 3600) // 60
        s = countdown_seconds % 60
        tz = datetime.timezone(datetime.timedelta(hours=8))
        now_local = datetime.datetime.now(datetime.timezone.utc).astimezone(tz)
        exec_local = now_local + datetime.timedelta(seconds=countdown_seconds)
        now_local_str = now_local.strftime('%Y/%m/%d %H:%M:%S')
        exec_local_str = exec_local.strftime('%Y/%m/%d %H:%M:%S')
        await query.edit_message_text(
            f"✅ 倒计时任务已创建！\n"
            f"📅 创建时间：{now_local_str} (GMT+8)\n"
            f"⏳ 将在 {h}小时 {m}分钟 {s}秒 后执行\n"
            f"🕐 预计执行：{exec_local_str} (GMT+8)"
        )
        return ConversationHandler.END
    else:
        await query.edit_message_text('❌ 未知操作。')
        return ConversationHandler.END

# 执行任务
async def _execute_scheduled_task(application: Application, task: dict):
    try:
        # 计算延迟
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        delay = 0
        if task.get('type') == 'schedule':
            dt_utc = datetime.datetime.fromisoformat(task['schedule_time_utc'])
            if dt_utc.tzinfo is None:
                dt_utc = dt_utc.replace(tzinfo=datetime.timezone.utc)
            delay = (dt_utc - now_utc).total_seconds()
        elif task.get('type') == 'timer':
            # 优先使用创建时记录的 UTC 执行时间；否则回退为 created_at + countdown_seconds
            exec_at = task.get('execute_time_utc')
            if exec_at:
                try:
                    dt_utc = datetime.datetime.fromisoformat(exec_at)
                    if dt_utc.tzinfo is None:
                        dt_utc = dt_utc.replace(tzinfo=datetime.timezone.utc)
                    delay = (dt_utc - now_utc).total_seconds()
                except Exception:
                    delay = int(task.get('countdown_seconds', 0))
            else:
                cs = int(task.get('countdown_seconds', 0))
                created_str = task.get('created_at_utc') or task.get('created_at')
                if created_str:
                    try:
                        cdt = datetime.datetime.fromisoformat(created_str)
                        # 对于旧任务：若无 tzinfo，则尽量判断；优先将 created_at_utc 视为 UTC，否则视为 GMT+8
                        if cdt.tzinfo is None:
                            if task.get('created_at_utc'):
                                cdt = cdt.replace(tzinfo=datetime.timezone.utc)
                            else:
                                tz8 = datetime.timezone(datetime.timedelta(hours=8))
                                cdt = cdt.replace(tzinfo=tz8)
                        cdt_utc = cdt.astimezone(datetime.timezone.utc)
                        elapsed = (now_utc - cdt_utc).total_seconds()
                        delay = cs - elapsed
                    except Exception:
                        delay = cs
                else:
                    delay = cs
        if delay > 0:
            await asyncio.sleep(delay)
        # 准备目标群组
        groups_all = load_groups()
        id_set = set(task.get('group_ids', []))
        target_groups = [g for g in groups_all if g.get('chat_id') in id_set]
        target_description = task.get('target_description') or f"{len(target_groups)} 个群组"
        # 执行发送
        # 将当前任务ID和发起用户暂存到 bot 以便写入日志
        try:
            setattr(application.bot, 'current_task_id', task.get('id'))
            setattr(application.bot, 'current_task_user_name', task.get('created_by_username'))
        except Exception:
            pass
        result = await _send_messages_to_groups(
            application.bot,
            task.get('messages', []),
            target_groups,
            task.get('merge_mode', False),
            task.get('created_by_user_id'),
            target_description,
            origin=('scheduled' if task.get('type') == 'schedule' else 'timer'),
            scheduled_time=task.get('schedule_time_utc'),
            countdown_seconds=task.get('countdown_seconds'),
            task_id=task.get('id'),
            created_by_username=task.get('created_by_username')
        )
        # 清理临时属性
        try:
            delattr(application.bot, 'current_task_id')
        except Exception:
            pass
        try:
            delattr(application.bot, 'current_task_user_name')
        except Exception:
            pass
        # 在创建处通知发送完成（同一会话，回复原消息）
        try:
            tz = datetime.timezone(datetime.timedelta(hours=8))
            now_local = datetime.datetime.now(datetime.timezone.utc).astimezone(tz)
            now_local_str = now_local.strftime('%Y/%m/%d %H:%M:%S')
            typ = '定时' if task.get('type') == 'schedule' else '倒计时'
            msg_count = len(task.get('messages', []))
            origin_chat_id = task.get('origin_chat_id') or task.get('created_by_user_id')
            origin_message_id = task.get('origin_message_id')
            # 组织失败详情文本
            lines = [
                f"✅ {typ}任务发送完成",
                f"📅 完成时间：{now_local_str} (GMT+8)",
                f"🎯 目标：{target_description}",
                f"📦 消息条数：{msg_count}",
                f"📊 成功群组：{result.get('success_count', 0)}/{result.get('target_count', 0)}",
                f"⏱️ 耗时：{result.get('elapsed_str', '')}"
            ]
            failed_groups = result.get('failed_groups') or []
            if failed_groups:
                lines.append("❌ 失败详情：")
                for fg in failed_groups:
                    name = fg.get('name') or '未知'
                    gid = fg.get('id')
                    reason = fg.get('reason') or '未知原因'
                    lines.append(f"- {name}（ID: {gid}）：{reason}")
            text = "\n".join(lines)
            await application.bot.send_message(
                chat_id=origin_chat_id,
                text=text,
                reply_to_message_id=origin_message_id
            )
        except Exception as e:
            logger.warning(f"发送完成通知失败：{e}")
        # 更新任务状态
        tasks = load_scheduled_tasks()
        for t in tasks:
            if t.get('id') == task.get('id'):
                t['status'] = 'completed'
                t['executed_at'] = datetime.datetime.now().isoformat()
                break
        save_scheduled_tasks(tasks)
        # 从运行中集合移除，避免重复调度
        try:
            started_ids = application.bot_data.get('running_task_ids')
            if started_ids is not None:
                started_ids.discard(task.get('id'))
        except Exception:
            pass
        # 清理任务句柄
        try:
            handles = application.bot_data.get('task_handles')
            if handles is not None:
                handles.pop(task.get('id'), None)
        except Exception:
            pass
    except asyncio.CancelledError as e:
        # 任务被取消：更新状态、清理并尽力通知创建者
        tasks = load_scheduled_tasks()
        try:
            for t in tasks:
                if t.get('id') == task.get('id'):
                    t['status'] = 'cancelled'
                    t['error'] = 'cancelled'
                    t['executed_at'] = datetime.datetime.now().isoformat()
                    break
            save_scheduled_tasks(tasks)
        except Exception:
            pass
        try:
            started_ids = application.bot_data.get('running_task_ids')
            if started_ids is not None:
                started_ids.discard(task.get('id'))
        except Exception:
            pass
        try:
            handles = application.bot_data.get('task_handles')
            if handles is not None:
                handles.pop(task.get('id'), None)
        except Exception:
            pass
        try:
            origin_chat_id = task.get('origin_chat_id') or task.get('created_by_user_id')
            origin_message_id = task.get('origin_message_id')
            typ = '定时' if task.get('type') == 'schedule' else '倒计时'
            tz = datetime.timezone(datetime.timedelta(hours=8))
            now_local = datetime.datetime.now(datetime.timezone.utc).astimezone(tz)
            now_local_str = now_local.strftime('%Y/%m/%d %H:%M:%S')
            await application.bot.send_message(
                chat_id=origin_chat_id,
                text=f"⚠️ {typ}任务已取消\n📅 取消时间：{now_local_str} (GMT+8)",
                reply_to_message_id=origin_message_id
            )
        except Exception:
            pass
        return
    except Exception as e:
        logger.error(f"执行定时任务失败: {e}")
        tasks = load_scheduled_tasks()
        for t in tasks:
            if t.get('id') == task.get('id'):
                t['status'] = 'failed'
                t['error'] = str(e)
                t['executed_at'] = datetime.datetime.now().isoformat()
                break
        save_scheduled_tasks(tasks)
        # 从运行中集合移除，避免重复调度
        try:
            started_ids = application.bot_data.get('running_task_ids')
            if started_ids is not None:
                started_ids.discard(task.get('id'))
        except Exception:
            pass

# 核心发送逻辑（供定时任务调用）
async def _send_messages_to_groups(bot, messages: list, target_groups: list, merge_mode: bool, user_id: int, target_description: str, origin: str, scheduled_time: str = None, countdown_seconds: int = None, task_id: str = None, created_by_username: str = None):
    # 记录开始时间（性能计时与实际时间）
    start_time = time.perf_counter()
    start_dt = datetime.datetime.now()
    start_iso = start_dt.isoformat()
    start_str = start_dt.strftime('%Y-%m-%d %H:%M:%S')
    batch_ts = start_iso
    success_count = 0
    failed_count = 0
    total_messages_sent = 0
    sent_messages = []

    # 统计消息类型
    message_types = {}
    for msg in messages:
        msg_type = msg['type']
        message_types[msg_type] = message_types.get(msg_type, 0) + 1

    # 分离文本与媒体
    text_messages = [msg for msg in messages if msg['type'] == 'text']
    media_messages = [msg for msg in messages if msg['type'] in ['photo', 'video', 'document']]

    from telegram import InputMediaPhoto, InputMediaVideo, InputMediaDocument

    async def send_to_group(group):
        group_success = 0
        group_failed = 0
        group_sent_messages = []
        group_error = None
        try:
            await rate_limiter.wait_if_needed(group['chat_id'])
            # 合并媒体组
            if merge_mode and len(media_messages) > 1:
                media_group = []
                combined_text = ""
                if text_messages:
                    combined_text = "\n\n".join([t.get('text', '') for t in text_messages if t.get('text')])
                for i, message in enumerate(media_messages):
                    if message['type'] == 'photo':
                        file_id = message['file_id']
                        item = InputMediaPhoto(media=file_id, caption=(combined_text if i == 0 and combined_text else None))
                    elif message['type'] == 'video':
                        file_id = message['file_id']
                        item = InputMediaVideo(media=file_id, caption=(combined_text if i == 0 and combined_text else None))
                    else:
                        file_id = message['file_id']
                        item = InputMediaDocument(media=file_id, caption=(combined_text if i == 0 and combined_text else None))
                    media_group.append(item)
                sent_msgs = await bot.send_media_group(chat_id=group['chat_id'], media=media_group)
                for sent_msg in sent_msgs:
                    record = {'chat_id': group['chat_id'], 'message_id': sent_msg.message_id, 'user_id': user_id, 'timestamp': batch_ts}
                    group_sent_messages.append(record)
                    await _persist_sent_messages(user_id, [record])
                group_success += len(sent_msgs)
            else:
                if not merge_mode:
                    for i, message in enumerate(messages):
                        if i > 0:
                            await rate_limiter.wait_if_needed(group['chat_id'])
                        # 类型分发，避免依赖原消息存在
                        msg_type = message.get('type')
                        sent_msg = None
                        try:
                            if msg_type == 'text':
                                text = (message.get('text') or message.get('content') or '')
                                if text:
                                    sent_msg = await bot.send_message(chat_id=group['chat_id'], text=text)
                                else:
                                    sent_msg = await bot.copy_message(chat_id=group['chat_id'], from_chat_id=message['chat_id'], message_id=message['message_id'])
                            elif msg_type == 'photo' and message.get('file_id'):
                                sent_msg = await bot.send_photo(chat_id=group['chat_id'], photo=message['file_id'], caption=message.get('caption'))
                            elif msg_type == 'video' and message.get('file_id'):
                                sent_msg = await bot.send_video(chat_id=group['chat_id'], video=message['file_id'], caption=message.get('caption'))
                            elif msg_type == 'document' and message.get('file_id'):
                                sent_msg = await bot.send_document(chat_id=group['chat_id'], document=message['file_id'], caption=message.get('caption'))
                            else:
                                # 回退：未知类型或缺少 file_id 时按原消息复制
                                sent_msg = await bot.copy_message(chat_id=group['chat_id'], from_chat_id=message['chat_id'], message_id=message['message_id'])
                        except Exception:
                            # 回退：若发送接口异常，尝试复制原消息
                            sent_msg = await bot.copy_message(chat_id=group['chat_id'], from_chat_id=message['chat_id'], message_id=message['message_id'])
                        record = {'chat_id': group['chat_id'], 'message_id': sent_msg.message_id, 'user_id': user_id, 'timestamp': batch_ts}
                        group_sent_messages.append(record)
                        await _persist_sent_messages(user_id, [record])
                        group_success += 1
                elif len(media_messages) == 1:
                    msg = media_messages[0]
                    caption_parts = []
                    if msg.get('caption'):
                        caption_parts.append(msg.get('caption'))
                    if text_messages:
                        caption_parts.append("\n\n".join([t.get('text', '') for t in text_messages if t.get('text')]))
                    final_caption = "\n\n".join([p for p in caption_parts if p]) if caption_parts else None
                    if msg['type'] == 'photo':
                        sent_msg = await bot.send_photo(chat_id=group['chat_id'], photo=msg['file_id'], caption=final_caption)
                    elif msg['type'] == 'video':
                        sent_msg = await bot.send_video(chat_id=group['chat_id'], video=msg['file_id'], caption=final_caption)
                    elif msg['type'] == 'document':
                        sent_msg = await bot.send_document(chat_id=group['chat_id'], document=msg['file_id'], caption=final_caption)
                    else:
                        sent_msg = await bot.copy_message(chat_id=group['chat_id'], from_chat_id=msg['chat_id'], message_id=msg['message_id'])
                    record = {'chat_id': group['chat_id'], 'message_id': sent_msg.message_id, 'user_id': user_id, 'timestamp': batch_ts}
                    group_sent_messages.append(record)
                    await _persist_sent_messages(user_id, [record])
                    group_success += 1
                else:
                    for i, message in enumerate(messages):
                        if i > 0:
                            await rate_limiter.wait_if_needed(group['chat_id'])
                        # 合并模式但不满足媒体条件时，逐条按类型发送
                        msg_type = message.get('type')
                        sent_msg = None
                        try:
                            if msg_type == 'text':
                                text = (message.get('text') or message.get('content') or '')
                                if text:
                                    sent_msg = await bot.send_message(chat_id=group['chat_id'], text=text)
                                else:
                                    sent_msg = await bot.copy_message(chat_id=group['chat_id'], from_chat_id=message['chat_id'], message_id=message['message_id'])
                            elif msg_type == 'photo' and message.get('file_id'):
                                sent_msg = await bot.send_photo(chat_id=group['chat_id'], photo=message['file_id'], caption=message.get('caption'))
                            elif msg_type == 'video' and message.get('file_id'):
                                sent_msg = await bot.send_video(chat_id=group['chat_id'], video=message['file_id'], caption=message.get('caption'))
                            elif msg_type == 'document' and message.get('file_id'):
                                sent_msg = await bot.send_document(chat_id=group['chat_id'], document=message['file_id'], caption=message.get('caption'))
                            else:
                                sent_msg = await bot.copy_message(chat_id=group['chat_id'], from_chat_id=message['chat_id'], message_id=message['message_id'])
                        except Exception:
                            sent_msg = await bot.copy_message(chat_id=group['chat_id'], from_chat_id=message['chat_id'], message_id=message['message_id'])
                        record = {'chat_id': group['chat_id'], 'message_id': sent_msg.message_id, 'user_id': user_id, 'timestamp': batch_ts}
                        group_sent_messages.append(record)
                        await _persist_sent_messages(user_id, [record])
                        group_success += 1
        except Exception as e:
            group_failed += len(messages)
            error_msg = str(e)
            logger.error(f"发送到群组 {group['chat_id']} 失败: {error_msg}")
            group_error = {
                'name': group.get('title', f"群组 {group['chat_id']}") if group else str(group.get('chat_id')),
                'id': group.get('chat_id'),
                'reason': error_msg
            }
            if group.get('alias'):
                group_error['name'] = f"{group['alias']} ({group_error['name']})"
            return {'success': group_success, 'failed': group_failed, 'sent_messages': group_sent_messages, 'error': group_error}
        return {'success': group_success, 'failed': group_failed, 'sent_messages': group_sent_messages, 'error': None}

    tasks = [send_to_group(g) for g in target_groups]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    failed_groups = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            failed_count += 1
            group = target_groups[i]
            group_name = group.get('title', f"群组 {group['chat_id']}")
            if group.get('alias'):
                group_name = f"{group['alias']} ({group_name})"
            failed_groups.append({'name': group_name, 'id': group['chat_id'], 'reason': str(result)})
            logger.error(f"发送到群组 {group['chat_id']} 时发生异常: {result}")
        else:
            if result['success'] > 0:
                success_count += 1
                total_messages_sent += result['success']
                sent_messages.extend(result['sent_messages'])
            else:
                failed_count += 1
            if result['error']:
                failed_groups.append(result['error'])

    groups_detail = []
    for i, group in enumerate(target_groups):
        group_name = group.get('title', f"群组 {group['chat_id']}")
        if group.get('alias'):
            group_name = f"{group['alias']} ({group_name})"
        detail = {
            'group_id': group['chat_id'],
            'group_name': group_name,
            'message_count': len(messages),
            'sent_count': 0,
            'failed_count': 0,
            'status': 'unknown'
        }
        try:
            result = results[i]
            if isinstance(result, Exception):
                detail['error_reason'] = str(result)
                detail['status'] = 'failed'
                detail['failed_count'] = len(messages)
            else:
                detail['sent_count'] = result.get('success', 0)
                detail['failed_count'] = result.get('failed', 0)
                detail['status'] = 'success' if detail['sent_count'] > 0 and not result.get('error') else 'failed'
                if result.get('error'):
                    err = result['error']
                    detail['error_reason'] = err.get('reason') if isinstance(err, dict) else str(err)
        except Exception:
            pass
        groups_detail.append(detail)

    # 记录日志（botlog）
    try:
        # 统一耗时计算（只计算一次并复用）
        elapsed_seconds = time.perf_counter() - start_time
        elapsed_str = format_elapsed(elapsed_seconds)
        # 记录完成时间戳（用于 batch_timestamp ）
        completed_dt = datetime.datetime.now()
        completed_iso = completed_dt.isoformat()
        payload = {
            'time': start_str,
            'timestamp': start_iso,
            'target_description': target_description,
            'target_count': len(target_groups),
            'message_count': len(messages),
            'messages_total': len(messages),
            'message_total': len(messages) * len(target_groups),
            'message_summary': summarize_messages_for_log(messages),
            'message_types': message_types,
            'success_count': success_count,
            'failed_count': failed_count,
            'total_messages_sent': total_messages_sent,
            'groups_detail': groups_detail,
            'failed_groups': failed_groups,
            'batch_timestamp': completed_iso,
            'origin': origin,
            'elapsed': elapsed_str,
            'elapsed_seconds': round(elapsed_seconds, 3),
            'user_id': user_id,
            'username': created_by_username
        }
        if scheduled_time:
            payload['scheduled_time_utc'] = scheduled_time
        if countdown_seconds is not None:
            payload['countdown_seconds'] = countdown_seconds
        # 记录关联的任务ID，并补齐用户名（如果可用）
        try:
            if task_id:
                payload['task_id'] = task_id
            # 若用户名为空，使用 user_id 字符串兜底
            if not payload.get('username'):
                payload['username'] = str(user_id) if user_id is not None else None
        except Exception:
            pass
        event_action = 'schedule_send' if origin == 'scheduled' else ('timer_send' if origin == 'timer' else 'send')
        append_botlog(None, event_action, payload)
    except Exception as e:
        logger.error(f"记录botlog失败: {e}")

    # 记录用户友好日志
    logs = load_logs()
    logs.append({
        'timestamp': datetime.datetime.now().isoformat(),
        'target_description': target_description,
        'target_count': len(target_groups),
        'message_count': len(messages),
        'message_types': message_types,
        'success_count': success_count,
        'failed_count': failed_count,
        'total_messages_sent': total_messages_sent,
        'content_preview': f"多消息群发 ({len(messages)}条消息)"
    })
    save_logs(logs)

    logger.info(f"定时/倒计时发送完成：成功群组 {success_count}，失败群组 {failed_count}，耗时 {elapsed_str}")
    return {
        'success_count': success_count,
        'failed_count': failed_count,
        'total_messages_sent': total_messages_sent,
        'elapsed_str': elapsed_str,
        'elapsed_seconds': round(elapsed_seconds, 3),
        'target_count': len(target_groups),
        'failed_groups': failed_groups,
        'groups_detail': groups_detail
    }

# 主函数
async def main():
    # 初始化数据文件
    init_data_files()
    try:
        logger.info(f"启动持久化配置: DATA_DIR={DATA_DIR}, Firestore={'启用' if firestore_enabled() else '未启用'}, project_id={os.getenv('GOOGLE_CLOUD_PROJECT') or os.getenv('GCLOUD_PROJECT')}, tmp_root={os.getenv('CLOUD_RUN_TMP_DIR','/tmp')}")
    except Exception:
        pass

    # 后台预热：异步加载已发送消息持久化数据并清理过期记录，避免阻塞启动
    global user_sent_messages
    async def _warm_sent_messages():
        try:
            loaded = load_sent_messages()
            # 将键规范为整数，兼容JSON字符串键
            tmp = {}
            for k, v in loaded.items():
                try:
                    ik = int(k)
                except Exception:
                    ik = k
                tmp[ik] = v
            prune_sent_messages(tmp)
            # 原子替换全局变量
            user_sent_messages = tmp
            # 保存清理后的结果，避免文件增长
            save_sent_messages(user_sent_messages)
            logger.info(f"后台预热 sent_messages 完成，users={len(user_sent_messages)}")
        except Exception as e:
            logger.error(f"后台预热 sent_messages 失败: {e}")
    # 启动后台任务
    try:
        asyncio.create_task(_warm_sent_messages())
    except Exception:
        # 兼容非事件循环上下文：延迟在应用启动后创建任务
        pass

    # 创建应用
    application = Application.builder().token(BOT_TOKEN).build()
    # 记录启动时间用于 /status 运行时长（使用 bot_data 存储）
    application.bot_data['start_time'] = datetime.datetime.now(datetime.timezone.utc)
    
    # 添加命令处理器
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("listgroups", list_groups))
    application.add_handler(CommandHandler("addgroup", add_group))
    application.add_handler(CommandHandler("listcategories", list_categories))
    application.add_handler(CommandHandler("addcategory", add_category))
    application.add_handler(CommandHandler("editcategory", edit_category))
    application.add_handler(CommandHandler("removecategory", remove_category))
    application.add_handler(CommandHandler("assign", assign_category))
    application.add_handler(CommandHandler("rename", rename_group))
    application.add_handler(CommandHandler("export", export_groups))
    application.add_handler(CommandHandler("logs", view_logs))
    application.add_handler(CommandHandler("botlog", view_botlog))
    application.add_handler(CommandHandler("exportbotlog", export_botlog_json))
    application.add_handler(CommandHandler("export_botlog", export_botlog))
    # 别名（不带下划线）
    application.add_handler(CommandHandler("exportbotlog", export_botlog))
    application.add_handler(CommandHandler("exportbotlogjson", export_botlog_json))
    application.add_handler(CommandHandler("whitelist", manage_whitelist))
    application.add_handler(CommandHandler("blacklist", manage_blacklist))
    application.add_handler(CommandHandler("delete", delete_messages))
    # 临时诊断：列出 Firestore 中以 prefix 开头的 sent_messages 文档（仅管理员可用）
    async def diag_sent_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            uid = update.effective_user.id
            if uid not in ADMIN_IDS:
                await update.message.reply_text('仅管理员可用')
                return
            arg = None
            if context.args:
                arg = context.args[0]
            prefix = arg or 'sent_messages_'
            from db import list_document_ids_with_prefix
            found = list_document_ids_with_prefix(prefix)
            if found:
                await update.message.reply_text(f'Found {len(found)} docs with prefix "{prefix}":\n' + '\n'.join(found))
            else:
                await update.message.reply_text(f'No docs with prefix "{prefix}"')
        except Exception as e:
            await update.message.reply_text(f'Diag failed: {e}')
    application.add_handler(CommandHandler("diag_sent", diag_sent_messages))
    application.add_handler(CommandHandler("cancel", cancel))
    application.add_handler(CommandHandler("tasks", list_tasks))
    application.add_handler(CommandHandler("list_tasks", list_tasks))
    application.add_handler(CommandHandler("cancel_task", cancel_task))
    application.add_handler(CommandHandler("status", status_command))
    
    # 添加/bot命令处理器 - 任何人都可在群组中使用
    application.add_handler(CommandHandler("bot", bot_command))
    
    # 添加按钮回调处理器 - 只处理特定的回调数据
    application.add_handler(CallbackQueryHandler(button_callback, pattern="^(demo_link|api_doc|game_list|game_resource)$"))
    
    # 群发消息的会话处理 - 必须在全局回调处理器之前添加
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('send', start_send)],
        states={
            SELECTING_TARGET: [CallbackQueryHandler(select_target, pattern="^(target_all|target_category_|target_specific|select_group_|back_to_target|cancel).*$")],
            SENDING_MESSAGE: [MessageHandler(filters.TEXT | filters.PHOTO | filters.VIDEO | filters.Document.ALL, receive_message)],
            CONFIRMING: [
                CallbackQueryHandler(handle_add_more, pattern="^(add_more|finish_adding|cancel)$"),
                CallbackQueryHandler(confirm_send, pattern="^(confirm_send|confirm_send_merge|confirm_send_separate|cancel)$")
            ]
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )
    application.add_handler(conv_handler)

    # 定时/倒计时群发的会话处理
    schedule_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler('schedule_send', start_schedule_send),
            CommandHandler('timer_send', start_timer_send)
        ],
        states={
            SELECTING_TARGET: [CallbackQueryHandler(select_target, pattern="^(target_all|target_category_|target_specific|select_group_|back_to_target|cancel).*$")],
            SENDING_MESSAGE: [MessageHandler(filters.TEXT | filters.PHOTO | filters.VIDEO | filters.Document.ALL, receive_message)],
            CONFIRMING: [
                CallbackQueryHandler(handle_add_more_schedule, pattern="^(add_more|finish_adding|cancel)$"),
                CallbackQueryHandler(confirm_create_schedule_timer, pattern="^(confirm_create_schedule|confirm_create_timer|cancel)$"),
                CallbackQueryHandler(handle_block_confirm_send_in_schedule, pattern="^(confirm_send|confirm_send_merge|confirm_send_separate)$")
            ],
            SCHEDULE_SET_TIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, input_schedule_time)],
            TIMER_SET_COUNTDOWN: [MessageHandler(filters.TEXT & ~filters.COMMAND, input_timer_countdown)],
            SCHEDULE_CONFIRM_CREATE: [
                CallbackQueryHandler(confirm_create_schedule_timer, pattern="^(confirm_create_schedule|confirm_create_timer|cancel)$")
            ]
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )
    application.add_handler(schedule_conv_handler)

    # 仅@机器人触发按钮：监听群文本消息（需放在会话处理器之后，避免拦截定时/倒计时输入）
    application.add_handler(MessageHandler(filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND, handle_bot_mention))
    
    # 添加回调查询处理器 - 必须在 ConversationHandler 之后添加
    application.add_handler(CallbackQueryHandler(handle_remove_category_callback, pattern="^(remove_from_group|delete_category)$"))
    application.add_handler(CallbackQueryHandler(handle_whitelist_callback, pattern="^whitelist_"))
    application.add_handler(CallbackQueryHandler(handle_blacklist_callback, pattern="^blacklist_"))
    application.add_handler(CallbackQueryHandler(
        handle_delete_callback,
        pattern="^(delete_cancel|back_to_main|show_detailed_list|quick_delete_all_|quick_delete_select_|toggle_group_|select_all_groups_|delete_selected_|page_|search_page_|group_page_|preview_selected_|preview_batch_|search_by_group|delete_search_|preview_search_|toggle_batch_|delete_selected_batches|preview_selected_batches).*"
    ))
    
    # 添加消息处理器来处理用户的后续输入（仅私聊），避免拦截群消息
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, handle_text_input))
    
    # 处理群组事件
    application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, group_handler))
    application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_TITLE, handle_chat_title_update))
    # 监听机器人自身成员状态变化（被踢/离开）
    application.add_handler(ChatMemberHandler(handle_my_chat_member_update, ChatMemberHandler.MY_CHAT_MEMBER))
    # 兜底：服务消息中的机器人离群
    application.add_handler(MessageHandler(filters.StatusUpdate.LEFT_CHAT_MEMBER, handle_bot_left_member))
    
    # 添加通用群组消息处理器来检测机器人在群组中的存在
    application.add_handler(MessageHandler(filters.ChatType.GROUPS & ~filters.COMMAND, detect_group_presence))
    
    # 全局错误处理器：记录所有未捕获错误
    application.add_error_handler(handle_global_error)
    
    # 启动机器人
    await application.initialize()
    await application.start()
    await application.updater.start_polling()

    # 启动时恢复未完成任务，并启动后台监控防止遗漏
    async def _compute_task_delay_for_resume(task: dict, now_utc: datetime.datetime) -> float:
        try:
            if task.get('type') == 'schedule':
                dt_utc = datetime.datetime.fromisoformat(task['schedule_time_utc'])
                if dt_utc.tzinfo is None:
                    dt_utc = dt_utc.replace(tzinfo=datetime.timezone.utc)
                return (dt_utc - now_utc).total_seconds()
            elif task.get('type') == 'timer':
                exec_at = task.get('execute_time_utc')
                if exec_at:
                    dt_utc = datetime.datetime.fromisoformat(exec_at)
                    if dt_utc.tzinfo is None:
                        dt_utc = dt_utc.replace(tzinfo=datetime.timezone.utc)
                    return (dt_utc - now_utc).total_seconds()
                # 兼容旧任务：使用 created_at(+tz) + countdown_seconds
                cs = int(task.get('countdown_seconds', 0))
                created_str = task.get('created_at_utc') or task.get('created_at')
                if created_str:
                    cdt = datetime.datetime.fromisoformat(created_str)
                    if cdt.tzinfo is None:
                        if task.get('created_at_utc'):
                            cdt = cdt.replace(tzinfo=datetime.timezone.utc)
                        else:
                            tz8 = datetime.timezone(datetime.timedelta(hours=8))
                            cdt = cdt.replace(tzinfo=tz8)
                    cdt_utc = cdt.astimezone(datetime.timezone.utc)
                    elapsed = (now_utc - cdt_utc).total_seconds()
                    return cs - elapsed
                return cs
        except Exception:
            return 0

    async def resume_pending_tasks_once():
        try:
            pending_tasks = load_scheduled_tasks()
            started_ids = application.bot_data.get('running_task_ids')
            if started_ids is None:
                started_ids = set()
                application.bot_data['running_task_ids'] = started_ids
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            resumed = 0
            for t in pending_tasks:
                if t.get('status') != 'scheduled':
                    continue
                tid = t.get('id')
                if tid in started_ids:
                    continue
                try:
                    delay = await _compute_task_delay_for_resume(t, now_utc)
                except Exception:
                    delay = None
                # 记录恢复计划到 botlog，便于排查
                try:
                    append_botlog(None, 'task_resume', {
                        'task_id': tid,
                        'type': t.get('type'),
                        'expected_delay_seconds': round(delay, 3) if isinstance(delay, (int, float)) else None,
                        'execute_time_utc': t.get('schedule_time_utc') or t.get('execute_time_utc'),
                        'origin': 'startup'
                    })
                except Exception:
                    pass
                try:
                    handles = application.bot_data.setdefault('task_handles', {})
                    handles[tid] = asyncio.create_task(_execute_scheduled_task(application, t))
                except Exception:
                    asyncio.create_task(_execute_scheduled_task(application, t))
                started_ids.add(tid)
                resumed += 1
            if resumed:
                logger.info(f"恢复调度任务 {resumed} 个")
        except Exception as e:
            logger.error(f"启动定时任务失败: {e}")

    async def monitor_pending_tasks():
        while True:
            try:
                await resume_pending_tasks_once()
            except Exception:
                pass
            await asyncio.sleep(10)

    # 首次恢复与启动后台监控
    await resume_pending_tasks_once()
    asyncio.create_task(monitor_pending_tasks())

    # 健康检查服务已在 main() 开头启动，避免重复启动

    logger.info("Bot started")

    # 保持运行直到按下Ctrl-C
    print("机器人已启动！按Ctrl+C停止运行。")
    # 在最新版本中使用asyncio.Event()来模拟idle功能
    stop_signal = asyncio.Event()
    await stop_signal.wait()

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
