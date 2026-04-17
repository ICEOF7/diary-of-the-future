# https://t.me/lamabio

import logging
import os
import sqlite3
import calendar
import tempfile
import random
import csv
import json
from datetime import datetime, date, timedelta
from typing import List, Tuple, Optional, Dict, Any
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters
from openai import OpenAI
load_dotenv()
BOT_TOKEN = os.getenv('BOT_TOKEN')
if not BOT_TOKEN:
    raise RuntimeError('❌ BOT_TOKEN не найден в .env')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
if not OPENAI_API_KEY:
    raise RuntimeError('❌ OPENAI_API_KEY не найден в .env')
client = OpenAI(api_key=OPENAI_API_KEY)
DB_PATH = 'diary.db'
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger('LamaDiary')
REQUIRED_CHANNELS = ['@lamabio', '@lLamaInCraft']
ADMIN_IDS = os.getenv('ADMIN_IDS', '')
ADMINS = {int(x) for x in ADMIN_IDS.split(',') if x.strip().isdigit()}

def is_admin(chat_id: int) -> bool:
    return chat_id in ADMINS

async def check_subscription_raw(user_id: int, bot) -> bool:
    for channel in REQUIRED_CHANNELS:
        try:
            member = await bot.get_chat_member(chat_id=channel, user_id=user_id)
            if member.status in ('left', 'kicked'):
                return False
        except Exception as e:
            logger.error(f'check_subscription_raw error for {channel}: {e}')
            return False
    return True

async def ensure_subscribed(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat:
        return False
    if context.user_data.get('sub_ok'):
        return True
    ok = await check_subscription_raw(user.id, context.bot)
    if ok:
        context.user_data['sub_ok'] = True
        return True
    text = 'Чтобы пользоваться *Дневником будущего*, нужно подписаться на эти каналы:\n\n• @lamabio\n• @lLamaInCraft\n\nПосле подписки вернись в бота и нажми кнопку ниже 👇'
    kb = InlineKeyboardMarkup([[InlineKeyboardButton('📢 Открыть @lamabio', url='https://t.me/lamabio')], [InlineKeyboardButton('📢 Открыть @lLamaInCraft', url='https://t.me/lLamaInCraft')], [InlineKeyboardButton('✅ Проверить подписку', callback_data='check_sub')]])
    if update.callback_query:
        await update.callback_query.message.reply_text(text, parse_mode='Markdown', reply_markup=kb)
    else:
        await chat.send_message(text, parse_mode='Markdown', reply_markup=kb)
    return False

def now() -> datetime:
    return datetime.now()

def today_date() -> date:
    return datetime.now().date()

def format_dt(dt: datetime) -> str:
    return dt.strftime('%d.%m.%Y %H:%M')

def format_d(dt: datetime) -> str:
    return dt.strftime('%d.%m.%Y')

def format_delta(target: datetime, base: Optional[datetime]=None) -> str:
    if base is None:
        base = now()
    seconds = int((target - base).total_seconds())
    sign = 1
    if seconds < 0:
        sign = -1
        seconds = -seconds
    days = seconds // 86400
    hours = seconds % 86400 // 3600
    minutes = seconds % 3600 // 60
    parts = []
    if days:
        parts.append(f'{days}д')
    if hours:
        parts.append(f'{hours}ч')
    if minutes or not parts:
        parts.append(f'{minutes}м')
    s = ' '.join(parts)
    if sign > 0:
        return f'осталось ~ {s}'
    else:
        return f'просрочено на ~ {s}'

def get_conn():
    return sqlite3.connect(DB_PATH)

def init_db() -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('\n        CREATE TABLE IF NOT EXISTS users (\n            chat_id INTEGER PRIMARY KEY,\n            username TEXT,\n            first_name TEXT,\n            last_name TEXT,\n            joined_at TEXT NOT NULL,\n            last_active_at TEXT NOT NULL\n        )\n        ')
    cur.execute('\n        CREATE TABLE IF NOT EXISTS reminders (\n            id INTEGER PRIMARY KEY AUTOINCREMENT,\n            chat_id INTEGER NOT NULL,\n            text TEXT NOT NULL,\n            remind_at TEXT NOT NULL,\n            sent INTEGER NOT NULL DEFAULT 0,\n            created_at TEXT NOT NULL,\n            ai_comment TEXT\n        )\n        ')
    for alter in ["ALTER TABLE reminders ADD COLUMN kind TEXT NOT NULL DEFAULT 'reminder'", 'ALTER TABLE reminders ADD COLUMN priority INTEGER NOT NULL DEFAULT 2', 'ALTER TABLE reminders ADD COLUMN tags TEXT', 'ALTER TABLE reminders ADD COLUMN done INTEGER NOT NULL DEFAULT 0', 'ALTER TABLE reminders ADD COLUMN done_at TEXT', 'ALTER TABLE reminders ADD COLUMN attachments TEXT']:
        try:
            cur.execute(alter)
        except sqlite3.OperationalError:
            pass
    cur.execute('\n        CREATE TABLE IF NOT EXISTS goals (\n            id INTEGER PRIMARY KEY AUTOINCREMENT,\n            chat_id INTEGER NOT NULL,\n            text TEXT NOT NULL,\n            deadline TEXT NOT NULL,\n            done INTEGER NOT NULL DEFAULT 0,\n            created_at TEXT NOT NULL,\n            done_at TEXT\n        )\n        ')
    for alter in ['ALTER TABLE goals ADD COLUMN overdue_notified INTEGER NOT NULL DEFAULT 0', 'ALTER TABLE goals ADD COLUMN priority INTEGER NOT NULL DEFAULT 2', 'ALTER TABLE goals ADD COLUMN tags TEXT']:
        try:
            cur.execute(alter)
        except sqlite3.OperationalError:
            pass
    cur.execute('\n        CREATE TABLE IF NOT EXISTS habits (\n            id INTEGER PRIMARY KEY AUTOINCREMENT,\n            chat_id INTEGER NOT NULL,\n            name TEXT NOT NULL,\n            created_at TEXT NOT NULL,\n            active INTEGER NOT NULL DEFAULT 1\n        )\n        ')
    cur.execute('\n        CREATE TABLE IF NOT EXISTS habit_logs (\n            id INTEGER PRIMARY KEY AUTOINCREMENT,\n            habit_id INTEGER NOT NULL,\n            date TEXT NOT NULL,\n            done INTEGER NOT NULL DEFAULT 1\n        )\n        ')
    cur.execute('\n        CREATE TABLE IF NOT EXISTS user_settings (\n            chat_id INTEGER PRIMARY KEY,\n            ai_comments INTEGER NOT NULL DEFAULT 1,\n            panic_notifications INTEGER NOT NULL DEFAULT 1,\n            weekly_review INTEGER NOT NULL DEFAULT 1\n        )\n        ')
    try:
        cur.execute('ALTER TABLE user_settings ADD COLUMN last_nudge_at TEXT')
    except sqlite3.OperationalError:
        pass
    cur.execute('\n        CREATE TABLE IF NOT EXISTS journal_entries (\n            id INTEGER PRIMARY KEY AUTOINCREMENT,\n            chat_id INTEGER NOT NULL,\n            created_at TEXT NOT NULL,\n            text TEXT NOT NULL\n        )\n        ')
    try:
        cur.execute('ALTER TABLE journal_entries ADD COLUMN attachments TEXT')
    except sqlite3.OperationalError:
        pass
    cur.execute('\n        CREATE TABLE IF NOT EXISTS daily_activity (\n            id INTEGER PRIMARY KEY AUTOINCREMENT,\n            chat_id INTEGER NOT NULL,\n            date TEXT NOT NULL,\n            tasks_done INTEGER NOT NULL DEFAULT 0,\n            goals_done INTEGER NOT NULL DEFAULT 0,\n            habits_done INTEGER NOT NULL DEFAULT 0,\n            journal_entries INTEGER NOT NULL DEFAULT 0\n        )\n        ')
    for alter in ['ALTER TABLE daily_activity ADD COLUMN meta_actions INTEGER NOT NULL DEFAULT 0', 'ALTER TABLE daily_activity ADD COLUMN day_review_sent INTEGER NOT NULL DEFAULT 0']:
        try:
            cur.execute(alter)
        except sqlite3.OperationalError:
            pass
    cur.execute('\n        CREATE TABLE IF NOT EXISTS audit_log (\n            id INTEGER PRIMARY KEY AUTOINCREMENT,\n            chat_id INTEGER NOT NULL,\n            created_at TEXT NOT NULL,\n            action TEXT NOT NULL,\n            payload TEXT\n        )\n        ')
    conn.commit()
    conn.close()

def touch_user_from_update(update: Update) -> None:
    try:
        chat = update.effective_chat
        user = update.effective_user
        if not chat:
            return
        chat_id = chat.id
        username = user.username if user else None
        first_name = user.first_name if user else None
        last_name = user.last_name if user else None
        now_iso = now().isoformat()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute('SELECT joined_at FROM users WHERE chat_id = ?', (chat_id,))
        row = cur.fetchone()
        if row:
            cur.execute('\n                UPDATE users\n                SET username = ?, first_name = ?, last_name = ?, last_active_at = ?\n                WHERE chat_id = ?\n                ', (username, first_name, last_name, now_iso, chat_id))
        else:
            cur.execute('\n                INSERT INTO users (chat_id, username, first_name, last_name, joined_at, last_active_at)\n                VALUES (?, ?, ?, ?, ?, ?)\n                ', (chat_id, username, first_name, last_name, now_iso, now_iso))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f'touch_user_from_update error: {e}')

def get_users_page(sort: str, page: int, page_size: int=50) -> Tuple[List[Tuple[Any, ...]], int, int, int]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM users')
    total = cur.fetchone()[0] or 0
    if total == 0:
        conn.close()
        return ([], 0, 0, 1)
    total_pages = (total + page_size - 1) // page_size
    current_page = max(1, min(page, total_pages))
    offset = (current_page - 1) * page_size
    if sort == 'name':
        order_by = "COALESCE(first_name,'') || ' ' || COALESCE(last_name,'') || ' ' || COALESCE(username,'') COLLATE NOCASE"
    elif sort == 'active':
        order_by = 'datetime(last_active_at) DESC'
    else:
        order_by = 'datetime(joined_at) ASC'
    cur.execute(f'\n        SELECT chat_id, username, first_name, last_name, joined_at, last_active_at\n        FROM users\n        ORDER BY {order_by}\n        LIMIT ? OFFSET ?\n        ', (page_size, offset))
    rows = cur.fetchall()
    conn.close()
    return (rows, total, total_pages, current_page)

def get_all_users() -> List[Tuple[Any, ...]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('\n        SELECT chat_id, username, first_name, last_name, joined_at, last_active_at\n        FROM users\n        ORDER BY datetime(joined_at) ASC\n        ')
    rows = cur.fetchall()
    conn.close()
    return rows

def log_audit(chat_id: int, action: str, payload: str='') -> None:
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute('\n            INSERT INTO audit_log (chat_id, created_at, action, payload)\n            VALUES (?, ?, ?, ?)\n            ', (chat_id, now().isoformat(), (action or '')[:100], (payload or '')[:1000]))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f'audit error: {e}')

def get_audit_entries(chat_id: int, limit: int=30) -> List[Tuple[str, str, str]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('\n        SELECT created_at, action, payload\n        FROM audit_log\n        WHERE chat_id = ?\n        ORDER BY datetime(created_at) DESC\n        LIMIT ?\n        ', (chat_id, limit))
    rows = cur.fetchall()
    conn.close()
    return rows

def humanize_action(action: str) -> str:
    mapping = {'start': 'Запуск бота', 'help': 'Открыта помощь', 'export': 'Экспорт данных', 'plan': 'Построен план', 'week_review': 'Еженедельный обзор', 'create_reminder': 'Создано напоминание', 'create_task': 'Создана задача', 'create_todo': 'Создано дело без срока', 'create_goal': 'Создана цель', 'complete_task': 'Выполнено дело', 'complete_goal': 'Выполнена цель', 'delete_task': 'Удалено дело', 'snooze_task': 'Перенесено напоминание', 'create_habit': 'Создана привычка', 'habit_done': 'Привычка отмечена', 'journal_entry': 'Запись в дневник', 'set_toggle_ai_comments': 'Смена настройки: ИИ-комментарии', 'set_toggle_panic_notifications': 'Смена настройки: мягкая паника', 'set_toggle_weekly_review': 'Смена настройки: еженедельный обзор', 'auto_reminder_sent': 'Отправлено напоминание', 'goal_overdue_notify': 'Уведомление о просроченной цели', 'stats': 'Просмотр статистики', 'search': 'Поиск'}
    return mapping.get(action, action)

def ensure_user_settings(chat_id: int) -> Dict[str, int]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('SELECT ai_comments, panic_notifications, weekly_review FROM user_settings WHERE chat_id = ?', (chat_id,))
    row = cur.fetchone()
    if row:
        conn.close()
        return {'ai_comments': row[0], 'panic_notifications': row[1], 'weekly_review': row[2]}
    cur.execute('INSERT INTO user_settings (chat_id, ai_comments, panic_notifications, weekly_review) VALUES (?, 1, 1, 1)', (chat_id,))
    conn.commit()
    conn.close()
    return {'ai_comments': 1, 'panic_notifications': 1, 'weekly_review': 1}

def get_user_settings(chat_id: int) -> Dict[str, int]:
    return ensure_user_settings(chat_id)

def set_user_setting(chat_id: int, field: str, value: int) -> None:
    if field not in ('ai_comments', 'panic_notifications', 'weekly_review'):
        return
    conn = get_conn()
    cur = conn.cursor()
    ensure_user_settings(chat_id)
    cur.execute(f'UPDATE user_settings SET {field} = ? WHERE chat_id = ?', (value, chat_id))
    conn.commit()
    conn.close()

def get_last_nudge(chat_id: int) -> Optional[date]:
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute('SELECT last_nudge_at FROM user_settings WHERE chat_id = ?', (chat_id,))
        row = cur.fetchone()
    except sqlite3.OperationalError:
        conn.close()
        return None
    conn.close()
    if row and row[0]:
        try:
            return datetime.fromisoformat(row[0]).date()
        except Exception:
            return None
    return None

def set_last_nudge(chat_id: int) -> None:
    d_str = today_date().isoformat()
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute('UPDATE user_settings SET last_nudge_at = ? WHERE chat_id = ?', (d_str, chat_id))
        conn.commit()
    except sqlite3.OperationalError:
        pass
    conn.close()
MOTIVATION_SNIPPETS = ['Маленький шаг сегодня лучше идеального шага завтра 💪', 'Ты двигаешься вперёд, даже если сейчас так не кажется.', 'Главное — не остановиться. Остальное догоним 🚀', 'Каждое выполненное дело — плюс к твоей будущей версии 🔥', 'Сейчас ты стал(а) чуточку сильнее, чем был(а) час назад.']

def get_motivation() -> str:
    return random.choice(MOTIVATION_SNIPPETS)

def extract_attachments_from_message(msg) -> Optional[str]:
    attachments = []
    if msg.photo:
        file_id = msg.photo[-1].file_id
        attachments.append({'t': 'photo', 'id': file_id})
    if msg.video:
        attachments.append({'t': 'video', 'id': msg.video.file_id})
    if msg.document:
        attachments.append({'t': 'document', 'id': msg.document.file_id})
    if msg.audio:
        attachments.append({'t': 'audio', 'id': msg.audio.file_id})
    if msg.voice:
        attachments.append({'t': 'voice', 'id': msg.voice.file_id})
    if msg.video_note:
        attachments.append({'t': 'video_note', 'id': msg.video_note.file_id})
    if msg.animation:
        attachments.append({'t': 'animation', 'id': msg.animation.file_id})
    return json.dumps(attachments, ensure_ascii=False) if attachments else None

def parse_attachments(attachments_str: Optional[str]) -> List[Dict[str, str]]:
    if not attachments_str:
        return []
    try:
        data = json.loads(attachments_str)
        if isinstance(data, list):
            return data
        return []
    except Exception:
        return []

async def send_with_attachments(bot, chat_id: int, attachments: List[Dict[str, str]], caption: str, reply_markup=None, parse_mode: Optional[str]='Markdown'):
    if not attachments:
        return await bot.send_message(chat_id, caption, reply_markup=reply_markup, parse_mode=parse_mode)
    att = attachments[0]
    t = att.get('t')
    fid = att.get('id')
    try:
        if t == 'photo':
            return await bot.send_photo(chat_id, fid, caption=caption, reply_markup=reply_markup, parse_mode=parse_mode)
        if t == 'video':
            return await bot.send_video(chat_id, fid, caption=caption, reply_markup=reply_markup, parse_mode=parse_mode)
        if t == 'document':
            return await bot.send_document(chat_id, fid, caption=caption, reply_markup=reply_markup, parse_mode=parse_mode)
        if t == 'audio':
            return await bot.send_audio(chat_id, fid, caption=caption, reply_markup=reply_markup, parse_mode=parse_mode)
        if t == 'voice':
            return await bot.send_voice(chat_id, fid, caption=caption, reply_markup=reply_markup, parse_mode=parse_mode)
        if t == 'animation':
            return await bot.send_animation(chat_id, fid, caption=caption, reply_markup=reply_markup, parse_mode=parse_mode)
        if t == 'video_note':
            await bot.send_video_note(chat_id, fid)
            return await bot.send_message(chat_id, caption, reply_markup=reply_markup, parse_mode=parse_mode)
    except Exception as e:
        logger.error(f'send_with_attachments error: {e}')
    return await bot.send_message(chat_id, caption, reply_markup=reply_markup, parse_mode=parse_mode)
GOAL_TEMPLATES = [{'id': 'exam', 'title': 'Подготовиться к экзамену', 'text': 'Подготовиться к экзамену по [предмету] и сдать его на высокую оценку.', 'hint': 'Замени [предмет] на свой.'}, {'id': 'health', 'title': 'Улучшить здоровье', 'text': 'Стабильно выполнять режим: сон, питание и тренировки для улучшения самочувствия.', 'hint': 'Можно добавить вид спорта или конкретные метрики.'}, {'id': 'project', 'title': 'Запустить мини-проект', 'text': 'Сделать и запустить личный мини-проект, доведя его до первого результата.', 'hint': 'Например: пет-проект, телеграм-бот, лендинг и т.п.'}]

def ai_generate_reminder_comment(chat_id: int, text: str, remind_at: datetime) -> str:
    settings = get_user_settings(chat_id)
    if not settings.get('ai_comments', 1):
        return ''
    try:
        when_str = format_dt(remind_at)
        prompt = f'Ты — дружелюбный бот-дневник будущего. Пользователь создал напоминание. Сделай короткое (1–2 предложения) эмоциональное описание от твоего лица, как будто ты поддерживаешь и мягко мотивируешь пользователя.\n\nКогда напомнить: {when_str}\nТекст напоминания: {text}\n\nПиши по-русски, на «ты». Не используй слишком длинные фразы.'
        resp = client.chat.completions.create(model='gpt-4o-mini', messages=[{'role': 'system', 'content': 'Ты дружелюбный дневник будущего.'}, {'role': 'user', 'content': prompt}], max_tokens=120)
        return (resp.choices[0].message.content or '').strip()
    except Exception as e:
        logger.error(f'AI error (reminder): {e}')
        return ''

def ai_generate_goal_done_comment(chat_id: int, text: str, deadline: datetime, done_at: datetime) -> str:
    settings = get_user_settings(chat_id)
    if not settings.get('ai_comments', 1):
        return ''
    try:
        ddl_str = format_dt(deadline)
        done_str = format_dt(done_at)
        prompt = f'Пользователь выполнил свою цель. Сделай короткое (1–3 предложения) тёплое и мотивирующее сообщение от лица дневника, отметь, что ты гордишься прогрессом, можешь сравнить дедлайн и реальное завершение.\n\nЦель: {text}\nДедлайн: {ddl_str}\nФактическое выполнение: {done_str}\n\nПиши по-русски, на «ты».'
        resp = client.chat.completions.create(model='gpt-4o-mini', messages=[{'role': 'system', 'content': 'Ты вдохновляющий дневник будущего.'}, {'role': 'user', 'content': prompt}], max_tokens=160)
        return (resp.choices[0].message.content or '').strip()
    except Exception as e:
        logger.error(f'AI error (goal done): {e}')
        return ''

def ai_generate_plan(text: str) -> str:
    try:
        prompt = f'Пользователь хочет составить план.\nСделай подробный, но структурированный план в виде списка шагов с примерными сроками.\n\nЗапрос пользователя: {text}\n\nПиши по-русски, структурированно, с маркерами или нумерацией.'
        resp = client.chat.completions.create(model='gpt-4o-mini', messages=[{'role': 'system', 'content': 'Ты помогаешь планировать задачи и цели.'}, {'role': 'user', 'content': prompt}], max_tokens=400)
        return (resp.choices[0].message.content or '').strip()
    except Exception as e:
        logger.error(f'AI error (plan): {e}')
        return 'Не удалось составить план, попробуй ещё раз позже.'

def ai_week_review(summary: str) -> str:
    try:
        prompt = f'Сделай короткий разбор недели по продуктивности и целям.\nДай мягкую обратную связь, отметь успехи, предложи 2–3 улучшения.\n\nДанные по неделе:\n{summary}\n\nПиши по-русски, дружелюбно.'
        resp = client.chat.completions.create(model='gpt-4o-mini', messages=[{'role': 'system', 'content': 'Ты коуч по продуктивности и лайтовый психолог.'}, {'role': 'user', 'content': prompt}], max_tokens=300)
        return (resp.choices[0].message.content or '').strip()
    except Exception as e:
        logger.error(f'AI error (week review): {e}')
        return ''

def ai_day_review(summary: str) -> str:
    try:
        prompt = f'Сделай короткий разбор дня по продуктивности и целям.\nОтметь, что получилось хорошо, и предложи 1–2 идеи, как сделать завтра чуть лучше.\n\nДанные по дню:\n{summary}\n\nПиши по-русски, дружелюбно, 2–5 предложений.'
        resp = client.chat.completions.create(model='gpt-4o-mini', messages=[{'role': 'system', 'content': 'Ты коуч по продуктивности и мягкий напарник по планированию.'}, {'role': 'user', 'content': prompt}], max_tokens=280)
        return (resp.choices[0].message.content or '').strip()
    except Exception as e:
        logger.error(f'AI error (day review): {e}')
        return ''

def extract_tags(text: str) -> str:
    tags = []
    for part in text.split():
        if part.startswith('#') and len(part) > 1:
            t = part[1:].strip().strip(',.?!:;')
            if t:
                tags.append(t.lower())
    return ','.join(sorted(set(tags))) if tags else ''

def add_reminder(chat_id: int, text: str, remind_at: datetime, kind: str='reminder', priority: int=2, attachments: Optional[str]=None) -> None:
    tags = extract_tags(text)
    ai_comment = '' if kind == 'todo' else ai_generate_reminder_comment(chat_id, text, remind_at)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('\n        INSERT INTO reminders (chat_id, text, remind_at, sent, created_at, ai_comment,\n                               kind, priority, tags, done, done_at, attachments)\n        VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, 0, NULL, ?)\n        ', (chat_id, text, remind_at.isoformat(), now().isoformat(), ai_comment, kind, priority, tags, attachments))
    conn.commit()
    conn.close()

def get_pending_reminders(now_dt: datetime) -> List[Tuple[int, int, str, str, Optional[str], str, int]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("\n        SELECT id, chat_id, text, ai_comment, attachments, kind, priority\n        FROM reminders\n        WHERE sent = 0\n          AND kind != 'todo'\n          AND datetime(remind_at) <= datetime(?)\n          AND done = 0\n        ", (now_dt.isoformat(),))
    rows = cur.fetchall()
    conn.close()
    return rows

def mark_reminder_sent(reminder_id: int) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('UPDATE reminders SET sent = 1 WHERE id = ?', (reminder_id,))
    conn.commit()
    conn.close()

def get_future_reminders(chat_id: int) -> List[Tuple[int, str, str, str, str, int, Optional[str]]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("\n        SELECT id, remind_at, text, ai_comment, kind, priority, attachments\n        FROM reminders\n        WHERE chat_id = ?\n          AND sent = 0\n          AND done = 0\n          AND kind != 'todo'\n        ORDER BY datetime(remind_at) ASC\n        ", (chat_id,))
    rows = cur.fetchall()
    conn.close()
    return rows

def get_todos(chat_id: int) -> List[Tuple[int, str, int, Optional[str]]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("\n        SELECT id, text, priority, attachments\n        FROM reminders\n        WHERE chat_id = ?\n          AND kind = 'todo'\n          AND done = 0\n        ORDER BY datetime(created_at) ASC\n        ", (chat_id,))
    rows = cur.fetchall()
    conn.close()
    return rows

def delete_reminder(chat_id: int, reminder_id: int) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('DELETE FROM reminders WHERE id = ? AND chat_id = ?', (reminder_id, chat_id))
    deleted = cur.rowcount > 0
    conn.commit()
    conn.close()
    return deleted

def snooze_reminder(chat_id: int, reminder_id: int, days: int=1) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('SELECT remind_at, kind FROM reminders WHERE id = ? AND chat_id = ?', (reminder_id, chat_id))
    row = cur.fetchone()
    if not row:
        conn.close()
        return False
    old_dt, kind = row
    if kind == 'todo':
        conn.close()
        return False
    old_dt = datetime.fromisoformat(old_dt)
    new_dt = old_dt + timedelta(days=days)
    cur.execute('UPDATE reminders SET remind_at = ?, sent = 0 WHERE id = ?', (new_dt.isoformat(), reminder_id))
    conn.commit()
    conn.close()
    return True

def complete_task(chat_id: int, reminder_id: int) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('\n        UPDATE reminders\n        SET done = 1, done_at = ?\n        WHERE id = ? AND chat_id = ? AND done = 0\n        ', (now().isoformat(), reminder_id, chat_id))
    updated = cur.rowcount > 0
    conn.commit()
    conn.close()
    return updated

def add_goal(chat_id: int, text: str, deadline: datetime, priority: int=2) -> None:
    tags = extract_tags(text)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('\n        INSERT INTO goals (chat_id, text, deadline, done, created_at, done_at,\n                           overdue_notified, priority, tags)\n        VALUES (?, ?, ?, 0, ?, NULL, 0, ?, ?)\n        ', (chat_id, text, deadline.isoformat(), now().isoformat(), priority, tags))
    conn.commit()
    conn.close()

def get_goals_active(chat_id: int, now_dt: datetime) -> List[Tuple[int, str, str, int]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('\n        SELECT id, deadline, text, priority\n        FROM goals\n        WHERE chat_id = ? AND done = 0\n          AND datetime(deadline) >= datetime(?)\n        ORDER BY datetime(deadline) ASC\n        ', (chat_id, now_dt.isoformat()))
    rows = cur.fetchall()
    conn.close()
    return rows

def get_goals_overdue(chat_id: int, now_dt: datetime) -> List[Tuple[int, str, str, int]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('\n        SELECT id, deadline, text, priority\n        FROM goals\n        WHERE chat_id = ? AND done = 0\n          AND datetime(deadline) < datetime(?)\n        ORDER BY datetime(deadline) ASC\n        ', (chat_id, now_dt.isoformat()))
    rows = cur.fetchall()
    conn.close()
    return rows

def get_goals_done(chat_id: int, limit: Optional[int]=10) -> List[Tuple[int, str, str, str]]:
    conn = get_conn()
    cur = conn.cursor()
    query = '\n        SELECT id, deadline, text, done_at\n        FROM goals\n        WHERE chat_id = ? AND done = 1\n        ORDER BY datetime(done_at) DESC\n    '
    if limit is not None:
        query += ' LIMIT ?'
        cur.execute(query, (chat_id, limit))
    else:
        cur.execute(query, (chat_id,))
    rows = cur.fetchall()
    conn.close()
    return rows

def get_goals_stats(chat_id: int, now_dt: datetime) -> Tuple[int, int, int]:
    active = len(get_goals_active(chat_id, now_dt))
    overdue = len(get_goals_overdue(chat_id, now_dt))
    done_all = len(get_goals_done(chat_id, None))
    return (active, overdue, done_all)

def mark_goal_done(chat_id: int, goal_id: int) -> Optional[Tuple[str, datetime, datetime]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('\n        SELECT text, deadline FROM goals\n        WHERE id = ? AND chat_id = ? AND done = 0\n        ', (goal_id, chat_id))
    row = cur.fetchone()
    if not row:
        conn.close()
        return None
    text, ddl_str = row
    deadline_dt = datetime.fromisoformat(ddl_str)
    done_at = now()
    cur.execute('\n        UPDATE goals\n        SET done = 1, done_at = ?\n        WHERE id = ? AND chat_id = ?\n        ', (done_at.isoformat(), goal_id, chat_id))
    conn.commit()
    conn.close()
    return (text, deadline_dt, done_at)

def get_goals_for_overdue_notification(now_dt: datetime) -> List[Tuple[int, int, str, str]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('\n        SELECT id, chat_id, text, deadline\n        FROM goals\n        WHERE done = 0\n          AND datetime(deadline) <= datetime(?)\n          AND (overdue_notified IS NULL OR overdue_notified = 0)\n        ', (now_dt.isoformat(),))
    rows = cur.fetchall()
    conn.close()
    return rows

def mark_goal_overdue_notified(goal_id: int) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('UPDATE goals SET overdue_notified = 1 WHERE id = ?', (goal_id,))
    conn.commit()
    conn.close()

def add_habit(chat_id: int, name: str) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('\n        INSERT INTO habits (chat_id, name, created_at, active)\n        VALUES (?, ?, ?, 1)\n        ', (chat_id, name, now().isoformat()))
    conn.commit()
    conn.close()

def get_active_habits(chat_id: int) -> List[Tuple[int, str]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('\n        SELECT id, name FROM habits\n        WHERE chat_id = ? AND active = 1\n        ORDER BY id ASC\n        ', (chat_id,))
    rows = cur.fetchall()
    conn.close()
    return rows

def mark_habit_today(habit_id: int) -> None:
    d_str = today_date().strftime('%Y-%m-%d')
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('\n        SELECT id FROM habit_logs\n        WHERE habit_id = ? AND date = ?\n        ', (habit_id, d_str))
    row = cur.fetchone()
    if not row:
        cur.execute('\n            INSERT INTO habit_logs (habit_id, date, done)\n            VALUES (?, ?, 1)\n            ', (habit_id, d_str))
    conn.commit()
    conn.close()

def get_habit_stats(chat_id: int, days: int=7) -> str:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('SELECT id, name FROM habits WHERE chat_id = ? AND active = 1', (chat_id,))
    habits = cur.fetchall()
    if not habits:
        conn.close()
        return 'У тебя пока нет активных привычек.'
    since = (today_date() - timedelta(days=days - 1)).strftime('%Y-%m-%d')
    stats_lines = []
    for hid, name in habits:
        cur.execute('\n            SELECT date, done\n            FROM habit_logs\n            WHERE habit_id = ? AND date >= ?\n            ', (hid, since))
        logs = cur.fetchall()
        done_days = sum((1 for _, done in logs if done))
        stats_lines.append(f'• {name}: {done_days}/{days} дней за последние {days} дней')
    conn.close()
    current, best = get_streak(chat_id)
    streak_line = f'🔥 Текущий стрик: {current} дней подряд (лучший: {best})\n\n'
    return '📊 Статистика привычек:\n' + streak_line + '\n'.join(stats_lines)

def add_journal_entry(chat_id: int, text: str, attachments: Optional[str]=None) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('\n        INSERT INTO journal_entries (chat_id, created_at, text, attachments)\n        VALUES (?, ?, ?, ?)\n        ', (chat_id, now().isoformat(), text, attachments))
    conn.commit()
    conn.close()

def get_recent_journal_entries(chat_id: int, limit: int=5) -> List[Tuple[str, str, Optional[str]]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('\n        SELECT created_at, text, attachments\n        FROM journal_entries\n        WHERE chat_id = ?\n        ORDER BY datetime(created_at) DESC\n        LIMIT ?\n        ', (chat_id, limit))
    rows = cur.fetchall()
    conn.close()
    return rows

def record_activity(chat_id: int, field: str) -> bool:
    d_str = today_date().strftime('%Y-%m-%d')
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('\n        SELECT id, tasks_done, goals_done, habits_done, journal_entries, meta_actions, day_review_sent\n        FROM daily_activity\n        WHERE chat_id = ? AND date = ?\n        ', (chat_id, d_str))
    row = cur.fetchone()
    if row:
        act_id, tasks, goals, habits, journal_entries, meta_actions, day_review_sent = row
        if field == 'tasks':
            tasks += 1
        elif field == 'goals':
            goals += 1
        elif field == 'habits':
            habits += 1
        elif field == 'journal':
            journal_entries += 1
        elif field == 'meta':
            meta_actions += 1
        cur.execute('\n            UPDATE daily_activity\n            SET tasks_done = ?, goals_done = ?, habits_done = ?, journal_entries = ?, meta_actions = ?, day_review_sent = ?\n            WHERE id = ?\n            ', (tasks, goals, habits, journal_entries, meta_actions, day_review_sent, act_id))
        conn.commit()
        conn.close()
        return False
    else:
        tasks_done = 1 if field == 'tasks' else 0
        goals_done = 1 if field == 'goals' else 0
        habits_done = 1 if field == 'habits' else 0
        journal_entries = 1 if field == 'journal' else 0
        meta_actions = 1 if field == 'meta' else 0
        day_review_sent = 0
        cur.execute('\n            INSERT INTO daily_activity (chat_id, date, tasks_done, goals_done, habits_done, journal_entries, meta_actions, day_review_sent)\n            VALUES (?, ?, ?, ?, ?, ?, ?, ?)\n            ', (chat_id, d_str, tasks_done, goals_done, habits_done, journal_entries, meta_actions, day_review_sent))
        conn.commit()
        conn.close()
        return True

def get_streak(chat_id: int) -> Tuple[int, int]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('\n        SELECT date, tasks_done, goals_done, habits_done, journal_entries, meta_actions, day_review_sent\n        FROM daily_activity\n        WHERE chat_id = ?\n        ORDER BY date ASC\n        ', (chat_id,))
    rows = cur.fetchall()
    conn.close()
    if not rows:
        return (0, 0)
    today = today_date()
    best_streak = 0
    current_streak = 0
    cur_streak = 0
    prev_date: Optional[date] = None
    for d_str, tasks, goals, habits, journal_entries, meta_actions, _dr in rows:
        day = datetime.strptime(d_str, '%Y-%m-%d').date()
        total = tasks + goals + habits + journal_entries + meta_actions
        if total <= 0:
            continue
        if prev_date is not None and day == prev_date + timedelta(days=1):
            cur_streak += 1
        else:
            cur_streak = 1
        if cur_streak > best_streak:
            best_streak = cur_streak
        if day == today:
            current_streak = cur_streak
        prev_date = day
    if prev_date != today:
        current_streak = 0
    return (current_streak, best_streak)

def build_export_text(chat_id: int) -> str:
    conn = get_conn()
    cur = conn.cursor()
    current_streak, best_streak = get_streak(chat_id)
    parts = []
    parts.append('=== Стрик ===')
    parts.append(f'Текущий стрик: {current_streak} дней подряд')
    parts.append(f'Лучший стрик: {best_streak} дней\n')
    cur.execute('\n        SELECT id, text, remind_at, kind, priority, tags, sent, done, attachments\n        FROM reminders\n        WHERE chat_id = ?\n        ORDER BY datetime(remind_at) ASC\n        ', (chat_id,))
    rems = cur.fetchall()
    parts.append('=== Напоминания, задачи и дела без срока ===')
    if not rems:
        parts.append('Пусто.')
    else:
        for rid, text, remind_at, kind, priority, tags, sent, done, attachments in rems:
            dt = datetime.fromisoformat(remind_at)
            status = 'выполнено' if done else 'отправлено' if sent else 'ожидает'
            att_flag = ' | вложения: есть' if attachments else ''
            parts.append(f"[{rid}] {format_dt(dt)} | {kind} | приоритет {priority} | {status}{att_flag} | теги: {tags or '-'}\n{text}")
    cur.execute('\n        SELECT id, text, deadline, done, done_at, priority, tags\n        FROM goals\n        WHERE chat_id = ?\n        ORDER BY datetime(deadline) ASC\n        ', (chat_id,))
    goals = cur.fetchall()
    parts.append('\n=== Цели ===')
    if not goals:
        parts.append('Пусто.')
    else:
        for gid, text, deadline, done, done_at, priority, tags in goals:
            ddl_dt = datetime.fromisoformat(deadline)
            if done and done_at:
                done_dt = datetime.fromisoformat(done_at)
                status = f'выполнено {format_dt(done_dt)}'
            elif ddl_dt < now():
                status = 'просрочено'
            else:
                status = 'активно'
            parts.append(f"[{gid}] до {format_dt(ddl_dt)} | приоритет {priority} | {status} | теги: {tags or '-'}\n{text}")
    cur.execute('SELECT id, name, created_at, active FROM habits WHERE chat_id = ? ORDER BY id ASC', (chat_id,))
    habits = cur.fetchall()
    parts.append('\n=== Привычки ===')
    if not habits:
        parts.append('Пусто.')
    else:
        for hid, name, created_at, active in habits:
            status = 'активна' if active else 'выключена'
            parts.append(f'[{hid}] {name} | {status} | создана {created_at}')
    cur.execute('\n        SELECT created_at, text, attachments\n        FROM journal_entries\n        WHERE chat_id = ?\n        ORDER BY datetime(created_at) ASC\n        ', (chat_id,))
    journal = cur.fetchall()
    parts.append('\n=== Журнал ===')
    if not journal:
        parts.append('Пусто.')
    else:
        for created_at, text, attachments in journal:
            att_flag = ' [вложение]' if attachments else ''
            parts.append(f'[{created_at}]{att_flag} {text}')
    conn.close()
    return '\n'.join(parts)

def build_search_results(chat_id: int, query: str) -> str:
    q = query.strip()
    if not q:
        return 'Запрос пустой. Напиши слово или #тег, по которому искать.'
    is_tag = q.startswith('#') and len(q) > 1
    tag_value = None
    if is_tag:
        tag_value = q[1:].strip().lower()
    conn = get_conn()
    cur = conn.cursor()
    parts: List[str] = []
    parts.append(f'🔍 Результаты по запросу: *{q}*\n')
    if is_tag:
        cur.execute('\n            SELECT id, text, remind_at, kind, priority\n            FROM reminders\n            WHERE chat_id = ?\n              AND (LOWER(tags) LIKE ?)\n            ORDER BY datetime(remind_at) DESC\n            LIMIT 15\n            ', (chat_id, f'%{tag_value}%'))
    else:
        cur.execute('\n            SELECT id, text, remind_at, kind, priority\n            FROM reminders\n            WHERE chat_id = ?\n              AND (text LIKE ? OR tags LIKE ?)\n            ORDER BY datetime(remind_at) DESC\n            LIMIT 15\n            ', (chat_id, f'%{q}%', f'%{q}%'))
    rems = cur.fetchall()
    parts.append('🧾 *Дела и напоминания:*')
    if not rems:
        parts.append('— Ничего не найдено.')
    else:
        for rid, text, remind_at, kind, priority in rems:
            try:
                dt = datetime.fromisoformat(remind_at)
                dt_str = format_dt(dt)
            except Exception:
                dt_str = remind_at
            kind_emoji = '🔔' if kind == 'reminder' else '✅' if kind == 'task' else '📝'
            parts.append(f'{kind_emoji} [{rid}] {priority_emoji(priority)} {dt_str}\n{text}')
    if is_tag:
        cur.execute('\n            SELECT id, text, deadline, done\n            FROM goals\n            WHERE chat_id = ?\n              AND (LOWER(tags) LIKE ?)\n            ORDER BY datetime(deadline) DESC\n            LIMIT 15\n            ', (chat_id, f'%{tag_value}%'))
    else:
        cur.execute('\n            SELECT id, text, deadline, done\n            FROM goals\n            WHERE chat_id = ?\n              AND (text LIKE ? OR tags LIKE ?)\n            ORDER BY datetime(deadline) DESC\n            LIMIT 15\n            ', (chat_id, f'%{q}%', f'%{q}%'))
    goals = cur.fetchall()
    parts.append('\n🎯 *Цели:*')
    if not goals:
        parts.append('— Ничего не найдено.')
    else:
        for gid, text, deadline, done in goals:
            try:
                ddl_dt = datetime.fromisoformat(deadline)
                ddl_str = format_dt(ddl_dt)
            except Exception:
                ddl_str = deadline
            status = '✅ выполнена' if done else '⏳ активна'
            parts.append(f'[{gid}] {status} до {ddl_str}\n{text}')
    if is_tag:
        like_pattern = f'%{tag_value}%'
    else:
        like_pattern = f'%{q}%'
    cur.execute('\n        SELECT created_at, text\n        FROM journal_entries\n        WHERE chat_id = ?\n          AND text LIKE ?\n        ORDER BY datetime(created_at) DESC\n        LIMIT 10\n        ', (chat_id, like_pattern))
    journal = cur.fetchall()
    parts.append('\n📔 *Дневник:*')
    if not journal:
        parts.append('— Ничего не найдено.')
    else:
        for created_at, text in journal:
            try:
                dt = datetime.fromisoformat(created_at)
                dt_str = format_dt(dt)
            except Exception:
                dt_str = created_at
            snippet = text.strip()
            if len(snippet) > 200:
                snippet = snippet[:200] + '...'
            parts.append(f'• {dt_str}\n{snippet}')
    conn.close()
    return '\n'.join(parts)
MONTH_NAMES_RU = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь', 'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь']

def priority_emoji(p: int) -> str:
    return {1: '🟢', 2: '🟡', 3: '🔴'}.get(p, '🟡')

def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('➕ Новое дело', callback_data='menu_new')], [InlineKeyboardButton('🎯 Сегодня', callback_data='menu_today')], [InlineKeyboardButton('🎯 Цели', callback_data='menu_goals'), InlineKeyboardButton('📊 Привычки', callback_data='menu_habits')], [InlineKeyboardButton('📔 Дневник', callback_data='menu_journal'), InlineKeyboardButton('📋 Напоминания', callback_data='menu_list')], [InlineKeyboardButton('🔍 Поиск', callback_data='menu_search')], [InlineKeyboardButton('🧠 Разбор', callback_data='menu_review')], [InlineKeyboardButton('🧾 История', callback_data='menu_audit')], [InlineKeyboardButton('⚙ Настройки', callback_data='menu_settings'), InlineKeyboardButton('❓ Помощь', callback_data='menu_help')]])

def reminders_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('➕ Новое дело', callback_data='menu_new')], [InlineKeyboardButton('📊 Привычки', callback_data='menu_habits')], [InlineKeyboardButton('⬅ В меню', callback_data='back_main')]])

def new_item_type_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('🔔 Напоминание', callback_data='new_type:reminder'), InlineKeyboardButton('✅ Задача', callback_data='new_type:task')], [InlineKeyboardButton('📝 Без срока', callback_data='new_type:todo'), InlineKeyboardButton('🎯 Большая цель', callback_data='new_type:goal')], [InlineKeyboardButton('⬅ В меню', callback_data='back_main')]])

def priority_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('🔴 Высокий', callback_data='priority:3'), InlineKeyboardButton('🟡 Средний', callback_data='priority:2'), InlineKeyboardButton('🟢 Низкий', callback_data='priority:1')], [InlineKeyboardButton('Пропустить', callback_data='priority:skip')]])

def reminder_keyboard(rem_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('✅ Выполнено', callback_data=f'task_done:{rem_id}')], [InlineKeyboardButton('🔁 На завтра', callback_data=f'snooze:{rem_id}'), InlineKeyboardButton('❌ Удалить', callback_data=f'del:{rem_id}')]])

def todo_keyboard(rem_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('✅ Выполнено', callback_data=f'task_done:{rem_id}'), InlineKeyboardButton('❌ Удалить', callback_data=f'del:{rem_id}')]])

def goal_keyboard(goal_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('✅ Выполнено', callback_data=f'goal_done:{goal_id}')]])

def goals_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('➕ Новая цель', callback_data='goal_new')], [InlineKeyboardButton('📚 Шаблоны целей', callback_data='goal_templates')], [InlineKeyboardButton('📋 Активные', callback_data='goal_list_active'), InlineKeyboardButton('⏰ Просроченные', callback_data='goal_list_overdue')], [InlineKeyboardButton('✅ Выполненные', callback_data='goal_list_done')], [InlineKeyboardButton('⬅ В меню', callback_data='back_main')]])

def habits_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('➕ Новая привычка', callback_data='habit_new')], [InlineKeyboardButton('✅ Отметить сегодня', callback_data='habit_mark')], [InlineKeyboardButton('📈 Статистика', callback_data='habit_stats')], [InlineKeyboardButton('⬅ В меню', callback_data='back_main')]])

def journal_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('📝 Новая запись', callback_data='journal_new')], [InlineKeyboardButton('📚 Последние записи', callback_data='journal_list')], [InlineKeyboardButton('⬅ В меню', callback_data='back_main')]])

def settings_keyboard(settings: Dict[str, int]) -> InlineKeyboardMarkup:
    ai = 'Вкл' if settings.get('ai_comments', 1) else 'Выкл'
    panic = 'Вкл' if settings.get('panic_notifications', 1) else 'Выкл'
    weekly = 'Вкл' if settings.get('weekly_review', 1) else 'Выкл'
    return InlineKeyboardMarkup([[InlineKeyboardButton(f'🤖 ИИ-комментарии: {ai}', callback_data='set_toggle:ai_comments')], [InlineKeyboardButton(f'⏰ Мягкая паника: {panic}', callback_data='set_toggle:panic_notifications')], [InlineKeyboardButton(f'📆 Еженедельный обзор: {weekly}', callback_data='set_toggle:weekly_review')], [InlineKeyboardButton('⬅ В меню', callback_data='back_main')]])

def review_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('📝 План с ИИ', callback_data='review_plan')], [InlineKeyboardButton('📊 Разбор недели', callback_data='review_week')], [InlineKeyboardButton('⬅ В меню', callback_data='back_main')]])

def build_calendar(year: int, month: int) -> InlineKeyboardMarkup:
    calendar.setfirstweekday(calendar.MONDAY)
    month_cal = calendar.monthcalendar(year, month)
    keyboard: List[List[InlineKeyboardButton]] = []
    month_name = MONTH_NAMES_RU[month - 1]
    keyboard.append([InlineKeyboardButton(f'{month_name} {year}', callback_data='cal:ignore')])
    week_days = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']
    keyboard.append([InlineKeyboardButton(d, callback_data='cal:ignore') for d in week_days])
    for week in month_cal:
        row = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(' ', callback_data='cal:ignore'))
            else:
                day_str = f'{day:02d}'
                date_str = f'{year:04d}-{month:02d}-{day_str}'
                row.append(InlineKeyboardButton(day_str, callback_data=f'cal:day:{date_str}'))
        keyboard.append(row)
    if month == 1:
        prev_month = 12
        prev_year = year - 1
    else:
        prev_month = month - 1
        prev_year = year
    if month == 12:
        next_month = 1
        next_year = year + 1
    else:
        next_month = month + 1
        next_year = year
    keyboard.append([InlineKeyboardButton('⬅️', callback_data=f'cal:nav:{prev_year}:{prev_month:02d}'), InlineKeyboardButton('Отмена', callback_data='cancel_new'), InlineKeyboardButton('➡️', callback_data=f'cal:nav:{next_year}:{next_month:02d}')])
    return InlineKeyboardMarkup(keyboard)

def build_hour_keyboard() -> InlineKeyboardMarkup:
    buttons = []
    for h in range(24):
        label = f'{h:02d}'
        cb = f'time:hour:{label}'
        buttons.append(InlineKeyboardButton(label, callback_data=cb))
    rows = [buttons[i:i + 6] for i in range(0, len(buttons), 6)]
    rows.append([InlineKeyboardButton('Отмена', callback_data='cancel_new')])
    return InlineKeyboardMarkup(rows)

def build_minute_keyboard() -> InlineKeyboardMarkup:
    buttons = []
    for m in range(60):
        label = f'{m:02d}'
        cb = f'time:min:{label}'
        buttons.append(InlineKeyboardButton(label, callback_data=cb))
    rows = [buttons[i:i + 8] for i in range(0, len(buttons), 8)]
    rows.append([InlineKeyboardButton('Отмена', callback_data='cancel_new')])
    return InlineKeyboardMarkup(rows)

def get_help_text() -> str:
    return '📘 *Дневник будущего — краткая справка*\n\nЯ помогаю тебе:\n• сохранять напоминания и задачи,\n• ставить цели с дедлайнами,\n• отслеживать привычки,\n• вести личный дневник,\n• держать стрик ежедневной активности 🔥.\n\nИИ здесь работает точечно:\n• помогает составить план через кнопку `🧠 Разбор`,\n• делает разбор недели,\n• добавляет эмоцию к некоторым напоминаниям и выполненным целям.\n\nДополнительно:\n• `/stats` — сводка по прогрессу за сегодня,\n• автодайджест дня вечером и мягкие напоминания, если пропадаешь.\n\nВсё остальное — твой чистый контроль через кнопки и простое управление ✨'

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_subscribed(update, context):
        return
    touch_user_from_update(update)
    chat_id = update.effective_chat.id
    log_audit(chat_id, 'start', '/start')
    settings = get_user_settings(chat_id)
    current_streak, best_streak = get_streak(chat_id)
    streak_line = f'🔥 Стрик: {current_streak} дней подряд (лучший: {best_streak})'
    text = f"👋 Привет! Я *Дневник будущего*.\n\nХрани планы, цели, привычки и мысли — я напомню о важном.\n\n{streak_line}\n\nВыбирай действие через кнопки ниже 👇\n\n🤖 ИИ-комментарии: {('включены' if settings.get('ai_comments', 1) else 'выключены')}\n⏰ Мягкая паника по целям: {('включена' if settings.get('panic_notifications', 1) else 'выключена')}"
    await update.message.reply_text(text, parse_mode='Markdown', reply_markup=main_menu_keyboard())

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_subscribed(update, context):
        return
    touch_user_from_update(update)
    chat_id = update.effective_chat.id
    log_audit(chat_id, 'help', '/help')
    await update.message.reply_text(get_help_text(), parse_mode='Markdown', reply_markup=main_menu_keyboard())

async def export_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_subscribed(update, context):
        return
    touch_user_from_update(update)
    chat_id = update.effective_chat.id
    log_audit(chat_id, 'export', '/export')
    txt = build_export_text(chat_id)
    with tempfile.NamedTemporaryFile('w+', delete=False, suffix='.txt', encoding='utf-8') as f:
        f.write(txt)
        fname = f.name
    await update.message.reply_document(document=InputFile(open(fname, 'rb'), filename='lama_diary_export.txt'), caption='Вот твой бэкап из Дневника будущего 📦')
    first = record_activity(chat_id, 'meta')
    if first:
        current, best = get_streak(chat_id)
        await update.message.reply_text(f'🔥 Новый день в стрике: {current} дней подряд (лучший: {best}).')

async def plan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_subscribed(update, context):
        return
    touch_user_from_update(update)
    chat_id = update.effective_chat.id
    args = ' '.join(context.args) if context.args else ''
    if not args:
        await update.message.reply_text('Напиши, что ты хочешь спланировать.\n\nНапример: `/plan подготовиться к экзамену за 2 недели`', parse_mode='Markdown')
        return
    log_audit(chat_id, 'plan', args)
    await update.message.reply_text('Думаю над планом... 🤔')
    plan_text = ai_generate_plan(args)
    await update.message.reply_text(plan_text)
    first = record_activity(chat_id, 'meta')
    if first:
        current, best = get_streak(chat_id)
        await update.message.reply_text(f'🔥 Ты начал новый день с планирования. Стрик: {current} дней подряд (лучший: {best}).')

async def run_week_review(chat_id: int, base_message, context: ContextTypes.DEFAULT_TYPE):
    log_audit(chat_id, 'week_review', '/week_review')
    end = now()
    start_dt = end - timedelta(days=7)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('\n        SELECT COUNT(*) FROM goals\n        WHERE chat_id = ? AND done = 1\n          AND datetime(done_at) BETWEEN datetime(?) AND datetime(?)\n        ', (chat_id, start_dt.isoformat(), end.isoformat()))
    goals_done = cur.fetchone()[0]
    cur.execute('\n        SELECT COUNT(*) FROM goals\n        WHERE chat_id = ? AND done = 0\n          AND datetime(deadline) BETWEEN datetime(?) AND datetime(?)\n        ', (chat_id, start_dt.isoformat(), end.isoformat()))
    goals_due = cur.fetchone()[0]
    cur.execute('\n        SELECT COUNT(*) FROM reminders\n        WHERE chat_id = ? AND done = 1\n          AND datetime(done_at) BETWEEN datetime(?) AND datetime(?)\n        ', (chat_id, start_dt.isoformat(), end.isoformat()))
    tasks_done = cur.fetchone()[0]
    cur.execute('\n        SELECT COUNT(*) FROM habit_logs hl\n        JOIN habits h ON h.id = hl.habit_id\n        WHERE h.chat_id = ? AND hl.done = 1\n          AND datetime(hl.date) BETWEEN datetime(?) AND datetime(?)\n        ', (chat_id, start_dt.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')))
    habits_done = cur.fetchone()[0]
    conn.close()
    current_streak, best_streak = get_streak(chat_id)
    summary = f'Период: {format_d(start_dt)} — {format_d(end)}\nЦелей выполнено: {goals_done}\nЦелей с дедлайном в этот период (ещё не закрыты): {goals_due}\nЗадач/напоминаний выполнено: {tasks_done}\nОтмеченных привычек: {habits_done}\nТекущий стрик: {current_streak} дней подряд, лучший: {best_streak}.\n'
    await base_message.reply_text('Делаю разбор недели... 🧠')
    ai_text = ai_week_review(summary)
    if ai_text:
        await base_message.reply_text(f'📊 Итоги недели:\n\n{summary}\n\n🤖 Разбор:\n{ai_text}')
    else:
        await base_message.reply_text(f'📊 Итоги недели:\n\n{summary}')
    context.user_data['flow'] = 'week_reflection'
    context.user_data['week_reflection_period'] = f'{format_d(start_dt)} — {format_d(end)}'
    await base_message.reply_text('А теперь немного рефлексии ✨\n\nНапиши одним сообщением:\n• чем ты больше всего гордишься за эту неделю;\n• что хочешь изменить в следующей.\n\nЯ сохраню это в дневнике.', parse_mode='Markdown')
    first = record_activity(chat_id, 'meta')
    if first:
        current, best = get_streak(chat_id)
        await base_message.reply_text(f'🔥 Обзор недели открыл новый день стрика! Уже {current} дней подряд (лучший: {best}).')

async def week_review_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_subscribed(update, context):
        return
    touch_user_from_update(update)
    chat_id = update.effective_chat.id
    await run_week_review(chat_id, update.message, context)

def build_stats_text(chat_id: int) -> str:
    today = today_date()
    today_str = today.strftime('%Y-%m-%d')
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("\n        SELECT COUNT(*) FROM reminders\n        WHERE chat_id = ? AND done = 0 AND kind != 'todo'\n        ", (chat_id,))
    open_rem = cur.fetchone()[0]
    cur.execute('\n        SELECT COUNT(*) FROM reminders\n        WHERE chat_id = ? AND done = 1 AND date(done_at) = date(?)\n        ', (chat_id, today_str))
    done_rem_today = cur.fetchone()[0]
    cur.execute("\n        SELECT COUNT(*) FROM reminders\n        WHERE chat_id = ? AND done = 0 AND kind = 'todo'\n        ", (chat_id,))
    open_todos = cur.fetchone()[0]
    now_dt = now()
    active_goals, overdue_goals, done_goals_all = get_goals_stats(chat_id, now_dt)
    cur.execute('\n        SELECT COUNT(*) FROM goals\n        WHERE chat_id = ? AND done = 1 AND date(done_at) = date(?)\n        ', (chat_id, today_str))
    done_goals_today = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM habits WHERE chat_id = ? AND active = 1', (chat_id,))
    active_habits = cur.fetchone()[0]
    cur.execute('\n        SELECT COUNT(*) FROM habit_logs hl\n        JOIN habits h ON h.id = hl.habit_id\n        WHERE h.chat_id = ? AND hl.done = 1 AND date(hl.date) = date(?)\n        ', (chat_id, today_str))
    habits_today = cur.fetchone()[0]
    cur.execute('\n        SELECT COUNT(*) FROM journal_entries\n        WHERE chat_id = ? AND date(created_at) = date(?)\n        ', (chat_id, today_str))
    journal_today = cur.fetchone()[0]
    conn.close()
    current_streak, best_streak = get_streak(chat_id)
    text = f'📊 *Твоя статистика за сегодня ({format_d(now())}):*\n\n🔔 Напоминания/задачи: открыто — *{open_rem}*, выполнено сегодня — *{done_rem_today}*\n📝 Дела без срока: открыто — *{open_todos}*\n\n🎯 Цели: активных — *{active_goals}*, просроченных — *{overdue_goals}*, выполнено сегодня — *{done_goals_today}*\n\n📊 Привычки: активных — *{active_habits}*, отмечено сегодня — *{habits_today}*\n📔 Записей в дневнике сегодня: *{journal_today}*\n\n🔥 Стрик: *{current_streak}* дней подряд (лучший: *{best_streak}*)'
    return text

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_subscribed(update, context):
        return
    touch_user_from_update(update)
    chat_id = update.effective_chat.id
    log_audit(chat_id, 'stats', '/stats')
    text = build_stats_text(chat_id)
    await update.message.reply_text(text, parse_mode='Markdown', reply_markup=main_menu_keyboard())

async def show_audit(chat_id: int, base_message, context: ContextTypes.DEFAULT_TYPE):
    entries = get_audit_entries(chat_id, limit=30)
    current, best = get_streak(chat_id)
    if not entries:
        await base_message.reply_text(f'🧾 История пока пустая.\n\n🔥 Стрик: {current} дней подряд (лучший: {best})', reply_markup=main_menu_keyboard())
        return
    lines = []
    lines.append('🧾 История последних действий:')
    lines.append(f'🔥 Стрик: {current} дней подряд (лучший: {best})')
    lines.append('')
    for created_at, action, payload in entries:
        try:
            dt = datetime.fromisoformat(created_at)
            dt_str = format_dt(dt)
        except Exception:
            dt_str = created_at
        label = humanize_action(action)
        line = f'• {dt_str} — {label}'
        if payload:
            line += f'\n  {payload}'
        lines.append(line)
    text = '\n'.join(lines)
    await base_message.reply_text(text, reply_markup=main_menu_keyboard())

async def show_reminders(chat_id: int, base_message, context: ContextTypes.DEFAULT_TYPE):
    reminders = get_future_reminders(chat_id)
    todos = get_todos(chat_id)
    current, best = get_streak(chat_id)
    if not reminders and (not todos):
        await base_message.reply_text(f'Пока нет будущих напоминаний, задач и дел без срока 💤\n\n🔥 Стрик: {current} дней подряд (лучший: {best})', reply_markup=reminders_menu_keyboard())
        return
    await base_message.reply_text(f'📋 Твои дела.\n🔥 Стрик: {current} дней подряд (лучший: {best})', reply_markup=reminders_menu_keyboard())
    if reminders:
        await base_message.reply_text('📋 Будущие напоминания и задачи:')
        for rem_id, remind_at, text, ai_comment, kind, priority, attachments_str in reminders:
            dt = datetime.fromisoformat(remind_at)
            kind_emoji = '🔔' if kind == 'reminder' else '✅'
            attachments = parse_attachments(attachments_str)
            caption = f'{kind_emoji} *[{rem_id}]* {priority_emoji(priority)}\n🕒 {format_dt(dt)}\n📝 {text}'
            if ai_comment:
                caption += f'\n🤖 От дневника: {ai_comment}'
            await send_with_attachments(context.bot, chat_id, attachments, caption, reply_markup=reminder_keyboard(rem_id), parse_mode='Markdown')
    if todos:
        await base_message.reply_text('📝 Дела без срока:')
        for rem_id, text, priority, attachments_str in todos:
            attachments = parse_attachments(attachments_str)
            caption = f'📝 *[{rem_id}]* {priority_emoji(priority)}\n📝 {text}'
            await send_with_attachments(context.bot, chat_id, attachments, caption, reply_markup=todo_keyboard(rem_id), parse_mode='Markdown')

async def show_goals_list(chat_id: int, base_message, context: ContextTypes.DEFAULT_TYPE, mode: str):
    now_dt = now()
    if mode == 'active':
        goals = get_goals_active(chat_id, now_dt)
        title = '🎯 Активные цели:'
        empty = 'Активных целей пока нет. Поставь новую цель через `➕ Новая цель`.'
    elif mode == 'overdue':
        goals = get_goals_overdue(chat_id, now_dt)
        title = '⏰ Просроченные цели:'
        empty = 'Просроченных целей нет. Уже хорошо 😎'
    else:
        done_goals = get_goals_done(chat_id, 10)
        if not done_goals:
            await base_message.reply_text('Ты ещё не отмечал цели выполненными. Всё впереди 💪', reply_markup=goals_menu_keyboard())
            return
        await base_message.reply_text('✅ Недавно выполненные цели:', reply_markup=goals_menu_keyboard())
        for goal_id, deadline, text, done_at in done_goals:
            ddl_dt = datetime.fromisoformat(deadline)
            done_dt = datetime.fromisoformat(done_at)
            delta_str = format_delta(ddl_dt, done_dt)
            msg = f'🆔 *{goal_id}*\n🎯 Цель: {text}\n⏳ Дедлайн: {format_dt(ddl_dt)}\n🏁 Выполнено: {format_dt(done_dt)} ({delta_str})'
            await base_message.reply_text(msg, parse_mode='Markdown')
        return
    if not goals:
        await base_message.reply_text(empty, parse_mode='Markdown', reply_markup=goals_menu_keyboard())
        return
    await base_message.reply_text(title, reply_markup=goals_menu_keyboard())
    for goal_id, deadline, text, priority in goals:
        ddl_dt = datetime.fromisoformat(deadline)
        delta_str = format_delta(ddl_dt, now_dt)
        msg = f'🆔 *{goal_id}* {priority_emoji(priority)}\n🎯 Цель: {text}\n⏳ Дедлайн: {format_dt(ddl_dt)} ({delta_str})'
        await base_message.reply_text(msg, parse_mode='Markdown', reply_markup=goal_keyboard(goal_id))

async def show_today(chat_id: int, base_message, context: ContextTypes.DEFAULT_TYPE):
    today_str = today_date().strftime('%Y-%m-%d')
    now_dt = now()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("\n        SELECT id, text, remind_at, kind, priority\n        FROM reminders\n        WHERE chat_id = ?\n          AND done = 0\n          AND kind != 'todo'\n          AND date(remind_at) = date(?)\n        ORDER BY datetime(remind_at) ASC\n        ", (chat_id, today_str))
    today_rems = cur.fetchall()
    cur.execute('\n        SELECT id, text, deadline, priority\n        FROM goals\n        WHERE chat_id = ?\n          AND done = 0\n          AND date(deadline) = date(?)\n        ORDER BY datetime(deadline) ASC\n        ', (chat_id, today_str))
    today_goals = cur.fetchall()
    cur.execute('\n        SELECT id, text, deadline, priority\n        FROM goals\n        WHERE chat_id = ?\n          AND done = 0\n          AND datetime(deadline) > datetime(?)\n          AND datetime(deadline) <= datetime(?)\n        ORDER BY datetime(deadline) ASC\n        ', (chat_id, now_dt.isoformat(), (now_dt + timedelta(days=3)).isoformat()))
    next_goals = cur.fetchall()
    conn.close()
    current_streak, best_streak = get_streak(chat_id)
    streak_line = f'🔥 Стрик: {current_streak} дней подряд (лучший: {best_streak})'
    lines = [f'🎯 *Фокус на сегодня* — {format_d(now_dt)}', streak_line, '']
    if not today_rems and (not today_goals) and (not next_goals):
        lines.append('На сегодня ничего не запланировано. Можно добавить что-нибудь через `➕ Новое дело` 🙂')
        await base_message.reply_text('\n'.join(lines), parse_mode='Markdown', reply_markup=main_menu_keyboard())
        return
    if today_rems:
        lines.append('🔔 *Напоминания и задачи сегодня:*')
        for rid, text, remind_at, kind, priority in today_rems:
            dt = datetime.fromisoformat(remind_at)
            kind_emoji = '🔔' if kind == 'reminder' else '✅'
            lines.append(f"{kind_emoji} [{rid}] {priority_emoji(priority)} {dt.strftime('%H:%M')} — {text}")
        lines.append('')
    if today_goals:
        lines.append('🎯 *Цели с дедлайном сегодня:*')
        for gid, text, deadline, priority in today_goals:
            ddl_dt = datetime.fromisoformat(deadline)
            lines.append(f"🎯 [{gid}] {priority_emoji(priority)} до {ddl_dt.strftime('%H:%M')} — {text}")
        lines.append('')
    if next_goals:
        lines.append('📌 *Цели на ближайшие дни:*')
        for gid, text, deadline, priority in next_goals:
            ddl_dt = datetime.fromisoformat(deadline)
            delta_str = format_delta(ddl_dt, now_dt)
            lines.append(f'🎯 [{gid}] {priority_emoji(priority)} до {format_dt(ddl_dt)} ({delta_str}) — {text}')
    await base_message.reply_text('\n'.join(lines), parse_mode='Markdown', reply_markup=main_menu_keyboard())

def build_day_summary(chat_id: int, d: date) -> str:
    day_str = d.strftime('%Y-%m-%d')
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('\n        SELECT COUNT(*) FROM reminders\n        WHERE chat_id = ? AND done = 1 AND date(done_at) = date(?)\n        ', (chat_id, day_str))
    done_rem = cur.fetchone()[0]
    cur.execute('\n        SELECT COUNT(*) FROM goals\n        WHERE chat_id = ? AND done = 1 AND date(done_at) = date(?)\n        ', (chat_id, day_str))
    done_goals = cur.fetchone()[0]
    cur.execute('\n        SELECT COUNT(*) FROM habit_logs hl\n        JOIN habits h ON h.id = hl.habit_id\n        WHERE h.chat_id = ? AND hl.done = 1 AND date(hl.date) = date(?)\n        ', (chat_id, day_str))
    done_habits = cur.fetchone()[0]
    cur.execute('\n        SELECT COUNT(*) FROM journal_entries\n        WHERE chat_id = ? AND date(created_at) = date(?)\n        ', (chat_id, day_str))
    journal_entries = cur.fetchone()[0]
    conn.close()
    summary = f'Дата: {format_d(datetime.combine(d, datetime.min.time()))}\nЗадач/напоминаний выполнено: {done_rem}\nЦелей отмечено выполненными: {done_goals}\nОтмеченных привычек: {done_habits}\nЗаписей в дневнике: {journal_entries}\n'
    return summary

async def check_day_review(context: ContextTypes.DEFAULT_TYPE):
    now_dt = now()
    if not 21 <= now_dt.hour < 22:
        return
    today = today_date()
    today_str = today.strftime('%Y-%m-%d')
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('\n        SELECT id, chat_id, day_review_sent\n        FROM daily_activity\n        WHERE date = ?\n        ', (today_str,))
    rows = cur.fetchall()
    conn.close()
    for act_id, chat_id, day_review_sent in rows:
        if day_review_sent:
            continue
        summary = build_day_summary(chat_id, today)
        ai_text = ai_day_review(summary)
        msg_text = f'🌙 *Итоги дня*\n\n{summary}'
        if ai_text:
            msg_text += f'\n🤖 Разбор:\n{ai_text}'
        try:
            await context.bot.send_message(chat_id, msg_text, parse_mode='Markdown')
            entry_text = f"[Итоги дня {format_d(now_dt)}]\n{summary}\n\n{ai_text or ''}"
            add_journal_entry(chat_id, entry_text, attachments=None)
        except Exception as e:
            logger.error(f'day_review send error: {e}')
        conn2 = get_conn()
        cur2 = conn2.cursor()
        cur2.execute('UPDATE daily_activity SET day_review_sent = 1 WHERE id = ?', (act_id,))
        conn2.commit()
        conn2.close()

async def check_inactive_job(context: ContextTypes.DEFAULT_TYPE):
    today = today_date()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('\n        SELECT chat_id, MAX(date) as last_date\n        FROM daily_activity\n        GROUP BY chat_id\n        ')
    rows = cur.fetchall()
    conn.close()
    for chat_id, last_date_str in rows:
        try:
            last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
        except Exception:
            continue
        delta_days = (today - last_date).days
        if delta_days < 3:
            continue
        last_nudge = get_last_nudge(chat_id)
        if last_nudge is not None and (today - last_nudge).days < 3:
            continue
        text = 'Хей 👋\n\nЯ тут заметил, что мы давно ничего не отмечали вместе.\nЕсли хочешь — можем:\n• обновить цели,\n• накидать простой план,\n• или ты просто выговоришься в дневнике.\n\nНажми `/start`, чтобы вернуться в поток 💫'
        try:
            await context.bot.send_message(chat_id, text)
        except Exception as e:
            logger.error(f'nudge send error: {e}')
            continue
        set_last_nudge(chat_id)

async def mark_message_status(query, suffix: str):
    msg = query.message
    base_text = msg.text or msg.caption or ''
    new_text = base_text + suffix
    has_media = bool(msg.photo or msg.video or msg.document or msg.audio or msg.voice or msg.animation or msg.video_note)
    if has_media:
        await msg.edit_caption(caption=new_text, parse_mode='Markdown')
    else:
        await msg.edit_text(new_text, parse_mode='Markdown')

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ''
    chat_id = query.message.chat_id
    if data == 'check_sub':
        ok = await check_subscription_raw(update.effective_user.id, context.bot)
        if ok:
            context.user_data['sub_ok'] = True
            await query.message.reply_text('✅ Спасибо за подписку! Теперь можно пользоваться ботом 🙌', reply_markup=main_menu_keyboard())
        else:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton('📢 Открыть @lamabio', url='https://t.me/lamabio')], [InlineKeyboardButton('📢 Открыть @lLamaInCraft', url='https://t.me/lLamaInCraft')], [InlineKeyboardButton('✅ Проверить подписку', callback_data='check_sub')]])
            await query.message.reply_text('Похоже, ты ещё не подписался на оба канала.\n\nПроверь, что ты подписан на:\n• @lamabio\n• @lLamaInCraft\n\nПосле этого снова нажми «✅ Проверить подписку».', reply_markup=kb)
        return
    if not await ensure_subscribed(update, context):
        return
    touch_user_from_update(update)
    if data == 'back_main':
        context.user_data.clear()
        await query.message.reply_text('Главное меню:', reply_markup=main_menu_keyboard())
        return
    if data == 'menu_new':
        context.user_data.clear()
        await query.message.reply_text('Что создаём? Выбери тип дела:', reply_markup=new_item_type_keyboard())
        return
    if data == 'menu_search':
        context.user_data.clear()
        context.user_data['flow'] = 'search'
        context.user_data['awaiting_search_query'] = True
        await query.message.reply_text('🔍 Что ищем?\n\nМожно написать слово (например: `экзамен`) или тег (`#работа`, `#спорт`).\nЯ посмотрю в делах, целях и дневнике.', parse_mode='Markdown')
        return
    if data.startswith('new_type:'):
        t = data.split(':', 1)[1]
        if t not in ('reminder', 'task', 'goal', 'todo'):
            return
        context.user_data.clear()
        context.user_data['creation_type'] = t
        if t == 'todo':
            context.user_data['flow'] = 'new_item'
            context.user_data['awaiting_text'] = True
            context.user_data['new_dt'] = now()
            await query.message.reply_text('📝 Напиши текст дела, которое не привязано к дате и времени.\nНапример: `Почистить стол #дом`', parse_mode='Markdown')
            return
        context.user_data['flow'] = 'new_item'
        td = today_date()
        kb = build_calendar(td.year, td.month)
        label = 'напоминания' if t == 'reminder' else 'задачи' if t == 'task' else 'цели'
        await query.message.reply_text(f'📅 Выбери дату для *{label}*:', parse_mode='Markdown', reply_markup=kb)
        return
    if data == 'menu_today':
        await show_today(chat_id, query.message, context)
        return
    if data == 'menu_goals':
        now_dt = now()
        active, overdue, done_all = get_goals_stats(chat_id, now_dt)
        current_streak, best_streak = get_streak(chat_id)
        summary = f'🎯 *Раздел целей*\n\nАктивных целей: *{active}*\nПросроченных: *{overdue}*\nВсего выполнено: *{done_all}*\n\n🔥 Стрик: {current_streak} дней подряд (лучший: {best_streak})\n\nВыбери действие:'
        await query.message.reply_text(summary, parse_mode='Markdown', reply_markup=goals_menu_keyboard())
        return
    if data == 'goal_new':
        context.user_data.clear()
        context.user_data['creation_type'] = 'goal'
        context.user_data['flow'] = 'new_item'
        td = today_date()
        kb = build_calendar(td.year, td.month)
        await query.message.reply_text('📅 Выбери дедлайн для *цели*:', parse_mode='Markdown', reply_markup=kb)
        return
    if data == 'goal_templates':
        lines = ['📚 *Шаблоны целей*:\n']
        for tpl in GOAL_TEMPLATES:
            lines.append(f"• *{tpl['title']}*\n  {tpl['hint']}")
        kb_rows = [[InlineKeyboardButton(tpl['title'], callback_data=f"goal_tpl:{tpl['id']}")] for tpl in GOAL_TEMPLATES]
        kb_rows.append([InlineKeyboardButton('⬅ Назад к целям', callback_data='menu_goals')])
        kb = InlineKeyboardMarkup(kb_rows)
        await query.message.reply_text('\n'.join(lines), parse_mode='Markdown', reply_markup=kb)
        return
    if data.startswith('goal_tpl:'):
        tpl_id = data.split(':', 1)[1]
        tpl = next((t for t in GOAL_TEMPLATES if t['id'] == tpl_id), None)
        if not tpl:
            await query.message.reply_text('Не удалось найти шаблон цели.')
            return
        context.user_data.clear()
        context.user_data['creation_type'] = 'goal'
        context.user_data['flow'] = 'new_item'
        context.user_data['new_text'] = tpl['text']
        context.user_data['from_template'] = True
        td = today_date()
        kb = build_calendar(td.year, td.month)
        await query.message.reply_text(f"🧩 Шаблон цели:\n\n*{tpl['title']}*\n{tpl['text']}\n\n{tpl['hint']}\n\nТеперь выбери дедлайн для этой цели:", parse_mode='Markdown', reply_markup=kb)
        return
    if data == 'goal_list_active':
        await show_goals_list(chat_id, query.message, context, 'active')
        return
    if data == 'goal_list_overdue':
        await show_goals_list(chat_id, query.message, context, 'overdue')
        return
    if data == 'goal_list_done':
        await show_goals_list(chat_id, query.message, context, 'done')
        return
    if data == 'menu_list':
        await show_reminders(chat_id, query.message, context)
        return
    if data == 'menu_help':
        await query.message.reply_text(get_help_text(), parse_mode='Markdown', reply_markup=main_menu_keyboard())
        return
    if data == 'menu_habits':
        current, best = get_streak(chat_id)
        await query.message.reply_text(f'📊 Привычки. Что делаем?\n\n🔥 Стрик: {current} дней подряд (лучший: {best})', reply_markup=habits_menu_keyboard())
        return
    if data == 'habit_new':
        context.user_data.clear()
        context.user_data['flow'] = 'new_habit'
        await query.message.reply_text('Напиши название привычки. Например: `Читать 20 минут`.', parse_mode='Markdown')
        return
    if data == 'habit_mark':
        habits = get_active_habits(chat_id)
        if not habits:
            await query.message.reply_text('У тебя пока нет активных привычек. Добавь одну через `➕ Новая привычка`.', parse_mode='Markdown', reply_markup=habits_menu_keyboard())
            return
        await query.message.reply_text('Выбери, что ты сделал сегодня:')
        for hid, name in habits:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton('✅ Сегодня сделано', callback_data=f'habit_mark_now:{hid}')]])
            await query.message.reply_text(f'• {name}', reply_markup=kb)
        return
    if data.startswith('habit_mark_now:'):
        try:
            hid = int(data.split(':', 1)[1])
        except ValueError:
            return
        mark_habit_today(hid)
        log_audit(chat_id, 'habit_done', f'habit_id={hid}')
        first = record_activity(chat_id, 'habits')
        await query.message.edit_text(query.message.text + '\n\n✅ Сегодня отмечено!')
        if first:
            current, best = get_streak(chat_id)
            await query.message.reply_text(f'🔥 Новый день в твоём стрике! Уже {current} дней подряд (лучший: {best}).')
        return
    if data == 'habit_stats':
        stats = get_habit_stats(chat_id)
        await query.message.reply_text(stats, reply_markup=habits_menu_keyboard())
        return
    if data == 'menu_journal':
        current, best = get_streak(chat_id)
        await query.message.reply_text(f'📔 Дневник. Что делаем?\n\n🔥 Стрик: {current} дней подряд (лучший: {best})', reply_markup=journal_menu_keyboard())
        return
    if data == 'journal_new':
        context.user_data.clear()
        context.user_data['flow'] = 'new_journal'
        await query.message.reply_text('Напиши, что хочешь сохранить в дневнике.\nНапример: как прошёл день, что порадовало, что волнует.\nМожно и фотку/видео приложить — я сохраню вместе с записью.')
        return
    if data == 'journal_list':
        entries = get_recent_journal_entries(chat_id, 5)
        if not entries:
            await query.message.reply_text('В дневнике пока пусто. Попробуй написать первую запись 🙂', reply_markup=journal_menu_keyboard())
            return
        await query.message.reply_text('📚 Последние записи:', reply_markup=journal_menu_keyboard())
        for created_at, text, attachments_str in entries:
            try:
                dt = datetime.fromisoformat(created_at)
                dt_str = format_dt(dt)
            except Exception:
                dt_str = created_at
            attachments = parse_attachments(attachments_str)
            caption = f'*{dt_str}*\n{text}'
            await send_with_attachments(context.bot, chat_id, attachments, caption, reply_markup=None, parse_mode='Markdown')
        return
    if data == 'menu_settings':
        settings = get_user_settings(chat_id)
        await query.message.reply_text('⚙ Настройки:', reply_markup=settings_keyboard(settings))
        return
    if data.startswith('set_toggle:'):
        field = data.split(':', 1)[1]
        settings = get_user_settings(chat_id)
        current_val = settings.get(field, 1)
        new_val = 0 if current_val else 1
        set_user_setting(chat_id, field, new_val)
        log_audit(chat_id, f'set_toggle_{field}', f'value={new_val}')
        settings = get_user_settings(chat_id)
        await query.message.edit_reply_markup(reply_markup=settings_keyboard(settings))
        return
    if data == 'menu_audit':
        await show_audit(chat_id, query.message, context)
        return
    if data == 'menu_review':
        current, best = get_streak(chat_id)
        await query.message.reply_text(f'🧠 Разбор.\n\nЗдесь ты можешь:\n• составить план с ИИ по любой задаче;\n• получить разбор недели по целям, делам и привычкам.\n\n🔥 Стрик: {current} дней подряд (лучший: {best})', reply_markup=review_menu_keyboard())
        return
    if data == 'review_plan':
        context.user_data.clear()
        context.user_data['flow'] = 'plan_text'
        await query.message.reply_text('Напиши, что ты хочешь спланировать.\n\nНапример: `Подготовиться к экзамену за 2 недели`', parse_mode='Markdown')
        return
    if data == 'review_week':
        await run_week_review(chat_id, query.message, context)
        return
    if data == 'cancel_new':
        context.user_data.clear()
        await query.message.reply_text('Создание отменено.', reply_markup=main_menu_keyboard())
        return
    if data.startswith('del:'):
        try:
            rem_id = int(data.split(':', 1)[1])
        except ValueError:
            await query.message.reply_text('Некорректный ID дела.')
            return
        ok = delete_reminder(chat_id, rem_id)
        if ok:
            log_audit(chat_id, 'delete_task', f'id={rem_id}')
            await mark_message_status(query, '\n\n✅ *Удалено*')
        else:
            await query.message.reply_text('❌ Дело не найдено.')
        return
    if data.startswith('snooze:'):
        try:
            rem_id = int(data.split(':', 1)[1])
        except ValueError:
            await query.message.reply_text('Некорректный ID напоминания.')
            return
        ok = snooze_reminder(chat_id, rem_id, days=1)
        if ok:
            log_audit(chat_id, 'snooze_task', f'id={rem_id}, days=1')
            await query.message.reply_text('🔁 Перенёс на завтра в то же время.')
        else:
            await query.message.reply_text('❌ Не удалось перенести (для дел без срока перенос не нужен).')
        return
    if data.startswith('task_done:'):
        try:
            rem_id = int(data.split(':', 1)[1])
        except ValueError:
            await query.message.reply_text('Некорректный ID дела.')
            return
        ok = complete_task(chat_id, rem_id)
        if ok:
            log_audit(chat_id, 'complete_task', f'id={rem_id}')
            await mark_message_status(query, '\n\n🏁 *Выполнено!*')
            first = record_activity(chat_id, 'tasks')
            await query.message.reply_text(get_motivation())
            if first:
                current, best = get_streak(chat_id)
                await query.message.reply_text(f'🔥 Новый день стрика: {current} дней подряд (лучший: {best}).')
        else:
            await query.message.reply_text('❌ Дело не найдено или уже выполнено.')
        return
    if data.startswith('goal_done:'):
        try:
            goal_id = int(data.split(':', 1)[1])
        except ValueError:
            await query.message.reply_text('Некорректный ID цели.')
            return
        result = mark_goal_done(chat_id, goal_id)
        if not result:
            await query.message.reply_text('❌ Цель не найдена или уже выполнена.')
            return
        text, ddl_dt, done_dt = result
        log_audit(chat_id, 'complete_goal', f'id={goal_id}, {text}')
        await mark_message_status(query, '\n\n🏁 *Цель выполнена!*')
        ai_text = ai_generate_goal_done_comment(chat_id, text, ddl_dt, done_dt)
        if ai_text:
            await query.message.reply_text(f'🤖 От дневника:\n{ai_text}')
        else:
            await query.message.reply_text('Горжусь тобой, цель закрыта 💪')
        await query.message.reply_text(get_motivation())
        first = record_activity(chat_id, 'goals')
        if first:
            current, best = get_streak(chat_id)
            await query.message.reply_text(f'🔥 Новый день стрика: {current} дней подряд (лучший: {best}).')
        return
    if data.startswith('cal:'):
        parts = data.split(':')
        if len(parts) >= 2 and parts[1] == 'ignore':
            return
        if len(parts) >= 4 and parts[1] == 'nav':
            try:
                year = int(parts[2])
                month = int(parts[3])
            except ValueError:
                return
            kb = build_calendar(year, month)
            await query.message.edit_reply_markup(reply_markup=kb)
            return
        if len(parts) >= 3 and parts[1] == 'day':
            date_str = parts[2]
            try:
                picked = datetime.strptime(date_str, '%Y-%m-%d').date()
            except ValueError:
                await query.message.reply_text('Не удалось распознать дату, попробуй ещё раз.')
                return
            context.user_data['new_date'] = picked
            kb = build_hour_keyboard()
            creation_type = context.user_data.get('creation_type', 'reminder')
            if creation_type == 'goal':
                label = 'дедлайна цели'
            else:
                label = 'дела'
            await query.message.reply_text(f"📅 Дата {label}: {picked.strftime('%d.%m.%Y')}\nТеперь выбери *час*:", parse_mode='Markdown', reply_markup=kb)
            return
    if data.startswith('time:hour:'):
        try:
            hour = int(data.split(':')[2])
        except ValueError:
            await query.message.reply_text('Не удалось распознать час, попробуй ещё раз.')
            return
        context.user_data['new_hour'] = hour
        kb = build_minute_keyboard()
        await query.message.reply_text(f'🕒 Час: {hour:02d}\nТеперь выбери *минуты*:', parse_mode='Markdown', reply_markup=kb)
        return
    if data.startswith('time:min:'):
        try:
            minute = int(data.split(':')[2])
        except ValueError:
            await query.message.reply_text('Не удалось распознать минуты, попробуй ещё раз.')
            return
        picked_date: Optional[date] = context.user_data.get('new_date')
        picked_hour: Optional[int] = context.user_data.get('new_hour')
        if picked_date is None or picked_hour is None:
            await query.message.reply_text('Что-то пошло не так. Попробуй начать заново через /start.', reply_markup=main_menu_keyboard())
            context.user_data.clear()
            return
        dt = datetime(year=picked_date.year, month=picked_date.month, day=picked_date.day, hour=picked_hour, minute=minute)
        if dt <= now():
            await query.message.reply_text('⛔ Это время уже в прошлом. Попробуй выбрать будущий момент.', reply_markup=main_menu_keyboard())
            context.user_data.clear()
            return
        context.user_data['new_dt'] = dt
        creation_type = context.user_data.get('creation_type', 'reminder')
        if context.user_data.get('new_text'):
            context.user_data['flow'] = 'new_item'
            context.user_data['awaiting_text'] = False
            await query.message.reply_text('Выбери приоритет для этого дела:', reply_markup=priority_keyboard())
            return
        context.user_data['awaiting_text'] = True
        context.user_data['flow'] = 'new_item'
        if creation_type == 'goal':
            what = 'цели'
        elif creation_type == 'task':
            what = 'задачи'
        else:
            what = 'напоминания'
        await query.message.reply_text(f'📌 Момент для {what}: *{format_dt(dt)}*\nТеперь напиши *текст {what}* обычным сообщением.\nМожно добавить теги через #, например: `Сделать ДЗ #школа #математика`.\nМожно прикрепить фото/видео — я их привяжу к этому делу.', parse_mode='Markdown')
        return
    if data.startswith('priority:'):
        val = data.split(':', 1)[1]
        text: Optional[str] = context.user_data.get('new_text')
        dt: Optional[datetime] = context.user_data.get('new_dt')
        creation_type = context.user_data.get('creation_type', 'reminder')
        from_template = context.user_data.get('from_template', False)
        attachments: Optional[str] = context.user_data.get('attachments')
        if not text:
            await query.message.reply_text('Текст дела потерялся. Попробуй ещё раз через /start.', reply_markup=main_menu_keyboard())
            context.user_data.clear()
            return
        if val == 'skip':
            priority = 2
        else:
            try:
                priority = int(val)
            except ValueError:
                priority = 2
        if dt is None:
            dt = now()
        if creation_type == 'goal':
            add_goal(chat_id, text, dt, priority=priority)
            log_audit(chat_id, 'create_goal', f'{format_dt(dt)} | {text}')
            now_dt = now()
            delta_str = format_delta(dt, now_dt)
            await query.message.reply_text(f'🎯 Цель сохранена!\n⏳ Дедлайн: *{format_dt(dt)}* ({delta_str})\n🎯 Цель: {text}', parse_mode='Markdown', reply_markup=goals_menu_keyboard())
            if from_template:
                await query.message.reply_text('🧠 Думаю над черновиком плана по этой цели...')
                plan_text = ai_generate_plan(text)
                await query.message.reply_text(f'📝 Черновик плана по цели:\n\n{plan_text}', parse_mode='Markdown')
            first = record_activity(chat_id, 'goals')
            if first:
                current, best = get_streak(chat_id)
                await query.message.reply_text(f'🔥 Новый день стрика: {current} дней подряд (лучший: {best}).')
        else:
            kind = creation_type
            add_reminder(chat_id, text, dt, kind=kind, priority=priority, attachments=attachments)
            if kind == 'reminder':
                kind_emoji = '🔔'
                when_part = f'🕒 *Когда:* {format_dt(dt)}\n'
                action = 'create_reminder'
            elif kind == 'task':
                kind_emoji = '✅'
                when_part = f'🕒 *Срок:* {format_dt(dt)}\n'
                action = 'create_task'
            else:
                kind_emoji = '📝'
                when_part = '🕒 *Без конкретного срока*\n'
                action = 'create_todo'
            log_audit(chat_id, action, f'{format_dt(dt)} | {text}')
            await query.message.reply_text(f'{kind_emoji} Дело сохранено!\n{when_part}📝 *Что:* {text}\nПриоритет: {priority_emoji(priority)}', parse_mode='Markdown', reply_markup=main_menu_keyboard())
            first = record_activity(chat_id, 'tasks')
            if first:
                current, best = get_streak(chat_id)
                await query.message.reply_text(f'🔥 Новый день стрика: {current} дней подряд (лучший: {best}).')
        context.user_data.clear()
        return

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_subscribed(update, context):
        return
    touch_user_from_update(update)
    msg = update.message
    chat_id = msg.chat_id
    text = (msg.text or msg.caption or '').strip()
    attachments_json = extract_attachments_from_message(msg)
    has_media = bool(attachments_json)
    flow = context.user_data.get('flow')
    if flow == 'search' and context.user_data.get('awaiting_search_query'):
        if not text:
            await msg.reply_text('Запрос пустой. Напиши слово или #тег, по которому искать.')
            return
        log_audit(chat_id, 'search', text[:200])
        result = build_search_results(chat_id, text)
        context.user_data.clear()
        await msg.reply_text(result, parse_mode='Markdown', reply_markup=main_menu_keyboard())
        return
    if flow == 'new_item' and context.user_data.get('awaiting_text'):
        if not text and (not has_media):
            await msg.reply_text('Текст пустой. Напиши, что хочешь сделать или прикрепи медиа.')
            return
        new_text = text if text else 'Дело без текста (есть вложение)'
        context.user_data['new_text'] = new_text
        context.user_data['attachments'] = attachments_json
        context.user_data['awaiting_text'] = False
        await msg.reply_text('Выбери приоритет для этого дела:', reply_markup=priority_keyboard())
        return
    if flow == 'new_habit':
        if not text:
            await msg.reply_text('Название привычки не может быть пустым. Напиши, что именно хочешь отслеживать.')
            return
        add_habit(chat_id, text)
        log_audit(chat_id, 'create_habit', text)
        context.user_data.clear()
        await msg.reply_text(f'📊 Привычка добавлена: {text}', reply_markup=habits_menu_keyboard())
        first = record_activity(chat_id, 'habits')
        if first:
            current, best = get_streak(chat_id)
            await msg.reply_text(f'🔥 Новый день стрика: {current} дней подряд (лучший: {best}).')
        return
    if flow == 'new_journal':
        if not text and (not has_media):
            await msg.reply_text('Запись пустая. Напиши хоть что-нибудь или прикрепи медиа 🙂')
            return
        entry_text = text if text else '[Запись без текста, только медиа]'
        add_journal_entry(chat_id, entry_text, attachments=attachments_json)
        log_audit(chat_id, 'journal_entry', entry_text[:300])
        context.user_data.clear()
        await msg.reply_text('📔 Запись сохранена в дневнике.', reply_markup=journal_menu_keyboard())
        first = record_activity(chat_id, 'journal')
        if first:
            current, best = get_streak(chat_id)
            await msg.reply_text(f'🔥 Новый день в твоём стрике! Уже {current} дней подряд (лучший: {best}).')
        return
    if flow == 'plan_text':
        if not text:
            await msg.reply_text('Опиши, что ты хочешь спланировать.')
            return
        log_audit(chat_id, 'plan', text)
        await msg.reply_text('Думаю над планом... 🤔')
        plan_text = ai_generate_plan(text)
        await msg.reply_text(plan_text)
        context.user_data.clear()
        first = record_activity(chat_id, 'meta')
        if first:
            current, best = get_streak(chat_id)
            await msg.reply_text(f'🔥 Ты начал новый день с планирования. Стрик: {current} дней подряд (лучший: {best}).')
        return
    if flow == 'week_reflection':
        if not text:
            await msg.reply_text('Напиши пару мыслей о прошедшей неделе — хотя бы в одном-двух предложениях 🙂')
            return
        period = context.user_data.get('week_reflection_period', '')
        entry = f'[Рефлексия недели {period}]\n{text}'
        add_journal_entry(chat_id, entry, attachments=None)
        log_audit(chat_id, 'journal_entry', entry[:300])
        context.user_data.clear()
        await msg.reply_text('🙏 Сохранил твою рефлексию недели в дневнике.', reply_markup=main_menu_keyboard())
        first = record_activity(chat_id, 'journal')
        if first:
            current, best = get_streak(chat_id)
            await msg.reply_text(f'🔥 Новый день в твоём стрике! Уже {current} дней подряд (лучший: {best}).')
        return
    await msg.reply_text('Я не до конца понял, что ты хочешь сделать.\nИспользуй кнопки или команды `/start`, `/help`, `/stats`, `/plan`, `/week_review`, `/export`.', reply_markup=main_menu_keyboard())

async def check_job(context: ContextTypes.DEFAULT_TYPE):
    now_dt = now()
    pending = get_pending_reminders(now_dt)
    for rem_id, chat_id, text, ai_comment, attachments_str, kind, priority in pending:
        try:
            attachments = parse_attachments(attachments_str)
            kind_emoji = '🔔' if kind == 'reminder' else '✅'
            caption = f'⏰ {kind_emoji} *Напоминание!*\n\n📝 {text}'
            if ai_comment:
                caption += f'\n\n🤖 От дневника: {ai_comment}'
            await send_with_attachments(context.bot, chat_id, attachments, caption, reply_markup=reminder_keyboard(rem_id), parse_mode='Markdown')
            mark_reminder_sent(rem_id)
            log_audit(chat_id, 'auto_reminder_sent', text[:200])
        except Exception as e:
            logger.error(e)
    overdue_goals = get_goals_for_overdue_notification(now_dt)
    for goal_id, chat_id, text, deadline in overdue_goals:
        try:
            settings = get_user_settings(chat_id)
            ddl_dt = datetime.fromisoformat(deadline)
            delta_str = format_delta(ddl_dt, now_dt)
            if settings.get('panic_notifications', 1):
                msg = f'⏰ *Цель просрочена!*\n\n🎯 Цель: {text}\n⏳ Дедлайн был: {format_dt(ddl_dt)} ({delta_str})\n\nЕсли она всё ещё важна — всё равно можешь сделать её и отметить как `✅ Выполнено` в разделе `🎯 Цели`.'
                await context.bot.send_message(chat_id, msg, parse_mode='Markdown')
            log_audit(chat_id, 'goal_overdue_notify', f'{format_dt(ddl_dt)} | {text[:200]}')
            mark_goal_overdue_notified(goal_id)
        except Exception as e:
            logger.error(e)

async def members_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    if not is_admin(chat_id):
        await update.message.reply_text('Команда доступна только админам.')
        return
    touch_user_from_update(update)
    sort = 'joined'
    page = 1
    if context.args:
        for arg in context.args:
            if arg.startswith('sort='):
                v = arg.split('=', 1)[1]
                if v in ('joined', 'name', 'active'):
                    sort = v
            elif arg.startswith('page='):
                try:
                    page = int(arg.split('=', 1)[1])
                except ValueError:
                    pass
    if page < 1:
        page = 1
    rows, total, total_pages, current_page = get_users_page(sort, page, page_size=50)
    if total == 0 or not rows:
        await update.message.reply_text('Пока нет ни одного пользователя в БД.')
        return
    lines = []
    lines.append('👥 Пользователи бота')
    lines.append(f'Всего: {total}')
    lines.append(f'Страница: {current_page}/{total_pages}, сортировка: {sort}')
    lines.append('')
    start_index = (current_page - 1) * 50 + 1
    for idx, (uid, username, first_name, last_name, joined_at, last_active_at) in enumerate(rows, start=start_index):
        name_parts = [p for p in [first_name, last_name] if p]
        display_name = ' '.join(name_parts) if name_parts else '(без имени)'
        uname = f'@{username}' if username else '-'
        try:
            joined_dt = datetime.fromisoformat(joined_at)
            joined_str = format_dt(joined_dt)
        except Exception:
            joined_str = joined_at or '-'
        try:
            active_dt = datetime.fromisoformat(last_active_at)
            active_str = format_dt(active_dt)
        except Exception:
            active_str = last_active_at or '-'
        lines.append(f'{idx}. {display_name} {uname}\n   ID: {uid}\n   Вход: {joined_str}\n   Активность: {active_str}')
    text = '\n'.join(lines)
    await update.message.reply_text(text, disable_web_page_preview=True)

async def members_export_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    if not is_admin(chat_id):
        await update.message.reply_text('Команда доступна только админам.')
        return
    touch_user_from_update(update)
    rows = get_all_users()
    if not rows:
        await update.message.reply_text('Пока нет пользователей для экспорта.')
        return
    with tempfile.NamedTemporaryFile('w+', delete=False, suffix='.csv', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(['chat_id', 'username', 'first_name', 'last_name', 'joined_at', 'last_active_at'])
        for uid, username, first_name, last_name, joined_at, last_active_at in rows:
            writer.writerow([uid, username or '', first_name or '', last_name or '', joined_at or '', last_active_at or ''])
        fname = f.name
    await update.message.reply_document(document=InputFile(open(fname, 'rb'), filename='users_export.csv'), caption="Экспорт пользователей (CSV с разделителем ';', можно открывать в Excel).")

async def debug_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    if not is_admin(chat_id):
        await update.message.reply_text('Команда доступна только админам.')
        return
    touch_user_from_update(update)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM users')
    users_cnt = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM reminders')
    reminders_cnt = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM goals')
    goals_cnt = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM habits')
    habits_cnt = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM journal_entries')
    journal_cnt = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM daily_activity')
    activity_cnt = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM audit_log')
    audit_cnt = cur.fetchone()[0]
    conn.close()
    text = f'🛠 DEBUG\nusers: {users_cnt}\nreminders: {reminders_cnt}\ngoals: {goals_cnt}\nhabits: {habits_cnt}\njournal_entries: {journal_cnt}\ndaily_activity: {activity_cnt}\naudit_log: {audit_cnt}\n'
    await update.message.reply_text(text)

def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()
    app.job_queue.run_repeating(check_job, interval=60, first=5)
    app.job_queue.run_repeating(check_day_review, interval=600, first=60)
    app.job_queue.run_repeating(check_inactive_job, interval=3600, first=120)
    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('help', help_cmd))
    app.add_handler(CommandHandler('export', export_cmd))
    app.add_handler(CommandHandler('plan', plan_cmd))
    app.add_handler(CommandHandler('week_review', week_review_cmd))
    app.add_handler(CommandHandler('stats', stats_cmd))
    app.add_handler(CommandHandler('members', members_cmd))
    app.add_handler(CommandHandler('members_export', members_export_cmd))
    app.add_handler(CommandHandler('debug', debug_cmd))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, on_text))
    app.run_polling()
if __name__ == '__main__':
    main()
