import os
import re
import io
import time
import random
import asyncio
import logging
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import aiohttp
from aiohttp import web

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandObject
from aiogram.enums import ChatAction
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BotCommand,
    BotCommandScopeDefault,
    BotCommandScopeChat,
    ReactionTypeEmoji,
    BufferedInputFile
)
from dotenv import load_dotenv

try:
    import google.generativeai as genai
    HAS_GENAI = True
except ImportError:
    HAS_GENAI = False

try:
    from huggingface_hub import InferenceClient
    HAS_HF_HUB = True
except ImportError:
    HAS_HF_HUB = False

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
try:
    ADMIN_ID = int(os.getenv("ADMIN_ID", "0").strip() or "0")
except Exception:
    ADMIN_ID = 0

PORT = int(os.getenv("PORT", 8080))

DB_FILE = "bot_chats.db"
MSK_TIMEZONE = timezone(timedelta(hours=3))

manual_sleep_mode = False
manual_sleep_reason = "технический перерыв"

# Глубина непрерывной памяти (сколько последних сообщений загружать в контекст ИИ)
MEMORY_HISTORY_LIMIT = 30
# Сколько сообщений хранить в БД на пару чат+пользователь (защита от раздувания БД)
MEMORY_DB_KEEP = 300

# --- ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ ---
def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS registered_chats (
                chat_id INTEGER PRIMARY KEY,
                chat_type TEXT,
                chat_title TEXT,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_balances (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                custom_nick TEXT,
                balance INTEGER DEFAULT 1000,
                last_bonus TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bot_settings (
                setting_key TEXT PRIMARY KEY,
                setting_val TEXT
            )
        """)
        # Память диалога: теперь с привязкой к конкретному пользователю
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chat_memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                user_id INTEGER,
                role TEXT,
                content TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_facts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                user_id INTEGER,
                fact TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Миграция старой таблицы памяти (без колонки user_id)
        cursor.execute("PRAGMA table_info(chat_memory)")
        cols = [c[1] for c in cursor.fetchall()]
        if "user_id" not in cols:
            cursor.execute("ALTER TABLE chat_memory ADD COLUMN user_id INTEGER DEFAULT 0")
        conn.commit()

init_db()

# --- ПАМЯТЬ (ПО КОНКРЕТНОМУ ЧЕЛОВЕКУ) ---
def save_message_to_memory(chat_id: int, user_id: int, role: str, text: str):
    """Сохраняет сообщение в постоянную память конкретного пользователя"""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO chat_memory (chat_id, user_id, role, content) VALUES (?, ?, ?, ?)",
                (chat_id, user_id, role, text[:4000])
            )
            # Автоматическая обрезка: оставляем последние MEMORY_DB_KEEP сообщений
            cursor.execute(
                """DELETE FROM chat_memory WHERE chat_id = ? AND user_id = ? AND id NOT IN
                   (SELECT id FROM chat_memory WHERE chat_id = ? AND user_id = ? ORDER BY id DESC LIMIT ?)""",
                (chat_id, user_id, chat_id, user_id, MEMORY_DB_KEEP)
            )
            conn.commit()
    except Exception as e:
        logging.error(f"Ошибка сохранения памяти: {e}")

def get_chat_memory(chat_id: int, user_id: int, limit: int = MEMORY_HISTORY_LIMIT) -> list:
    """Извлекает историю диалога конкретного пользователя в хронологическом порядке"""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT role, content FROM chat_memory WHERE chat_id = ? AND user_id = ? ORDER BY id DESC LIMIT ?",
                (chat_id, user_id, limit)
            )
            rows = cursor.fetchall()
            return [{"role": r[0], "text": r[1]} for r in reversed(rows)]
    except Exception as e:
        logging.error(f"Ошибка чтения памяти: {e}")
        return []

def clear_chat_memory(chat_id: int, user_id: int | None = None):
    """Очищает память диалога (конкретного пользователя или всего чата)"""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            if user_id is None:
                cursor.execute("DELETE FROM chat_memory WHERE chat_id = ?", (chat_id,))
            else:
                cursor.execute("DELETE FROM chat_memory WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))
            conn.commit()
    except Exception as e:
        logging.error(f"Ошибка очистки памяти: {e}")

# --- ФАКТЫ О ПОЛЬЗОВАТЕЛЕ (долгосрочная память) ---
def save_user_fact(chat_id: int, user_id: int, fact: str):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO user_facts (chat_id, user_id, fact) VALUES (?, ?, ?)",
                (chat_id, user_id, fact[:500])
            )
            # Не более 20 фактов на человека в чате
            cursor.execute(
                """DELETE FROM user_facts WHERE chat_id = ? AND user_id = ? AND id NOT IN
                   (SELECT id FROM user_facts WHERE chat_id = ? AND user_id = ? ORDER BY id DESC LIMIT 20)""",
                (chat_id, user_id, chat_id, user_id)
            )
            conn.commit()
    except Exception as e:
        logging.error(f"Ошибка сохранения факта: {e}")

def get_user_facts(chat_id: int, user_id: int) -> list:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT fact FROM user_facts WHERE chat_id = ? AND user_id = ? ORDER BY id DESC LIMIT 20",
                (chat_id, user_id)
            )
            return [r[0] for r in cursor.fetchall()]
    except Exception:
        return []

def delete_user_fact(chat_id: int, user_id: int, idx: int) -> bool:
    facts = get_user_facts(chat_id, user_id)
    if 0 <= idx < len(facts):
        try:
            with sqlite3.connect(DB_FILE) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM user_facts WHERE chat_id = ? AND user_id = ? AND fact = ?",
                    (chat_id, user_id, facts[idx])
                )
                conn.commit()
            return True
        except Exception:
            return False
    return False

def build_user_context(chat_id: int, user_id: int, display_name: str) -> str:
    """Собирает блок контекста о пользователе для системного промпта"""
    facts = get_user_facts(chat_id, user_id)
    if not facts:
        return ""
    lines = "\n".join(f"• {f}" for f in facts)
    return f"\n\nФакты о пользователе {display_name} (запомни их и учитывай в ответах):\n{lines}"

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ БАЗЫ ---
def save_chat_to_db(chat: types.Chat):
    title = chat.title or chat.full_name or chat.username or "Без названия"
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO registered_chats (chat_id, chat_type, chat_title, last_seen) VALUES (?, ?, ?, CURRENT_TIMESTAMP)", (chat.id, chat.type, title))
        conn.commit()

def get_all_chats_from_db():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT chat_id, chat_type, chat_title FROM registered_chats")
        return cursor.fetchall()

def get_db_setting(key: str, default: str = "") -> str:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT setting_val FROM bot_settings WHERE setting_key = ?", (key,))
            row = cursor.fetchone()
            return row[0] if row else default
    except Exception:
        return default

def set_db_setting(key: str, val: str):
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO bot_settings (setting_key, setting_val) VALUES (?, ?)", (key, val))
        conn.commit()

def save_user_profile(user: types.User):
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT balance FROM user_balances WHERE user_id = ?", (user.id,))
        row = cursor.fetchone()
        init_bal = 1_000_000 if (ADMIN_ID != 0 and user.id == ADMIN_ID) else 1000
        if row is None:
            cursor.execute("INSERT INTO user_balances (user_id, username, full_name, balance) VALUES (?, ?, ?, ?)", (user.id, user.username or "", user.full_name, init_bal))
        else:
            cursor.execute("UPDATE user_balances SET username = ?, full_name = ? WHERE user_id = ?", (user.username or "", user.full_name, user.id))
        conn.commit()

def get_display_name(user_id: int, fallback_name: str = "Игрок") -> str:
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT custom_nick, full_name, username FROM user_balances WHERE user_id = ?", (user_id,))
        row = cursor.fetchone()
        if row:
            if row[0] and row[0].strip():
                return row[0].strip()
            if row[1] and row[1].strip():
                return row[1].strip()
            if row[2] and row[2].strip():
                return f"@{row[2].strip()}"
        return fallback_name

def get_user_balance(user_id: int) -> int:
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT balance FROM user_balances WHERE user_id = ?", (user_id,))
        row = cursor.fetchone()
        if row is not None:
            return row[0]
        init_bal = 1_000_000 if (ADMIN_ID != 0 and user_id == ADMIN_ID) else 1000
        cursor.execute("INSERT INTO user_balances (user_id, balance) VALUES (?, ?)", (user_id, init_bal))
        conn.commit()
        return init_bal

def alter_user_balance(user_id: int, delta: int) -> int:
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT balance FROM user_balances WHERE user_id = ?", (user_id,))
        row = cursor.fetchone()
        if row is None:
            new_bal = max(0, 1000 + delta)
            cursor.execute("INSERT INTO user_balances (user_id, balance) VALUES (?, ?)", (user_id, new_bal))
        else:
            new_bal = max(0, row[0] + delta)
            cursor.execute("UPDATE user_balances SET balance = ? WHERE user_id = ?", (new_bal, user_id))
        conn.commit()
        return new_bal

def set_exact_balance(user_id: int, val: int):
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO user_balances (user_id, balance) VALUES (?, ?) ON CONFLICT(user_id) DO UPDATE SET balance = ?", (user_id, val, val))
        conn.commit()

def parse_int(text: str) -> int | None:
    """Безопасный парсинг целого числа"""
    try:
        return int(text.strip().replace(",", "").replace(" ", ""))
    except (ValueError, AttributeError):
        return None

# --- МОДЕЛИ И КЛЮЧИ ---
raw_gemini_keys = os.getenv("GEMINI_API_KEY", "")
GEMINI_KEYS = [k.strip() for k in re.split(r'[,;\s\n]+', raw_gemini_keys) if k.strip()]
current_gemini_index = 0

raw_hf_tokens = os.getenv("HF_TOKEN", "")
HF_TOKENS = [t.strip() for t in re.split(r'[,;\s\n]+', raw_hf_tokens) if t.strip()]
current_hf_index = 0

AVAILABLE_MODELS = {
    "gemini": {"name": "✨ Gemini 2.5 Flash", "provider": "google", "model_id": "gemini-2.5-flash"},
    "gemma": {"name": "💎 Gemma 3 27B (HF)", "provider": "huggingface", "model_id": "google/gemma-3-27b-it:preferred"},
    "glm": {"name": "🌟 GLM 4.5 Air (HF)", "provider": "huggingface", "model_id": "zai-org/GLM-4.5-Air:preferred"},
    "qwen": {"name": "🇨🇳 Qwen 3 32B (HF)", "provider": "huggingface", "model_id": "Qwen/Qwen3-32B:preferred"},
}

current_active_model_key = get_db_setting("active_model", "gemini")
if current_active_model_key not in AVAILABLE_MODELS:
    current_active_model_key = "gemini"

# Бесплатные модели изображений через HF Router (работают с обычным HF_TOKEN,
# бесплатный тариф: https://huggingface.co/settings/tokens — без карты)
HF_TEXT2IMG_MODELS = ["black-forest-labs/FLUX.1-schnell", "stabilityai/stable-diffusion-3.5-large-turbo"]
HF_IMG2IMG_MODELS = ["Qwen/Qwen-Image-Edit", "timbrooks/instruct-pix2pix"]

TRIGGER_WORDS = ["ланикс", "латекс", "линукс", "lanix", "linux"]
REACTIONS_POOL = ["🔥", "⚡", "👍", "💡", "🤖", "🚀", "❤️", "🎉", "👀", "👌"]

CASINO_HOURLY_LIMIT = 5
user_casino_spins = defaultdict(list)
user_football_kicks = defaultdict(list)

ttt_games = {}
ttt_game_id_counter = 1

WIN_COMBINATIONS = [
    (0, 1, 2), (3, 4, 5), (6, 7, 8),
    (0, 3, 6), (1, 4, 7), (2, 5, 8),
    (0, 4, 8), (2, 4, 6)
]

active_puzzles = {}
puzzle_id_counter = 1
PUZZLE_BANK = [
    {"question": "🧩 У меня есть города, но нет домов; леса, но нет деревьев; реки, но нет воды. Что я такое?", "options": ["Карта", "Глобус", "Пустыня", "Зеркало"], "correct": 0},
    {"question": "🧩 Что летает без крыльев, плачет без глаз и никогда не возвращается?", "options": ["Ветер", "Облако", "Время", "Эхо"], "correct": 1},
    {"question": "🧩 Если у вас есть 3 яблока и вы забрали 2, сколько яблок у вас?", "options": ["1 яблоко", "2 яблока", "3 яблока", "0 яблок"], "correct": 1},
    {"question": "🧩 У какого колеса автомобиля нет износа при движении на полной скорости?", "options": ["Переднее левое", "Заднее правое", "Запасное", "Все изнашиваются"], "correct": 2},
    {"question": "🧩 Числовая загадка: 2 + 2 * 2 = ?", "options": ["8", "6", "4", "16"], "correct": 1}
]

WINDOW_SECONDS = 30
MAX_STICKERS_LIMIT = 5
MAX_DUPLICATE_MESSAGES = 5
MAX_REPEATED_WORDS_IN_MSG = 5

PRESET_MODES = {
    "default": {"title": "🤖 Стандартный", "prompt": "Ты вежливый, живой и универсальный чат-помощник Ланикс. Отвечай понятно, структурированно и по делу. У тебя отличная память: учитывай всю историю беседы с пользователем."},
    "coder": {"title": "💻 Программист", "prompt": "Ты Senior Fullstack разработчик Ланикс. Пиши чистый, оптимизированный код с комментариями, помни контекст проекта пользователя."},
    "translator": {"title": "🌍 Переводчик", "prompt": "Ты профессиональный переводчик. Переводи текст, сохраняя естественность и контекст беседы."},
    "creative": {"title": "💡 Креативщик", "prompt": "Ты генератор идей и копирайтер Ланикс. Пиши живо, образно и с юмором, помни всё, что предлагал ранее."},
    "brief": {"title": "⚡ Кратко", "prompt": "Отвечай максимально кратко, тезисно, без вступлений и заключений."}
}

current_mode_name = get_db_setting("current_mode_name", "default")
current_system_prompt = get_db_setting("custom_system_prompt", PRESET_MODES.get(current_mode_name, PRESET_MODES["default"])["prompt"])

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
user_tracker = defaultdict(lambda: {"stickers": [], "messages": []})
_bot_info_cache = None

async def get_bot_info():
    """Кэшированный get_me — не дергаем API на каждое сообщение"""
    global _bot_info_cache
    if _bot_info_cache is None:
        _bot_info_cache = await bot.get_me()
    return _bot_info_cache

def is_admin(user_id: int) -> bool:
    return ADMIN_ID != 0 and user_id == ADMIN_ID

def is_bot_sleeping() -> tuple[bool, str]:
    if manual_sleep_mode:
        return True, manual_sleep_reason
    now_msk = datetime.now(MSK_TIMEZONE)
    if 0 <= now_msk.hour < 6:
        return True, "ночной технический перерыв (с 00:00 до 06:00 по МСК)"
    return False, ""

def check_game_hourly_limit(user_id: int, storage: dict) -> tuple[bool, int, int]:
    now = time.time()
    storage[user_id] = [ts for ts in storage[user_id] if now - ts < 3600]
    if len(storage[user_id]) >= CASINO_HOURLY_LIMIT:
        oldest = storage[user_id][0]
        wait_minutes = max(1, int((3600 - (now - oldest)) // 60) + 1)
        return False, 0, wait_minutes
    storage[user_id].append(now)
    return True, CASINO_HOURLY_LIMIT - len(storage[user_id]), 0

# --- ГЕНЕРАЦИЯ ИЗОБРАЖЕНИЙ (HF Router — бесплатно с обычным токеном) ---
async def _hf_image_call(token: str, prompt: str, input_image_bytes: bytes | None) -> bytes:
    client = InferenceClient(token=token)
    def _sync_call():
        if input_image_bytes:
            last_err = None
            for model in HF_IMG2IMG_MODELS:
                try:
                    pil_img = client.image_to_image(input_image_bytes, prompt=prompt, model=model)
                    buf = io.BytesIO()
                    pil_img.save(buf, format="PNG")
                    return buf.getvalue()
                except Exception as e:
                    last_err = e
            raise last_err
        else:
            last_err = None
            for model in HF_TEXT2IMG_MODELS:
                try:
                    pil_img = client.text_to_image(prompt, model=model)
                    buf = io.BytesIO()
                    pil_img.save(buf, format="PNG")
                    return buf.getvalue()
                except Exception as e:
                    last_err = e
            raise last_err
    return await asyncio.to_thread(_sync_call)

async def _pollinations_image(prompt: str) -> bytes:
    """Запасной бесплатный генератор — без какого-либо ключа"""
    from urllib.parse import quote
    url = f"https://image.pollinations.ai/prompt/{quote(prompt)}?width=1024&height=1024&nologo=true"
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
        async with session.get(url) as r:
            if r.status == 200:
                data = await r.read()
                if len(data) > 10000:
                    return data
    raise Exception("Pollinations не ответил")

async def generate_or_edit_image(prompt: str, input_image_bytes: bytes | None = None) -> bytes:
    global current_hf_index
    last_error = None
    if HF_TOKENS and HAS_HF_HUB:
        for _ in range(len(HF_TOKENS)):
            active_token = HF_TOKENS[current_hf_index % len(HF_TOKENS)]
            try:
                return await _hf_image_call(active_token, prompt, input_image_bytes)
            except Exception as e:
                last_error = e
                current_hf_index = (current_hf_index + 1) % len(HF_TOKENS)
                await asyncio.sleep(0.3)
    # Фолбэк без ключа: текст-в-картинку через Pollinations
    if not input_image_bytes:
        try:
            return await _pollinations_image(prompt)
        except Exception as e:
            last_error = e
    raise Exception(f"Image error: {last_error}")

# --- НЕЙРОСЕТИ (ТЕКСТ С ПОДДЕРЖКОЙ ПАМЯТИ) ---
async def generate_ai_response(prompt: str, system_prompt: str, image_bytes: bytes | None = None,
                               history: list | None = None, user_context: str = "") -> str:
    global current_gemini_index, current_hf_index
    chosen = AVAILABLE_MODELS.get(current_active_model_key, AVAILABLE_MODELS["gemini"])
    full_system = system_prompt + user_context

    if chosen["provider"] == "google" and GEMINI_KEYS and HAS_GENAI:
        try:
            active_key = GEMINI_KEYS[current_gemini_index % len(GEMINI_KEYS)]
            genai.configure(api_key=active_key)
            model = genai.GenerativeModel(model_name=chosen["model_id"], system_instruction=full_system)

            contents = []
            if history:
                for msg in history:
                    r = "user" if msg["role"] == "user" else "model"
                    contents.append({"role": r, "parts": [msg["text"]]})

            if image_bytes:
                contents.append({"role": "user", "parts": [{"mime_type": "image/jpeg", "data": image_bytes}, prompt]})
            else:
                contents.append({"role": "user", "parts": [prompt]})

            resp = await model.generate_content_async(contents)
            return resp.text or "Пустой ответ."
        except Exception as e:
            logging.warning(f"Gemini error: {e}")
            current_gemini_index = (current_gemini_index + 1) % len(GEMINI_KEYS)

    if HF_TOKENS:
        hf_model = chosen["model_id"] if chosen["provider"] == "huggingface" else "google/gemma-3-27b-it:preferred"
        for _ in range(len(HF_TOKENS)):
            active_token = HF_TOKENS[current_hf_index % len(HF_TOKENS)]
            try:
                url = "https://router.huggingface.co/v1/chat/completions"
                headers = {"Authorization": f"Bearer {active_token}", "Content-Type": "application/json"}
                messages_payload = [{"role": "system", "content": full_system}]
                if history:
                    for msg in history:
                        messages_payload.append({"role": msg["role"], "content": msg["text"]})
                messages_payload.append({"role": "user", "content": prompt})
                payload = {"model": hf_model, "messages": messages_payload, "max_tokens": 2048}
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=45)) as session:
                    async with session.post(url, headers=headers, json=payload) as r:
                        if r.status == 200:
                            data = await r.json()
                            return data["choices"][0]["message"]["content"]
                        raise Exception(f"HF status {r.status}: {await r.text()[:200]}")
            except Exception as e:
                logging.warning(f"HF error: {e}")
                current_hf_index = (current_hf_index + 1) % len(HF_TOKENS)
                await asyncio.sleep(0.3)

    return "⚠️ Не удалось получить ответ от нейросети. Попробуйте еще раз."

# --- АНТИСПАМ И ТРИГГЕРЫ ---
SPAM_PATTERNS = [r"казино\s*онлайн", r"casino\s*online", r"ставки на спорт", r"1win", r"1xbet", r"заработок в интернете", r"криптосигнал", r"t\.me/\+[a-zA-Z0-9_\-]+", r"интим[а-я]*", r"порно"]

def check_spam_and_flood(message: types.Message) -> tuple[bool, str]:
    chat_id, user_id, now = message.chat.id, message.from_user.id, time.time()
    record = user_tracker[(chat_id, user_id)]
    record["stickers"] = [ts for ts in record["stickers"] if now - ts < WINDOW_SECONDS]
    record["messages"] = [(ts, txt) for ts, txt in record["messages"] if now - ts < WINDOW_SECONDS]

    if message.sticker:
        record["stickers"].append(now)
        if len(record["stickers"]) > MAX_STICKERS_LIMIT:
            record["stickers"].clear()
            return True, f"флуд стикерами (более {MAX_STICKERS_LIMIT} за {WINDOW_SECONDS}с)"

    msg_text = message.text or message.caption or ""
    if msg_text:
        lowered = msg_text.strip().lower()
        for p in SPAM_PATTERNS:
            if re.search(p, lowered):
                return True, "запрещенные спам-слова / реклама"
        words = re.findall(r'\b[a-zA-Zа-яА-ЯёЁ0-9_-]{3,}\b', lowered)
        if words:
            counts = {}
            for w in words:
                counts[w] = counts.get(w, 0) + 1
                if counts[w] >= MAX_REPEATED_WORDS_IN_MSG:
                    return True, f"повтор слова более {MAX_REPEATED_WORDS_IN_MSG} раз («{w}»)"
        record["messages"].append((now, lowered))
        if sum(1 for _, txt in record["messages"] if txt == lowered) >= MAX_DUPLICATE_MESSAGES:
            record["messages"].clear()
            return True, f"флуд одинаковыми сообщениями ({MAX_DUPLICATE_MESSAGES} подряд)"
    return False, ""

def check_bot_trigger(text: str, message: types.Message, bot_user_id: int, bot_username: str | None) -> tuple[bool, str]:
    if message.chat.type == "private":
        return True, text
    if message.reply_to_message and message.reply_to_message.from_user and message.reply_to_message.from_user.id == bot_user_id:
        return True, text
    if bot_username and f"@{bot_username.lower()}" in text.lower():
        cleaned = re.sub(f"@{bot_username}", "", text, flags=re.IGNORECASE).strip()
        return True, cleaned or "Привет! Чем могу помочь?"
    for trigger in TRIGGER_WORDS:
        match = re.search(rf"^{trigger}[,\s:]*(.*)$", text, flags=re.IGNORECASE)
        if match:
            return True, match.group(1).strip() or "Привет! Я на связи."
        if trigger in text.lower():
            return True, text
    return False, ""

# --- КЛАВИАТУРЫ ---
def get_modes_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤖 Стандартный", callback_data="mode_default"), InlineKeyboardButton(text="💻 Программист", callback_data="mode_coder")],
        [InlineKeyboardButton(text="🌍 Переводчик", callback_data="mode_translator"), InlineKeyboardButton(text="💡 Креатив", callback_data="mode_creative")],
        [InlineKeyboardButton(text="⚡ Краткий режим", callback_data="mode_brief")]
    ])

def get_models_keyboard() -> InlineKeyboardMarkup:
    buttons = []
    for k, info in AVAILABLE_MODELS.items():
        prefix = "✅ " if k == current_active_model_key else ""
        buttons.append([InlineKeyboardButton(text=f"{prefix}{info['name']}", callback_data=f"setmodel_{k}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def check_ttt_winner(board: list, symbol: str) -> bool:
    return any(board[a] == board[b] == board[c] == symbol for a, b, c in WIN_COMBINATIONS)

def get_ttt_board_keyboard(game_id: int, board: list, is_finished: bool = False) -> InlineKeyboardMarkup:
    symbol_map = {' ': '⬜', 'X': '❌', 'O': '⭕'}
    keyboard = []
    for r in range(3):
        row_buttons = []
        for c in range(3):
            idx = r * 3 + c
            val = board[idx]
            cb_data = "ttt_noop" if (is_finished or val != ' ') else f"ttt_m:{game_id}:{idx}"
            row_buttons.append(InlineKeyboardButton(text=symbol_map[val], callback_data=cb_data))
        keyboard.append(row_buttons)
    if not is_finished:
        keyboard.append([InlineKeyboardButton(text="🏳️ Сдаться", callback_data=f"ttt_surrender:{game_id}")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# --- РЕГИСТРАЦИЯ КОМАНД В ТЕЛЕГРАМ ---
async def setup_bot_commands(bot_instance: Bot):
    user_cmds = [
        BotCommand(command="start", description="🚀 Запустить бота"),
        BotCommand(command="help", description="📖 Все команды"),
        BotCommand(command="myid", description="🆔 Мой Telegram ID"),
        BotCommand(command="balance", description="💰 Профиль и баланс"),
        BotCommand(command="top", description="🏆 Таблица лидеров"),
        BotCommand(command="nick", description="🏷 Сменить ник (/nick имя)"),
        BotCommand(command="casino", description="🎰 Слоты"),
        BotCommand(command="football", description="⚽ Пенальти"),
        BotCommand(command="puzzle", description="🧩 Головоломка (+150)"),
        BotCommand(command="ttt", description="🎮 Крестики-Нолики"),
        BotCommand(command="bonus", description="🎁 Бонус (+250)"),
        BotCommand(command="remember", description="💾 Запомнить факт о себе"),
        BotCommand(command="forget", description="🗑 Забыть факт (/forget номер)"),
        BotCommand(command="clear", description="🧹 Очистить мою память"),
    ]
    try:
        await bot_instance.set_my_commands(commands=user_cmds, scope=BotCommandScopeDefault())
    except Exception as e:
        logging.warning(f"Ошибка команд по умолчанию: {e}")

    if ADMIN_ID != 0:
        try:
            admin_cmds = user_cmds + [
                BotCommand(command="addcoins", description="💰 Накрутить монеты (/addcoins 1000)"),
                BotCommand(command="setcoins", description="💰 Точный баланс (/setcoins 1000)"),
                BotCommand(command="broadcast", description="📢 Рассылка всем"),
                BotCommand(command="stats", description="👥 Статистика"),
                BotCommand(command="model", description="🧠 Сменить нейросеть"),
                BotCommand(command="modes", description="🎛 Выбрать роль"),
                BotCommand(command="sleep", description="😴 Тех. перерыв"),
                BotCommand(command="wakeup", description="🌅 Разбудить бота"),
                BotCommand(command="setprompt", description="✏️ Задать промпт"),
            ]
            await bot_instance.set_my_commands(commands=admin_cmds, scope=BotCommandScopeChat(chat_id=ADMIN_ID))
        except Exception as e:
            logging.warning(f"Ошибка админ-команд: {e}")

# --- ХЭНДЛЕРЫ ВЫБОРА МОДЕЛЕЙ И РОЛЕЙ ---
@dp.message(Command("model"))
async def model_cmd(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply("⛔ Команда доступна только администратору бота.")
        return
    current_name = AVAILABLE_MODELS.get(current_active_model_key, {}).get("name", "Неизвестно")
    await message.reply(
        f"🧠 <b>Выберите активную нейросеть:</b>\n\nТекущая модель: <b>{current_name}</b>",
        reply_markup=get_models_keyboard(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("setmodel_"))
async def set_model_callback(callback: types.CallbackQuery):
    global current_active_model_key
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Только администратор может менять нейросеть!", show_alert=True)
        return
    model_key = callback.data.replace("setmodel_", "")
    if model_key in AVAILABLE_MODELS:
        current_active_model_key = model_key
        set_db_setting("active_model", model_key)
        name = AVAILABLE_MODELS[model_key]["name"]
        await callback.answer(f"Модель выбрана: {name}")
        await callback.message.edit_text(
            f"🧠 <b>Выберите активную нейросеть:</b>\n\n✅ <b>Текущая активная модель: {name}</b>",
            reply_markup=get_models_keyboard(),
            parse_mode="HTML"
        )

@dp.message(Command("modes"))
async def modes_cmd(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply("⛔ Команда доступна только администратору бота.")
        return
    current_title = PRESET_MODES.get(current_mode_name, {}).get("title", "Пользовательский")
    await message.reply(
        f"🎛 <b>Выберите стиль / роль бота:</b>\n\nТекущий режим: <b>{current_title}</b>",
        reply_markup=get_modes_keyboard(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("mode_"))
async def switch_mode_callback(callback: types.CallbackQuery):
    global current_mode_name, current_system_prompt
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Только администратор может менять режим!", show_alert=True)
        return
    mode_key = callback.data.replace("mode_", "")
    if mode_key in PRESET_MODES:
        current_mode_name = mode_key
        current_system_prompt = PRESET_MODES[mode_key]["prompt"]
        set_db_setting("current_mode_name", current_mode_name)
        set_db_setting("custom_system_prompt", current_system_prompt)
        mode_title = PRESET_MODES[mode_key]["title"]
        await callback.answer(f"Режим: {mode_title}")
        await callback.message.edit_text(
            f"🎛 <b>Выберите стиль / роль бота:</b>\n\n✅ <b>Режим переключен на: {mode_title}</b>",
            reply_markup=get_modes_keyboard(),
            parse_mode="HTML"
        )

# --- АДМИН-ПРОМПТЫ ---
@dp.message(Command("setprompt"))
async def set_prompt_cmd(message: types.Message, command: CommandObject):
    global current_system_prompt, current_mode_name
    if not is_admin(message.from_user.id):
        await message.reply("⛔ Только для администратора.")
        return
    if not command.args:
        await message.reply("Использование: <code>/setprompt Ты лучший помощник...</code>", parse_mode="HTML")
        return
    current_mode_name = "custom"
    current_system_prompt = command.args.strip()
    set_db_setting("current_mode_name", "custom")
    set_db_setting("custom_system_prompt", current_system_prompt)
    await message.reply(f"✅ <b>Системный промпт сохранен в базу SQLite:</b>\n<code>{current_system_prompt}</code>", parse_mode="HTML")

@dp.message(Command("getprompt"))
async def get_prompt_cmd(message: types.Message):
    if is_admin(message.from_user.id):
        await message.reply(f"📋 <b>Текущий системный промпт:</b>\n<code>{current_system_prompt}</code>", parse_mode="HTML")

@dp.message(Command("resetprompt"))
async def reset_prompt_cmd(message: types.Message):
    global current_system_prompt, current_mode_name
    if is_admin(message.from_user.id):
        current_mode_name = "default"
        current_system_prompt = PRESET_MODES["default"]["prompt"]
        set_db_setting("current_mode_name", "default")
        set_db_setting("custom_system_prompt", current_system_prompt)
        await message.reply("🔄 Промпт сброшен до стандартного.")

# --- БАЗОВЫЕ КОМАНДЫ ---
@dp.message(Command("myid"))
@dp.message(Command("id"))
async def myid_cmd(message: types.Message):
    u_id = message.from_user.id
    is_adm_str = "👑 <b>Вы администратор бота!</b>" if is_admin(u_id) else f"👤 Обычный пользователь (в Render задан ADMIN_ID: <code>{ADMIN_ID}</code>)"
    await message.reply(f"🆔 <b>Ваш Telegram ID:</b> <code>{u_id}</code>\n💬 <b>ID этого чата:</b> <code>{message.chat.id}</code>\n\n{is_adm_str}", parse_mode="HTML")

@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    save_chat_to_db(message.chat)
    save_user_profile(message.from_user)
    get_user_balance(message.from_user.id)
    nick = get_display_name(message.from_user.id, message.from_user.first_name)

    if is_admin(message.from_user.id):
        await message.reply(
            f"👋 <b>Панель Администратора ({nick})</b>\n\n"
            f"🧠 Модель: <b>{AVAILABLE_MODELS[current_active_model_key]['name']}</b>\n"
            f"💰 Баланс: <b>{get_user_balance(message.from_user.id):,} монет</b>\n"
            f"💾 Персональная память: <b>Включена</b>\n\n"
            f"👑 <b>Команды управления:</b>\n"
            f"• <code>/model</code> — выбор нейросети\n"
            f"• <code>/modes</code> — выбор стиля ответов\n"
            f"• <code>/addcoins 50000</code> — начислить монеты\n"
            f"• <code>/setcoins 100000</code> — установить точный баланс\n"
            f"• <code>/broadcast [текст]</code> — рассылка по чатам\n"
            f"• <code>/stats</code> — статистика пользователей",
            reply_markup=get_modes_keyboard(),
            parse_mode="HTML"
        )
    else:
        await message.reply(
            f"👋 Привет, <b>{nick}</b>! 😊\n\n"
            f"Я бот-помощник <b>Ланикс</b> ✨\n\n"
            f"🧠 <b>Я помню всё, о чём мы говорим</b>, и знаю факты о тебе!\n"
            f"💾 <code>/remember мне нравится пицца</code> — запомнить факт\n"
            f"🗑 <code>/forget 1</code> — забыть факт по номеру\n"
            f"💰 <b>Экономика:</b> /balance, /top, /bonus\n"
            f"🎮 <b>Игры:</b> /casino, /football, /puzzle, /ttt\n"
            f"🎨 <b>Рисование:</b> <code>ланикс нарисуй [запрос]</code>\n"
            f"🧹 <code>/clear</code> — очистить мою память о тебе\n"
            f"📖 <b>Все команды:</b> /help",
            parse_mode="HTML"
        )

@dp.message(Command("help"))
async def help_cmd(message: types.Message):
    text = (
        "📖 <b>СПИСОК КОМАНД БОТА:</b>\n\n"
        "🧠 <b>Память (персональная, для каждого человека):</b>\n"
        "• Я автоматически помню наш диалог и факты о тебе\n"
        "• <code>/remember текст</code> — записать факт о себе\n"
        "• <code>/forget номер</code> — удалить факт (номера смотри в /remember без аргументов)\n"
        "• <code>/clear</code> — забыть всю историю диалога с тобой\n\n"
        "💰 <b>Экономика:</b>\n"
        "• <code>/balance</code> — профиль и баланс\n"
        "• <code>/top</code> — рейтинг богачей\n"
        "• <code>/nick имя</code> — сменить ник\n"
        "• <code>/bonus</code> — ежедневный бонус (+250)\n\n"
        "🎮 <b>Мини-игры:</b>\n"
        "• <code>/casino</code> — слоты (50/спин, макс 5/час)\n"
        "• <code>/football</code> — пенальти (50/удар, макс 5/час)\n"
        "• <code>/puzzle</code> — загадка (+150)\n"
        "• <code>/ttt</code> (ответом на сообщение друга) — крестики-нолики\n\n"
        "🎨 <b>Медиа:</b>\n"
        "• <code>ланикс нарисуй [текст]</code> — сгенерировать картинку\n"
        "• Отправь фото с подписью «ланикс нарисуй ...» — перерисовать/отредактировать\n"
        "• Отправь фото с вопросом — я его распознаю и отвечу"
    )
    if is_admin(message.from_user.id):
        text += (
            "\n\n👑 <b>Админ-команды:</b>\n"
            "• <code>/model</code> — выбор нейросети\n"
            "• <code>/modes</code> — выбор роли\n"
            "• <code>/addcoins ID сумма</code> — начислить монеты\n"
            "• <code>/setcoins ID сумма</code> — установить баланс\n"
            "• <code>/broadcast текст</code> — рассылка\n"
            "• <code>/stats</code> — статистика"
        )
    await message.reply(text, parse_mode="HTML")

# --- ПАМЯТЬ: ФАКТЫ ---
@dp.message(Command("remember"))
async def remember_cmd(message: types.Message, command: CommandObject):
    uid, cid = message.from_user.id, message.chat.id
    if not command.args:
        facts = get_user_facts(cid, uid)
        if not facts:
            await message.reply("💾 У меня пока нет сохранённых фактов о тебе.\nЗаписать: <code>/remember мне нравится рок-музыка</code>", parse_mode="HTML")
            return
        lines = "\n".join(f"{i+1}. {f}" for i, f in enumerate(facts))
        await message.reply(f"💾 <b>Факты о тебе в этом чате:</b>\n{lines}\n\nУдалить: <code>/forget номер</code>", parse_mode="HTML")
        return
    save_user_fact(cid, uid, command.args.strip())
    await message.reply(f"💾 <b>Запомнил:</b> <i>{command.args.strip()}</i> ✨\nБуду учитывать в наших разговорах!", parse_mode="HTML")

@dp.message(Command("forget"))
async def forget_cmd(message: types.Message, command: CommandObject):
    if not command.args:
        await message.reply("Использование: <code>/forget 1</code> — удалить факт под номером (список: /remember)", parse_mode="HTML")
        return
    idx = parse_int(command.args)
    if idx is None or idx < 1:
        await message.reply("⚠️ Укажите номер факта, например: <code>/forget 1</code>", parse_mode="HTML")
        return
    if delete_user_fact(message.chat.id, message.from_user.id, idx - 1):
        await message.reply(f"🗑 Забыл факт №{idx}.")
    else:
        await message.reply("⚠️ Факт с таким номером не найден. Список: /remember")

# --- ОЧИСТКА ПАМЯТИ (только своя) ---
@dp.message(Command("clear"))
@dp.message(Command("reset"))
async def clear_history_cmd(message: types.Message):
    clear_chat_memory(message.chat.id, message.from_user.id)
    await message.reply("🧹 <b>Я забыл всю историю нашего диалога!</b> Начинаем с чистого листа ✨\n(факты из /remember остались — удали их через /forget)", parse_mode="HTML")

# --- АДМИН-НАКРУТКА ---
@dp.message(Command("addcoins"))
async def addcoins_cmd(message: types.Message, command: CommandObject):
    if not is_admin(message.from_user.id):
        await message.reply("⛔ У вас нет прав администратора! Проверьте /myid и переменную ADMIN_ID.")
        return
    if not command.args:
        await message.reply("Использование: <code>/addcoins 50000</code> или <code>/addcoins ID 50000</code>", parse_mode="HTML")
        return

    parts = command.args.split()
    if message.reply_to_message and message.reply_to_message.from_user:
        target_id = message.reply_to_message.from_user.id
        amt = parse_int(parts[0])
    elif len(parts) >= 2:
        target_id = parse_int(parts[0])
        amt = parse_int(parts[1])
    else:
        target_id = message.from_user.id
        amt = parse_int(parts[0])

    if amt is None or (len(parts) >= 2 and target_id is None):
        await message.reply("⚠️ Сумма должна быть числом. Пример: <code>/addcoins 50000</code>", parse_mode="HTML")
        return

    new_b = alter_user_balance(target_id, amt)
    await message.reply(f"👑 <b>Начислено {amt:,} монет пользователю {get_display_name(target_id)}!</b> 🎉\n💰 Баланс: <b>{new_b:,} монет</b>.", parse_mode="HTML")

@dp.message(Command("setcoins"))
async def setcoins_cmd(message: types.Message, command: CommandObject):
    if not is_admin(message.from_user.id):
        await message.reply("⛔ У вас нет прав администратора!")
        return
    if not command.args:
        await message.reply("Использование: <code>/setcoins 100000</code> или <code>/setcoins ID 100000</code>", parse_mode="HTML")
        return

    parts = command.args.split()
    if message.reply_to_message and message.reply_to_message.from_user:
        target_id = message.reply_to_message.from_user.id
        val = parse_int(parts[0])
    elif len(parts) >= 2:
        target_id = parse_int(parts[0])
        val = parse_int(parts[1])
    else:
        target_id = message.from_user.id
        val = parse_int(parts[0])

    if val is None or val < 0 or (len(parts) >= 2 and target_id is None):
        await message.reply("⚠️ Баланс должен быть неотрицательным числом.", parse_mode="HTML")
        return

    set_exact_balance(target_id, val)
    await message.reply(f"👑 <b>Баланс пользователя {get_display_name(target_id)} установлен на: {val:,} монет.</b> ✨", parse_mode="HTML")

@dp.message(Command("broadcast"))
@dp.message(Command("send"))
async def broadcast_cmd(message: types.Message, command: CommandObject):
    if not is_admin(message.from_user.id):
        return
    reply_msg = message.reply_to_message
    broadcast_text = command.args.strip() if command.args else None
    if not reply_msg and not broadcast_text:
        await message.reply("📢 Формат: `/broadcast <текст>` или ответьте на сообщение командой `/broadcast`.")
        return

    chats = get_all_chats_from_db()
    status_msg = await message.reply(f"🚀 Начинаю рассылку по **{len(chats)}** чатам...")
    success, errors = 0, 0
    for chat_id, _, _ in chats:
        try:
            if reply_msg:
                await reply_msg.copy_to(chat_id=chat_id)
            else:
                await bot.send_message(chat_id=chat_id, text=broadcast_text, parse_mode="Markdown")
            success += 1
            await asyncio.sleep(0.05)
        except Exception:
            errors += 1
    await status_msg.edit_text(f"✅ Рассылка завершена!\n• Доставлено: **{success}** 📬\n• Ошибок: **{errors}**", parse_mode="Markdown")

@dp.message(Command("stats"))
async def stats_cmd(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    chats = get_all_chats_from_db()
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM user_balances")
        total_players = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM chat_memory")
        total_mem = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM user_facts")
        total_facts = cursor.fetchone()[0]
    await message.reply(
        f"📊 **Статистика бота:**\n\n"
        f"• Чатов в базе: **{len(chats)}** 💬\n"
        f"• Игроков: **{total_players}** 👥\n"
        f"• Сообщений в памяти: **{total_mem}** 🧠\n"
        f"• Сохранено фактов: **{total_facts}** 💾",
        parse_mode="Markdown"
    )

@dp.message(Command("sleep"))
async def sleep_cmd(message: types.Message, command: CommandObject):
    global manual_sleep_mode, manual_sleep_reason
    if not is_admin(message.from_user.id):
        return
    manual_sleep_reason = command.args.strip() if command.args and command.args.strip() else "технический перерыв"
    manual_sleep_mode = True
    await message.reply(f"😴 **Бот отправлен в режим сна.**\n📌 Причина: _{manual_sleep_reason}_", parse_mode="Markdown")

@dp.message(Command("wakeup"))
async def wakeup_cmd(message: types.Message):
    global manual_sleep_mode
    if not is_admin(message.from_user.id):
        return
    manual_sleep_mode = False
    await message.reply("🌅 **Бот успешно проснулся и готов к работе!** 🚀", parse_mode="Markdown")

# --- ЭКОНОМИКА И ИГРЫ ---
@dp.message(Command("balance"))
@dp.message(Command("bal"))
async def balance_cmd(message: types.Message):
    save_chat_to_db(message.chat)
    save_user_profile(message.from_user)
    u_id = message.from_user.id
    bal = get_user_balance(u_id)
    nick = get_display_name(u_id, message.from_user.first_name)

    now = time.time()
    s_left = max(0, CASINO_HOURLY_LIMIT - len([t for t in user_casino_spins[u_id] if now - t < 3600]))
    k_left = max(0, CASINO_HOURLY_LIMIT - len([t for t in user_football_kicks[u_id] if now - t < 3600]))

    facts_count = len(get_user_facts(message.chat.id, u_id))
    await message.reply(
        f"👤 <b>Профиль игрока {nick}:</b>\n\n"
        f"🪙 Баланс: <b>{bal:,} монет</b>\n"
        f"💾 Фактов о тебе в памяти: <b>{facts_count}</b>\n\n"
        f"📊 Доступно в этом часе:\n"
        f"• 🎰 Слоты: <b>{s_left}/5</b>\n"
        f"• ⚽ Пенальти: <b>{k_left}/5</b>\n\n"
        f"💡 <i>Решай /puzzle чтобы заработать монет или забери /bonus!</i> ✨",
        parse_mode="HTML"
    )

@dp.message(Command("bonus"))
async def bonus_cmd(message: types.Message):
    save_chat_to_db(message.chat)
    save_user_profile(message.from_user)
    now = datetime.now(MSK_TIMEZONE)
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT last_bonus FROM user_balances WHERE user_id = ?", (message.from_user.id,))
        row = cursor.fetchone()
        if row and row[0]:
            last_t = datetime.fromisoformat(row[0])
            if now - last_t < timedelta(hours=24):
                hrs = int((timedelta(hours=24) - (now - last_t)).total_seconds() // 3600) + 1
                await message.reply(f"⏳ Бонус уже получен! Приходите через ~{hrs} ч. 😉")
                return
        new_bal = alter_user_balance(message.from_user.id, 250)
        cursor.execute("UPDATE user_balances SET last_bonus = ? WHERE user_id = ?", (now.isoformat(), message.from_user.id))
        conn.commit()
    await message.reply(f"🎁 Получено <b>+250 монет</b>! Баланс: <b>{new_bal:,} монет</b> 🎉", parse_mode="HTML")

@dp.message(Command("nick"))
@dp.message(Command("setnick"))
async def setnick_cmd(message: types.Message, command: CommandObject):
    if not command.args:
        await message.reply(f"Ваш ник: <b>{get_display_name(message.from_user.id)}</b>\nИзменить: <code>/nick НовыйНик</code>", parse_mode="HTML")
        return
    new_n = command.args.strip()[:24]
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE user_balances SET custom_nick = ? WHERE user_id = ?", (new_n, message.from_user.id))
        conn.commit()
    await message.reply(f"✅ Ник успешно изменен на: <b>{new_n}</b> ✨", parse_mode="HTML")

@dp.message(Command("top"))
async def top_cmd(message: types.Message):
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT user_id, custom_nick, full_name, username, balance FROM user_balances ORDER BY balance DESC LIMIT 10")
        rich = cursor.fetchall()
    lines = ["🏆 <b>ТОП-10 БОГАЧЕЙ ЛАНИКСА:</b> 👑\n"]
    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
    for i, r in enumerate(rich):
        lines.append(f"{medals[i]} <b>{r[1] or r[2] or r[3]}</b> — <code>{r[4]:,}</code> монет")
    await message.reply("\n".join(lines), parse_mode="HTML")

@dp.message(Command("casino"))
async def casino_cmd(message: types.Message):
    u_id = message.from_user.id
    if get_user_balance(u_id) < 50:
        await message.reply("Недостаточно монет для игры (нужно 50)! ❌ Заберите /bonus или решите /puzzle.")
        return
    allowed, left, wait_min = check_game_hourly_limit(u_id, user_casino_spins)
    if not allowed:
        await message.reply(f"Лимит 5 раз в час исчерпан! ⏳ Ждите ~{wait_min} мин.")
        return
    dice_msg = await message.answer_dice(emoji="🎰")
    await asyncio.sleep(2.5)
    val = dice_msg.dice.value
    delta = 1000 if val == 64 else (300 if val in [1, 22, 43] else (100 if val in [16, 32, 48] else -50))
    res = "🔥 ДЖЕКПОТ 777! (+1000 монет!) 🎉🎉🎉" if delta == 1000 else ("✨ ВЫИГРЫШ ТРИ В РЯД! (+300 монет) 💰" if delta == 300 else ("👍 ПАРА! (+100 монет) 💵" if delta == 100 else "❌ Не повезло! Проигрыш (-50 монет)"))
    new_b = alter_user_balance(u_id, delta)
    await message.reply(f"🎰 {res}\n💰 Баланс: <b>{new_b:,} монет</b> (Осталось: {left}/5)", parse_mode="HTML")

@dp.message(Command("football"))
async def football_cmd(message: types.Message):
    u_id = message.from_user.id
    if get_user_balance(u_id) < 50:
        await message.reply("Недостаточно монет для удара (нужно 50)! ❌")
        return
    allowed, left, wait_min = check_game_hourly_limit(u_id, user_football_kicks)
    if not allowed:
        await message.reply(f"Лимит 5 раз в час исчерпан! ⏳ Ждите ~{wait_min} мин.")
        return
    dice_msg = await message.answer_dice(emoji="⚽")
    await asyncio.sleep(2.5)
    val = dice_msg.dice.value
    delta = 250 if val == 5 else (150 if val == 4 else (100 if val == 3 else -50))
    res = "🚀 ГОЛ В ДЕВЯТКУ! (+250 монет) ⚽🥅🔥" if delta == 250 else ("⚽ ГОЛ В СЕТКУ! (+150 монет) 🎉" if delta == 150 else ("⚡ ОТ ШТАНГИ! (+100 монет) 🎯" if delta == 100 else "🧤 СЕЙВ ВРАТАРЯ / МИМО (-50 монет) ❌"))
    new_b = alter_user_balance(u_id, delta)
    await message.reply(f"⚽ {res}\n💰 Баланс: <b>{new_b:,} монет</b> (Осталось: {left}/5)", parse_mode="HTML")

# --- ГОЛОВОЛОМКА (/puzzle) ---
@dp.message(Command("puzzle"))
@dp.message(Command("riddle"))
async def puzzle_cmd(message: types.Message):
    global puzzle_id_counter
    save_chat_to_db(message.chat)
    save_user_profile(message.from_user)

    p_data = random.choice(PUZZLE_BANK)
    p_id = puzzle_id_counter
    puzzle_id_counter += 1

    active_puzzles[p_id] = {
        "user_id": message.from_user.id,
        "correct_idx": p_data["correct"],
        "correct_text": p_data["options"][p_data["correct"]],
        "solved": False
    }

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=opt, callback_data=f"puzzle_ans:{p_id}:{idx}")] for idx, opt in enumerate(p_data["options"])])
    await message.reply(
        f"🧠 <b>Головоломка для {get_display_name(message.from_user.id)}!</b> ✨\n\n"
        f"{p_data['question']}\n\n"
        f"💰 Награда: <b>+150 монет</b> 🎁\n👇 Выберите вариант ответа:",
        reply_markup=kb,
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("puzzle_ans:"))
async def puzzle_callback(callback: types.CallbackQuery):
    parts = callback.data.split(":")
    p_id = int(parts[1])
    ans_idx = int(parts[2])
    p = active_puzzles.pop(p_id, None)
    if not p:
        await callback.answer("Головоломка недействительна.", show_alert=True)
        return
    if callback.from_user.id != p["user_id"]:
        active_puzzles[p_id] = p  # вернём обратно, это не тот пользователь
        await callback.answer("⛔ Это не ваша загадка!", show_alert=True)
        return
    if ans_idx == p["correct_idx"]:
        new_b = alter_user_balance(callback.from_user.id, 150)
        await callback.message.edit_text(
            f"🎉 <b>ВЕРНО! ПРАВИЛЬНЫЙ ОТВЕТ!</b> 🥳\n\n"
            f"✅ Ответ: <b>{p['correct_text']}</b>\n"
            f"🎁 Начислено: <b>+150 монет</b>! Баланс: <b>{new_b:,} монет</b> 💰",
            parse_mode="HTML"
        )
        await callback.answer("Правильно! 🎉")
    else:
        await callback.message.edit_text(f"❌ <b>Неверно!</b> Ответ был: <b>{p['correct_text']}</b> 🥺", parse_mode="HTML")
        await callback.answer("Неверно! ❌")

# --- КРЕСТИКИ-НОЛИКИ (/ttt) ---
@dp.message(Command("ttt"))
@dp.message(Command("xo"))
async def ttt_cmd(message: types.Message):
    global ttt_game_id_counter
    if message.chat.type == "private":
        await message.reply("🎮 В крестики-нолики можно играть только в группах!")
        return
    challenger = message.from_user
    target = message.reply_to_message.from_user if (message.reply_to_message and message.reply_to_message.from_user) else None
    if target and (target.is_bot or target.id == challenger.id):
        await message.reply("⚠️ Нельзя вызвать на игру бота или самого себя!")
        return
    # Один активный вызов на пару игроков — чистим завершённые
    for gid in [g for g, game in ttt_games.items() if game["status"] != "playing"]:
        ttt_games.pop(gid, None)
    g_id = ttt_game_id_counter
    ttt_game_id_counter += 1
    ttt_games[g_id] = {
        "id": g_id, "chat_id": message.chat.id, "player_x_id": challenger.id,
        "player_x_name": get_display_name(challenger.id), "player_o_id": target.id if target else None,
        "player_o_name": get_display_name(target.id) if target else "Любой желающий",
        "board": [' '] * 9, "current_turn": "X", "status": "playing",
        "invited_id": target.id if target else None
    }
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⚔️ Принять", callback_data=f"ttt_acc:{g_id}"), InlineKeyboardButton(text="❌ Отклонить", callback_data=f"ttt_dec:{g_id}")]])
    await message.reply(f"🎮 <b>Крестики-Нолики!</b> ⚔️\n❌ {ttt_games[g_id]['player_x_name']} против ⭕ {ttt_games[g_id]['player_o_name']}", reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data.startswith("ttt_dec:"))
async def ttt_decline_cb(callback: types.CallbackQuery):
    g_id = int(callback.data.split(":")[1])
    g = ttt_games.pop(g_id, None)
    if g:
        await callback.message.edit_text("❌ Вызов отклонён.")

@dp.callback_query(F.data.startswith("ttt_acc:"))
async def ttt_acc_cb(callback: types.CallbackQuery):
    g_id = int(callback.data.split(":")[1])
    g = ttt_games.get(g_id)
    if not g or g["status"] != "playing":
        await callback.answer("Игра недействительна.")
        return
    if callback.from_user.id == g["player_x_id"]:
        await callback.answer("Вы не можете принять собственный вызов!")
        return
    # Если вызов адресован конкретному человеку — принять может только он
    if g.get("invited_id") and callback.from_user.id != g["invited_id"]:
        await callback.answer("⛔ Этот вызов адресован другому игроку!", show_alert=True)
        return
    g["player_o_id"] = callback.from_user.id
    g["player_o_name"] = get_display_name(callback.from_user.id)
    await callback.message.edit_text(f"⚔️ <b>Игра началась!</b> 🔥\n❌ {g['player_x_name']} VS ⭕ {g['player_o_name']}\n👉 Ход: ❌ <b>{g['player_x_name']}</b>", reply_markup=get_ttt_board_keyboard(g_id, g["board"]), parse_mode="HTML")

@dp.callback_query(F.data.startswith("ttt_m:"))
async def ttt_move_cb(callback: types.CallbackQuery):
    parts = callback.data.split(":")
    g_id, idx = int(parts[1]), int(parts[2])
    g = ttt_games.get(g_id)
    if not g or g["status"] != "playing":
        await callback.answer("Игра завершена.")
        return
    cur_sym = g["current_turn"]
    expected = g["player_x_id"] if cur_sym == "X" else g["player_o_id"]
    if callback.from_user.id != expected or g["board"][idx] != ' ':
        await callback.answer("⛔ Не ваш ход или клетка занята!")
        return
    g["board"][idx] = cur_sym
    if check_ttt_winner(g["board"], cur_sym):
        g["status"] = "finished"
        winner_id = g["player_x_id"] if cur_sym == "X" else g["player_o_id"]
        alter_user_balance(winner_id, 100)
        await callback.message.edit_text(f"🎉 <b>ПОБЕДА {cur_sym}!</b> (+100 монет) 🏆", reply_markup=get_ttt_board_keyboard(g_id, g["board"], is_finished=True), parse_mode="HTML")
        ttt_games.pop(g_id, None)
        return
    if ' ' not in g["board"]:
        g["status"] = "finished"
        await callback.message.edit_text("🤝 <b>НИЧЬЯ!</b> ⚖️", reply_markup=get_ttt_board_keyboard(g_id, g["board"], is_finished=True), parse_mode="HTML")
        ttt_games.pop(g_id, None)
        return
    g["current_turn"] = "O" if cur_sym == "X" else "X"
    next_n = g["player_o_name"] if g["current_turn"] == "O" else g["player_x_name"]
    await callback.message.edit_text(f"❌ {g['player_x_name']} VS ⭕ {g['player_o_name']}\n👉 Ход: {'⭕' if g['current_turn']=='O' else '❌'} {next_n}", reply_markup=get_ttt_board_keyboard(g_id, g["board"]), parse_mode="HTML")

@dp.callback_query(F.data.startswith("ttt_surrender:"))
async def ttt_surr_cb(callback: types.CallbackQuery):
    g_id = int(callback.data.split(":")[1])
    g = ttt_games.pop(g_id, None)
    if g:
        name = get_display_name(callback.from_user.id)
        await callback.message.edit_text(f"🏳️ {name} сдался. Игра завершена.")

@dp.callback_query(F.data == "ttt_noop")
async def ttt_noop(callback: types.CallbackQuery):
    await callback.answer()

# --- ФОТО И РИСОВАНИЕ ---
@dp.message(F.photo)
async def process_photo_message(message: types.Message):
    save_chat_to_db(message.chat)
    save_user_profile(message.from_user)
    caption = message.caption or ""
    uid, cid = message.from_user.id, message.chat.id
    is_draw = any(re.search(rf"\b{tr}\s+(нарисуй|перерисуй|сделай)\b", caption.lower()) for tr in TRIGGER_WORDS) or caption.lower().strip().startswith("нарисуй")
    try:
        photo = message.photo[-1]
        fs = io.BytesIO()
        await bot.download(photo, destination=fs)
        img_bytes = fs.getvalue()
        if is_draw:
            draw_p = re.sub(r'^(ланикс|латекс|линукс|lanix|linux)?\s*(нарисуй|перерисуй)\s*', '', caption, flags=re.IGNORECASE).strip() or "Improve this photo, make it artistic"
            await bot.send_chat_action(chat_id=cid, action=ChatAction.UPLOAD_PHOTO)
            try:
                res_b = await generate_or_edit_image(draw_p, input_image_bytes=img_bytes)
            except Exception as e:
                logging.warning(f"Ошибка редактирования фото: {e}")
                await message.reply("⚠️ Не удалось обработать изображение. Попробуйте позже.")
                return
            await message.reply_photo(BufferedInputFile(res_b, filename="art.png"), caption=f"🎨 <b>Готово:</b> <i>{draw_p}</i> ✨", parse_mode="HTML")
            return
        bot_info = await get_bot_info()
        should_reply, prompt_text = check_bot_trigger(caption, message, bot_info.id, bot_info.username)
        if should_reply or message.chat.type == "private":
            sleeping, sleep_reason = is_bot_sleeping()
            if sleeping:
                await message.reply(f"😴 Бот на техническом перерыве: {sleep_reason}")
                return
            await bot.send_chat_action(chat_id=cid, action=ChatAction.TYPING)
            history = get_chat_memory(cid, uid)
            u_ctx = build_user_context(cid, uid, get_display_name(uid, message.from_user.first_name))
            try:
                reply = await generate_ai_response(prompt_text or "Опиши подробно, что на этой картинке.", current_system_prompt, image_bytes=img_bytes, history=history, user_context=u_ctx)
            except Exception as e:
                logging.warning(f"Ошибка распознавания фото: {e}")
                await message.reply("⚠️ Не удалось распознать изображение. Попробуйте ещё раз.")
                return
            save_message_to_memory(cid, uid, "user", f"[Фото]: {prompt_text or 'Пользователь отправил фото'}")
            save_message_to_memory(cid, uid, "assistant", reply)
            await message.reply(reply)
    except Exception as e:
        logging.warning(f"Ошибка обработки фото: {e}")
        await message.reply("⚠️ Не удалось скачать или обработать фото.")

# --- ГЛАВНЫЙ ОБРАБОТЧИК ТЕКСТА ---
@dp.message()
async def process_chat_message(message: types.Message):
    save_chat_to_db(message.chat)
    save_user_profile(message.from_user)
    uid, cid = message.from_user.id, message.chat.id

    # Неизвестные команды со слэшем
    if message.text and message.text.startswith("/"):
        await message.reply("⚠️ Неизвестная команда. Введите /help для просмотра списка доступных команд 😊")
        return

    if message.chat.type in ["group", "supergroup"]:
        is_spam, reason = check_spam_and_flood(message)
        if is_spam:
            logging.info(f"Спам от {uid} в чате {cid}: {reason}")
            await message.delete()
            return

    if message.sticker or not message.text:
        return

    lowered = message.text.lower().strip()

    # Быстрые текстовые команды
    if lowered in ["баланс", "кошелек", "монеты"]:
        await balance_cmd(message)
        return
    elif lowered in ["казино", "слоты", "крутить"]:
        await casino_cmd(message)
        return
    elif lowered in ["футбол", "пенальти", "гол"]:
        await football_cmd(message)
        return
    elif lowered in ["топ", "лидеры"]:
        await top_cmd(message)
        return
    elif lowered in ["головоломка", "загадка"]:
        await puzzle_cmd(message)
        return

    # Генерация картинок по фразе «ланикс нарисуй ...»
    draw_match = re.search(r'^(?:ланикс|латекс|линукс|lanix|linux)?\s*(?:нарисуй|сгенерируй)\s+(.+)$', message.text, flags=re.IGNORECASE)
    if draw_match:
        draw_p = draw_match.group(1).strip()
        if len(draw_p) < 3:
            await message.reply("⚠️ Опиши подробнее, что нарисовать. Пример: <code>ланикс нарисуй кот в космосе</code>", parse_mode="HTML")
            return
        sleeping, _ = is_bot_sleeping()
        if sleeping and message.chat.type != "private":
            return
        try:
            await bot.send_chat_action(chat_id=cid, action=ChatAction.UPLOAD_PHOTO)
            gen_b = await generate_or_edit_image(draw_p)
            await message.reply_photo(BufferedInputFile(gen_b, filename="art.png"), caption=f"🎨 <b>Готово:</b> <i>{draw_p}</i> ✨", parse_mode="HTML")
            return
        except Exception as e:
            logging.warning(f"Ошибка генерации изображения: {e}")
            await message.reply("⚠️ Не удалось сгенерировать изображение. Попробуйте позже.")
            return

    bot_info = await get_bot_info()
    should_reply, prompt_text = check_bot_trigger(message.text, message, bot_user_id=bot_info.id, bot_username=bot_info.username)
    sleeping, sleep_reason = is_bot_sleeping()

    if should_reply:
        if sleeping:
            await message.reply(f"😴 <b>Бот на техническом перерыве:</b> <i>{sleep_reason}</i>\nПриходите позже! 🌅", parse_mode="HTML")
            return
        try:
            await message.react([ReactionTypeEmoji(emoji=random.choice(REACTIONS_POOL))])
        except Exception:
            pass
        await bot.send_chat_action(chat_id=cid, action=ChatAction.TYPING)
        try:
            # 1. История диалога конкретного человека + сохранённые факты о нём
            history_context = get_chat_memory(cid, uid)
            u_ctx = build_user_context(cid, uid, get_display_name(uid, message.from_user.first_name))

            # 2. Генерация ответа с полным знанием контекста
            reply = await generate_ai_response(prompt_text, current_system_prompt, history=history_context, user_context=u_ctx)

            # 3. Сохранение в память
            save_message_to_memory(cid, uid, "user", prompt_text)
            save_message_to_memory(cid, uid, "assistant", reply)

            await message.reply(reply)
        except Exception as e:
            logging.warning(f"Ошибка генерации ответа: {e}")
            await message.reply("⚠️ Ошибка генерации ответа. Попробуйте еще раз.")

# --- ВЕБ-СЕРВЕР HEALTH CHECK ДЛЯ RENDER ---
async def handle_ping(request):
    return web.Response(text="Bot Lanix (Personal Memory Edition) is running!")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

# --- ТОЧКА ВХОДА ---
async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    init_db()
    try:
        await setup_bot_commands(bot)
    except Exception as e:
        logging.warning(f"Команды пропущены: {e}")
    await start_web_server()
    logging.info("Бот Lanix с персональной памятью успешно запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
