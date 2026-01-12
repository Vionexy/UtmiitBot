import asyncio
import httpx
from io import BytesIO
import fitz  # PyMuPDF
import hashlib
import os
from PIL import Image
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
import aiosqlite
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup, BotCommand, CallbackQuery
import random

# Токен бота
API_TOKEN = os.getenv("API_TOKEN")
bot = AsyncTeleBot(API_TOKEN)
# ID админа
ADMIN_CHAT_ID = 6986627524
# ID файлов для скачивания и их метаданные
SCHEDULE_FILES = {
    "monday": {
        "id": "1d7xrNLd8qpde_5jLvBdJjG9e3eOsjohK",
        "name": "понедельник",
        "link": "https://drive.google.com/file/d/1d7xrNLd8qpde_5jLvBdJjG9e3eOsjohK/view?usp=sharing"
    },
    "tuesday": {
        "id": "1qHNHC7uwXdECuEMfDoPiuv5bX0Ip0OpQ",
        "name": "вторник",
        "link": "https://drive.google.com/file/d/1qHNHC7uwXdECuEMfDoPiuv5bX0Ip0OpQ/view?usp=sharing"
    },
    "wednesday": {
        "id": "1hWMqMdeU2rcrNMx4jbOCr5ofGixsIJwA",
        "name": "среду",
        "link": "https://drive.google.com/file/d/1hWMqMdeU2rcrNMx4jbOCr5ofGixsIJwA/view?usp=sharing"
    },
    "thursday": {
        "id": "1O649rLM_VuBO31VF49noXfp1Evr-XfCN",
        "name": "четверг",
        "link": "https://drive.google.com/file/d/1O649rLM_VuBO31VF49noXfp1Evr-XfCN/view?usp=sharing"
    },
    "friday": {
        "id": "1YmQGiirdBryJlI3tx0SdU-g1gGm-6AaW",
        "name": "пятницу",
        "link": "https://drive.google.com/file/d/1YmQGiirdBryJlI3tx0SdU-g1gGm-6AaW/view?usp=sharing"
    },
    "saturday": {
        "id": "1hkXSDN-Dz86QGeyjhLZ7jlvSd9sMwmex",
        "name": "субботу",
        "link": "https://drive.google.com/file/d/1hkXSDN-Dz86QGeyjhLZ7jlvSd9sMwmex/view?usp=sharing"
    }
}
# Расписание звонков
CALL_SCHEDULE = {
    "monday_calls": """
<b>Понедельник</b>
<b>1⃣ </b> 8:30–9:15 | 9:20–10:05
<b>2⃣ </b> 10:15–11:00
🍴 <b>Обед:</b> 11:00–11:15
<b>2⃣ </b> 11:15–12:00
🍴 <b>Обед:</b> 12:00–12:30
🕐 <b>Классные часы:</b> 12:30–13:00
<b>3⃣ </b> 13:05–13:50 | 13:55–14:40
<b>4⃣ </b> 14:45–15:30 | 15:35–16:20
""",
    "thursday_calls": """
<b>Четверг</b>
<b>1⃣ </b> 8:30–9:15 | 9:20–10:05
<b>2⃣ </b> 10:15–11:00
🍴 <b>Обед:</b> 11:00–11:15
<b>2⃣ </b> 11:15–12:00
🍴 <b>Обед:</b> 12:00–12:30
<b>3⃣ </b> 12:30–13:15 | 13:20–14:05
<b>4⃣ </b> 14:10–14:55 | 15:00–15:45
🕐 <b>Классные часы (1 курс):</b> 15:50–16:20
""",
    "other_calls": """
<b>Другие дни</b>
<b>1⃣ </b> 8:30–9:15 | 9:20–10:05
<b>2⃣ </b> 10:15–11:00
🍴 <b>Обед:</b> 11:00–11:15
<b>2⃣ </b> 11:15–12:00
🍴 <b>Обед:</b> 12:00–12:40
<b>3⃣ </b> 12:40–13:25 | 13:30–14:15
<b>4⃣ </b> 14:25–15:10 | 15:15–16:00
<b>5⃣ </b> 16:05–16:50 | 16:55–17:40
"""
}
# Кэш для хранения изображений расписания
schedule_image_cache: Dict[str, List[BytesIO]] = {}
schedule_hash_cache: Dict[str, str] = {}
# Словарь для хранения ID сообщений с расписанием для каждого пользователя
user_schedule_messages: Dict[int, List[int]] = {}
# Хранилище для списков пользователей с пагинацией (для админа)
admin_lists_cache: Dict[int, Dict[str, List[str]]] = {}  # {chat_id: {'users': list, 'subscribers': list}}
# Количество элементов на странице
ITEMS_PER_PAGE = 50
# Новый счётчик для доната
user_donate_counter: Dict[int, int] = {}
# Стоимость хостинга
HOSTING_PRICE = 150


async def get_db_connection() -> aiosqlite.Connection:
    """Создает новое асинхронное соединение с базой данных."""
    return await aiosqlite.connect("subscribers.db")


async def db_execute(query: str, params: tuple = (), fetch: bool = False, commit: bool = False):
    """Универсальная функция для выполнения запросов к БД."""
    conn = await get_db_connection()
    cursor = await conn.execute(query, params)
    result = await cursor.fetchall() if fetch else None
    if commit:
        await conn.commit()
    await conn.close()
    return result


# Универсальные вспомогательные функции
async def register_and_log_user(user, chat_id: int) -> None:
    """Регистрирует нового пользователя (если нужно) и логирует взаимодействие."""
    first_name = getattr(user, "first_name", "") or ""
    last_name = getattr(user, "last_name", "") or ""
    username = getattr(user, "username", "") or ""
    await register_user_if_new(chat_id, first_name, last_name, username)
    await log_interaction(chat_id)


async def build_stats_text() -> str:
    """Формирует текст статистики (используется в /stats и admin_stats)."""
    total_all = await get_total_all_users()
    subscribed = await get_total_users()
    daily = await get_daily_users()
    return (
        f"📊Статистика:\n\nВсего использовали: {total_all}\n"
        f"Подписано на рассылку: {subscribed}\nАктивных сегодня: {daily}"
    )


def build_donate_settings_text(warning_enabled: bool) -> str:
    """Формирует текст меню управления донатом."""
    return (
        f"💳Управление донатом\n\n"
        f"🔔Предупреждение: {'ВКЛ' if warning_enabled else 'ВЫКЛ'}\n"
        f"💸Стоимость хостинга: {HOSTING_PRICE}₽/мес\n\n"
        f"Используйте кнопку ниже для переключения:"
    )


# Функции для работы с настройками
async def get_setting(key: str, default: str = "") -> str:
    """Получает значение настройки из базы."""
    result = await db_execute("SELECT value FROM bot_settings WHERE key = ?", (key,), fetch=True)
    return result[0][0] if result else default


async def set_setting(key: str, value: str) -> None:
    """Устанавливает значение настройки в базу."""
    await db_execute("INSERT OR REPLACE INTO bot_settings (key, value) VALUES (?, ?)", (key, value), commit=True)


async def is_donate_warning_enabled() -> bool:
    """Проверяет, включено ли предупреждение о донате."""
    warning_status = await get_setting("donate_warning", "1")
    return warning_status == "1"


# Функция для проверки, показывать ли донат (примерно раз в 1-3 раза)
def should_show_donate(chat_id: int) -> bool:
    """Возвращает True примерно раз в 1-3 запроса расписания"""
    if chat_id not in user_donate_counter:
        user_donate_counter[chat_id] = 0
    user_donate_counter[chat_id] += 1
    if user_donate_counter[chat_id] >= random.randint(1, 3):
        user_donate_counter[chat_id] = 0  # сбрасываем счётчик
        return True
    return False


# Новая функция для получения предупреждения о хостинге
async def get_hosting_warning() -> str:
    """Вычисляет дни до конца подписки хостинга (28 число каждого месяца по новосибирскому времени)
    и возвращает предупреждение только с 18 по 28 число, если включено"""
    if not await is_donate_warning_enabled():
        return ""
    now = datetime.now(timezone(timedelta(hours=7)))
    if not (18 <= now.day <= 28):
        return ""
    end_date = datetime(now.year, now.month, 28, tzinfo=timezone(timedelta(hours=7)))
    days_left = (end_date.date() - now.date()).days
    if days_left > 1:
        return f"❗Бот перестанет работать через {days_left} дней."
    elif days_left == 1:
        return "❗Бот перестанет работать через 1 день."
    elif days_left == 0:
        return "❗Бот перестанет работать сегодня."
    return ""


# Функция для получения текста доната
async def get_donate_text() -> str:
    """Возвращает текст доната с предупреждением о хостинге"""
    hosting_warning = await get_hosting_warning()
    donate_base_text = f"""
<a href="https://www.sberbank.com/sms/pbpn?requisiteNumber=79950614483"><u>Поддержите</u></a> работу бота — сервер стоит {HOSTING_PRICE}₽/мес.
Любая сумма поможет оплатить сервер.
Я студент, как и вы — сделал бота для удобства всем.
"""

    if hosting_warning:
        return f"{hosting_warning}\n{donate_base_text}"
    else:
        return donate_base_text


# Функции меню
def create_main_menu(is_admin: bool = False) -> InlineKeyboardMarkup:
    """Создает главное меню с опциональной кнопкой статистики для админа."""
    menu = InlineKeyboardMarkup()
    menu.row(InlineKeyboardButton("🗓️Расписание", callback_data="schedule"))
    menu.row(
        InlineKeyboardButton("🔔Звонки", callback_data="bell"),
        InlineKeyboardButton("📬Рассылка", callback_data="mailing"),
    )
    if is_admin:
        menu.row(InlineKeyboardButton("📊Статистика", callback_data="admin_stats"))
    return menu


def create_stats_menu() -> InlineKeyboardMarkup:
    """Создает меню статистики для админа."""
    menu = InlineKeyboardMarkup()
    menu.row(InlineKeyboardButton("👥Список пользователей", callback_data="list_users"))
    menu.row(InlineKeyboardButton("👥Список подписчиков", callback_data="list_subscribers"))
    menu.row(InlineKeyboardButton("💳Управление донатом", callback_data="donate_settings"))
    menu.row(InlineKeyboardButton("Меню", callback_data="back_to_main"))
    return menu


def create_donate_settings_menu(warning_enabled: bool) -> InlineKeyboardMarkup:
    """Создает меню управления донатом для админа."""
    menu = InlineKeyboardMarkup()

    # Одна кнопка-переключатель
    if warning_enabled:
        button_text = "🔴Выключить предупреждение о донате"
    else:
        button_text = "🟢Включить предупреждение о донате"

    menu.row(InlineKeyboardButton(button_text, callback_data="toggle_donate_warning"))
    menu.row(InlineKeyboardButton("Назад к статистике", callback_data="admin_stats"))
    return menu


def create_schedule_menu() -> InlineKeyboardMarkup:
    """Создает меню выбора дня расписания."""
    days = [("Понедельник", "monday"), ("Вторник", "tuesday"), ("Среда", "wednesday"),
            ("Четверг", "thursday"), ("Пятница", "friday"), ("Суббота", "saturday")]
    menu = InlineKeyboardMarkup()
    for i in range(0, len(days), 3):
        menu.add(*[InlineKeyboardButton(name, callback_data=f"schedule_{key}") for name, key in days[i:i+3]])
    menu.row(InlineKeyboardButton("Меню", callback_data="back_to_main"))
    return menu


def create_calls_menu() -> InlineKeyboardMarkup:
    """Создает меню расписания звонков."""
    return InlineKeyboardMarkup().add(
        InlineKeyboardButton("Понедельник", callback_data="monday_calls"),
        InlineKeyboardButton("Четверг", callback_data="thursday_calls"),
        InlineKeyboardButton("Другие дни", callback_data="other_calls"),
        InlineKeyboardButton("Меню", callback_data="back_to_main"),
    )


def create_mailing_menu(subscribed_status: bool) -> InlineKeyboardMarkup:
    """Создает меню управления рассылкой."""
    menu = InlineKeyboardMarkup()
    if subscribed_status:
        menu.row(InlineKeyboardButton("Отписаться от рассылки", callback_data="unsubscribe"))
    else:
        menu.row(InlineKeyboardButton("Подписаться на рассылку", callback_data="subscribe"))
    menu.row(InlineKeyboardButton("Меню", callback_data="back_to_main"))
    return menu


def create_pagination_markup(list_type: str, page: int, total_pages: int) -> InlineKeyboardMarkup:
    """Создает разметку пагинации."""
    markup = InlineKeyboardMarkup()
    row = []
    if page > 1:
        row.append(InlineKeyboardButton("◀️Назад", callback_data=f"{list_type}*page*{page - 1}"))
    if page < total_pages:
        row.append(InlineKeyboardButton("Далее▶️", callback_data=f"{list_type}*page*{page + 1}"))
    if row:
        markup.row(*row)
    markup.row(InlineKeyboardButton("Назад к статистике", callback_data="admin_stats"))
    return markup


# Функции работы с файлами
async def download_pdf(
        file_id: str, retries: int = 3, delay: float = 2.0
) -> Tuple[Optional[bytes], Optional[str]]:
    """Скачивает PDF с Google Drive с повторными попытками."""
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/pdf",
    }
    for attempt in range(retries):
        try:
            async with httpx.AsyncClient(
                    timeout=30.0, follow_redirects=True
            ) as client:
                response = await client.get(download_url, headers=headers)
                if response.status_code == 429:
                    await asyncio.sleep(delay * (2 ** attempt))
                    continue
                response.raise_for_status()
                if response.content.startswith(b"%PDF"):
                    return response.content, None
                return None, f"Получен некорректный PDF для {file_id}"
        except httpx.HTTPStatusError as http_error:
            if attempt == retries - 1:
                return None, f"Ошибка HTTP при скачивании {file_id}: {http_error}"
        except httpx.RequestError as request_error:
            if attempt == retries - 1:
                return None, f"Ошибка сети при скачивании {file_id}: {request_error}"
        await asyncio.sleep(delay * (2 ** attempt))
    return None, f"Не удалось скачать PDF для {file_id} после {retries} попыток"


def resize_image_if_needed(
        img: Image.Image, max_size: int = 2000
) -> Image.Image:
    """Изменяет размер изображения, если оно слишком большое для Telegram."""
    if img.width > max_size or img.height > max_size:
        img.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
    return img


async def pdf_to_images(pdf_content: bytes) -> List[BytesIO]:
    """Конвертирует ВСЕ страницы PDF в список изображений с использованием Pillow.
    Изменяет размер изображений, если они превышают лимит Telegram.
    """
    try:
        doc = fitz.open(stream=pdf_content, filetype="pdf")
        images = []
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            pix = page.get_pixmap(dpi=150)
            # Используем Pillow для сжатия изображения
            img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
            img = resize_image_if_needed(img)  # Добавляем resize
            img_buffer = BytesIO()
            img.save(img_buffer, format="PNG", optimize=True, quality=85)
            img_buffer.seek(0)
            images.append(img_buffer)
        doc.close()
        return images
    except Exception as e:
        raise ValueError(f"Ошибка конвертации PDF в изображение: {e}")


def get_file_hash(content: bytes) -> str:
    """Вычисляет SHA-256 хэш содержимого файла."""
    return hashlib.sha256(content).hexdigest()


async def init_db() -> None:
    """Инициализирует базу данных с таблицами и индексами."""
    conn = await get_db_connection()
    tables = [
        "CREATE TABLE IF NOT EXISTS subscribers (chat_id INTEGER PRIMARY KEY, joined_date TEXT)",
        "CREATE TABLE IF NOT EXISTS schedule_updates (day TEXT PRIMARY KEY, last_hash TEXT)",
        "CREATE TABLE IF NOT EXISTS all_users (chat_id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, username TEXT, first_interaction_date TEXT)",
        "CREATE TABLE IF NOT EXISTS interactions (chat_id INTEGER, interaction_date TEXT)",
        "CREATE TABLE IF NOT EXISTS bot_settings (key TEXT PRIMARY KEY, value TEXT)",
        "CREATE INDEX IF NOT EXISTS idx_subscribers_chat_id ON subscribers (chat_id)",
        "CREATE INDEX IF NOT EXISTS idx_schedule_updates_day ON schedule_updates (day)",
        "CREATE INDEX IF NOT EXISTS idx_interactions_date ON interactions (interaction_date)"
    ]
    for table in tables:
        await conn.execute(table)
    try:
        cursor = await conn.execute("PRAGMA table_info(all_users)")
        columns = [row[1] for row in await cursor.fetchall()]
        if "username" not in columns:
            await conn.execute("ALTER TABLE all_users ADD COLUMN username TEXT")
    except Exception as e:
        print(f"Error in init_db migration: {e}")
    await conn.commit()
    await conn.close()


async def is_subscribed(chat_id: int) -> bool:
    """Проверяет, подписан ли пользователь на рассылку."""
    result = await db_execute("SELECT 1 FROM subscribers WHERE chat_id = ?", (chat_id,), fetch=True)
    return bool(result)


async def subscribe_user(chat_id: int) -> None:
    """Добавляет пользователя в подписчиков."""
    await db_execute("INSERT OR IGNORE INTO subscribers (chat_id, joined_date) VALUES (?, ?)",
                     (chat_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S")), commit=True)


async def unsubscribe_user(chat_id: int) -> None:
    """Удаляет пользователя из подписчиков."""
    await db_execute("DELETE FROM subscribers WHERE chat_id = ?", (chat_id,), commit=True)


async def get_last_hash(day: str) -> Optional[str]:
    """Получает хэш содержимого файла из базы."""
    result = await db_execute("SELECT last_hash FROM schedule_updates WHERE day = ?", (day,), fetch=True)
    return result[0][0] if result else None


async def update_last_hash(day: str, hash_value: str) -> None:
    """Обновляет хэш содержимого в базе."""
    await db_execute("INSERT OR REPLACE INTO schedule_updates (day, last_hash) VALUES (?, ?)",
                     (day, hash_value), commit=True)


async def get_all_subscribers() -> List[int]:
    """Получает всех подписчиков."""
    result = await db_execute("SELECT chat_id FROM subscribers", fetch=True)
    return [row[0] for row in result] if result else []


async def get_total_users() -> int:
    """Получает общее число подписчиков."""
    result = await db_execute("SELECT COUNT(*) FROM subscribers", fetch=True)
    return result[0][0] if result else 0


async def get_total_all_users() -> int:
    """Получает общее число всех пользователей, кто взаимодействовал с ботом."""
    result = await db_execute("SELECT COUNT(*) FROM all_users", fetch=True)
    return result[0][0] if result else 0


async def get_daily_users() -> int:
    """Получает число уникальных пользователей за текущий день."""
    result = await db_execute("SELECT COUNT(DISTINCT chat_id) FROM interactions WHERE interaction_date = ?",
                              (datetime.now().strftime("%Y-%m-%d"),), fetch=True)
    return result[0][0] if result else 0


async def get_all_users_list() -> List[str]:
    """Получает список всех пользователей с юзернеймами и именами."""
    result = await db_execute(
        "SELECT username, first_name, last_name FROM all_users WHERE username IS NOT NULL AND username != '' ORDER BY first_interaction_date DESC",
        fetch=True)
    return [f"@{row[0]} ({row[1]} {row[2]})" for row in result] if result else []


async def get_subscribers_list() -> List[str]:
    """Получает список подписчиков с юзернеймами и именами."""
    result = await db_execute(
        """SELECT u.username, u.first_name, u.last_name FROM all_users u
           INNER JOIN subscribers s ON u.chat_id = s.chat_id
           WHERE u.username IS NOT NULL AND u.username != ''
           ORDER BY s.joined_date DESC""", fetch=True)
    return [f"@{row[0]} ({row[1]} {row[2]})" for row in result] if result else []


async def log_interaction(chat_id: int) -> None:
    """Логирует взаимодействие пользователя за день."""
    await db_execute("INSERT INTO interactions (chat_id, interaction_date) VALUES (?, ?)",
                     (chat_id, datetime.now().strftime("%Y-%m-%d")), commit=True)


async def register_user_if_new(chat_id: int, first_name: str, last_name: str, username: str = None) -> None:
    """Регистрирует нового пользователя в all_users, если его нет."""
    result = await db_execute("SELECT 1 FROM all_users WHERE chat_id = ?", (chat_id,), fetch=True)
    if not result:
        await db_execute("INSERT INTO all_users (chat_id, first_name, last_name, username, first_interaction_date) VALUES (?, ?, ?, ?, ?)",
                         (chat_id, first_name or "", last_name or "", username or "", datetime.now().strftime("%Y-%m-%d %H:%M:%S")), commit=True)


async def handle_pagination(call, chat_id: int, items: List[str], page: int, list_type: str, title: str) -> None:
    """Обрабатывает пагинацию для списков."""
    total_pages = (len(items) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    start = (page - 1) * ITEMS_PER_PAGE
    page_items = items[start:start + ITEMS_PER_PAGE]
    text = f"👥 Список {title} (страница {page}/{total_pages}):\n\n" + "\n".join(page_items)
    markup = create_pagination_markup(list_type, page, total_pages)
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=text, reply_markup=markup)
    except Exception as e:
        print(f"Error editing {title} list: {e}")
        await bot.send_message(chat_id, text, reply_markup=markup)


async def delete_previous_schedule_messages(chat_id: int) -> None:
    """Удаляет предыдущие сообщения с расписанием для пользователя."""
    if chat_id in user_schedule_messages:
        for msg_id in user_schedule_messages[chat_id]:
            try:
                await bot.delete_message(chat_id, msg_id)
            except Exception as e:
                print(f"Error deleting message {msg_id}: {e}")
        user_schedule_messages[chat_id] = []


async def get_cached_images(day: str) -> Optional[List[BytesIO]]:
    """Получает изображения из кэша, если они есть и актуальны.
    Если кэш устарел, скачивает заново для проверки.
    """
    if day in schedule_image_cache:
        file_info = SCHEDULE_FILES.get(day)
        if not file_info:
            return None
        pdf_content, error_msg = await download_pdf(file_info["id"])
        if pdf_content:
            current_hash = get_file_hash(pdf_content)
            if current_hash == schedule_hash_cache.get(day):
                return schedule_image_cache[day]
            else:
                # Не очищаем глобальный кэш здесь, только проверяем
                pass
    return None


async def cache_images(
        day: str, images: List[BytesIO], hash_value: str
) -> None:
    """Сохраняет изображения в кэш."""
    schedule_image_cache[day] = images
    schedule_hash_cache[day] = hash_value


async def check_schedule_updates() -> None:
    """Периодически проверяет обновления расписания и отправляет рассылку подписчикам."""
    # Очищаем кэш только при запуске, не в цикле
    schedule_image_cache.clear()
    schedule_hash_cache.clear()
    while True:
        try:
            current_time = datetime.now(
                timezone.utc
            ).astimezone(timezone(timedelta(hours=7)))
            current_weekday = current_time.weekday()
            day_mapping = {
                0: "tuesday",
                1: "wednesday",
                2: "thursday",
                3: "friday",
                4: "saturday",
                5: "monday",
                6: "monday",
            }
            day_to_send = day_mapping.get(current_weekday)
            if day_to_send is None or day_to_send not in SCHEDULE_FILES:
                await asyncio.sleep(900)
                continue
            file_info = SCHEDULE_FILES[day_to_send]
            try:
                pdf_content, error_msg = await download_pdf(file_info["id"])
                if not pdf_content:
                    await asyncio.sleep(900)
                    continue
                current_hash = get_file_hash(pdf_content)
                last_hash = await get_last_hash(day_to_send)
                if current_hash != last_hash or last_hash is None:
                    try:
                        image_buffers = await pdf_to_images(pdf_content)
                        await cache_images(day_to_send, image_buffers, current_hash)
                        subscribers = await get_all_subscribers()
                        if subscribers:
                            successful_sends = 0
                            failed_sends = 0
                            delay = (
                                0.2
                                if len(subscribers) < 100
                                else 1.0  # Увеличиваем задержку для больших списков
                            )
                            for i, subscriber_id in enumerate(subscribers):
                                try:
                                    if i > 0:
                                        await asyncio.sleep(delay)
                                    for j, img_buffer in enumerate(image_buffers):
                                        img_buffer.seek(0)
                                        caption = None
                                        if j == len(image_buffers) - 1:
                                            caption = f"🔄Обновлено расписание на {file_info['name']}\n📎<a href=\"{file_info['link']}\">Ссылка на расписание</a>"
                                            if should_show_donate(subscriber_id):
                                                caption += f"\n\n{await get_donate_text()}"
                                        await bot.send_photo(
                                            subscriber_id, photo=img_buffer,
                                            caption=caption, parse_mode="HTML" if caption else None
                                        )
                                    successful_sends += 1
                                    await log_interaction(subscriber_id)
                                except Exception as send_error:
                                    failed_sends += 1
                                    print(f"Error sending to subscriber {subscriber_id}: {send_error}")
                        await update_last_hash(day_to_send, current_hash)
                    except Exception as processing_error:
                        print(f"Error processing file {day_to_send}: {processing_error}")
                else:
                    pass
            except Exception as day_error:
                print(f"Error processing day {day_to_send}: {day_error}")
            await asyncio.sleep(900)
        except Exception as critical_error:
            print(f"Critical error in check_schedule_updates: {critical_error}")
            await asyncio.sleep(60)


# Обработчики сообщений
@bot.message_handler(commands=["start"])
async def start_handler(message) -> None:
    await register_and_log_user(message.from_user, message.chat.id)
    is_admin = message.chat.id == ADMIN_CHAT_ID
    await bot.send_message(
        message.chat.id,
        f"Привет, {message.from_user.first_name}!😊",
        reply_markup=create_main_menu(is_admin),
    )


@bot.message_handler(commands=["schedule"])
async def schedule_handler(message) -> None:
    await register_and_log_user(message.from_user, message.chat.id)
    await bot.send_message(
        message.chat.id, "Выберите день недели☺️", reply_markup=create_schedule_menu()
    )


@bot.message_handler(commands=["bell"])
async def bell_handler(message) -> None:
    await register_and_log_user(message.from_user, message.chat.id)
    await bot.send_message(
        message.chat.id, "Информация о звонках🫨", reply_markup=create_calls_menu()
    )


@bot.message_handler(commands=["mailing"])
async def mailing_handler(message) -> None:
    await register_and_log_user(message.from_user, message.chat.id)
    subscribed = await is_subscribed(message.chat.id)
    status_text = (
        "Вы подписаны на рассылку!✅" if subscribed else "Вы отписаны от рассылки!❎"
    )
    await bot.send_message(
        message.chat.id, status_text, reply_markup=create_mailing_menu(subscribed)
    )


@bot.message_handler(commands=["stats"])
async def stats_handler(message) -> None:
    if message.chat.id != ADMIN_CHAT_ID:
        await bot.send_message(message.chat.id, "Доступ запрещен")
        return
    text = await build_stats_text()
    await bot.send_message(message.chat.id, text, reply_markup=create_stats_menu())


# Обработчик callback для кнопки "Расписание" в главном меню
@bot.callback_query_handler(func=lambda call: call.data == "schedule")
async def schedule_menu_handler(call: CallbackQuery) -> None:
    try:
        await register_and_log_user(call.from_user, call.message.chat.id)
        await bot.edit_message_text(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            text="Выберите день недели☺️",
            reply_markup=create_schedule_menu(),
        )
    except Exception as e:
        await bot.answer_callback_query(call.id, text="Ошибка. Попробуйте позже.")
        print(f"Error in schedule_menu_handler: {e}")


# Обработчик callback для выбора конкретного дня расписания
@bot.callback_query_handler(func=lambda call: call.data.startswith("schedule_"))
async def schedule_day_handler(call: CallbackQuery) -> None:
    try:
        day = call.data.split("_")[1]
        await register_and_log_user(call.from_user, call.message.chat.id)
        if day not in SCHEDULE_FILES:
            await bot.answer_callback_query(
                call.id, text="Неизвестный день. Вернитесь в меню."
            )
            return
        file_info = SCHEDULE_FILES[day]
        await bot.answer_callback_query(call.id, text="🔄Загружается расписание...")
        try:
            await bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception as e:
            print(f"Error deleting menu: {e}")
        await delete_previous_schedule_messages(call.message.chat.id)
        cached_images = await get_cached_images(day)
        if cached_images:
            image_buffers = cached_images
        else:
            pdf_content, error_msg = await download_pdf(file_info["id"])
            if not pdf_content:
                error_message = await bot.send_message(
                    call.message.chat.id,
                    text=f"❌Не удалось загрузить расписание\n<a href=\"{file_info['link']}\">Ссылка на расписание</a>",
                    reply_markup=create_schedule_menu(),
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
                if call.message.chat.id not in user_schedule_messages:
                    user_schedule_messages[call.message.chat.id] = []
                user_schedule_messages[call.message.chat.id].append(
                    error_message.message_id
                )
                return
            image_buffers = await pdf_to_images(pdf_content)
            current_hash = get_file_hash(pdf_content)
            await cache_images(day, image_buffers, current_hash)
        if call.message.chat.id not in user_schedule_messages:
            user_schedule_messages[call.message.chat.id] = []
        for i, img_buffer in enumerate(image_buffers):
            img_buffer.seek(0)
            caption = None
            if i == len(image_buffers) - 1:
                caption = f"🔄Обновлено расписание на {file_info['name']}\n📎<a href=\"{file_info['link']}\">Ссылка на расписание</a>"
                if should_show_donate(call.message.chat.id):
                    caption += f"\n\n{await get_donate_text()}"
            sent_message = await bot.send_photo(
                call.message.chat.id, photo=img_buffer,
                caption=caption, parse_mode="HTML" if caption else None,
                reply_markup=create_schedule_menu() if i == len(image_buffers) - 1 else None
            )
            user_schedule_messages[call.message.chat.id].append(sent_message.message_id)
    except Exception as callback_error:
        await bot.answer_callback_query(call.id, text="Ошибка. Попробуйте позже.")
        print(f"Error in schedule_day_handler: {callback_error}")


# Обработчик callback для остальных кнопок
@bot.callback_query_handler(func=lambda call: True)
async def callback_query_handler(call: CallbackQuery) -> None:
    try:
        chat_id = call.message.chat.id
        await register_and_log_user(call.from_user, chat_id)
        if call.data == "admin_stats":
            if chat_id != ADMIN_CHAT_ID:
                await bot.answer_callback_query(call.id, text="Доступ запрещен")
                return
            text = await build_stats_text()
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=text,
                reply_markup=create_stats_menu(),
            )
            return
        elif call.data.startswith("list_users*page*") or call.data == "list_users":
            if chat_id != ADMIN_CHAT_ID:
                await bot.answer_callback_query(call.id, text="Доступ запрещен")
                return
            page = 1 if call.data == "list_users" else int(call.data.split("*")[-1])
            if call.data == "list_users":
                admin_lists_cache[chat_id] = {'users': await get_all_users_list(),
                                              'subscribers': admin_lists_cache.get(chat_id, {}).get('subscribers', [])}
            users_list = admin_lists_cache.get(chat_id, {}).get('users', [])
            await handle_pagination(call, chat_id, users_list, page, "list_users", "пользователей")
            return
        elif call.data.startswith("list_subscribers*page*") or call.data == "list_subscribers":
            if chat_id != ADMIN_CHAT_ID:
                await bot.answer_callback_query(call.id, text="Доступ запрещен")
                return
            page = 1 if call.data == "list_subscribers" else int(call.data.split("*")[-1])
            if call.data == "list_subscribers":
                admin_lists_cache[chat_id] = {'subscribers': await get_subscribers_list(),
                                              'users': admin_lists_cache.get(chat_id, {}).get('users', [])}
            subscribers_list = admin_lists_cache.get(chat_id, {}).get('subscribers', [])
            await handle_pagination(call, chat_id, subscribers_list, page, "list_subscribers", "подписчиков")
            return
        elif call.data == "donate_settings":
            if chat_id != ADMIN_CHAT_ID:
                await bot.answer_callback_query(call.id, text="Доступ запрещен")
                return

            warning_enabled = await is_donate_warning_enabled()

            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=build_donate_settings_text(warning_enabled),
                reply_markup=create_donate_settings_menu(warning_enabled),
            )
            return
        elif call.data == "toggle_donate_warning":
            if chat_id != ADMIN_CHAT_ID:
                await bot.answer_callback_query(call.id, text="Доступ запрещен")
                return

            current_status = await is_donate_warning_enabled()
            new_status = not current_status

            await set_setting("donate_warning", "1" if new_status else "0")

            status_text = "включено" if new_status else "выключено"
            await bot.answer_callback_query(call.id, text=f"Предупреждение {status_text}")

            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=build_donate_settings_text(new_status),
                reply_markup=create_donate_settings_menu(new_status),
            )
            return
        if call.data == "mailing":
            subscribed = await is_subscribed(chat_id)
            status_text = (
                "Вы подписаны на рассылку!✅"
                if subscribed
                else "Вы отписаны от рассылки!❎"
            )
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=status_text,
                reply_markup=create_mailing_menu(subscribed),
            )
        elif call.data in CALL_SCHEDULE:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=CALL_SCHEDULE[call.data],
                parse_mode="HTML",
                reply_markup=create_calls_menu(),
            )
        elif call.data == "bell":
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text="Информация о звонках🫨",
                reply_markup=create_calls_menu(),
            )
        elif call.data == "subscribe":
            await subscribe_user(chat_id)
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text="Вы подписаны на рассылку!✅",
                reply_markup=create_mailing_menu(True),
            )
        elif call.data == "unsubscribe":
            await unsubscribe_user(chat_id)
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text="Вы отписаны от рассылки!❎",
                reply_markup=create_mailing_menu(False),
            )
        elif call.data == "back_to_main":
            await delete_previous_schedule_messages(chat_id)
            is_admin = chat_id == ADMIN_CHAT_ID
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    text="Выберите кнопку ниже😊",
                    reply_markup=create_main_menu(is_admin),
                )
            except Exception as e:
                print(f"Error returning to menu: {e}")
                await bot.send_message(
                    chat_id,
                    text="Выберите кнопку ниже😊",
                    reply_markup=create_main_menu(is_admin),
                )
    except Exception as callback_error:
        await bot.answer_callback_query(call.id, text="Ошибка. Попробуйте позже.")
        print(f"Error in callback_handler ({call.data}): {callback_error}")


async def set_bot_commands() -> None:
    """Устанавливает команды бота."""
    await bot.set_my_commands(
        [
            BotCommand("start", "🚀Старт"),
            BotCommand("schedule", "🗓️Расписание"),
            BotCommand("bell", "🔔Звонки"),
            BotCommand("mailing", "📬Рассылка"),
        ]
    )


async def main() -> None:
    """Главная функция запуска бота."""
    await init_db()
    await set_bot_commands()
    asyncio.create_task(check_schedule_updates())
    asyncio.create_task(log_stats_periodically())
    await bot.polling(non_stop=True, skip_pending=True)


async def log_stats_periodically() -> None:
    """Периодически логирует статистику в консоль."""
    while True:
        total_users = await get_total_users()
        total_all = await get_total_all_users()
        print(f"Stats: subscribers {total_users}, all users {total_all}")
        await asyncio.sleep(3600)


if __name__ == "__main__":
    if os.name == "nt":
        os.system("chcp 65001 > nul")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped by user")
    except Exception as startup_error:
        print(f"Critical error on startup: {startup_error}")