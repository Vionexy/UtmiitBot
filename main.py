import asyncio
import httpx
from io import BytesIO
import fitz
import hashlib
import os
from PIL import Image
from datetime import datetime, timezone, timedelta
import aiosqlite
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup, BotCommand

# настройки
API_TOKEN = os.getenv("API_TOKEN")
if not API_TOKEN:
    raise RuntimeError("Переменная окружения API_TOKEN не установлена. Задай её на хостинге.")

bot = AsyncTeleBot(API_TOKEN)
ADMIN_CHAT_ID = 6986627524
HOSTING_PRICE = 150
ITEMS_PER_PAGE = 50

# файлы расписания с гугл диска
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

# расписание звонков
CALL_SCHEDULE = {
    "monday_calls": """
<b>Понедельник</b>  

<b>1⃣ </b> 8:30–9:15 | 9:20–10:05

<b>2⃣ </b> 10:15–11:00  
🍴  <b>Обед:</b> 11:00–11:15
<b>2⃣ </b> 11:15–12:00  

🍴  <b>Обед:</b> 12:00–12:30  

🕐  <b>Классные часы:</b> 12:30–13:00  

<b>3⃣ </b> 13:05–13:50 | 13:55–14:40  

<b>4⃣ </b> 14:45–15:30 | 15:35–16:20  
""",
    "thursday_calls": """
<b>Четверг</b>  

<b>1⃣ </b> 8:30–9:15 | 9:20–10:05  

<b>2⃣ </b> 10:15–11:00  
🍴  <b>Обед:</b> 11:00–11:15  
<b>2⃣ </b> 11:15–12:00  

🍴  <b>Обед:</b> 12:00–12:30  

<b>3⃣ </b> 12:30–13:15 | 13:20–14:05

<b>4⃣ </b> 14:10–14:55 | 15:00–15:45

🕐  <b>Классные часы (1 курс):</b> 15:50–16:20
""",
    "other_calls": """
<b>Другие дни</b>  

<b>1⃣ </b> 8:30–9:15 | 9:20–10:05  

<b>2⃣ </b> 10:15–11:00  
🍴  <b>Обед:</b> 11:00–11:15  
<b>2⃣ </b> 11:15–12:00  

🍴  <b>Обед:</b> 12:00–12:40  

<b>3⃣ </b> 12:40–13:25 | 13:30–14:15  

<b>4⃣ </b> 14:25–15:10 | 15:15–16:00  

<b>5⃣ </b> 16:05–16:50 | 16:55–17:40
"""
}

# кэш картинок
schedule_cache = {}
admin_lists_cache = {}


# база данных

async def get_db():
    return await aiosqlite.connect("subscribers.db")


async def init_db():
    conn = await get_db()

    # таблицы
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS subscribers (
            chat_id INTEGER PRIMARY KEY,
            joined_date TEXT
        )
    """)

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS schedule_updates (
            day TEXT PRIMARY KEY,
            last_hash TEXT,
            last_sent_date TEXT
        )
    """)

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS all_users (
            chat_id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            username TEXT,
            first_interaction_date TEXT
        )
    """)

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS interactions (
            chat_id INTEGER,
            interaction_date TEXT
        )
    """)

    # индексы
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_subscribers_chat_id ON subscribers (chat_id)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_interactions_date ON interactions (interaction_date)")

    await conn.commit()
    await conn.close()


async def add_user(chat_id, first_name, last_name, username):
    conn = await get_db()
    cursor = await conn.execute("SELECT 1 FROM all_users WHERE chat_id = ?", (chat_id,))
    exists = await cursor.fetchone()

    if not exists:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        await conn.execute(
            "INSERT INTO all_users (chat_id, first_name, last_name, username, first_interaction_date) VALUES (?, ?, ?, ?, ?)",
            (chat_id, first_name or "", last_name or "", username or "", now)
        )
        await conn.commit()

    await conn.close()


async def log_interaction(chat_id):
    conn = await get_db()
    today = datetime.now().strftime("%Y-%m-%d")
    await conn.execute("INSERT INTO interactions (chat_id, interaction_date) VALUES (?, ?)", (chat_id, today))
    await conn.commit()
    await conn.close()


async def is_subscribed(chat_id):
    conn = await get_db()
    cursor = await conn.execute("SELECT 1 FROM subscribers WHERE chat_id = ?", (chat_id,))
    result = await cursor.fetchone()
    await conn.close()
    return result is not None


async def subscribe_user(chat_id):
    conn = await get_db()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    await conn.execute("INSERT OR IGNORE INTO subscribers (chat_id, joined_date) VALUES (?, ?)", (chat_id, now))
    await conn.commit()
    await conn.close()


async def unsubscribe_user(chat_id):
    conn = await get_db()
    await conn.execute("DELETE FROM subscribers WHERE chat_id = ?", (chat_id,))
    await conn.commit()
    await conn.close()


async def get_all_subscribers():
    conn = await get_db()
    cursor = await conn.execute("SELECT chat_id FROM subscribers")
    rows = await cursor.fetchall()
    await conn.close()
    return [row[0] for row in rows]


async def get_last_hash(day):
    conn = await get_db()
    cursor = await conn.execute("SELECT last_hash FROM schedule_updates WHERE day = ?", (day,))
    result = await cursor.fetchone()
    await conn.close()
    return result[0] if result else None


async def get_last_sent_date(day):
    conn = await get_db()
    cursor = await conn.execute("SELECT last_sent_date FROM schedule_updates WHERE day = ?", (day,))
    result = await cursor.fetchone()
    await conn.close()
    return result[0] if result else None


async def update_schedule_sent(day, hash_value, sent_date):
    conn = await get_db()
    await conn.execute(
        "INSERT OR REPLACE INTO schedule_updates (day, last_hash, last_sent_date) VALUES (?, ?, ?)",
        (day, hash_value, sent_date)
    )
    await conn.commit()
    await conn.close()


async def get_stats():
    conn = await get_db()

    cursor = await conn.execute("SELECT COUNT(*) FROM all_users")
    total_users = (await cursor.fetchone())[0]

    cursor = await conn.execute("SELECT COUNT(*) FROM subscribers")
    subscribers = (await cursor.fetchone())[0]

    today = datetime.now().strftime("%Y-%m-%d")
    cursor = await conn.execute("SELECT COUNT(DISTINCT chat_id) FROM interactions WHERE interaction_date = ?", (today,))
    daily = (await cursor.fetchone())[0]

    await conn.close()
    return total_users, subscribers, daily


async def get_all_users_list():
    conn = await get_db()
    cursor = await conn.execute("""
        SELECT username, first_name, last_name 
        FROM all_users 
        WHERE username IS NOT NULL AND username != '' 
        ORDER BY first_interaction_date DESC
    """)
    rows = await cursor.fetchall()
    await conn.close()
    return [f"@{row[0]} ({row[1]} {row[2]})" for row in rows]


async def get_subscribers_list():
    conn = await get_db()
    cursor = await conn.execute("""
        SELECT u.username, u.first_name, u.last_name 
        FROM all_users u
        INNER JOIN subscribers s ON u.chat_id = s.chat_id
        WHERE u.username IS NOT NULL AND u.username != ''
        ORDER BY s.joined_date DESC
    """)
    rows = await cursor.fetchall()
    await conn.close()
    return [f"@{row[0]} ({row[1]} {row[2]})" for row in rows]


# работа с файлами

async def download_pdf(file_id):
    url = f"https://drive.google.com/uc?export=download&id={file_id}"

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        response = await client.get(url)

        if response.status_code == 200 and response.content.startswith(b"%PDF"):
            return response.content, None
        else:
            return None, "Ошибка загрузки"


def pdf_to_images(pdf_content):
    doc = fitz.open(stream=pdf_content, filetype="pdf")
    images = []

    for page_num in range(len(doc)):
        page = doc.load_page(page_num)
        pix = page.get_pixmap(dpi=150)

        img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)

        # уменьшаем если большое
        if img.width > 2000 or img.height > 2000:
            img.thumbnail((2000, 2000), Image.Resampling.LANCZOS)

        buffer = BytesIO()
        img.save(buffer, format="PNG", optimize=True, quality=85)
        buffer.seek(0)
        images.append(buffer)

    doc.close()
    return images


def get_file_hash(content):
    return hashlib.sha256(content).hexdigest()


# меню

def main_menu(is_admin=False):
    menu = InlineKeyboardMarkup()
    menu.row(InlineKeyboardButton("🗓️Расписание", callback_data="schedule"))
    menu.row(
        InlineKeyboardButton("🔔Звонки", callback_data="bell"),
        InlineKeyboardButton("📬Рассылка", callback_data="mailing"),
    )
    if is_admin:
        menu.row(InlineKeyboardButton("📊Статистика", callback_data="admin_stats"))
    return menu


def schedule_menu():
    days = [
        ("Понедельник", "monday"),
        ("Вторник", "tuesday"),
        ("Среда", "wednesday"),
        ("Четверг", "thursday"),
        ("Пятница", "friday"),
        ("Суббота", "saturday")
    ]

    menu = InlineKeyboardMarkup()
    for i in range(0, len(days), 3):
        menu.add(*[InlineKeyboardButton(name, callback_data=f"schedule_{key}") for name, key in days[i:i + 3]])
    menu.row(InlineKeyboardButton("Меню", callback_data="back_to_main"))
    return menu


def calls_menu():
    menu = InlineKeyboardMarkup()
    menu.add(
        InlineKeyboardButton("Понедельник", callback_data="monday_calls"),
        InlineKeyboardButton("Четверг", callback_data="thursday_calls"),
        InlineKeyboardButton("Другие дни", callback_data="other_calls")
    )
    menu.row(InlineKeyboardButton("Меню", callback_data="back_to_main"))
    return menu


def mailing_menu(subscribed):
    menu = InlineKeyboardMarkup()
    if subscribed:
        menu.row(InlineKeyboardButton("Отписаться", callback_data="unsubscribe"))
    else:
        menu.row(InlineKeyboardButton("Подписаться", callback_data="subscribe"))
    menu.row(InlineKeyboardButton("Меню", callback_data="back_to_main"))
    return menu


def stats_menu():
    menu = InlineKeyboardMarkup()
    menu.row(InlineKeyboardButton("👥Список пользователей", callback_data="list_users"))
    menu.row(InlineKeyboardButton("👥Список подписчиков", callback_data="list_subscribers"))
    menu.row(InlineKeyboardButton("Меню", callback_data="back_to_main"))
    return menu


def pagination_menu(list_type, page, total_pages):
    menu = InlineKeyboardMarkup()
    row = []

    if page > 1:
        row.append(InlineKeyboardButton("◀️Назад", callback_data=f"{list_type}*page*{page - 1}"))
    if page < total_pages:
        row.append(InlineKeyboardButton("Далее▶️", callback_data=f"{list_type}*page*{page + 1}"))

    if row:
        menu.row(*row)
    menu.row(InlineKeyboardButton("Назад к статистике", callback_data="admin_stats"))
    return menu


def get_donate_text():
    return '<a href="https://www.sberbank.com/sms/pbpn?requisiteNumber=79950614483"><u>Поддержите</u></a> бота для стабильной работы❤️'


# рассылка расписания

async def check_schedule_updates():
    schedule_cache.clear()

    while True:
        try:
            # время UTC+7
            now = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=7)))
            weekday = now.weekday()

            # следующий учебный день
            next_day = {
                0: "tuesday",  # пн -> вт
                1: "wednesday",  # вт -> ср
                2: "thursday",  # ср -> чт
                3: "friday",  # чт -> пт
                4: "saturday",  # пт -> сб
                5: "monday",  # сб -> пн
                6: "monday",  # вс -> пн
            }.get(weekday)

            if not next_day or next_day not in SCHEDULE_FILES:
                await asyncio.sleep(600)  # 10 минут
                continue

            file_info = SCHEDULE_FILES[next_day]

            # качаем pdf
            pdf_content, error = await download_pdf(file_info["id"])
            if not pdf_content:
                print(f"Ошибка скачивания {next_day}: {error}")
                await asyncio.sleep(600)  # 10 минут
                continue

            current_hash = get_file_hash(pdf_content)
            last_hash = await get_last_hash(next_day)
            today = now.strftime("%Y-%m-%d")

            # проверяем изменилось ли
            schedule_changed = (current_hash != last_hash or last_hash is None)

            if schedule_changed:
                # в картинки
                loop = asyncio.get_event_loop()
                images = await loop.run_in_executor(None, pdf_to_images, pdf_content)
                schedule_cache[next_day] = images
                schedule_cache[f"{next_day}_hash"] = current_hash

                # отправляем всем подписчикам
                subscribers = await get_all_subscribers()
                if subscribers:
                    caption_text = f"🔄Обновлено расписание на {file_info['name']}" if schedule_changed else f"📚Расписание на {file_info['name']}"

                    for subscriber_id in subscribers:
                        try:
                            for j, img in enumerate(images):
                                img.seek(0)
                                caption = None

                                if j == len(images) - 1:
                                    caption = f"{caption_text}\n📎<a href=\"{file_info['link']}\">Ссылка на расписание</a>\n\n{get_donate_text()}"

                                await bot.send_photo(
                                    subscriber_id,
                                    photo=img,
                                    caption=caption,
                                    parse_mode="HTML" if caption else None
                                )

                            await log_interaction(subscriber_id)
                            await asyncio.sleep(0.2)

                        except Exception as e:
                            print(f"Ошибка отправки подписчику {subscriber_id}: {e}")

                # записываем в базу
                await update_schedule_sent(next_day, current_hash, today)

            await asyncio.sleep(600)  # 10 минут

        except Exception as e:
            print(f"Ошибка в check_schedule_updates: {e}")
            await asyncio.sleep(60)


# команды

@bot.message_handler(commands=["start"])
async def start(message):
    user = message.from_user
    chat_id = message.chat.id

    await add_user(chat_id, user.first_name, user.last_name, user.username)
    await log_interaction(chat_id)

    is_admin = chat_id == ADMIN_CHAT_ID
    await bot.send_message(
        chat_id,
        f"Привет, {user.first_name}!😊",
        reply_markup=main_menu(is_admin),
        parse_mode="HTML"
    )


@bot.message_handler(commands=["schedule"])
async def schedule(message):
    user = message.from_user
    chat_id = message.chat.id

    await add_user(chat_id, user.first_name, user.last_name, user.username)
    await log_interaction(chat_id)

    await bot.send_message(chat_id, "Выберите день недели☺️", reply_markup=schedule_menu())


@bot.message_handler(commands=["bell"])
async def bell(message):
    user = message.from_user
    chat_id = message.chat.id

    await add_user(chat_id, user.first_name, user.last_name, user.username)
    await log_interaction(chat_id)

    await bot.send_message(chat_id, "Информация о звонках🫨", reply_markup=calls_menu())


@bot.message_handler(commands=["mailing"])
async def mailing(message):
    user = message.from_user
    chat_id = message.chat.id

    await add_user(chat_id, user.first_name, user.last_name, user.username)
    await log_interaction(chat_id)

    subscribed = await is_subscribed(chat_id)
    text = "Вы подписаны на рассылку!✅" if subscribed else "Вы отписаны от рассылки!❎"
    await bot.send_message(chat_id, text, reply_markup=mailing_menu(subscribed))


@bot.message_handler(commands=["stats"])
async def stats(message):
    if message.chat.id != ADMIN_CHAT_ID:
        await bot.send_message(message.chat.id, "Доступ запрещен")
        return

    total, subscribers, daily = await get_stats()
    text = f"📊Статистика:\n\nВсего использовали: {total}\nПодписано на рассылку: {subscribers}\nАктивных сегодня: {daily}"
    await bot.send_message(message.chat.id, text, reply_markup=stats_menu())


# кнопки

@bot.callback_query_handler(func=lambda call: call.data == "schedule")
async def schedule_callback(call):
    user = call.from_user
    chat_id = call.message.chat.id

    await add_user(chat_id, user.first_name, user.last_name, user.username)
    await log_interaction(chat_id)

    try:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text="Выберите день недели☺️",
            reply_markup=schedule_menu()
        )
    except:
        await bot.send_message(chat_id, "Выберите день недели☺️", reply_markup=schedule_menu())


@bot.callback_query_handler(func=lambda call: call.data.startswith("schedule_"))
async def schedule_day(call):
    day = call.data.split("_")[1]
    user = call.from_user
    chat_id = call.message.chat.id

    await add_user(chat_id, user.first_name, user.last_name, user.username)
    await log_interaction(chat_id)

    if day not in SCHEDULE_FILES:
        await bot.answer_callback_query(call.id, text="Неизвестный день")
        return

    file_info = SCHEDULE_FILES[day]
    await bot.answer_callback_query(call.id, text="Загружаю...")

    # проверяем актуальность кэша
    pdf_content, error = await download_pdf(file_info["id"])
    if not pdf_content:
        await bot.send_message(
            chat_id,
            f"❌Не удалось загрузить\n📎<a href=\"{file_info['link']}\">Ссылка на расписание</a>",
            reply_markup=schedule_menu(),
            parse_mode="HTML"
        )
        return

    current_hash = get_file_hash(pdf_content)
    cached_images = schedule_cache.get(day)

    # если кэша нет или файл изменился - обновляем
    if not cached_images or current_hash != schedule_cache.get(f"{day}_hash"):
        loop = asyncio.get_event_loop()
        images = await loop.run_in_executor(None, pdf_to_images, pdf_content)
        schedule_cache[day] = images
        schedule_cache[f"{day}_hash"] = current_hash
    else:
        images = cached_images

    # шлем
    for i, img in enumerate(images):
        # создаем новый BytesIO из содержимого
        img.seek(0)
        img_copy = BytesIO(img.read())
        img.seek(0)  # возвращаем позицию для следующего использования

        caption = None
        markup = None

        if i == len(images) - 1:
            caption = f"📚Расписание на {file_info['name']}\n📎<a href=\"{file_info['link']}\">Ссылка на расписание</a>\n\n{get_donate_text()}"
            markup = schedule_menu()

        await bot.send_photo(chat_id, photo=img_copy, caption=caption, parse_mode="HTML" if caption else None,
                             reply_markup=markup)


@bot.callback_query_handler(func=lambda call: True)
async def callback_handler(call):
    chat_id = call.message.chat.id
    user = call.from_user

    await add_user(chat_id, user.first_name, user.last_name, user.username)
    await log_interaction(chat_id)

    # админка
    if call.data == "admin_stats":
        if chat_id != ADMIN_CHAT_ID:
            await bot.answer_callback_query(call.id, text="Доступ запрещен")
            return

        total, subscribers, daily = await get_stats()
        text = f"📊Статистика:\n\nВсего использовали: {total}\nПодписано на рассылку: {subscribers}\nАктивных сегодня: {daily}"
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text=text,
            reply_markup=stats_menu()
        )
        return

    # списки юзеров
    if call.data.startswith("list_users"):
        if chat_id != ADMIN_CHAT_ID:
            await bot.answer_callback_query(call.id, text="Доступ запрещен")
            return

        page = 1 if call.data == "list_users" else int(call.data.split("*")[-1])

        if call.data == "list_users":
            users_list = await get_all_users_list()
            admin_lists_cache[chat_id] = {'users': users_list}
        else:
            users_list = admin_lists_cache.get(chat_id, {}).get('users', [])

        total_pages = (len(users_list) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
        start = (page - 1) * ITEMS_PER_PAGE
        page_items = users_list[start:start + ITEMS_PER_PAGE]

        text = f"👥 Список пользователей (страница {page}/{total_pages}):\n\n" + "\n".join(page_items)
        markup = pagination_menu("list_users", page, total_pages)

        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text=text,
            reply_markup=markup
        )
        return

    if call.data.startswith("list_subscribers"):
        if chat_id != ADMIN_CHAT_ID:
            await bot.answer_callback_query(call.id, text="Доступ запрещен")
            return

        page = 1 if call.data == "list_subscribers" else int(call.data.split("*")[-1])

        if call.data == "list_subscribers":
            subs_list = await get_subscribers_list()
            admin_lists_cache[chat_id] = {'subscribers': subs_list}
        else:
            subs_list = admin_lists_cache.get(chat_id, {}).get('subscribers', [])

        total_pages = (len(subs_list) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
        start = (page - 1) * ITEMS_PER_PAGE
        page_items = subs_list[start:start + ITEMS_PER_PAGE]

        text = f"👥 Список подписчиков (страница {page}/{total_pages}):\n\n" + "\n".join(page_items)
        markup = pagination_menu("list_subscribers", page, total_pages)

        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text=text,
            reply_markup=markup
        )
        return

    # остальное
    if call.data == "mailing":
        subscribed = await is_subscribed(chat_id)
        text = "Вы подписаны на рассылку!✅" if subscribed else "Вы отписаны от рассылки!❎"
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text=text,
            reply_markup=mailing_menu(subscribed)
        )

    elif call.data in CALL_SCHEDULE:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text=CALL_SCHEDULE[call.data],
            parse_mode="HTML",
            reply_markup=calls_menu()
        )

    elif call.data == "bell":
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text="Информация о звонках🫨",
            reply_markup=calls_menu()
        )

    elif call.data == "subscribe":
        await subscribe_user(chat_id)
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text="Вы подписаны на рассылку!✅",
            reply_markup=mailing_menu(True)
        )

    elif call.data == "unsubscribe":
        await unsubscribe_user(chat_id)
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text="Вы отписаны от рассылки!❎",
            reply_markup=mailing_menu(False)
        )

    elif call.data == "back_to_main":
        is_admin = chat_id == ADMIN_CHAT_ID
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text="Выберите кнопку ниже😊",
                reply_markup=main_menu(is_admin)
            )
        except:
            await bot.send_message(chat_id, "Выберите кнопку ниже😊", reply_markup=main_menu(is_admin))


# запуск

async def set_commands():
    await bot.set_my_commands([
        BotCommand("start", "🚀Старт"),
        BotCommand("schedule", "🗓️Расписание"),
        BotCommand("bell", "🔔Звонки"),
        BotCommand("mailing", "📬Рассылка"),
    ])


async def log_stats():
    while True:
        total, subscribers, _ = await get_stats()
        print(f"Stats: subscribers {subscribers}, all users {total}")
        await asyncio.sleep(3600)


async def main():
    await init_db()
    await set_commands()

    asyncio.create_task(check_schedule_updates())
    asyncio.create_task(log_stats())

    await bot.polling(non_stop=True, skip_pending=True)


if __name__ == "__main__":
    if os.name == "nt":
        os.system("chcp 65001 > nul")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped by user")
    except Exception as e:
        print(f"Critical error: {e}")