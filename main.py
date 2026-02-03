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
from collections import defaultdict
import time

# токен бота
API_TOKEN = os.getenv("API_TOKEN")
if not API_TOKEN:
    raise RuntimeError("Нужен API_TOKEN")

bot = AsyncTeleBot(API_TOKEN)
ADMIN_ID = 6986627524

# расписания с гугла
SCHEDULE_FILES = {
    "monday": {"id": "1d7xrNLd8qpde_5jLvBdJjG9e3eOsjohK", "name": "понедельник",
               "link": "https://drive.google.com/file/d/1d7xrNLd8qpde_5jLvBdJjG9e3eOsjohK/view"},
    "tuesday": {"id": "1qHNHC7uwXdECuEMfDoPiuv5bX0Ip0OpQ", "name": "вторник",
                "link": "https://drive.google.com/file/d/1qHNHC7uwXdECuEMfDoPiuv5bX0Ip0OpQ/view"},
    "wednesday": {"id": "1hWMqMdeU2rcrNMx4jbOCr5ofGixsIJwA", "name": "среду",
                  "link": "https://drive.google.com/file/d/1hWMqMdeU2rcrNMx4jbOCr5ofGixsIJwA/view"},
    "thursday": {"id": "1O649rLM_VuBO31VF49noXfp1Evr-XfCN", "name": "четверг",
                 "link": "https://drive.google.com/file/d/1O649rLM_VuBO31VF49noXfp1Evr-XfCN/view"},
    "friday": {"id": "1YmQGiirdBryJlI3tx0SdU-g1gGm-6AaW", "name": "пятницу",
               "link": "https://drive.google.com/file/d/1YmQGiirdBryJlI3tx0SdU-g1gGm-6AaW/view"},
    "saturday": {"id": "1hkXSDN-Dz86QGeyjhLZ7jlvSd9sMwmex", "name": "субботу",
                 "link": "https://drive.google.com/file/d/1hkXSDN-Dz86QGeyjhLZ7jlvSd9sMwmex/view"},
}

# звонки
CALLS = {
    "monday_calls": """<b>Понедельник</b>

<b>1⃣</b> 8:30–9:15 | 9:20–10:05

<b>2⃣</b> 10:15–11:00
🍴 <b>Обед:</b> 11:00–11:15
<b>2⃣</b> 11:15–12:00

🍴 <b>Обед:</b> 12:00–12:30

🕐 <b>Классные часы:</b> 12:30–13:00

<b>3⃣</b> 13:05–13:50 | 13:55–14:40

<b>4⃣</b> 14:45–15:30 | 15:35–16:20""",

    "thursday_calls": """<b>Четверг</b>

<b>1⃣</b> 8:30–9:15 | 9:20–10:05

<b>2⃣</b> 10:15–11:00
🍴 <b>Обед:</b> 11:00–11:15
<b>2⃣</b> 11:15–12:00

🍴 <b>Обед:</b> 12:00–12:30

<b>3⃣</b> 12:30–13:15 | 13:20–14:05

<b>4⃣</b> 14:10–14:55 | 15:00–15:45

🕐 <b>Классные часы (1 курс):</b> 15:50–16:20""",

    "other_calls": """<b>Другие дни</b>

<b>1⃣</b> 8:30–9:15 | 9:20–10:05

<b>2⃣</b> 10:15–11:00
🍴 <b>Обед:</b> 11:00–11:15
<b>2⃣</b> 11:15–12:00

🍴 <b>Обед:</b> 12:00–12:40

<b>3⃣</b> 12:40–13:25 | 13:30–14:15

<b>4⃣</b> 14:25–15:10 | 15:15–16:00

<b>5⃣</b> 16:05–16:50 | 16:55–17:40"""
}

# кэш и локи
schedule_cache = {}
hash_cache = {}
cache_ts = {}
locks = defaultdict(asyncio.Lock)
admin_data = {}
send_limit = asyncio.Semaphore(25)  # макс 25 отправок одновременно

PAGE_SIZE = 50
CACHE_TTL = 3600

# глобальное соединение с бд
db_conn = None


async def get_db():
    global db_conn
    if db_conn is None:
        db_conn = await aiosqlite.connect("subscribers.db")
    return db_conn


async def init_db():
    db = await get_db()

    await db.execute("""CREATE TABLE IF NOT EXISTS subscribers (
        chat_id INTEGER PRIMARY KEY, joined_date TEXT)""")

    await db.execute("""CREATE TABLE IF NOT EXISTS schedule_updates (
        day TEXT PRIMARY KEY, last_hash TEXT, last_sent_date TEXT)""")

    await db.execute("""CREATE TABLE IF NOT EXISTS all_users (
        chat_id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, 
        username TEXT, first_interaction_date TEXT)""")

    await db.execute("""CREATE TABLE IF NOT EXISTS interactions (
        chat_id INTEGER, interaction_date TEXT)""")

    await db.execute("CREATE INDEX IF NOT EXISTS idx_sub ON subscribers(chat_id)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_day ON schedule_updates(day)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_int ON interactions(interaction_date)")

    # миграция
    try:
        cur = await db.execute("PRAGMA table_info(all_users)")
        cols = [r[1] for r in await cur.fetchall()]
        if "username" not in cols:
            await db.execute("ALTER TABLE all_users ADD COLUMN username TEXT")
    except:
        pass

    try:
        cur = await db.execute("PRAGMA table_info(schedule_updates)")
        cols = [r[1] for r in await cur.fetchall()]
        if "last_sent_date" not in cols:
            await db.execute("ALTER TABLE schedule_updates ADD COLUMN last_sent_date TEXT")
    except:
        pass

    await db.commit()


async def save_user(chat_id, fname, lname, uname):
    db = await get_db()
    cur = await db.execute("SELECT 1 FROM all_users WHERE chat_id=?", (chat_id,))
    if not await cur.fetchone():
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        await db.execute("INSERT INTO all_users VALUES (?,?,?,?,?)",
                         (chat_id, fname or "", lname or "", uname or "", now))
        await db.commit()


async def log_action(chat_id):
    db = await get_db()
    today = datetime.now().strftime("%Y-%m-%d")
    await db.execute("INSERT INTO interactions VALUES (?,?)", (chat_id, today))
    await db.commit()


async def check_sub(chat_id):
    db = await get_db()
    cur = await db.execute("SELECT 1 FROM subscribers WHERE chat_id=?", (chat_id,))
    res = await cur.fetchone()

    return res is not None


async def add_sub(chat_id):
    db = await get_db()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    await db.execute("INSERT OR IGNORE INTO subscribers VALUES (?,?)", (chat_id, now))
    await db.commit()


async def del_sub(chat_id):
    db = await get_db()
    await db.execute("DELETE FROM subscribers WHERE chat_id=?", (chat_id,))
    await db.commit()


async def get_subs():
    db = await get_db()
    cur = await db.execute("SELECT chat_id FROM subscribers")
    rows = await cur.fetchall()

    return [r[0] for r in rows]


async def get_users():
    db = await get_db()
    cur = await db.execute("SELECT chat_id FROM all_users")
    rows = await cur.fetchall()

    return [r[0] for r in rows]


async def get_hash_db(day):
    db = await get_db()
    cur = await db.execute("SELECT last_hash FROM schedule_updates WHERE day=?", (day,))
    res = await cur.fetchone()

    return res[0] if res else None


async def save_hash(day, h, date):
    db = await get_db()
    await db.execute("INSERT OR REPLACE INTO schedule_updates VALUES (?,?,?)", (day, h, date))
    await db.commit()


async def get_stats():
    db = await get_db()
    cur = await db.execute("SELECT COUNT(*) FROM all_users")
    total = (await cur.fetchone())[0]
    cur = await db.execute("SELECT COUNT(*) FROM subscribers")
    subs = (await cur.fetchone())[0]
    today = datetime.now().strftime("%Y-%m-%d")
    cur = await db.execute("SELECT COUNT(DISTINCT chat_id) FROM interactions WHERE interaction_date=?", (today,))
    daily = (await cur.fetchone())[0]

    return total, subs, daily


async def users_list():
    db = await get_db()
    cur = await db.execute("""SELECT username, first_name, last_name FROM all_users 
        WHERE username IS NOT NULL AND username != '' ORDER BY first_interaction_date DESC""")
    rows = await cur.fetchall()

    return [f"@{r[0]} ({r[1]} {r[2]})" for r in rows]


async def subs_list():
    db = await get_db()
    cur = await db.execute("""SELECT u.username, u.first_name, u.last_name FROM all_users u
        INNER JOIN subscribers s ON u.chat_id = s.chat_id
        WHERE u.username IS NOT NULL AND u.username != '' ORDER BY s.joined_date DESC""")
    rows = await cur.fetchall()

    return [f"@{r[0]} ({r[1]} {r[2]})" for r in rows]


# === PDF ===

async def download_pdf(fid):
    url = f"https://drive.google.com/uc?export=download&id={fid}"
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/pdf"}

    for i in range(3):
        try:
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
                r = await c.get(url, headers=headers)
                if r.status_code == 429:
                    await asyncio.sleep(2 ** i)
                    continue
                r.raise_for_status()
                if r.content.startswith(b"%PDF"):
                    return r.content, None
                return None, "не pdf"
        except Exception as e:
            if i == 2:
                return None, str(e)
            await asyncio.sleep(2 ** i)
    return None, "не скачалось"


def make_images(pdf):
    doc = fitz.open(stream=pdf, filetype="pdf")
    imgs = []
    for i in range(len(doc)):
        page = doc.load_page(i)
        pix = page.get_pixmap(dpi=150)
        img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
        if img.width > 2000 or img.height > 2000:
            img.thumbnail((2000, 2000), Image.Resampling.LANCZOS)
        buf = BytesIO()
        img.save(buf, format="PNG", optimize=True)
        buf.seek(0)
        imgs.append(buf)
    doc.close()
    return imgs


def calc_hash(data):
    return hashlib.sha256(data).hexdigest()


def donate_link():
    return '❤️<a href="https://www.sberbank.com/sms/pbpn?requisiteNumber=79950614483">Поддержать бота</a> - сервер платный, буду благодарен за помощь!'


# === МЕНЮ ===

def menu_main(admin=False):
    m = InlineKeyboardMarkup()
    m.row(InlineKeyboardButton("🗓️Расписание", callback_data="schedule"))
    m.row(InlineKeyboardButton("🔔Звонки", callback_data="bell"),
          InlineKeyboardButton("📬Рассылка", callback_data="mailing"))
    if admin:
        m.row(InlineKeyboardButton("📊Статистика", callback_data="admin_stats"))
    return m


def menu_days():
    days = [("Понедельник", "monday"), ("Вторник", "tuesday"), ("Среда", "wednesday"),
            ("Четверг", "thursday"), ("Пятница", "friday"), ("Суббота", "saturday")]
    m = InlineKeyboardMarkup()
    for i in range(0, 6, 3):
        m.add(*[InlineKeyboardButton(n, callback_data=f"day_{k}") for n, k in days[i:i + 3]])
    m.row(InlineKeyboardButton("Меню", callback_data="main"))
    return m


def menu_calls():
    m = InlineKeyboardMarkup()
    m.add(InlineKeyboardButton("Понедельник", callback_data="monday_calls"),
          InlineKeyboardButton("Четверг", callback_data="thursday_calls"),
          InlineKeyboardButton("Другие дни", callback_data="other_calls"))
    m.row(InlineKeyboardButton("Меню", callback_data="main"))
    return m


def menu_mail(subbed):
    m = InlineKeyboardMarkup()
    if subbed:
        m.row(InlineKeyboardButton("Отписаться", callback_data="unsub"))
    else:
        m.row(InlineKeyboardButton("Подписаться", callback_data="sub"))
    m.row(InlineKeyboardButton("Меню", callback_data="main"))
    return m


def menu_stats():
    m = InlineKeyboardMarkup()
    m.row(InlineKeyboardButton("👥Пользователи", callback_data="list_users"))
    m.row(InlineKeyboardButton("👥Подписчики", callback_data="list_subs"))
    m.row(InlineKeyboardButton("Меню", callback_data="main"))
    return m


def menu_pages(typ, pg, total):
    m = InlineKeyboardMarkup()
    btns = []
    if pg > 1:
        btns.append(InlineKeyboardButton("◀️", callback_data=f"{typ}_{pg - 1}"))
    if pg < total:
        btns.append(InlineKeyboardButton("▶️", callback_data=f"{typ}_{pg + 1}"))
    if btns:
        m.row(*btns)
    m.row(InlineKeyboardButton("📊Статистика", callback_data="admin_stats"))
    return m


# === КЭШ ===

async def from_cache(day):
    if day in schedule_cache and time.time() - cache_ts.get(day, 0) < CACHE_TTL:
        return schedule_cache[day]
    # чистим если устарел
    schedule_cache.pop(day, None)
    hash_cache.pop(day, None)
    cache_ts.pop(day, None)
    return None


async def to_cache(day, imgs, h):
    schedule_cache[day] = imgs
    hash_cache[day] = h
    cache_ts[day] = time.time()


# === РАССЫЛКА ===

async def send_to_user(uid, imgs, info, caption):
    async with send_limit:  # ограничение параллельных отправок
        try:
            for j, img in enumerate(imgs):
                img.seek(0)
                copy = BytesIO(img.read())
                img.seek(0)
                cap = None
                if j == len(imgs) - 1:
                    cap = f"{caption}\n📎<a href=\"{info['link']}\">Расписание</a>\n\n{donate_link()}"
                await bot.send_photo(uid, copy, caption=cap, parse_mode="HTML" if cap else None)
            await log_action(uid)
        except Exception as e:
            print(f"err send {uid}: {e}")
            raise


async def mass_send(users, imgs, info, caption):
    ok = err = 0
    # шлём всем параллельно, семафор сам ограничит
    tasks = [send_to_user(u, imgs, info, caption) for u in users]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            err += 1
        else:
            ok += 1
    return ok, err


# === ПРОВЕРКА ОБНОВЛЕНИЙ ===

async def check_loop():
    schedule_cache.clear()
    hash_cache.clear()
    cache_ts.clear()

    while True:
        try:
            now = datetime.now(timezone(timedelta(hours=7)))
            wd = now.weekday()

            next_day = {0: "tuesday", 1: "wednesday", 2: "thursday",
                        3: "friday", 4: "saturday", 5: "monday", 6: "monday"}.get(wd)

            if not next_day or next_day not in SCHEDULE_FILES:
                await asyncio.sleep(900)
                continue

            info = SCHEDULE_FILES[next_day]
            pdf, err = await download_pdf(info["id"])

            if not pdf:
                print(f"не скачал {next_day}: {err}")
                await asyncio.sleep(900)
                continue

            cur_hash = calc_hash(pdf)
            old_hash = await get_hash_db(next_day)

            if cur_hash != old_hash:
                imgs = make_images(pdf)
                await to_cache(next_day, imgs, cur_hash)

                subs = await get_subs()
                if subs:
                    ok, fail = await mass_send(subs, imgs, info, f"🔄Расписание на {info['name']}")
                    print(f"рассылка: {ok} ок, {fail} ошибок")

                await save_hash(next_day, cur_hash, now.strftime("%Y-%m-%d"))
            else:
                print(f"без изменений: {next_day}")

            await asyncio.sleep(900)

        except Exception as e:
            print(f"ошибка check: {e}")
            await asyncio.sleep(60)


# === КОМАНДЫ ===

@bot.message_handler(commands=["start"])
async def cmd_start(msg):
    u = msg.from_user
    await save_user(msg.chat.id, u.first_name, u.last_name, u.username)
    await log_action(msg.chat.id)
    await bot.send_message(msg.chat.id, f"Привет, {u.first_name}!😊",
                           reply_markup=menu_main(msg.chat.id == ADMIN_ID))


@bot.message_handler(commands=["schedule"])
async def cmd_schedule(msg):
    u = msg.from_user
    await save_user(msg.chat.id, u.first_name, u.last_name, u.username)
    await log_action(msg.chat.id)
    await bot.send_message(msg.chat.id, "Выберите день☺️", reply_markup=menu_days())


@bot.message_handler(commands=["bell"])
async def cmd_bell(msg):
    u = msg.from_user
    await save_user(msg.chat.id, u.first_name, u.last_name, u.username)
    await log_action(msg.chat.id)
    await bot.send_message(msg.chat.id, "Информация о звонках🔔", reply_markup=menu_calls())


@bot.message_handler(commands=["mailing"])
async def cmd_mailing(msg):
    u = msg.from_user
    await save_user(msg.chat.id, u.first_name, u.last_name, u.username)
    await log_action(msg.chat.id)
    subbed = await check_sub(msg.chat.id)
    txt = "Вы подписаны✅" if subbed else "Вы отписаны❎"
    await bot.send_message(msg.chat.id, txt, reply_markup=menu_mail(subbed))


@bot.message_handler(commands=["stats"])
async def cmd_stats(msg):
    if msg.chat.id != ADMIN_ID:
        await bot.send_message(msg.chat.id, "нет доступа")
        return
    total, subs, daily = await get_stats()
    await bot.send_message(msg.chat.id,
                           f"📊Статистика:\n/broadcast \n\nВсего: {total}\nПодписаны: {subs}\nСегодня: {daily}",
                           reply_markup=menu_stats())


@bot.message_handler(commands=["broadcast"])
async def cmd_broadcast(msg):
    if msg.chat.id != ADMIN_ID:
        return
    txt = msg.text.replace("/broadcast", "").strip()
    if not txt:
        await bot.send_message(msg.chat.id, "напиши /broadcast текст")
        return

    users = await get_users()
    if not users:
        await bot.send_message(msg.chat.id, "нет юзеров")
        return

    markup = InlineKeyboardMarkup()
    markup.row(InlineKeyboardButton("✅Да", callback_data="bc_yes"),
               InlineKeyboardButton("❌Нет", callback_data="bc_no"))

    admin_data[msg.chat.id] = {"bc_text": txt, "bc_users": users}
    await bot.send_message(msg.chat.id,
                           f"Отправить {len(users)} людям?\n\n<b>Текст:</b>\n{txt}",
                           parse_mode="HTML", reply_markup=markup)


# === КОЛБЕКИ ===

@bot.callback_query_handler(func=lambda c: c.data == "schedule")
async def cb_schedule(call):
    u = call.from_user
    await save_user(call.message.chat.id, u.first_name, u.last_name, u.username)
    await log_action(call.message.chat.id)
    try:
        await bot.edit_message_text(chat_id=call.message.chat.id,
                                    message_id=call.message.message_id, text="Выберите день☺️",
                                    reply_markup=menu_days())
    except:
        await bot.send_message(call.message.chat.id, "Выберите день☺️", reply_markup=menu_days())


@bot.callback_query_handler(func=lambda c: c.data.startswith("day_"))
async def cb_day(call):
    day = call.data[4:]
    u = call.from_user
    await save_user(call.message.chat.id, u.first_name, u.last_name, u.username)
    await log_action(call.message.chat.id)

    if day not in SCHEDULE_FILES:
        await bot.answer_callback_query(call.id, "неизвестный день")
        return

    info = SCHEDULE_FILES[day]
    await bot.answer_callback_query(call.id, "Загружаю...")

    try:
        async with locks[day]:
            cached = await from_cache(day)
            if cached:
                imgs = cached
            else:
                pdf, err = await download_pdf(info["id"])
                if not pdf:
                    await bot.send_message(call.message.chat.id,
                                           f"❌Ошибка\n<a href=\"{info['link']}\">Открыть</a>",
                                           reply_markup=menu_days(), parse_mode="HTML")
                    return
                imgs = make_images(pdf)
                await to_cache(day, imgs, calc_hash(pdf))

        for j, img in enumerate(imgs):
            img.seek(0)
            copy = BytesIO(img.read())
            img.seek(0)
            cap = None
            markup = None
            if j == len(imgs) - 1:
                cap = f"📚Расписание на {info['name']}\n📎<a href=\"{info['link']}\">Ссылка на расписание</a>\n\n{donate_link()}"
                markup = menu_days()
            await bot.send_photo(call.message.chat.id, copy, caption=cap,
                                 parse_mode="HTML" if cap else None, reply_markup=markup)

    except Exception as e:
        await bot.answer_callback_query(call.id, "ошибка")
        print(f"err day: {e}")


@bot.callback_query_handler(func=lambda c: True)
async def cb_other(call):
    cid = call.message.chat.id
    u = call.from_user
    await save_user(cid, u.first_name, u.last_name, u.username)
    await log_action(cid)

    try:
        data = call.data

        # админка
        if data == "admin_stats":
            if cid != ADMIN_ID:
                await bot.answer_callback_query(call.id, "нет")
                return
            total, subs, daily = await get_stats()
            await bot.edit_message_text(chat_id=cid, message_id=call.message.message_id,
                                        text=f"📊Статистика:\n/broadcast \n\nВсего: {total}\nПодписаны: {subs}\nСегодня: {daily}",
                                        reply_markup=menu_stats())

        elif data.startswith("list_users"):
            if cid != ADMIN_ID:
                return
            pg = 1 if data == "list_users" else int(data.split("_")[-1])
            if data == "list_users":
                admin_data[cid] = admin_data.get(cid, {})
                admin_data[cid]["ul"] = await users_list()
            ul = admin_data.get(cid, {}).get("ul", [])
            pages = (len(ul) + PAGE_SIZE - 1) // PAGE_SIZE or 1
            start = (pg - 1) * PAGE_SIZE
            items = ul[start:start + PAGE_SIZE]
            await bot.edit_message_text(chat_id=cid, message_id=call.message.message_id,
                                        text=f"👥Пользователи ({pg}/{pages}):\n\n" + "\n".join(items),
                                        reply_markup=menu_pages("list_users", pg, pages))

        elif data.startswith("list_subs"):
            if cid != ADMIN_ID:
                return
            pg = 1 if data == "list_subs" else int(data.split("_")[-1])
            if data == "list_subs":
                admin_data[cid] = admin_data.get(cid, {})
                admin_data[cid]["sl"] = await subs_list()
            sl = admin_data.get(cid, {}).get("sl", [])
            pages = (len(sl) + PAGE_SIZE - 1) // PAGE_SIZE or 1
            start = (pg - 1) * PAGE_SIZE
            items = sl[start:start + PAGE_SIZE]
            await bot.edit_message_text(chat_id=cid, message_id=call.message.message_id,
                                        text=f"👥Подписчики ({pg}/{pages}):\n\n" + "\n".join(items),
                                        reply_markup=menu_pages("list_subs", pg, pages))

        # broadcast
        elif data == "bc_yes":
            if cid != ADMIN_ID:
                return
            d = admin_data.get(cid, {})
            txt = d.get("bc_text")
            users = d.get("bc_users", [])
            if not txt or not users:
                return

            await bot.edit_message_text(chat_id=cid, message_id=call.message.message_id, text="🔄...")
            ok = err = 0
            for i in range(0, len(users), 20):
                batch = users[i:i + 20]
                for uid in batch:
                    try:
                        await bot.send_message(uid, txt, parse_mode="HTML")
                        ok += 1
                    except:
                        err += 1
                if i + 20 < len(users):
                    await asyncio.sleep(1)

            await bot.send_message(cid, f"✅Готово\nОтправлено: {ok}\nОшибок: {err}")
            admin_data.pop(cid, None)

        elif data == "bc_no":
            await bot.edit_message_text(chat_id=cid, message_id=call.message.message_id, text="❌Отменено")
            admin_data.pop(cid, None)

        # меню
        elif data == "mailing":
            subbed = await check_sub(cid)
            txt = "Вы подписаны✅" if subbed else "Вы отписаны❎"
            await bot.edit_message_text(chat_id=cid, message_id=call.message.message_id,
                                        text=txt, reply_markup=menu_mail(subbed))

        elif data in CALLS:
            await bot.edit_message_text(chat_id=cid, message_id=call.message.message_id,
                                        text=CALLS[data], parse_mode="HTML", reply_markup=menu_calls())

        elif data == "bell":
            await bot.edit_message_text(chat_id=cid, message_id=call.message.message_id,
                                        text="Информация о звонках🔔", reply_markup=menu_calls())

        elif data == "sub":
            await add_sub(cid)
            await bot.edit_message_text(chat_id=cid, message_id=call.message.message_id,
                                        text="Вы подписаны✅", reply_markup=menu_mail(True))

        elif data == "unsub":
            await del_sub(cid)
            await bot.edit_message_text(chat_id=cid, message_id=call.message.message_id,
                                        text="Вы отписаны❎", reply_markup=menu_mail(False))

        elif data == "main":
            admin = cid == ADMIN_ID
            try:
                await bot.edit_message_text(chat_id=cid, message_id=call.message.message_id,
                                            text="Выберите кнопку ниже😊", reply_markup=menu_main(admin))
            except:
                await bot.send_message(cid, "Выберите кнопку ниже😊", reply_markup=menu_main(admin))

    except Exception as e:
        await bot.answer_callback_query(call.id, "ошибка")
        print(f"cb err: {e}")


# === ЗАПУСК ===

async def setup():
    await bot.set_my_commands([
        BotCommand("start", "🚀Старт"),
        BotCommand("schedule", "🗓️Расписание"),
        BotCommand("bell", "🔔Звонки"),
        BotCommand("mailing", "📬Рассылка"),
    ])


async def stats_log():
    while True:
        try:
            t, s, _ = await get_stats()
            print(f"stats: {s} subs, {t} total")
        except:
            pass
        await asyncio.sleep(3600)


async def main():
    await init_db()
    await setup()
    asyncio.create_task(check_loop())
    asyncio.create_task(stats_log())
    print("бот запущен")
    await bot.polling(non_stop=True, skip_pending=True)


if __name__ == "__main__":
    if os.name == "nt":
        os.system("chcp 65001 > nul")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nстоп")
    except Exception as e:
        print(f"ошибка: {e}")