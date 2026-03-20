import asyncio
import random
import re
import aiosqlite
import logging
import uuid
import os
from datetime import datetime, timedelta
from pyrogram import Client, enums
from pyrogram.errors import (
    SessionPasswordNeeded, FloodWait, PhoneCodeExpired, PhoneCodeInvalid,
    PhoneNumberInvalid, PhoneNumberBanned, PhoneNumberFlood,
    PhoneNumberUnoccupied, NetworkMigrateError, PhoneMigrateError,
    UserDeactivated, UserDeactivatedBan, PeerFlood, UserBannedInChannel,
    ChatWriteForbidden, SlowmodeWait,
)
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

# ─── SOZLAMALAR ───────────────────────────────────────────
API_ID        = 39613506
API_HASH      = "25c8bbeba9f1d15a50b9115a261c51c3"
BOT_TOKEN     = "8406914832:AAGtStXY32nrHe7n7P14_Mq0wQ5T940SC1k"
ADMIN_ID      = 7767070578
DB_NAME       = "final_bot.db"
DEFAULT_LIMIT = 2000
WARN_PERCENT  = 0.80

# Ban xavfi chegaralari
BAN_WARN_ERRORS   = 3   # Ketma-ket xato soni — sariq ogohlantirish
BAN_DANGER_ERRORS = 7   # Ketma-ket xato soni — qizil xavf
FLOOD_WARN_SEC    = 60  # FloodWait bu soniyadan oshsa — ogohlantirish

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher(storage=MemoryStorage())
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

login_sessions: dict = {}

# ─── DELAY VARIANTLARI ────────────────────────────────────
DELAY_OPTIONS = [
    ("1 daqiqa",  60,   "🟡", "O'rtacha xavf"),
    ("3 daqiqa",  180,  "🟢", "Xavfsiz"),
    ("5 daqiqa",  300,  "🟢", "Xavfsiz"),
    ("7 daqiqa",  420,  "🟢", "Xavfsiz"),
    ("30 daqiqa", 1800, "🟢", "Juda xavfsiz"),
    ("1 soat",    3600, "🟢", "Eng xavfsiz"),
]
GROUP_PAUSE = 15

# ─── FSM ──────────────────────────────────────────────────
class LoginState(StatesGroup):
    waiting_phone   = State()
    waiting_code    = State()   # to'liq kod bir marta
    waiting_2fa     = State()

class BroadcastState(StatesGroup):
    waiting_text  = State()
    waiting_count = State()
    waiting_delay = State()

class AdminState(StatesGroup):
    waiting_reset_password = State()

# ─── DATABASE ─────────────────────────────────────────────
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(f"""
            CREATE TABLE IF NOT EXISTS users (
                user_id       INTEGER PRIMARY KEY,
                expiry_date   TEXT,
                is_active     INTEGER DEFAULT 0,
                daily_limit   INTEGER DEFAULT {DEFAULT_LIMIT},
                used_today    INTEGER DEFAULT 0,
                phone         TEXT,
                full_name     TEXT,
                consec_errors INTEGER DEFAULT 0
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS keys (
                key_code   TEXT PRIMARY KEY,
                days       INTEGER,
                is_used    INTEGER DEFAULT 0,
                created_at TEXT,
                used_by    INTEGER
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_groups (
                user_id  INTEGER,
                chat_id  INTEGER,
                title    TEXT,
                selected INTEGER DEFAULT 1,
                PRIMARY KEY (user_id, chat_id)
            )""")
        # Eski DB uchun ustunlar qo'shish
        for col_def in [
            "ALTER TABLE users ADD COLUMN full_name TEXT",
            "ALTER TABLE users ADD COLUMN consec_errors INTEGER DEFAULT 0",
            "ALTER TABLE keys ADD COLUMN created_at TEXT",
            "ALTER TABLE keys ADD COLUMN used_by INTEGER",
        ]:
            try:
                await db.execute(col_def)
            except Exception:
                pass
        await db.commit()
    log.info("Database tayyor")

async def get_user(uid: int):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM users WHERE user_id=?", (uid,))
        return await cur.fetchone()

async def check_access(uid: int) -> bool:
    if uid == ADMIN_ID:
        return True
    u = await get_user(uid)
    if not u or not u["is_active"]:
        return False
    return datetime.now() < datetime.strptime(u["expiry_date"], "%Y-%m-%d %H:%M:%S")

async def get_user_groups(uid: int):
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT chat_id, title, selected FROM user_groups WHERE user_id=? ORDER BY title",
            (uid,)
        )
        return await cur.fetchall()

async def get_selected_groups(uid: int):
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT chat_id, title FROM user_groups WHERE user_id=? AND selected=1",
            (uid,)
        )
        return await cur.fetchall()

# ─── MATN VARIATSIYALARI ──────────────────────────────────
EMOJIS_SETS = [
    ["🔥", "💥", "⚡️"],
    ["✅", "💎", "🏆"],
    ["📢", "📣", "🎯"],
    ["🚀", "🌟", "✨"],
    ["👇", "👉", "📍"],
]
PREFIXES = [
    "E'tibor bering! ", "Muhim: ", "Diqqat! ", "📢 Yangilik: ", "", "", "",
]
SUFFIXES = [
    "\n\n✅ Sifat kafolatlangan.",
    "\n\n👇 Batafsil ma'lumot uchun murojaat qiling.",
    "\n\n🔥 Cheklangan taklif!",
    "\n\n💎 Faqat bugun!",
    "", "",
]

def make_variation(original_text: str, index: int) -> str:
    emoji_group = EMOJIS_SETS[index % len(EMOJIS_SETS)]
    em = random.choice(emoji_group)
    variant = index % 6
    if variant == 0:
        return f"{random.choice(PREFIXES)}{original_text} {em}"
    elif variant == 1:
        return f"{em} {original_text}"
    elif variant == 2:
        return f"{original_text}{random.choice(SUFFIXES)}"
    elif variant == 3:
        return f"{em} {random.choice(PREFIXES)}{original_text}"
    elif variant == 4:
        em2 = random.choice(EMOJIS_SETS[(index + 2) % len(EMOJIS_SETS)])
        return f"{original_text}\n\n{em} {em2}"
    else:
        lines = original_text.strip().split("\n")
        result = "\n\n".join(lines)
        return f"{em} {result} {em}"

# ─── KLAVIATURA ───────────────────────────────────────────
def main_menu(uid: int):
    b = ReplyKeyboardBuilder()
    b.button(text="📱 Akkauntni ulash")
    b.button(text="👥 Guruhlarni tanlash")
    b.button(text="🚀 Xabar yuborish")
    b.button(text="📊 Mening holatim")
    if uid == ADMIN_ID:
        b.button(text="⚙️ Admin Panel")
    b.adjust(2)
    return b.as_markup(resize_keyboard=True)

def cancel_kb():
    b = ReplyKeyboardBuilder()
    b.button(text="❌ Bekor qilish")
    return b.as_markup(resize_keyboard=True)

def delay_kb():
    b = InlineKeyboardBuilder()
    for label, secs, risk_em, risk_txt in DELAY_OPTIONS:
        b.button(text=f"⏱ {label}  {risk_em} {risk_txt}", callback_data=f"delay_{secs}")
    b.button(text="❌ Bekor qilish", callback_data="delay_cancel")
    b.adjust(1)
    return b.as_markup()

def groups_inline_kb(groups):
    b = InlineKeyboardBuilder()
    for chat_id, title, selected in groups:
        icon  = "✅" if selected else "⬜️"
        label = (title[:28] + "…") if len(title) > 30 else title
        b.button(text=f"{icon} {label}", callback_data=f"grp_{chat_id}")
    b.button(text="✔️ Saqlash", callback_data="grp_save")
    b.adjust(1)
    return b.as_markup()

# ─── /start ───────────────────────────────────────────────
@dp.message(Command("start"))
async def cmd_start(msg: types.Message, state: FSMContext):
    await state.clear()
    uid = msg.from_user.id
    if not await check_access(uid):
        await msg.answer(
            "🔒 Botdan foydalanish uchun litsenziya kaliti kerak.\n\n"
            "Kalitni pastga yuboring:"
        )
        return
    await msg.answer("✅ Xush kelibsiz!", reply_markup=main_menu(uid))

# ─── /panel ───────────────────────────────────────────────
@dp.message(Command("panel"))
async def cmd_panel(msg: types.Message, state: FSMContext):
    await state.clear()
    if msg.from_user.id != ADMIN_ID:
        return
    await _show_admin_panel(msg)

@dp.message(F.text == "⚙️ Admin Panel")
async def btn_admin_panel(msg: types.Message, state: FSMContext):
    await state.clear()
    if msg.from_user.id != ADMIN_ID:
        return
    await _show_admin_panel(msg)

async def _show_admin_panel(msg: types.Message):
    b = InlineKeyboardBuilder()
    b.button(text="🔑 1 kunlik kalit",    callback_data="gen_key_1")
    b.button(text="🔑 7 kunlik kalit",    callback_data="gen_key_7")
    b.button(text="🔑 30 kunlik kalit",   callback_data="gen_key_30")
    b.button(text="📋 Kalitlar ro'yxati", callback_data="list_keys")
    b.button(text="📊 Foydalanuvchilar",  callback_data="user_stats")
    b.button(text="🔄 Limitlarni reset",  callback_data="reset_limits")
    b.adjust(2, 1, 1, 1)
    await msg.answer("💻 <b>Admin panel:</b>", reply_markup=b.as_markup(), parse_mode="HTML")

# ─── LITSENZIYA ───────────────────────────────────────────
@dp.message(lambda m: m.text and len(m.text) == 36 and m.text.count("-") == 4)
async def activate_key(msg: types.Message, state: FSMContext):
    uid = msg.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT days FROM keys WHERE key_code=? AND is_used=0", (msg.text.strip(),)
        )
        row = await cur.fetchone()
        if not row:
            await msg.answer("❌ Noto'g'ri yoki allaqachon ishlatilgan kalit!")
            return
        expiry = datetime.now() + timedelta(days=row[0])
        await db.execute(
            "INSERT OR REPLACE INTO users "
            "(user_id, expiry_date, is_active, daily_limit) VALUES (?,?,1,?)",
            (uid, expiry.strftime("%Y-%m-%d %H:%M:%S"), DEFAULT_LIMIT)
        )
        await db.execute(
            "UPDATE keys SET is_used=1, used_by=? WHERE key_code=?",
            (uid, msg.text.strip())
        )
        await db.commit()
    await state.clear()
    await msg.answer(
        f"🎉 <b>Bot faollashtirildi!</b>\n"
        f"📅 Muddat: <b>{expiry.strftime('%Y-%m-%d')}</b>\n"
        f"🚀 Kunlik limit: <b>{DEFAULT_LIMIT}</b> ta",
        parse_mode="HTML",
        reply_markup=main_menu(uid)
    )

# ─── BEKOR QILISH ─────────────────────────────────────────
@dp.message(F.text == "❌ Bekor qilish")
async def cancel_handler(msg: types.Message, state: FSMContext):
    uid  = msg.from_user.id
    sess = login_sessions.pop(uid, None)
    if sess:
        try:
            await sess["client"].disconnect()
        except Exception:
            pass
    await state.clear()
    await msg.answer("↩️ Bekor qilindi.", reply_markup=main_menu(uid))

# ─── HOLAT ────────────────────────────────────────────────
@dp.message(F.text == "📊 Mening holatim")
async def my_stats(msg: types.Message, state: FSMContext):
    await state.clear()
    uid = msg.from_user.id
    u   = await get_user(uid)
    if not u:
        return await msg.answer("Siz hali ro'yxatdan o'tmagansiz.")
    grps = await get_selected_groups(uid)
    pct  = int(u["used_today"] / max(u["daily_limit"], 1) * 100)
    bar  = "█" * (pct // 10) + "░" * (10 - pct // 10)

    # Muddat hisoblash
    expiry_str = u["expiry_date"] or ""
    if uid == ADMIN_ID:
        expiry_info = "♾️ Cheksiz (Admin)"
    elif expiry_str:
        try:
            expiry_dt = datetime.strptime(expiry_str, "%Y-%m-%d %H:%M:%S")
            days_left = (expiry_dt - datetime.now()).days
            if days_left > 0:
                expiry_info = f"{expiry_dt.strftime('%Y-%m-%d')} ({days_left} kun qoldi)"
            elif days_left == 0:
                expiry_info = "⚠️ Bugun tugaydi!"
            else:
                expiry_info = f"❌ Tugagan ({expiry_dt.strftime('%Y-%m-%d')})"
        except Exception:
            expiry_info = expiry_str[:10]
    else:
        expiry_info = "—"

    txt = (
        f"👤 <b>Ma'lumotlaringiz:</b>\n\n"
        f"📅 Muddat: <code>{expiry_info}</code>\n"
        f"📱 Raqam: <code>{u['phone'] or 'Ulanmagan'}</code>\n"
        f"👥 Tanlangan guruhlar: <b>{len(grps)}</b> ta\n\n"
        f"📊 Limit: [{bar}] {pct}%\n"
        f"🚀 Bugun: <b>{u['used_today']}</b> / {u['daily_limit']}\n"
        f"📋 Qolgan: <b>{u['daily_limit'] - u['used_today']}</b>"
    )
    if pct >= 80:
        txt += "\n\n⚠️ <b>Limitga yaqinlashdingiz!</b>"
    await msg.answer(txt, parse_mode="HTML", reply_markup=main_menu(uid))

# ─── AKKAUNT ULASH ────────────────────────────────────────
@dp.message(F.text == "📱 Akkauntni ulash")
async def connect_acc(msg: types.Message, state: FSMContext):
    uid = msg.from_user.id
    if not await check_access(uid):
        return await msg.answer("❌ Litsenziya kerak!")

    u = await get_user(uid)
    if u and u["phone"]:
        name = u["full_name"] or "—"
        b = InlineKeyboardBuilder()
        b.button(text="🔄 Boshqa akkaunt ulash", callback_data="relink_account")
        await msg.answer(
            f"✅ <b>Akkaunt ulangan:</b>\n\n"
            f"👤 Ism: <b>{name}</b>\n"
            f"📱 Raqam: <code>{u['phone']}</code>\n\n"
            f"Boshqa akkaunt ulash uchun pastdagi tugmani bosing:",
            parse_mode="HTML",
            reply_markup=b.as_markup()
        )
        return

    await state.set_state(LoginState.waiting_phone)
    await msg.answer(
        "📱 Telefon raqamingizni kiriting:\n<code>+998901234567</code>",
        parse_mode="HTML", reply_markup=cancel_kb()
    )

@dp.callback_query(F.data == "relink_account")
async def relink_account(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    if not await check_access(uid):
        await call.answer("❌ Litsenziya kerak!")
        return
    await state.set_state(LoginState.waiting_phone)
    await call.message.answer(
        "📱 Yangi raqamni kiriting:\n<code>+998901234567</code>",
        parse_mode="HTML", reply_markup=cancel_kb()
    )
    await call.answer()

# ─── LOGIN: TELEFON ───────────────────────────────────────
@dp.message(LoginState.waiting_phone)
async def process_phone(msg: types.Message, state: FSMContext):
    uid   = msg.from_user.id
    phone = msg.text.strip()
    if not phone.startswith("+") or not phone[1:].isdigit():
        return await msg.answer("⚠️ Format: +998901234567")

    wait_msg = await msg.answer("📡 Ulanmoqda...")

    # Eski sessiya faylini tozalash
    session_file = f"sessions/{uid}.session"
    if os.path.exists(session_file):
        try:
            os.remove(session_file)
        except Exception:
            pass

    client = Client(f"sessions/{uid}", api_id=API_ID, api_hash=API_HASH, no_updates=True)
    try:
        await client.connect()
        sent = await client.send_code(phone)
        login_sessions[uid] = {
            "client": client,
            "phone":  phone,
            "hash":   sent.phone_code_hash,
        }
        await state.set_state(LoginState.waiting_code)
        await wait_msg.edit_text(
            "📩 <b>Tasdiqlash kodi yuborildi!</b>\n\n"
            "📱 Telegram ilovangizni oching — u yerga kod keladi.\n"
            "📟 Yoki SMS orqali kelishi mumkin.\n\n"
            "⌨️ <b>5 xonali kodni to'liq yuboring:</b>\n"
            "<i>(Masalan: 12345)</i>",
            parse_mode="HTML"
        )
    except PhoneNumberInvalid:
        await _cleanup_client(client)
        await state.clear()
        await wait_msg.edit_text(
            "❌ Noto'g'ri raqam.\n+998901234567 formatda kiriting.",
            reply_markup=main_menu(uid)
        )
    except PhoneNumberBanned:
        await _cleanup_client(client)
        await state.clear()
        await wait_msg.edit_text(
            "🚫 Bu raqam Telegram tomonidan bloklangan.",
            reply_markup=main_menu(uid)
        )
    except PhoneNumberFlood:
        await _cleanup_client(client)
        await state.clear()
        await wait_msg.edit_text(
            "⏳ Juda ko'p urinish. Bir necha soatdan keyin qayta urining.",
            reply_markup=main_menu(uid)
        )
    except PhoneNumberUnoccupied:
        await _cleanup_client(client)
        await state.clear()
        await wait_msg.edit_text(
            "❌ Bu raqam Telegramga ro'yxatdan o'tmagan.",
            reply_markup=main_menu(uid)
        )
    except (NetworkMigrateError, PhoneMigrateError):
        # DC o'zgarishi — yangi client bilan qayta urinish
        await _cleanup_client(client)
        client2 = Client(f"sessions/{uid}", api_id=API_ID, api_hash=API_HASH, no_updates=True)
        try:
            await client2.connect()
            sent = await client2.send_code(phone)
            login_sessions[uid] = {
                "client": client2,
                "phone":  phone,
                "hash":   sent.phone_code_hash,
            }
            await state.set_state(LoginState.waiting_code)
            await wait_msg.edit_text(
                "📩 <b>Kod yuborildi!</b>\n\n"
                "⌨️ <b>5 xonali kodni to'liq yuboring:</b>",
                parse_mode="HTML"
            )
        except Exception as e2:
            await _cleanup_client(client2)
            await state.clear()
            await wait_msg.edit_text(f"⚠️ Xato: {e2}", reply_markup=main_menu(uid))
    except Exception as e:
        await _cleanup_client(client)
        await state.clear()
        await wait_msg.edit_text(f"⚠️ Xato: {e}", reply_markup=main_menu(uid))

async def _cleanup_client(client):
    try:
        await client.disconnect()
    except Exception:
        pass

# ─── LOGIN: KOD (TO'LIQ, BIR MARTA) ─────────────────────
@dp.message(LoginState.waiting_code)
async def process_code(msg: types.Message, state: FSMContext):
    uid  = msg.from_user.id
    code = msg.text.strip().replace(" ", "").replace("-", "")

    if not code.isdigit():
        return await msg.answer(
            "⚠️ Faqat raqamlardan iborat kod yuboring.\n"
            "<i>Masalan: 12345</i>",
            parse_mode="HTML"
        )
    if len(code) not in (5, 6):
        return await msg.answer(
            f"⚠️ Kod 5 yoki 6 ta raqamdan iborat bo'lishi kerak.\n"
            f"Siz {len(code)} ta raqam yubordingiz."
        )

    if uid not in login_sessions:
        await state.clear()
        return await msg.answer("⚠️ Sessiya topilmadi. Qaytadan boshlang.", reply_markup=main_menu(uid))

    data = login_sessions[uid]
    try:
        await data["client"].sign_in(data["phone"], data["hash"], code)
        await _finish_login(uid, data["phone"], state, msg)
    except PhoneCodeExpired:
        await msg.answer("⏰ Kod muddati o'tdi. Yangi kod yuborilmoqda...")
        try:
            sent = await data["client"].send_code(data["phone"])
            login_sessions[uid]["hash"] = sent.phone_code_hash
            # state shu yerda qoladi — waiting_code
            await msg.answer(
                "📩 Yangi kod yuborildi!\n"
                "⌨️ <b>5 xonali kodni to'liq yuboring:</b>",
                parse_mode="HTML"
            )
        except Exception as e:
            login_sessions.pop(uid, None)
            await state.clear()
            await msg.answer(f"⚠️ Xato: {e}", reply_markup=main_menu(uid))
    except PhoneCodeInvalid:
        await msg.answer(
            "❌ Noto'g'ri kod!\n\n"
            "Telegram ilovangizdan kelgan kodni to'g'ri kiriting.\n"
            "⌨️ <b>5 xonali kodni to'liq yuboring:</b>",
            parse_mode="HTML"
        )
    except SessionPasswordNeeded:
        await state.set_state(LoginState.waiting_2fa)
        await msg.answer(
            "🔐 <b>Ikki bosqichli tasdiqlash (2FA) yoqilgan!</b>\n\n"
            "Parolingizni kiriting:",
            parse_mode="HTML"
        )
    except Exception as e:
        await msg.answer(f"⚠️ Xato: {e}")

# ─── LOGIN: 2FA ───────────────────────────────────────────
@dp.message(LoginState.waiting_2fa)
async def process_2fa(msg: types.Message, state: FSMContext):
    uid = msg.from_user.id
    if uid not in login_sessions:
        await state.clear()
        return await msg.answer("⚠️ Sessiya topilmadi.", reply_markup=main_menu(uid))
    try:
        await login_sessions[uid]["client"].check_password(msg.text.strip())
        await _finish_login(uid, login_sessions[uid]["phone"], state, msg)
    except Exception as e:
        await msg.answer(
            f"❌ Parol noto'g'ri: {e}\n\n"
            "Qaytadan kiriting:"
        )

async def _finish_login(uid: int, phone: str, state: FSMContext, msg: types.Message):
    sess      = login_sessions.pop(uid, None)
    full_name = "—"
    if sess:
        try:
            me        = await sess["client"].get_me()
            full_name = f"{me.first_name or ''} {me.last_name or ''}".strip() or "—"
        except Exception:
            pass
        await _cleanup_client(sess["client"])
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE users SET phone=?, full_name=? WHERE user_id=?",
            (phone, full_name, uid)
        )
        await db.commit()
    await state.clear()
    await msg.answer(
        f"🎉 <b>Akkaunt ulandi!</b>\n\n"
        f"👤 Ism: <b>{full_name}</b>\n"
        f"📱 Raqam: <code>{phone}</code>\n\n"
        "Endi <b>👥 Guruhlarni tanlash</b> tugmasini bosib,\n"
        "xabar yuboriladigan guruhlarni belgilang.",
        parse_mode="HTML", reply_markup=main_menu(uid)
    )

# ─── GURUHLARNI TANLASH ───────────────────────────────────
@dp.message(F.text == "👥 Guruhlarni tanlash")
async def select_groups(msg: types.Message, state: FSMContext):
    uid = msg.from_user.id
    if not await check_access(uid):
        return await msg.answer("❌ Litsenziya kerak!")
    u = await get_user(uid)
    if not u or not u["phone"]:
        return await msg.answer("❌ Avval akkauntni ulang!")
    await state.clear()
    scan_msg = await msg.answer("🔄 Guruhlar skanerlanmoqda...")

    client = Client(f"sessions/{uid}", api_id=API_ID, api_hash=API_HASH, no_updates=True)
    found  = []
    try:
        await client.start()
        async for dialog in client.get_dialogs():
            if dialog.chat.type not in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
                continue
            chat_id    = dialog.chat.id
            chat_title = dialog.chat.title or str(chat_id)
            try:
                me = await client.get_chat_member(chat_id, "me")
                if me.status in [enums.ChatMemberStatus.ADMINISTRATOR,
                                  enums.ChatMemberStatus.OWNER]:
                    found.append((chat_id, chat_title))
            except Exception:
                if dialog.chat.type == enums.ChatType.GROUP:
                    found.append((chat_id, chat_title))
                continue
    except Exception as e:
        await scan_msg.edit_text(f"⚠️ Xato: {e}")
        return
    finally:
        try:
            await client.stop()
        except Exception:
            pass

    if not found:
        await scan_msg.edit_text("❌ Siz admin bo'lgan guruh topilmadi.")
        return

    async with aiosqlite.connect(DB_NAME) as db:
        for cid, title in found:
            await db.execute(
                "INSERT OR IGNORE INTO user_groups (user_id, chat_id, title, selected) VALUES (?,?,?,1)",
                (uid, cid, title)
            )
            await db.execute(
                "UPDATE user_groups SET title=? WHERE user_id=? AND chat_id=?",
                (title, uid, cid)
            )
        await db.commit()

    groups = await get_user_groups(uid)
    await scan_msg.edit_text(
        f"👥 <b>{len(groups)} ta guruh topildi.</b>\n"
        f"✅ — tanlangan, ⬜️ — o'chirilgan\n\nKeraklilarini belgilang:",
        reply_markup=groups_inline_kb(groups), parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("grp_") & ~F.data.endswith("save"))
async def toggle_group(call: types.CallbackQuery):
    uid     = call.from_user.id
    chat_id = int(call.data[4:])
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT selected FROM user_groups WHERE user_id=? AND chat_id=?", (uid, chat_id)
        )
        row = await cur.fetchone()
        if row:
            await db.execute(
                "UPDATE user_groups SET selected=? WHERE user_id=? AND chat_id=?",
                (0 if row[0] else 1, uid, chat_id)
            )
            await db.commit()
    groups = await get_user_groups(uid)
    try:
        await call.message.edit_reply_markup(reply_markup=groups_inline_kb(groups))
    except Exception:
        pass
    await call.answer()

@dp.callback_query(F.data == "grp_save")
async def save_groups(call: types.CallbackQuery):
    uid    = call.from_user.id
    groups = await get_selected_groups(uid)
    await call.message.edit_text(
        f"✅ <b>Saqlandi!</b>\n\n"
        f"📋 Tanlangan guruhlar: <b>{len(groups)}</b> ta\n\n"
        + "\n".join(f"• {t}" for _, t in groups),
        parse_mode="HTML"
    )
    await call.answer("Saqlandi!")

# ─── XABAR YUBORISH — 1: matn ────────────────────────────
@dp.message(F.text == "🚀 Xabar yuborish")
async def start_broadcast(msg: types.Message, state: FSMContext):
    uid = msg.from_user.id
    if not await check_access(uid):
        return await msg.answer("❌ Litsenziya kerak!")
    u = await get_user(uid)
    if not u or not u["phone"]:
        return await msg.answer("❌ Avval akkauntni ulang!")
    if u["used_today"] >= u["daily_limit"]:
        return await msg.answer(f"❌ Bugungi limit tugagan ({u['daily_limit']} ta)!")
    groups = await get_selected_groups(uid)
    if not groups:
        return await msg.answer("❌ Avval guruhlarni tanlang!")
    remaining = u["daily_limit"] - u["used_today"]
    await state.set_state(BroadcastState.waiting_text)
    await msg.answer(
        f"✍️ Xabar matnini yozing:\n\n"
        f"👥 Tanlangan guruhlar: <b>{len(groups)}</b> ta\n"
        f"📊 Qolgan limit: <b>{remaining}</b>",
        parse_mode="HTML", reply_markup=cancel_kb()
    )

@dp.message(BroadcastState.waiting_text, F.text)
async def got_broadcast_text(msg: types.Message, state: FSMContext):
    await state.update_data(text=msg.text)
    await state.set_state(BroadcastState.waiting_count)
    await msg.answer(
        "🔢 <b>Necha marta yuborsin?</b>\n\n"
        "Raqam kiriting. Masalan: <code>5</code>",
        parse_mode="HTML"
    )

@dp.message(BroadcastState.waiting_count, F.text)
async def got_broadcast_count(msg: types.Message, state: FSMContext):
    text = msg.text.strip()
    if not text.isdigit() or int(text) < 1:
        return await msg.answer(
            "⚠️ Faqat musbat son kiriting. Masalan: <code>10</code>",
            parse_mode="HTML"
        )
    count = int(text)
    u     = await get_user(msg.from_user.id)
    remaining = u["daily_limit"] - u["used_today"]
    if count > remaining:
        return await msg.answer(
            f"⚠️ Kiritgan soni ({count}) limitdan oshib ketdi!\n"
            f"Maksimum: <b>{remaining}</b>",
            parse_mode="HTML"
        )
    await state.update_data(count=count)
    await state.set_state(BroadcastState.waiting_delay)
    await msg.answer(
        f"⏱ <b>Xabarlar orasidagi intervalni tanlang:</b>\n\n"
        f"🔢 Yuborish soni: <b>{count}</b> marta",
        parse_mode="HTML",
        reply_markup=delay_kb()
    )

@dp.callback_query(F.data.startswith("delay_"))
async def got_delay(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id

    if call.data == "delay_cancel":
        await state.clear()
        await call.message.edit_text("❌ Bekor qilindi.")
        await call.answer()
        return

    cur_state = await state.get_state()
    if cur_state != BroadcastState.waiting_delay:
        await call.answer()
        return

    delay_sec = int(call.data[6:])
    delay_lbl = next((l for l, s, _, __ in DELAY_OPTIONS if s == delay_sec), f"{delay_sec}s")

    data      = await state.get_data()
    orig_text = data["text"]
    count     = data["count"]
    await state.clear()

    groups    = await get_selected_groups(uid)
    u         = await get_user(uid)
    remaining = u["daily_limit"] - u["used_today"]

    if not groups:
        await call.message.edit_text("❌ Guruh tanlanmagan.")
        await call.answer()
        return
    if remaining <= 0:
        await call.message.edit_text("❌ Limit tugagan.")
        await call.answer()
        return

    total_sends = min(count * len(groups), remaining)
    group_names = "\n".join(f"  • {t}" for _, t in groups)
    await call.message.edit_text(
        f"🚀 <b>Yuborish boshlandi!</b>\n\n"
        f"🔢 Tur: <b>{count}</b> × {len(groups)} guruh = <b>{total_sends}</b> xabar\n"
        f"⏱ Interval: <b>{delay_lbl}</b>\n"
        f"👥 Guruhlar:\n{group_names}",
        parse_mode="HTML"
    )
    await call.answer()

    asyncio.create_task(
        _run_broadcast(uid, orig_text, count, delay_sec, delay_lbl, groups, call.message)
    )

# ─── BROADCAST ENGINE ─────────────────────────────────────
async def _run_broadcast(uid: int, orig_text: str, count: int, delay_sec: int,
                          delay_lbl: str, groups: list, status_msg: types.Message):
    u         = await get_user(uid)
    remaining = u["daily_limit"] - u["used_today"]
    sent      = 0
    errors    = 0
    warned_80 = False
    consec_err = 0          # ketma-ket xato hisoblagich
    total_max  = min(count * len(groups), remaining)

    client = Client(f"sessions/{uid}", api_id=API_ID, api_hash=API_HASH, no_updates=True)
    for attempt in range(3):
        try:
            await client.start()
            break
        except Exception as e:
            if attempt == 2:
                try:
                    await status_msg.edit_text(f"⚠️ Akkauntga kirishda xato: {e}")
                except Exception:
                    pass
                return
            await asyncio.sleep(5)

    try:
        for round_i in range(count):
            if sent >= remaining:
                break

            for grp_i, (chat_id, title) in enumerate(groups):
                if sent >= remaining:
                    break

                try:
                    variation_text = make_variation(orig_text, sent)
                    await client.send_message(chat_id, variation_text)
                    sent      += 1
                    consec_err = 0  # muvaffaqiyatli — xatolar nolga qaytadi

                    # Holat yangilash
                    try:
                        await status_msg.edit_text(
                            f"📤 <b>Yuborilmoqda...</b>\n\n"
                            f"🔄 Tur: <b>{round_i + 1}</b> / {count}\n"
                            f"📍 {title}\n"
                            f"✅ Jami: <b>{sent}</b> / {total_max}\n"
                            f"⏳ Guruh orasi: <b>{GROUP_PAUSE}s</b> | "
                            f"Tur orasi: <b>{delay_lbl}</b>",
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass

                    # 80% ogohlantirish
                    total_used = u["used_today"] + sent
                    if not warned_80 and total_used >= u["daily_limit"] * WARN_PERCENT:
                        warned_80 = True
                        try:
                            await bot.send_message(
                                uid,
                                f"⚠️ <b>Diqqat! Limitning 80% ishlatildi!</b>\n\n"
                                f"📊 Ishlatildi: <b>{total_used}</b> / {u['daily_limit']}\n"
                                f"📋 Qoldi: <b>{u['daily_limit'] - total_used}</b>\n\n"
                                f"Tejab foydalaning!",
                                parse_mode="HTML"
                            )
                        except Exception:
                            pass
                        # Admin ga ham xabar
                        try:
                            await bot.send_message(
                                ADMIN_ID,
                                f"⚠️ <b>Limit 80% ogohlantirishi!</b>\n\n"
                                f"👤 Foydalanuvchi: <code>{uid}</code>\n"
                                f"📊 Ishlatildi: <b>{total_used}</b> / {u['daily_limit']}",
                                parse_mode="HTML"
                            )
                        except Exception:
                            pass

                    # Kutish
                    is_last_group = grp_i == len(groups) - 1
                    is_last_round = round_i == count - 1
                    if not (is_last_group and is_last_round):
                        if is_last_group:
                            try:
                                await status_msg.edit_text(
                                    f"⏳ <b>Tur {round_i + 1} tugadi.</b>\n"
                                    f"✅ Yuborildi: <b>{sent}</b>\n"
                                    f"🔄 Keyingi tur {delay_lbl} dan keyin...",
                                    parse_mode="HTML"
                                )
                            except Exception:
                                pass
                            await asyncio.sleep(delay_sec)
                        else:
                            await asyncio.sleep(GROUP_PAUSE)

                except FloodWait as e:
                    log.warning("FloodWait %ss", e.value)
                    consec_err += 1

                    # Katta FloodWait — ban xavfi ogohlantirishi
                    if e.value >= FLOOD_WARN_SEC:
                        try:
                            await bot.send_message(
                                uid,
                                f"🚨 <b>BAN XAVFI!</b> Telegram {e.value} soniya kutishni talab qildi!\n\n"
                                f"📊 Hozircha yuborildi: <b>{sent}</b> ta\n"
                                f"⏸ Jarayon vaqtincha to'xtatildi.\n\n"
                                f"💡 Tavsiya: Interval uzaytiring va kamroq guruh tanlang.",
                                parse_mode="HTML"
                            )
                        except Exception:
                            pass

                    try:
                        await status_msg.edit_text(
                            f"⏳ <b>Telegram {e.value}s kutishni talab qildi...</b>\n"
                            f"✅ Hozircha: <b>{sent}</b>\n"
                            f"⚠️ Ban xavfi mavjud, sabr qiling...",
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass
                    await asyncio.sleep(e.value + 5)

                    # Qayta urinish
                    try:
                        variation_text = make_variation(orig_text, sent)
                        await client.send_message(chat_id, variation_text)
                        sent      += 1
                        consec_err = 0
                    except Exception as retry_err:
                        log.error("Qayta urinish xato: %s", retry_err)
                        errors    += 1
                        consec_err += 1

                except (PeerFlood, UserBannedInChannel, ChatWriteForbidden) as ban_err:
                    # Aniq ban/cheklash signallari
                    errors    += 1
                    consec_err += 1
                    log.error("Ban signali %s: %s", chat_id, ban_err)
                    try:
                        await bot.send_message(
                            uid,
                            f"🚫 <b>BLOKLASH SIGNALI!</b>\n\n"
                            f"Guruh: <b>{title}</b>\n"
                            f"Xato: <code>{type(ban_err).__name__}</code>\n\n"
                            f"⚠️ Telegram akkauntingizni cheklagan bo'lishi mumkin!\n"
                            f"Bir muddat to'xtating.",
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass

                except (UserDeactivated, UserDeactivatedBan) as deact_err:
                    # Akkaunt bloklangan
                    log.error("Akkaunt bloklandi: %s", deact_err)
                    try:
                        await status_msg.edit_text(
                            "🚫 <b>Akkauntingiz bloklangan!</b>\n\n"
                            "Telegram akkauntingiz ban yedi.\n"
                            "Yangi akkaunt ulab qayta boshlang.",
                            parse_mode="HTML"
                        )
                        await bot.send_message(
                            uid,
                            f"🚫 <b>AKKAUNT BLOKLANDI!</b>\n\n"
                            f"📱 Raqam: <code>{u['phone']}</code>\n"
                            f"Yangi akkaunt ulang.",
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass
                    return  # Butunlay to'xtatish

                except SlowmodeWait as sw:
                    log.warning("Slowmode %ss guruh %s", sw.value, chat_id)
                    errors += 1
                    await asyncio.sleep(sw.value)

                except Exception as ex:
                    log.error("Yuborish xatosi %s: %s", chat_id, ex)
                    errors    += 1
                    consec_err += 1

                # Ketma-ket xato chegarasini tekshirish
                if consec_err >= BAN_DANGER_ERRORS:
                    try:
                        await bot.send_message(
                            uid,
                            f"🔴 <b>XAVFLI HOLAT!</b>\n\n"
                            f"{consec_err} ta ketma-ket xato aniqlandi!\n"
                            f"📊 Yuborilgan: <b>{sent}</b>\n\n"
                            f"⛔ Xavfsizlik uchun jarayon <b>to'xtatildi</b>!\n"
                            f"Bir necha soat kutib qayta boshlang.",
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass
                    log.error("Ketma-ket xato chegarasi - to'xtatildi")
                    return

                elif consec_err >= BAN_WARN_ERRORS:
                    try:
                        await bot.send_message(
                            uid,
                            f"🟡 <b>BAN XAVFI OGOHLANTIRISHI!</b>\n\n"
                            f"{consec_err} ta ketma-ket xato!\n"
                            f"📊 Yuborilgan: <b>{sent}</b>\n\n"
                            f"⚠️ Ehtiyot bo'ling — interval uzaytiring.",
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass

    except Exception as e:
        log.error("Umumiy client xatosi: %s", e)
        try:
            await status_msg.edit_text(f"⚠️ Xato: {e}")
        except Exception:
            pass
        return
    finally:
        try:
            await client.stop()
        except Exception:
            pass

    # Natijalarni saqlash
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE users SET used_today = used_today + ? WHERE user_id=?",
            (sent, uid)
        )
        await db.commit()

    u2 = await get_user(uid)
    try:
        await status_msg.edit_text(
            f"✅ <b>Yakunlandi!</b>\n\n"
            f"📨 Yuborildi: <b>{sent}</b> / {total_max}\n"
            f"❌ Xatolar: <b>{errors}</b>\n"
            f"📊 Bugun jami: <b>{u2['used_today']}</b> / {u2['daily_limit']}\n"
            f"📋 Qolgan limit: <b>{u2['daily_limit'] - u2['used_today']}</b>",
            parse_mode="HTML"
        )
    except Exception:
        pass

    # Limit tugagan — admin ga xabar
    if u2["used_today"] >= u2["daily_limit"]:
        try:
            await bot.send_message(
                uid,
                f"🔴 <b>Bugungi limit tugadi!</b>\n\n"
                f"📊 {u2['daily_limit']} ta xabar yuborildi.\n"
                f"🔄 Limit har kuni yarim tunda yangilanadi.",
                parse_mode="HTML"
            )
        except Exception:
            pass
        try:
            await bot.send_message(
                ADMIN_ID,
                f"🔴 <b>Limit tugadi!</b>\n\n"
                f"👤 Foydalanuvchi: <code>{uid}</code>\n"
                f"📊 Jami: {u2['daily_limit']} ta ishlatildi.",
                parse_mode="HTML"
            )
        except Exception:
            pass

# ─── ADMIN CALLBACKS ──────────────────────────────────────
@dp.callback_query(F.data.startswith("gen_key_"))
async def gen_key(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    days    = int(call.data.split("_")[-1])
    new_key = str(uuid.uuid4())
    created = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO keys (key_code, days, is_used, created_at) VALUES (?,?,0,?)",
            (new_key, days, created)
        )
        await db.commit()
    await call.message.answer(
        f"🔑 Yangi kalit (<b>{days} kun</b>):\n\n"
        f"<code>{new_key}</code>\n\n"
        f"📅 Yaratildi: {created}",
        parse_mode="HTML"
    )
    await call.answer()

@dp.callback_query(F.data == "list_keys")
async def list_keys(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT key_code, days, is_used, created_at, used_by "
            "FROM keys ORDER BY created_at DESC LIMIT 20"
        )
        rows = await cur.fetchall()

    if not rows:
        await call.message.answer("Hali kalit yo'q.")
        await call.answer()
        return

    lines = ["🔑 <b>Kalitlar ro'yxati (oxirgi 20 ta):</b>\n"]
    for r in rows:
        status = "✅ Ishlatilgan" if r["is_used"] else "🟢 Faol"
        used_info = f" → <code>{r['used_by']}</code>" if r["used_by"] else ""
        lines.append(
            f"{status} | <b>{r['days']} kun</b>{used_info}\n"
            f"<code>{r['key_code']}</code>\n"
        )
    text = "\n".join(lines)
    if len(text) > 4000:
        for i in range(0, len(text), 4000):
            await call.message.answer(text[i:i+4000], parse_mode="HTML")
    else:
        await call.message.answer(text, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data == "user_stats")
async def user_stats(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT user_id, full_name, phone, used_today, daily_limit, expiry_date, is_active "
            "FROM users ORDER BY expiry_date DESC"
        )
        rows = await cur.fetchall()
    if not rows:
        return await call.message.answer("Hali foydalanuvchi yo'q.")

    now   = datetime.now()
    lines = ["👥 <b>Foydalanuvchilar:</b>\n"]
    for r in rows:
        expiry_str = r["expiry_date"] or ""
        if expiry_str:
            try:
                expiry_dt  = datetime.strptime(expiry_str, "%Y-%m-%d %H:%M:%S")
                days_left  = (expiry_dt - now).days
                hours_left = int(((expiry_dt - now).total_seconds() % 86400) / 3600)
                if days_left > 0:
                    exp_label = f"📅 {expiry_dt.strftime('%d.%m.%Y')} (<b>{days_left} kun {hours_left} soat</b> qoldi)"
                    status    = "✅"
                elif days_left == 0 and (expiry_dt - now).total_seconds() > 0:
                    exp_label = f"⚠️ Bugun tugaydi! ({hours_left} soat qoldi)"
                    status    = "⚠️"
                else:
                    exp_label = f"📅 {expiry_dt.strftime('%d.%m.%Y')} (tugagan)"
                    status    = "❌"
            except Exception:
                exp_label = expiry_str[:10]
                status    = "✅" if r["is_active"] else "❌"
        else:
            exp_label = "—"
            status    = "❌"

        pct  = int(r["used_today"] / max(r["daily_limit"], 1) * 100)
        bar  = "█" * (pct // 10) + "░" * (10 - pct // 10)
        name = r["full_name"] or "—"
        lines.append(
            f"{status} <b>{name}</b>\n"
            f"   🆔 <code>{r['user_id']}</code>\n"
            f"   📱 {r['phone'] or '—'}\n"
            f"   {exp_label}\n"
            f"   [{bar}] {pct}% ({r['used_today']}/{r['daily_limit']})\n"
        )

    text = "\n".join(lines)
    if len(text) > 4000:
        for i in range(0, len(text), 4000):
            await call.message.answer(text[i:i+4000], parse_mode="HTML")
    else:
        await call.message.answer(text, parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data == "reset_limits")
async def reset_limits_ask(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return
    await state.set_state(AdminState.waiting_reset_password)
    await call.message.answer(
        "🔐 <b>Reset uchun parolni kiriting:</b>",
        parse_mode="HTML",
        reply_markup=cancel_kb()
    )
    await call.answer()

@dp.message(AdminState.waiting_reset_password)
async def do_reset_limits(msg: types.Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID:
        await state.clear()
        return
    if msg.text.strip() != "2007":
        await msg.answer("❌ Noto'g'ri parol! Bekor qilindi.", reply_markup=main_menu(ADMIN_ID))
        await state.clear()
        return
    await state.clear()
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE users SET used_today = 0")
        await db.commit()
    await msg.answer(
        f"✅ <b>Barcha limitlar reset qilindi!</b>\n\n"
        f"🕐 {datetime.now().strftime('%H:%M:%S')}",
        parse_mode="HTML",
        reply_markup=main_menu(ADMIN_ID)
    )

# ─── KUNLIK AUTO-RESET ────────────────────────────────────
async def daily_reset_task():
    while True:
        now      = datetime.now()
        tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=5, microsecond=0)
        wait_sec = (tomorrow - now).total_seconds()
        log.info("Kunlik reset %d soniyadan keyin", int(wait_sec))
        await asyncio.sleep(wait_sec)
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("UPDATE users SET used_today = 0")
            await db.commit()
        log.info("✅ Kunlik limit reset qilindi")
        try:
            await bot.send_message(
                ADMIN_ID,
                f"🔄 <b>Kunlik limitlar yangilandi!</b>\n\n"
                f"📅 {datetime.now().strftime('%Y-%m-%d')}\n"
                "Barcha limitlar 0 ga qaytarildi.",
                parse_mode="HTML"
            )
        except Exception:
            pass

# ─── RUN ──────────────────────────────────────────────────
# Python 3.10+ va Pyrogram muvofiqligi: loop oldin global yaratiladi
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

async def main():
    os.makedirs("sessions", exist_ok=True)
    await init_db()
    log.info("Bot ishga tushdi")
    loop.create_task(daily_reset_task())
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("Bot to\'xtatildi")
    finally:
        try:
            loop.close()
        except Exception:
            pass
