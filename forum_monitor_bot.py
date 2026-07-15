"""
Асинхронный мониторинг Gmail (IMAP IDLE) + Telegram-бот.
Полностью на asyncio: aioimaplib вместо imaplib, aiohttp вместо requests.
3
Функциональность сохранена:
/setup /start /stop /ping /status /sell /debug /undebug
Хранение — те же JSON-файлы. Добавление пользователя через /setup
сразу запускает для него отдельную asyncio-задачу IDLE-мониторинга.
"""

import sys
import subprocess


def _ensure_package(pip_name, import_name=None):
    import_name = import_name or pip_name
    try:
        __import__(import_name)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", pip_name])


for _pip, _imp in [("aioimaplib", "aioimaplib"), ("aiohttp", "aiohttp"), ("beautifulsoup4", "bs4")]:
    _ensure_package(_pip, _imp)

import asyncio
import email
from email.header import decode_header
import json
import os
import datetime
import aiohttp
import aioimaplib
from bs4 import BeautifulSoup

# ------------------- НАСТРОЙКИ -------------------

IMAP_SERVER = "imap.gmail.com"
BOT_TOKEN = "8808314870:AAHmQRtoaxcJXGQr1EdOBlHzIro20RzhFPw"

SUBJECT_FILTER = "новая тема в отслеживаемом форуме"
FOLDER_SCAN_LIMIT = 30
IDLE_TIMEOUT = 25          # сек, сколько ждать push перед сменой папки
RECONNECT_DELAY = 10       # сек, пауза перед переподключением после ошибки

TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

DATA_DIR = "data"
SUBSCRIBERS_FILE = os.path.join(DATA_DIR, "subscribers.json")
PROCESSED_FILE = os.path.join(DATA_DIR, "processed.json")
ALLOWED_FILE = os.path.join(DATA_DIR, "allowed_users.json")
GMAIL_CREDENTIALS_FILE = os.path.join(DATA_DIR, "gmail_credentials.json")
USER_SETUP_STATE_FILE = os.path.join(DATA_DIR, "user_setup_state.json")
FOLDER_STATE_FILE = os.path.join(DATA_DIR, "folder_state.json")

ADMIN_USERNAMES = {"nehtootto", "yisroelwork"}

os.makedirs(DATA_DIR, exist_ok=True)

# ------------------- СОСТОЯНИЕ (для /status) -------------------

START_TIME = datetime.datetime.now()
LAST_CHECK_TIME = None
LAST_CHECK_OK = None
LAST_CHECK_ERROR = ""
LAST_NOTIFICATION_SUBJECT = ""
LAST_NOTIFICATION_TIME = None
state_lock = asyncio.Lock()

# ------------------- ХРАНИЛИЩЕ -------------------

def load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default
    return default


def save_json_sync(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[ERROR] Не удалось сохранить {path}: {e}")


async def save_json(path, data):
    await asyncio.to_thread(save_json_sync, path, data)


subscribers = set(load_json(SUBSCRIBERS_FILE, []))
processed_ids = set(load_json(PROCESSED_FILE, []))
allowed_users = set(u.lower() for u in load_json(ALLOWED_FILE, []))
gmail_credentials = load_json(GMAIL_CREDENTIALS_FILE, {})
user_setup_state = load_json(USER_SETUP_STATE_FILE, {})
folder_state = load_json(FOLDER_STATE_FILE, {})

subscribers_lock = asyncio.Lock()
processed_lock = asyncio.Lock()
allowed_lock = asyncio.Lock()
credentials_lock = asyncio.Lock()
setup_state_lock = asyncio.Lock()
folder_state_lock = asyncio.Lock()

# {user_id: asyncio.Task} — активные задачи IDLE-мониторинга
user_tasks: dict[str, asyncio.Task] = {}
tasks_lock = asyncio.Lock()

HTTP_SESSION: aiohttp.ClientSession | None = None

# ------------------- РЕЖИМ ОТЛАДКИ -------------------

debug_targets: dict[str, str | None] = {}
debug_setup_state: dict[str, dict] = {}


def get_debug_admins(target_user_id):
    result = []
    for admin_id, tgt in debug_targets.items():
        if tgt is None or tgt == target_user_id:
            try:
                result.append(int(admin_id))
            except (TypeError, ValueError):
                continue
    return result


async def notify_debug(debug_admins, text):
    for aid in debug_admins:
        await send_telegram_message(text, chat_id=aid)

# ------------------- ПОЧТА: ВСПОМОГАТЕЛЬНОЕ -------------------

def decode_mime_words(s):
    if not s:
        return ""
    decoded = decode_header(s)
    return "".join(
        (t.decode(enc or "utf-8", errors="ignore") if isinstance(t, bytes) else t)
        for t, enc in decoded
    )


def get_email_body(msg):
    html_body = ""
    text_body = ""
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            cdisp = str(part.get("Content-Disposition") or "")
            if "attachment" in cdisp:
                continue
            try:
                payload = part.get_payload(decode=True)
                if payload is None:
                    continue
                charset = part.get_content_charset() or "utf-8"
                content = payload.decode(charset, errors="ignore")
            except Exception:
                continue
            if ctype == "text/html":
                html_body += content
            elif ctype == "text/plain":
                text_body += content
    else:
        try:
            payload = msg.get_payload(decode=True)
            charset = msg.get_content_charset() or "utf-8"
            content = payload.decode(charset, errors="ignore") if payload else ""
        except Exception:
            content = ""
        if msg.get_content_type() == "text/html":
            html_body = content
        else:
            text_body = content
    return html_body, text_body


def extract_topic_link(html_body):
    if not html_body:
        return None
    soup = BeautifulSoup(html_body, "html.parser")
    for a in soup.find_all("a"):
        if "Посмотреть эту тему" in a.get_text(strip=True):
            return a.get("href")
    return None


def imap_utf7_decode(s):
    import re, base64
    def _dec(m):
        b64 = m.group(1)
        if b64 == "":
            return "&"
        b64 = b64.replace(",", "/")
        b64 += "=" * (-len(b64) % 4)
        return base64.b64decode(b64).decode("utf-16-be")
    return re.sub(r"&([^-]*)-", _dec, s)


def _parse_folder_list(raw_lines):
    result = []
    for line in raw_lines:
        if isinstance(line, bytes):
            line = line.decode(errors="ignore")
        if not line or line == "":
            continue
        if '"/"' in line:
            name = line.split('"/"')[-1].strip().strip('"')
        else:
            parts = line.split()
            if not parts:
                continue
            name = parts[-1].strip('"')
        result.append(name)
    return result


async def get_all_folders(imap):
    resp = await imap.list('""', '"*"')
    if resp.result != "OK":
        return []
    return _parse_folder_list(resp.lines)


async def test_gmail_connection(email_account, app_password):
    try:
        imap = aioimaplib.IMAP4_SSL(host=IMAP_SERVER, timeout=15)
        await imap.wait_hello_from_server()
        resp = await imap.login(email_account, app_password)
        await imap.logout()
        if resp.result != "OK":
            return False, str(resp.lines)
        return True, ""
    except Exception as e:
        return False, str(e)


async def fetch_folders_for_setup(email_account, app_password):
    """Логинится, получает список папок и разлогинивается. Используется на
    шаге 3 /setup, чтобы предложить пользователю выбор папки для мониторинга."""
    imap = None
    try:
        imap = aioimaplib.IMAP4_SSL(host=IMAP_SERVER, timeout=15)
        await imap.wait_hello_from_server()
        resp = await imap.login(email_account, app_password)
        if resp.result != "OK":
            return []
        folders = await get_all_folders(imap)
        return folders
    except Exception as e:
        print(f"[SETUP] Ошибка получения списка папок: {e}")
        return []
    finally:
        if imap is not None:
            try:
                await imap.logout()
            except Exception:
                pass

# ------------------- ПРОВЕРКА ОДНОЙ ПАПКИ -------------------

async def process_folder(imap, user_id, creds, folder, debug_admins):
    global LAST_CHECK_TIME, LAST_CHECK_OK, LAST_CHECK_ERROR
    global LAST_NOTIFICATION_SUBJECT, LAST_NOTIFICATION_TIME

    username = creds.get("username", "unknown")

    try:
        sresp = await imap.select(f'"{folder}"')
        if sresp.result != "OK":
            if debug_admins:
                await notify_debug(debug_admins, f"⚠️ [DEBUG] Папка «{folder}»: select() → {sresp.result}")
            return
    except Exception as e:
        if debug_admins:
            await notify_debug(debug_admins, f"⚠️ [DEBUG] Папка «{folder}»: ошибка select() → {e}")
        return

    current_uidvalidity = None
    try:
        for line in sresp.lines:
            line_s = line.decode(errors="ignore") if isinstance(line, bytes) else str(line)
            if "UIDVALIDITY" in line_s.upper():
                for tok in line_s.replace("(", " ").replace(")", " ").split():
                    if tok.isdigit():
                        current_uidvalidity = int(tok)
    except Exception:
        current_uidvalidity = None

    user_folder_state = folder_state.setdefault(user_id, {})
    saved = user_folder_state.get(folder)
    is_fresh = saved is None or current_uidvalidity is None or saved.get("uidvalidity") != current_uidvalidity

    uids_to_check = []
    max_uid_in_folder = saved.get("last_uid", 0) if saved else 0

    if is_fresh:
        try:
            resp = await imap.uid("search", "ALL")
        except Exception as e:
            if debug_admins:
                await notify_debug(debug_admins, f"⚠️ [DEBUG] Папка «{folder}»: ошибка uid search(ALL) → {e}")
            return
        if resp.result != "OK" or not resp.lines or not resp.lines[0]:
            user_folder_state[folder] = {"uidvalidity": current_uidvalidity, "last_uid": 0}
            return
        all_uids = sorted(int(x) for x in resp.lines[0].split())
        if not all_uids:
            user_folder_state[folder] = {"uidvalidity": current_uidvalidity, "last_uid": 0}
            return
        uids_to_check = all_uids[-FOLDER_SCAN_LIMIT:]
        max_uid_in_folder = all_uids[-1]
        if debug_admins:
            await notify_debug(
                debug_admins,
                f"📁 [DEBUG] Папка «{folder}»: первичная инициализация, всего {len(all_uids)}, "
                f"проверяется последних {len(uids_to_check)}",
            )
    else:
        last_uid = saved.get("last_uid", 0)
        try:
            resp = await imap.uid("search", f"{last_uid + 1}:*")
        except Exception as e:
            if debug_admins:
                await notify_debug(debug_admins, f"⚠️ [DEBUG] Папка «{folder}»: ошибка uid search(инкремент) → {e}")
            return
        candidate = []
        if resp.result == "OK" and resp.lines and resp.lines[0]:
            candidate = [int(x) for x in resp.lines[0].split()]
        uids_to_check = sorted(u for u in candidate if u > last_uid)
        max_uid_in_folder = max(uids_to_check) if uids_to_check else last_uid

    if not uids_to_check:
        user_folder_state[folder] = {"uidvalidity": current_uidvalidity, "last_uid": max_uid_in_folder}
        return

    for uid in uids_to_check:
        uid_str = str(uid)
        try:
            resp = await imap.uid("fetch", uid_str, "(BODY.PEEK[HEADER.FIELDS (SUBJECT MESSAGE-ID)])")
            if resp.result != "OK" or not resp.lines:
                continue

            header_bytes = b""
            for chunk in resp.lines:
                if isinstance(chunk, (bytes, bytearray)) and (b"Subject" in chunk or b"Message-ID" in chunk or b"\r\n" in chunk):
                    header_bytes += bytes(chunk)
            if not header_bytes:
                continue

            header_msg = email.message_from_bytes(header_bytes)
            message_id = f"{user_id}-{header_msg.get('Message-ID') or f'{folder}-UID{uid_str}'}"
            subject = decode_mime_words(header_msg.get("Subject", ""))

            if message_id in processed_ids:
                continue

            if SUBJECT_FILTER.lower() not in subject.lower():
                processed_ids.add(message_id)
                continue

            if debug_admins:
                await notify_debug(debug_admins, f"✅ [DEBUG] «{subject}» — совпадение с фильтром!")

            fresp = await imap.uid("fetch", uid_str, "(RFC822)")
            if fresp.result != "OK" or not fresp.lines:
                continue
            raw_email = b""
            for chunk in fresp.lines:
                if isinstance(chunk, (bytes, bytearray)):
                    raw_email += bytes(chunk)
            msg = email.message_from_bytes(raw_email)

            html_body, _ = get_email_body(msg)
            link = extract_topic_link(html_body)

            text = f"📢 {subject}\n\n👤 <b>От:</b> @{username}"
            if link:
                text += f"\n🔗 {link}"

            try:
                owner_chat_id = int(user_id)
            except (TypeError, ValueError):
                owner_chat_id = None

            if owner_chat_id is not None and owner_chat_id in subscribers:
                await send_telegram_message(text, chat_id=owner_chat_id)

            processed_ids.add(message_id)

            async with state_lock:
                LAST_NOTIFICATION_SUBJECT = subject
                LAST_NOTIFICATION_TIME = datetime.datetime.now()

            print(f"[MAIL] ({username}) Отправлено уведомление: {subject}")

        except Exception as e:
            print(f"[MAIL] ({username}) Ошибка обработки письма UID {uid_str}: {e}")
            continue

    user_folder_state[folder] = {"uidvalidity": current_uidvalidity, "last_uid": max_uid_in_folder}
    await save_json(PROCESSED_FILE, list(processed_ids))
    async with folder_state_lock:
        await save_json(FOLDER_STATE_FILE, folder_state)

    async with state_lock:
        LAST_CHECK_TIME = datetime.datetime.now()
        LAST_CHECK_OK = True
        LAST_CHECK_ERROR = ""

# ------------------- ЗАДАЧА МОНИТОРИНГА ОДНОГО ПОЛЬЗОВАТЕЛЯ (IDLE) -------------------

async def user_mail_task(user_id):
    """Постоянно живущая задача: держит одно IMAP-соединение с постоянным
    IDLE на выбранной пользователем папке (creds['folder']) — почти
    мгновенная реакция на новые письма именно в ней."""
    global LAST_CHECK_TIME, LAST_CHECK_OK, LAST_CHECK_ERROR

    while True:
        creds = gmail_credentials.get(user_id)
        if not creds:
            return  # пользователь удалён/не настроен — задача завершается

        username = creds.get("username", "unknown")
        folder = creds.get("folder") or "INBOX"
        imap = None
        try:
            imap = aioimaplib.IMAP4_SSL(host=IMAP_SERVER, timeout=30)
            await imap.wait_hello_from_server()
            resp = await imap.login(creds["email"], creds["password"])
            if resp.result != "OK":
                raise Exception(f"login failed: {resp.lines}")

            while True:
                creds = gmail_credentials.get(user_id)
                if not creds:
                    return
                if (creds.get("folder") or "INBOX") != folder:
                    # пользователь сменил папку через повторный /setup —
                    # переподключаемся уже к новой папке
                    break

                debug_admins = get_debug_admins(user_id)
                await process_folder(imap, user_id, creds, folder, debug_admins)

                # держим IDLE на этой же папке — как только сервер пришлёт
                # push (EXISTS/RECENT), сразу выходим и проверяем почту
                try:
                    idle_task = await imap.idle_start(timeout=IDLE_TIMEOUT)
                    await imap.wait_server_push(timeout=IDLE_TIMEOUT)
                    imap.idle_done()
                    await asyncio.wait_for(idle_task, timeout=5)
                except (asyncio.TimeoutError, aioimaplib.AioImapException):
                    pass

        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[IMAP] ({username}) Ошибка соединения: {e}")
            async with state_lock:
                LAST_CHECK_TIME = datetime.datetime.now()
                LAST_CHECK_OK = False
                LAST_CHECK_ERROR = str(e)
            await asyncio.sleep(RECONNECT_DELAY)
        finally:
            if imap is not None:
                try:
                    await imap.logout()
                except Exception:
                    pass


async def ensure_user_task(user_id):
    """Запускает (или перезапускает) задачу мониторинга для пользователя.
    Вызывается сразу после успешного /setup — без перезапуска всего скрипта."""
    async with tasks_lock:
        old = user_tasks.get(user_id)
        if old and not old.done():
            old.cancel()
        user_tasks[user_id] = asyncio.create_task(user_mail_task(user_id))


async def start_all_user_tasks():
    async with tasks_lock:
        for user_id in list(gmail_credentials.keys()):
            user_tasks[user_id] = asyncio.create_task(user_mail_task(user_id))

# ------------------- TELEGRAM -------------------

def is_admin(username):
    return bool(username) and username in ADMIN_USERNAMES


def has_start_access(username):
    return is_admin(username) or (bool(username) and username in allowed_users)


async def send_telegram_message(text, chat_id=None, parse_mode="HTML"):
    targets = [chat_id] if chat_id else list(subscribers)
    if not targets or HTTP_SESSION is None:
        return
    for cid in targets:
        try:
            async with HTTP_SESSION.post(
                f"{TG_API}/sendMessage",
                json={"chat_id": cid, "text": text, "parse_mode": parse_mode},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                await resp.read()
        except Exception as e:
            print(f"[TG] Ошибка отправки в {cid}: {e}")


def format_uptime():
    delta = datetime.datetime.now() - START_TIME
    days, rem = divmod(int(delta.total_seconds()), 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    parts = []
    if days: parts.append(f"{days}д")
    if hours: parts.append(f"{hours}ч")
    if minutes: parts.append(f"{minutes}м")
    parts.append(f"{seconds}с")
    return " ".join(parts)


async def handle_status(chat_id):
    async with state_lock:
        last_check = LAST_CHECK_TIME
        last_check_ok = LAST_CHECK_OK
        last_check_error = LAST_CHECK_ERROR
        last_notif_subj = LAST_NOTIFICATION_SUBJECT
        last_notif_time = LAST_NOTIFICATION_TIME

    configured_users = len(gmail_credentials)
    async with tasks_lock:
        alive_tasks = sum(1 for t in user_tasks.values() if not t.done())

    lines = ["🤖 <b>Статус бота</b>"]
    lines.append(f"⏱ Аптайм: <code>{format_uptime()}</code>")
    lines.append(f"👥 Подписчиков: <code>{len(subscribers)}</code>")
    lines.append(f"📧 Настроено почтовых аккаунтов: <code>{configured_users}</code>")
    lines.append(f"🔄 Активных IDLE-задач: <code>{alive_tasks}</code>")

    if last_check:
        status_txt = "✅ успешно" if last_check_ok else f"❌ ошибка ({last_check_error})"
        lines.append(f"📬 Последняя проверка: <code>{last_check.strftime('%Y-%m-%d %H:%M:%S')}</code> — {status_txt}")
    else:
        lines.append("📬 Последняя проверка: ещё не выполнялась")

    if last_notif_time:
        lines.append(f"🔔 Последнее уведомление: «<code>{last_notif_subj}</code>» (<code>{last_notif_time.strftime('%Y-%m-%d %H:%M:%S')}</code>)")
    else:
        lines.append("🔔 Уведомлений пока не было")

    await send_telegram_message("\n".join(lines), chat_id=chat_id)


async def handle_ping(chat_id, username):
    user_id = str(chat_id)
    creds = gmail_credentials.get(user_id)
    if not creds:
        await send_telegram_message(
            "⚠️ У вас ещё не настроена почта.\nИспользуйте /setup, чтобы подключить Gmail.",
            chat_id=chat_id,
        )
        return

    await send_telegram_message("⏳ Проверяю подключение к вашей почте...", chat_id=chat_id)
    ok, error = await test_gmail_connection(creds["email"], creds["password"])

    if ok:
        await send_telegram_message(
            f"✅ <b>Почта подключена и работает</b>\n📧 <code>{creds['email']}</code>",
            chat_id=chat_id,
        )
    else:
        await send_telegram_message(
            f"❌ <b>Не удалось подключиться к почте</b>\n"
            f"📧 <code>{creds['email']}</code>\n\n<code>{error}</code>\n\n"
            "Возможно, изменился пароль приложения. Используйте /setup, чтобы обновить учетные данные.",
            chat_id=chat_id,
        )


async def handle_debug(chat_id):
    if not gmail_credentials:
        await send_telegram_message(
            "⚠️ Пока нет ни одного пользователя с настроенной почтой (/setup).",
            chat_id=chat_id,
        )
        return

    admin_id = str(chat_id)
    idx_map = {}
    lines = ["🔍 <b>Режим отладки</b>", "Выберите, за кем следить — отправьте номер:", "", "0. Все пользователи"]
    for i, (uid, creds) in enumerate(gmail_credentials.items(), start=1):
        uname = creds.get("username", "unknown")
        lines.append(f"{i}. @{uname} (<code>{creds.get('email', '')}</code>)")
        idx_map[str(i)] = uid

    debug_setup_state[admin_id] = {"idx_map": idx_map}
    await send_telegram_message("\n".join(lines), chat_id=chat_id)


async def process_debug_selection(chat_id, text):
    admin_id = str(chat_id)
    state = debug_setup_state.get(admin_id)
    if not state:
        return

    choice = text.strip()
    if choice == "0":
        debug_targets[admin_id] = None
        debug_setup_state.pop(admin_id, None)
        await send_telegram_message(
            "✅ Режим отладки включен для <b>всех</b> пользователей.\nИспользуйте /undebug, чтобы выключить.",
            chat_id=chat_id,
        )
        return

    target_uid = state.get("idx_map", {}).get(choice)
    if not target_uid:
        await send_telegram_message(
            "❌ Не понял выбор. Отправьте номер из списка или /debug заново.", chat_id=chat_id
        )
        return

    debug_targets[admin_id] = target_uid
    debug_setup_state.pop(admin_id, None)
    creds = gmail_credentials.get(target_uid, {})
    uname = creds.get("username", "unknown")
    await send_telegram_message(
        f"✅ Режим отладки включен.\n👀 Мониторю: @{uname} (<code>{creds.get('email', '')}</code>)\n\n"
        "Используйте /undebug, чтобы выключить.",
        chat_id=chat_id,
    )


async def handle_undebug(chat_id):
    admin_id = str(chat_id)
    was_active = admin_id in debug_targets
    debug_setup_state.pop(admin_id, None)
    debug_targets.pop(admin_id, None)
    if was_active:
        await send_telegram_message("✅ Режим отладки выключен.", chat_id=chat_id)
    else:
        await send_telegram_message("ℹ️ Режим отладки и так был выключен.", chat_id=chat_id)


async def start_gmail_setup(chat_id, username):
    user_id = str(chat_id)
    async with setup_state_lock:
        user_setup_state[user_id] = {"step": 1, "username": username}
        await save_json(USER_SETUP_STATE_FILE, user_setup_state)

    await send_telegram_message(
        "📧 <b>Настройка Gmail</b>\n\nВведите ваш Gmail-адрес (например: your.email@gmail.com):",
        chat_id=chat_id,
    )


async def process_setup_step(chat_id, text, username):
    user_id = str(chat_id)
    state = user_setup_state.get(user_id, {})
    step = state.get("step", 1)

    if step == 1:
        email_account = text.strip()
        if "@gmail.com" not in email_account:
            await send_telegram_message(
                "❌ Пожалуйста, используйте Gmail адрес (должен содержать @gmail.com)", chat_id=chat_id
            )
            return

        async with setup_state_lock:
            user_setup_state[user_id] = {"step": 2, "email": email_account, "username": username}
            await save_json(USER_SETUP_STATE_FILE, user_setup_state)

        await send_telegram_message(
            f"✅ Email сохранён: <code>{email_account}</code>\n\n"
            "Теперь введите <b>пароль приложения Gmail</b>.\n\n"
            "Как получить пароль приложения:\n"
            "1. Откройте <a href='https://myaccount.google.com/security'>Google Account Security</a>\n"
            "2. Включите двухфакторную аутентификацию (если еще не включена)\n"
            "3. Вернитесь в Security и найдите 'App passwords'\n"
            "4. Выберите приложение 'Mail' и устройство 'Windows Computer' (или любое)\n"
            "5. Google сгенерирует пароль из 16 символов\n"
            "6. Скопируйте этот пароль (без пробелов) и отправьте его боту",
            chat_id=chat_id,
        )

    elif step == 2:
        app_password = text.strip().replace(" ", "")
        email_account = state.get("email", "")

        await send_telegram_message("⏳ Проверяю учетные данные...", chat_id=chat_id)
        ok, error = await test_gmail_connection(email_account, app_password)

        if ok:
            await send_telegram_message("⏳ Получаю список папок...", chat_id=chat_id)
            folders = await fetch_folders_for_setup(email_account, app_password)
            if not folders:
                folders = ["INBOX"]

            idx_map = {str(i): f for i, f in enumerate(folders, start=1)}
            async with setup_state_lock:
                user_setup_state[user_id] = {
                    "step": 3,
                    "email": email_account,
                    "password": app_password,
                    "username": username,
                    "idx_map": idx_map,
                }
                await save_json(USER_SETUP_STATE_FILE, user_setup_state)

            lines = ["📂 <b>Выберите папку для мониторинга</b>", "Отправьте номер:"]
            for i, f in idx_map.items():
                lines.append(f"{i}. {imap_utf7_decode(f)}")
            await send_telegram_message("\n".join(lines), chat_id=chat_id)
        else:
            await send_telegram_message(
                f"❌ <b>Ошибка подключения:</b>\n\n<code>{error}</code>\n\nПроверьте:\n"
                "✓ Правильность email и пароля\n"
                "✓ Включен ли IMAP в Gmail (Настройки → Пересылка и POP/IMAP)\n"
                "✓ Используете ли вы именно пароль приложения, а не обычный пароль\n\n"
                "Попробуем заново. Введите email:",
                chat_id=chat_id,
            )
            async with setup_state_lock:
                user_setup_state[user_id] = {"step": 1, "username": username}
                await save_json(USER_SETUP_STATE_FILE, user_setup_state)

    elif step == 3:
        idx_map = state.get("idx_map", {})
        choice = text.strip()
        folder = idx_map.get(choice)
        if not folder:
            await send_telegram_message(
                "❌ Не понял выбор. Отправьте номер из списка выше.", chat_id=chat_id
            )
            return

        email_account = state.get("email", "")
        app_password = state.get("password", "")

        async with credentials_lock:
            gmail_credentials[user_id] = {
                "email": email_account,
                "password": app_password,
                "username": username,
                "folder": folder,
            }
            await save_json(GMAIL_CREDENTIALS_FILE, gmail_credentials)

        async with setup_state_lock:
            user_setup_state.pop(user_id, None)
            await save_json(USER_SETUP_STATE_FILE, user_setup_state)

        # ключевое требование: сразу запускаем задачу мониторинга,
        # без перезапуска всего скрипта
        await ensure_user_task(user_id)

        await send_telegram_message(
            f"✅ <b>Настройка завершена!</b>\n\n📧 Почта: <code>{email_account}</code>\n"
            f"📂 Папка: <code>{folder}</code>\n\n"
            "Бот следит за этой папкой в реальном времени (IMAP IDLE) "
            "и отправит уведомление почти мгновенно.\n\n"
            "Команды:\n"
            "/start - подписаться на уведомления\n"
            "/stop - отписаться\n"
            "/setup - изменить учетные данные или папку\n"
            "/ping - проверить подключение к почте\n"
            "/status - статус бота (только админы)",
            chat_id=chat_id,
        )
        print(f"[SETUP] Пользователь @{username} ({email_account}) настроен, папка: {folder}")


async def telegram_polling():
    offset = None
    while True:
        try:
            params = {"timeout": 30}
            if offset:
                params["offset"] = offset
            async with HTTP_SESSION.get(
                f"{TG_API}/getUpdates", params=params, timeout=aiohttp.ClientTimeout(total=35)
            ) as resp:
                data = await resp.json()
            updates = data.get("result", [])

            for update in updates:
                offset = update["update_id"] + 1
                message = update.get("message")
                if not message:
                    continue

                chat_id = message["chat"]["id"]
                text = message.get("text", "").strip()
                from_user = message.get("from", {}) or {}
                username = (from_user.get("username") or "").lower()
                user_id = str(chat_id)

                if user_id in debug_setup_state and not text.startswith("/"):
                    await process_debug_selection(chat_id, text)
                    continue

                if user_id in user_setup_state and not text.startswith("/"):
                    await process_setup_step(chat_id, text, username)
                    continue

                if text == "/setup":
                    await start_gmail_setup(chat_id, username)

                elif text == "/start":
                    if not has_start_access(username):
                        await send_telegram_message(
                            "⛔ Доступ к подписке ограничен. Обратитесь к администратору, "
                            "чтобы вам выдали доступ.",
                            chat_id=chat_id,
                        )
                        continue
                    if chat_id not in subscribers:
                        async with subscribers_lock:
                            subscribers.add(chat_id)
                            await save_json(SUBSCRIBERS_FILE, list(subscribers))
                    await send_telegram_message(
                        "✅ Вы подписаны на уведомления о новых темах форума\n\nКоманды:\n"
                        "/setup - настроить свою почту\n/stop - отписаться\n"
                        "/ping - проверить подключение к почте\n"
                        "/status - статус бота (только админы)",
                        chat_id=chat_id,
                    )

                elif text == "/ping":
                    if not has_start_access(username):
                        await send_telegram_message(
                            "⛔ Доступ ограничен. Обратитесь к администратору, чтобы вам выдали доступ.",
                            chat_id=chat_id,
                        )
                        continue
                    await handle_ping(chat_id, username)

                elif text == "/stop":
                    if chat_id in subscribers:
                        async with subscribers_lock:
                            subscribers.discard(chat_id)
                            await save_json(SUBSCRIBERS_FILE, list(subscribers))
                    await send_telegram_message("❌ Вы отписаны от уведомлений", chat_id=chat_id)

                elif text == "/status":
                    if not is_admin(username):
                        await send_telegram_message("⛔ Команда доступна только администратору.", chat_id=chat_id)
                        continue
                    await handle_status(chat_id)

                elif text == "/debug":
                    if not is_admin(username):
                        await send_telegram_message("⛔ Команда доступна только администратору.", chat_id=chat_id)
                        continue
                    await handle_debug(chat_id)

                elif text == "/undebug":
                    if not is_admin(username):
                        await send_telegram_message("⛔ Команда доступна только администратору.", chat_id=chat_id)
                        continue
                    await handle_undebug(chat_id)

                elif text.startswith("/sell"):
                    if not is_admin(username):
                        await send_telegram_message("⛔ Команда доступна только администратору.", chat_id=chat_id)
                        continue
                    parts = text.split(maxsplit=1)
                    if len(parts) < 2 or not parts[1].strip():
                        await send_telegram_message("Используйте: /sell @username", chat_id=chat_id)
                        continue
                    target = parts[1].strip().lstrip("@").lower()
                    if target in allowed_users:
                        await send_telegram_message(f"Пользователь @{target} уже имеет доступ к /start.", chat_id=chat_id)
                    else:
                        async with allowed_lock:
                            allowed_users.add(target)
                            await save_json(ALLOWED_FILE, list(allowed_users))
                        await send_telegram_message(f"✅ Пользователю @{target} выдан доступ к /start.", chat_id=chat_id)

        except Exception as e:
            print(f"[TG] Ошибка polling: {e}")
            await asyncio.sleep(5)

# ------------------- ЗАПУСК -------------------

async def main():
    global HTTP_SESSION
    async with aiohttp.ClientSession() as session:
        HTTP_SESSION = session

        await start_all_user_tasks()
        tg_task = asyncio.create_task(telegram_polling())

        print("=" * 60)
        print("🤖 Форум-мониторинг бот (async/IDLE) запущен")
        print("=" * 60)
        print("Администраторы могут использовать /status для просмотра информации")
        print("Новые пользователи должны выполнить /setup для настройки Gmail")
        print("=" * 60)

        try:
            await tg_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Бот остановлен")
