"""
Мониторинг Gmail (IMAP) на предмет писем о новых темах форума
и рассылка уведомлений подписчикам Telegram-бота.
2
ВЕРСИЯ С ПОДДЕРЖКОЙ МНОГОПОЛЬЗОВАТЕЛЬСКОЙ НАСТРОЙКИ И АСИНХРОННОЙ АРХИТЕКТУРЫ:
Каждый пользователь сам настраивает свои учетные данные Gmail через команду /setup

Скрипт полностью самодостаточен: при первом запуске сам ставит
недостающие библиотеки (aiogram, aioimaplib, beautifulsoup4), поэтому для
хостинга достаточно одного этого файла.

Что изменилось:
1. Используется aioimaplib для асинхронного IMAP
2. Проверка почты через IMAP IDLE для мгновенного получения событий
3. Используется aiogram для асинхронной обработки Telegram команд
4. Каждый пользователь имеет свою отдельную задачу мониторинга
5. Добавление нового пользователя не требует перезапуска
6. Асинхронное сохранение данных в JSON с использованием threading.Lock
   для совместимости с aiofiles (если будут добавлены)

Администраторы (задаются по Telegram-username, без @): NehtoOtto, yisroelwork.
"""

import sys
import subprocess

# ------------------- АВТОУСТАНОВКА ЗАВИСИМОСТЕЙ -------------------

def _ensure_package(pip_name, import_name=None):
    import_name = import_name or pip_name
    try:
        __import__(import_name)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", pip_name])


for _pip_name, _import_name in [
    ("aiogram", "aiogram"),
    ("aioimaplib", "aioimaplib"),
    ("beautifulsoup4", "bs4"),
]:
    _ensure_package(_pip_name, _import_name)

import email
from email.header import decode_header
import json
import os
import asyncio
import threading
import datetime
import logging
from typing import Dict, Set, Optional, Any, List
from contextlib import asynccontextmanager

from bs4 import BeautifulSoup

# Импорт aiogram и aioimaplib после установки
import aiogram
from aiogram import Bot, Dispatcher, types, filters
from aiogram.utils import executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext

import aioimaplib
from aioimaplib import IMAP4_SSL

# ------------------- НАСТРОЙКИ -------------------

IMAP_SERVER = "imap.gmail.com"
BOT_TOKEN = "8808314870:AAHmQRtoaxcJXGQr1EdOBlHzIro20RzhFPw"

POLL_INTERVAL = 5                # как часто проверять почту, секунд
SUBJECT_FILTER = "новая тема в отслеживаемом форуме"
FOLDER_SCAN_LIMIT = 30           # сколько последних писем в каждой папке проверять

TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

DATA_DIR = "data"
SUBSCRIBERS_FILE = os.path.join(DATA_DIR, "subscribers.json")
PROCESSED_FILE = os.path.join(DATA_DIR, "processed.json")
ALLOWED_FILE = os.path.join(DATA_DIR, "allowed_users.json")
GMAIL_CREDENTIALS_FILE = os.path.join(DATA_DIR, "gmail_credentials.json")
USER_SETUP_STATE_FILE = os.path.join(DATA_DIR, "user_setup_state.json")
FOLDER_STATE_FILE = os.path.join(DATA_DIR, "folder_state.json")

# Администраторы бота (Telegram-username без "@", в нижнем регистре).
ADMIN_USERNAMES = {"nehtootto", "yisroelwork"}

os.makedirs(DATA_DIR, exist_ok=True)

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ------------------- СОСТОЯНИЕ БОТА (для /status) -------------------

START_TIME = datetime.datetime.now()
last_check_time = None
last_check_ok = None
last_check_error = ""
last_notification_subject = ""
last_notification_time = None

state_lock = threading.Lock()

# ------------------- ХРАНИЛИЩЕ (с async-совместимыми структурами) -------------------

subscribers: Set[int] = set()
processed_ids: Set[str] = set()
allowed_users: Set[str] = set()

# Хранилище учетных данных: {user_id: {"email": "...", "password": "...", "username": "..."}}
gmail_credentials: Dict[str, Dict[str, str]] = {}

# Состояние настройки пользователей: {user_id: {"step": 1 или 2, "email": "..."}}
user_setup_state: Dict[str, Dict[str, Any]] = {}

# Состояние UID-синхронизации по папкам: {user_id: {folder: {"uidvalidity": int, "last_uid": int}}}
folder_state: Dict[str, Dict[str, Dict[str, Any]]] = {}

# ------------------- РЕЖИМ ОТЛАДКИ (/debug, /undebug) -------------------

# Кто из админов сейчас в режиме отладки и за каким пользователем следит.
# {admin_chat_id (str): target_user_id (str) или None (значит "все пользователи")}
debug_targets: Dict[str, Optional[str]] = {}

# Состояние выбора пользователя после команды /debug (пока админ не ответил номером).
# {admin_chat_id (str): {"idx_map": {"1": user_id, "2": user_id, ...}}}
debug_setup_state: Dict[str, Dict[str, Dict[str, str]]] = {}

# Запущенные задачи мониторинга: {user_id: asyncio.Task}
monitoring_tasks: Dict[str, asyncio.Task] = {}

# Лок для thread-safe операций с глобальными структурами
data_lock = asyncio.Lock()

# ------------------- УТИЛИТЫ ДЛЯ РАБОТЫ С ДАННЫМИ -------------------

def load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Ошибка загрузки {path}: {e}")
            return default
    return default


def save_json(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Ошибка сохранения {path}: {e}")


async def load_all_data():
    """Асинхронная инициализация данных при старте."""
    global subscribers, processed_ids, allowed_users, gmail_credentials
    global user_setup_state, folder_state

    subscribers = set(load_json(SUBSCRIBERS_FILE, []))
    processed_ids = set(load_json(PROCESSED_FILE, []))
    allowed_users = set(u.lower() for u in load_json(ALLOWED_FILE, []))
    gmail_credentials = load_json(GMAIL_CREDENTIALS_FILE, {})
    user_setup_state = load_json(USER_SETUP_STATE_FILE, {})
    folder_state = load_json(FOLDER_STATE_FILE, {})


def get_debug_admins(target_user_id: str) -> List[int]:
    """Возвращает список chat_id адми��ов, которые сейчас отлаживают проверку
    почты для данного target_user_id (или включили отладку для всех)."""
    result = []
    for admin_id, tgt in debug_targets.items():
        if tgt is None or tgt == target_user_id:
            try:
                result.append(int(admin_id))
            except (TypeError, ValueError):
                continue
    return result


async def notify_debug(debug_admins: List[int], text: str, bot: Bot):
    for aid in debug_admins:
        try:
            await bot.send_message(aid, text, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Ошибка отправки debug сообщения в {aid}: {e}")
# ------------------- РАБОТА С ПОЧТОЙ -------------------

def decode_mime_words(s):
    if not s:
        return ""
    decoded = decode_header(s)
    return "".join(
        (t.decode(enc or "utf-8", errors="ignore") if isinstance(t, bytes) else t)
        for t, enc in decoded
    )


def get_email_body(msg):
    """Возвращает (html_body, text_body) письма."""
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
    """Ищет в HTML ссылку, спрятанную за текстом 'Посмотреть эту тему'."""
    if not html_body:
        return None
    soup = BeautifulSoup(html_body, "html.parser")
    for a in soup.find_all("a"):
        text = a.get_text(strip=True)
        if "Посмотреть эту тему" in text:
            return a.get("href")
    return None


async def get_all_folders(imap: aioimaplib.IMAP4_SSL):
    """Получить список всех папок IMAP."""
    status, folders = await imap.list()
    result = []
    if status == "OK":
        for f in folders:
            line = f.decode(errors="ignore")
            if '"/"' in line:
                name = line.split('"/"')[-1].strip().strip('"')
            else:
                name = line.split()[-1].strip('"')
            result.append(name)
    return result


async def test_gmail_connection(email_account: str, app_password: str):
    """Быстрая проверка, что логин по IMAP проходит."""
    try:
        imap = aioimaplib.IMAP4_SSL(IMAP_SERVER)
        await imap.wait_hello_from_server()
        await imap.login(email_account, app_password)
        await imap.logout()
        return True, ""
    except Exception as e:
        return False, str(e)


async def check_mail_for_user(
    user_id: str,
    email_account: str,
    app_password: str,
    username: str,
    bot: Bot,
):
    """Проверяет почту для конкретного пользователя с использованием IDLE."""
    global last_check_time, last_check_ok, last_check_error
    global last_notification_subject, last_notification_time

    debug_admins = get_debug_admins(user_id)
    check_started_at = datetime.datetime.now()

    if debug_admins:
        await notify_debug(
            debug_admins,
            f"🔍 [DEBUG {check_started_at.strftime('%H:%M:%S')}] Начата проверка почты "
            f"@{username} (<code>{email_account}</code>)",
            bot,
        )

    imap = None
    try:
        imap = aioimaplib.IMAP4_SSL(IMAP_SERVER)
        await imap.wait_hello_from_server()
        await imap.login(email_account, app_password)
    except Exception as e:
        logger.error(f"[IMAP] Ошибка подключения для {username} ({email_account}): {e}")
        with state_lock:
            last_check_time = datetime.datetime.now()
            last_check_ok = False
            last_check_error = str(e)
        if debug_admins:
            await notify_debug(
                debug_admins,
                f"❌ [DEBUG] Ошибка подключения к почте @{username} (<code>{email_account}</code>):\n<code>{e}</code>",
                bot,
            )
        return

    folders = await get_all_folders(imap)

    if debug_admins:
        await notify_debug(
            debug_admins,
            f"📂 [DEBUG] Найдено папок: {len(folders)} → {', '.join(folders) if folders else '—'}",
            bot,
        )

    debug_total_scanned = 0
    debug_total_matched = 0

    user_folder_state = folder_state.setdefault(user_id, {})

    # Обработка каждой папки
    for folder in folders:
        try:
            status, _ = await imap.select(f'"{folder}"', readonly=True)
            if status != "OK":
                if debug_admins:
                    await notify_debug(debug_admins, f"⚠️ [DEBUG] Папка «{folder}»: select() → {status}", bot)
                continue
        except Exception as e:
            if debug_admins:
                await notify_debug(debug_admins, f"⚠️ [DEBUG] Папка «{folder}»: ошибка select() → {e}", bot)
            continue

        # UIDVALIDITY приходит как untagged-ответ на SELECT
        try:
            uidval_raw = getattr(imap, "untagged_responses", {}).get("UIDVALIDITY")
            current_uidvalidity = int(uidval_raw[-1]) if uidval_raw else None
        except Exception:
            current_uidvalidity = None

        saved = user_folder_state.get(folder)
        is_fresh_folder = (
            saved is None
            or current_uidvalidity is None
            or saved.get("uidvalidity") != current_uidvalidity
        )

        if is_fresh_folder:
            # Впервые видим папку или сменился UIDVALIDITY
            try:
                status, data = await imap.uid("search", None, "ALL")
            except Exception as e:
                if debug_admins:
                    await notify_debug(debug_admins, f"⚠️ [DEBUG] Папка «{folder}»: ошибка uid search(ALL) → {e}", bot)
                continue
            if status != "OK" or not data or not data[0]:
                if debug_admins:
                    await notify_debug(debug_admins, f"📁 [DEBUG] Папка «{folder}»: писем нет (первичная инициализация)", bot)
                user_folder_state[folder] = {"uidvalidity": current_uidvalidity, "last_uid": 0}
                continue

            all_uids = sorted(int(x) for x in data[0].split())
            uids_to_check = all_uids[-FOLDER_SCAN_LIMIT:]
            max_uid_in_folder = all_uids[-1]

            if debug_admins:
                await notify_debug(
                    debug_admins,
                    f"📁 [DEBUG] Папка «{folder}»: первичная инициализация (UIDVALIDITY={current_uidvalidity}), "
                    f"всего писем {len(all_uids)}, будет проверено последних {len(uids_to_check)}",
                    bot,
                )
        else:
            last_uid = saved.get("last_uid", 0)
            try:
                status, data = await imap.uid("search", None, f"{last_uid + 1}:*")
            except Exception as e:
                if debug_admins:
                    await notify_debug(debug_admins, f"⚠️ [DEBUG] Папка «{folder}»: ошибка uid search(инкремент) → {e}", bot)
                continue

            candidate_uids = []
            if status == "OK" and data and data[0]:
                candidate_uids = [int(x) for x in data[0].split()]

            uids_to_check = sorted(u for u in candidate_uids if u > last_uid)
            max_uid_in_folder = max(uids_to_check) if uids_to_check else last_uid

            if debug_admins:
                await notify_debug(
                    debug_admins,
                    f"📁 [DEBUG] Папка «{folder}»: инкрементальная проверка от UID {last_uid + 1}, "
                    f"новых писем: {len(uids_to_check)}",
                    bot,
                )

        if not uids_to_check:
            user_folder_state[folder] = {
                "uidvalidity": current_uidvalidity,
                "last_uid": max(max_uid_in_folder, saved.get("last_uid", 0) if saved else 0),
            }
            continue

        for uid in uids_to_check:
            uid_str = str(uid)
            try:
                # Получаем только заголовки (быстро)
                status, header_data = await imap.uid(
                    "fetch", uid_str, "(BODY.PEEK[HEADER.FIELDS (SUBJECT MESSAGE-ID)])"
                )
                if status != "OK" or not header_data or header_data[0] is None:
                    if debug_admins:
                        await notify_debug(debug_admins, f"⚠️ [DEBUG] fetch header(UID {uid_str}) → {status}, пусто", bot)
                    continue

                header_bytes = header_data[0][1]
                header_msg = email.message_from_bytes(header_bytes)

                message_id = f"{user_id}-{header_msg.get('Message-ID') or f'{folder}-UID{uid_str}'}"
                subject = decode_mime_words(header_msg.get("Subject", ""))
                debug_total_scanned += 1

                if message_id in processed_ids:
                    if debug_admins:
                        await notify_debug(debug_admins, f"↩️ [DEBUG] «{subject}» — уже обработано ранее, пропуск", bot)
                    continue

                if SUBJECT_FILTER.lower() not in subject.lower():
                    if debug_admins:
                        await notify_debug(debug_admins, f"— [DEBUG] «{subject}» — тема не подходит под фильтр", bot)
                    async with data_lock:
                        processed_ids.add(message_id)
                    continue

                debug_total_matched += 1
                if debug_admins:
                    await notify_debug(debug_admins, f"✅ [DEBUG] «{subject}» — совпадение с фильтром!", bot)

                # Качаем полное письмо для извлечения ссылки
                status, msg_data = await imap.uid("fetch", uid_str, "(RFC822)")
                if status != "OK" or not msg_data or msg_data[0] is None:
                    if debug_admins:
                        await notify_debug(debug_admins, f"⚠️ [DEBUG] fetch full(UID {uid_str}) → {status}, пусто", bot)
                    continue

                raw_email = msg_data[0][1]
                msg = email.message_from_bytes(raw_email)

                html_body, _ = get_email_body(msg)
                link = extract_topic_link(html_body)

                if debug_admins:
                    await notify_debug(debug_admins, f"🔗 [DEBUG] Ссылка из письма: {link or '—'}", bot)

                text = f"📢 {subject}\n\n👤 <b>От:</b> @{username}"
                if link:
                    text += f"\n🔗 {link}"

                # Уведомление уходит владельцу этой почты
                try:
                    owner_chat_id = int(user_id)
                except (TypeError, ValueError):
                    owner_chat_id = None

                if owner_chat_id is not None and owner_chat_id in subscribers:
                    await bot.send_message(owner_chat_id, text, parse_mode="HTML")

                async with data_lock:
                    processed_ids.add(message_id)
                    last_notification_subject = subject
                    last_notification_time = datetime.datetime.now()

                logger.info(f"[MAIL] ({username}) Отправлено уведомление: {subject}")

            except Exception as e:
                logger.error(f"[MAIL] ({username}) Ошибка обработки письма (UID {uid_str}): {e}")
                continue

        user_folder_state[folder] = {
            "uidvalidity": current_uidvalidity,
            "last_uid": max(max_uid_in_folder, saved.get("last_uid", 0) if saved else 0),
        }

    try:
        await imap.logout()
    except Exception:
        pass

    # Сохраняем данные
    save_json(PROCESSED_FILE, list(processed_ids))
    save_json(FOLDER_STATE_FILE, folder_state)

    with state_lock:
        last_check_time = datetime.datetime.now()
        last_check_ok = True
        last_check_error = ""

    if debug_admins:
        duration = (datetime.datetime.now() - check_started_at).total_seconds()
        await notify_debug(
            debug_admins,
            f"🏁 [DEBUG] Проверка @{username} завершена за {duration:.2f}с. "
            f"Просмотрено писем: {debug_total_scanned}, совпадений: {debug_total_matched}",
            bot,
        )

    total_duration = (datetime.datetime.now() - check_started_at).total_seconds()
    if total_duration > POLL_INTERVAL:
        logger.warning(
            f"[WARN] Проверка почты @{username} заняла {total_duration:.1f}с, "
            f"что больше POLL_INTERVAL ({POLL_INTERVAL}с). "
            "Реальный интервал проверки этого пользователя растягивается."
        )
# ------------------- АСИНХРОННЫЙ МОНИТОРИНГ ПОЧТЫ (IDLE) -------------------

async def mail_monitor_with_idle(user_id: str, email_account: str, app_password: str, username: str, bot: Bot):
    """
    Асинхронный мониторинг одной почты с использованием IMAP IDLE.
    После обработки текущих писем переходит в режим IDLE для получения новых событий.
    """
    global last_check_time, last_check_ok, last_check_error

    debug_admins = get_debug_admins(user_id)
    logger.info(f"[MAIL MONITOR] Запущен мониторинг для @{username} ({email_account}) с IDLE")

    while True:
        try:
            # Подключение к серверу
            imap = aioimaplib.IMAP4_SSL(IMAP_SERVER)
            await imap.wait_hello_from_server()
            await imap.login(email_account, app_password)

            # Получаем список папок
            folders = await get_all_folders(imap)
            user_folder_state = folder_state.setdefault(user_id, {})

            # Обработка каждой папки
            for folder in folders:
                try:
                    status, _ = await imap.select(f'"{folder}"', readonly=True)
                    if status != "OK":
                        continue
                except Exception as e:
                    logger.error(f"[MAIL MONITOR] Ошибка select папки {folder}: {e}")
                    continue

                # UIDVALIDITY
                try:
                    uidval_raw = getattr(imap, "untagged_responses", {}).get("UIDVALIDITY")
                    current_uidvalidity = int(uidval_raw[-1]) if uidval_raw else None
                except Exception:
                    current_uidvalidity = None

                saved = user_folder_state.get(folder)
                is_fresh_folder = (
                    saved is None
                    or current_uidvalidity is None
                    or saved.get("uidvalidity") != current_uidvalidity
                )

                if is_fresh_folder:
                    # Первая инициализация
                    try:
                        status, data = await imap.uid("search", None, "ALL")
                    except Exception as e:
                        logger.error(f"[MAIL MONITOR] Ошибка search ALL для {folder}: {e}")
                        continue

                    if status == "OK" and data and data[0]:
                        all_uids = sorted(int(x) for x in data[0].split())
                        uids_to_check = all_uids[-FOLDER_SCAN_LIMIT:] if all_uids else []
                    else:
                        uids_to_check = []

                    max_uid_in_folder = all_uids[-1] if all_uids else 0
                    logger.info(
                        f"[MAIL MONITOR] Папка «{folder}»: инициализация, UIDVALIDITY={current_uidvalidity}, "
                        f"новых писем для проверки: {len(uids_to_check)}"
                    )
                else:
                    last_uid = saved.get("last_uid", 0)
                    try:
                        status, data = await imap.uid("search", None, f"{last_uid + 1}:*")
                    except Exception as e:
                        logger.error(f"[MAIL MONITOR] Ошибка search инкремент для {folder}: {e}")
                        continue

                    uids_to_check = []
                    if status == "OK" and data and data[0]:
                        uids_to_check = sorted(
                            int(u) for u in data[0].split() if int(u) > last_uid
                        )

                    max_uid_in_folder = max(uids_to_check) if uids_to_check else last_uid

                # Обработка найденных писем
                for uid in uids_to_check:
                    uid_str = str(uid)
                    try:
                        status, header_data = await imap.uid(
                            "fetch", uid_str, "(BODY.PEEK[HEADER.FIELDS (SUBJECT MESSAGE-ID)])"
                        )
                        if status != "OK" or not header_data or header_data[0] is None:
                            continue

                        header_bytes = header_data[0][1]
                        header_msg = email.message_from_bytes(header_bytes)

                        message_id = f"{user_id}-{header_msg.get('Message-ID') or f'{folder}-UID{uid_str}'}"
                        subject = decode_mime_words(header_msg.get("Subject", ""))

                        async with data_lock:
                            if message_id in processed_ids:
                                continue

                            if SUBJECT_FILTER.lower() not in subject.lower():
                                processed_ids.add(message_id)
                                continue

                        # Новое подходящее письмо
                        status, msg_data = await imap.uid("fetch", uid_str, "(RFC822)")
                        if status != "OK" or not msg_data or msg_data[0] is None:
                            continue

                        raw_email = msg_data[0][1]
                        msg = email.message_from_bytes(raw_email)

                        html_body, _ = get_email_body(msg)
                        link = extract_topic_link(html_body)

                        text = f"📢 {subject}\n\n👤 <b>От:</b> @{username}"
                        if link:
                            text += f"\n🔗 {link}"

                        owner_chat_id = int(user_id)
                        if owner_chat_id in subscribers:
                            await bot.send_message(owner_chat_id, text, parse_mode="HTML")

                        async with data_lock:
                            processed_ids.add(message_id)
                            last_notification_subject = subject
                            last_notification_time = datetime.datetime.now()

                        logger.info(f"[MAIL MONITOR] ({username}) Отправлено уведомление: {subject}")

                    except Exception as e:
                        logger.error(f"[MAIL MONITOR] Ошибка обработки письма (UID {uid_str}): {e}")
                        continue

                # Обновляем состояние папки
                user_folder_state[folder] = {
                    "uidvalidity": current_uidvalidity,
                    "last_uid": max(max_uid_in_folder, saved.get("last_uid", 0) if saved else 0),
                }

            # Переход в режим IDLE для ожидания новых писем
            logger.info(f"[MAIL MONITOR] Вход в режим IDLE для @{username}...")
            try:
                await imap.idle()
                # Ждем up to 29 минут (Gmail ограничение для IDLE)
                response = await imap.wait_push_response(timeout=1740)  # 29 минут
                if response:
                    logger.info(f"[MAIL MONITOR] IDLE response для @{username}: {response}")
            except asyncio.TimeoutError:
                logger.info(f"[MAIL MONITOR] Таймаут IDLE для @{username}, переподключение...")
            except aioimaplib.ImapAsyncError as e:
                logger.error(f"[MAIL MONITOR] Ошибка IDLE для @{username}: {e}")

            await imap.logout()
            await asyncio.sleep(POLL_INTERVAL)  # Небольшая задержка перед переподключением

        except aioimaplib.ImapError as e:
            logger.error(f"[MAIL MONITOR] Ошибка IMAP для @{username}: {e}")
            await asyncio.sleep(30)
        except Exception as e:
            logger.error(f"[MAIL MONITOR] Критическая ошибка для @{username}: {e}")
            await asyncio.sleep(30)


async def start_monitoring_for_user(user_id: str, email_account: str, app_password: str, username: str, bot: Bot):
    """Запустить или перезапустить мониторинг для пользователя."""
    # Останавливаем старую задачу, если есть
    if user_id in monitoring_tasks:
        monitoring_tasks[user_id].cancel()
        try:
            await monitoring_tasks[user_id]
        except asyncio.CancelledError:
            pass

    # Запускаем новую задачу
    task = asyncio.create_task(
        mail_monitor_with_idle(user_id, email_account, app_password, username, bot)
    )
    monitoring_tasks[user_id] = task
    logger.info(f"[MAIL MONITOR] Задача создана для пользователя {user_id}")


async def stop_monitoring_for_user(user_id: str):
    """Остановить мониторинг для пользователя."""
    if user_id in monitoring_tasks:
        monitoring_tasks[user_id].cancel()
        try:
            await monitoring_tasks[user_id]
        except asyncio.CancelledError:
            pass
        finally:
            del monitoring_tasks[user_id]
        logger.info(f"[MAIL MONITOR] Задача остановлена для пользователя {user_id}")
# ------------------- TELEGRAM HANDLERS (aiogram) -------------------

# Инициализация бота
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)


def is_admin(username: str) -> bool:
    """username ожидается уже в нижнем регистре, без '@'."""
    return bool(username) and username in ADMIN_USERNAMES


def has_start_access(username: str) -> bool:
    """Доступ к /start есть у админов и у тех, кому его выдали через /sell."""
    return is_admin(username) or (bool(username) and username in allowed_users)


async def send_telegram_message(text: str, chat_id: int = None, parse_mode: str = "HTML"):
    """Отправить сообщение подписчикам или конкретному chat_id."""
    if chat_id:
        try:
            await bot.send_message(chat_id, text, parse_mode=parse_mode)
        except Exception as e:
            logger.error(f"[TG] Ошибка отправки в {chat_id}: {e}")
    else:
        async with data_lock:
            targets = list(subscribers)
        for cid in targets:
            try:
                await bot.send_message(cid, text, parse_mode=parse_mode)
            except Exception as e:
                logger.error(f"[TG] Ошибка отправки в {cid}: {e}")


def format_uptime() -> str:
    delta = datetime.datetime.now() - START_TIME
    days, rem = divmod(int(delta.total_seconds()), 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}д")
    if hours:
        parts.append(f"{hours}ч")
    if minutes:
        parts.append(f"{minutes}м")
    parts.append(f"{seconds}с")
    return " ".join(parts)


async def handle_status(message: types.Message):
    """Показать статус бота (для администраторов)."""
    if not is_admin(message.from_user.username.lower()):
        await message.answer("⛔ Команда доступна только администратору.")
        return

    with state_lock:
        last_check = last_check_time
        last_check_ok_val = last_check_ok
        last_check_error_val = last_check_error
        last_notif_subj = last_notification_subject
        last_notif_time_val = last_notification_time

    configured_users = len(gmail_credentials)

    lines = ["🤖 <b>Статус бота</b>"]
    lines.append(f"⏱ Аптайм: <code>{format_uptime()}</code>")
    async with data_lock:
        lines.append(f"👥 Подписчиков: <code>{len(subscribers)}</code>")
    lines.append(f"📧 Настроено почтовых аккаунтов: <code>{configured_users}</code>")

    if last_check:
        status_txt = "✅ успешно" if last_check_ok_val else f"❌ ошибка ({last_check_error_val})"
        lines.append(
            f"📬 Последняя проверка: <code>{last_check.strftime('%Y-%m-%d %H:%M:%S')}</code> — {status_txt}"
        )
    else:
        lines.append("📬 Последняя проверка: ещё не выполнялась")

    if last_notif_time_val:
        lines.append(
            f"🔔 Последнее уведомление: «<code>{last_notif_subj}</code>» "
            f"(<code>{last_notif_time_val.strftime('%Y-%m-%d %H:%M:%S')}</code>)"
        )
    else:
        lines.append("🔔 Уведомлений пока не было")

    await message.answer("\n".join(lines), parse_mode="HTML")


async def handle_ping(message: types.Message):
    """Проверить, работает ли подключение к почте текущего пользователя."""
    if not has_start_access(message.from_user.username.lower()):
        await message.answer(
            "⛔ Доступ ограничен. Обратитесь к администратору, чтобы вам выдали доступ."
        )
        return

    user_id = str(message.chat.id)
    creds = gmail_credentials.get(user_id)

    if not creds:
        await message.answer(
            "⚠️ У вас ещё не настроена почта.\nИспользуйте /setup, чтобы подключить Gmail."
        )
        return

    await message.answer("⏳ Проверяю подключение к вашей почте...")

    ok, error = await test_gmail_connection(creds["email"], creds["password"])

    if ok:
        await message.answer(
            f"✅ <b>Почта подключена и работает</b>\n📧 <code>{creds['email']}</code>",
            parse_mode="HTML",
        )
    else:
        await message.answer(
            f"❌ <b>Не удалось подключиться к почте</b>\n"
            f"📧 <code>{creds['email']}</code>\n\n"
            f"<code>{error}</code>\n\n"
            "Возможно, изменился пароль приложения. Используйте /setup, чтобы обновить учетные данные.",
            parse_mode="HTML",
        )


async def handle_debug(message: types.Message):
    """Начать настройку режима отладки: показать список пользователей на выбор."""
    if not is_admin(message.from_user.username.lower()):
        await message.answer("⛔ Команда доступна только администратору.")
        return

    admin_id = str(message.chat.id)

    if not gmail_credentials:
        await message.answer(
            "⚠️ Пока нет ни одного пользователя с настроенной почтой (/setup)."
        )
        return

    idx_map = {}
    lines = ["🔍 <b>Режим отладки</b>", "Выберите, за кем следить — отправьте номер:", "", "0. Все пользователи"]
    for i, (uid, creds) in enumerate(gmail_credentials.items(), start=1):
        uname = creds.get("username", "unknown")
        lines.append(f"{i}. @{uname} (<code>{creds.get('email', '')}</code>)")
        idx_map[str(i)] = uid

    debug_setup_state[admin_id] = {"idx_map": idx_map}
    await message.answer("\n".join(lines), parse_mode="HTML")


async def process_debug_selection(message: types.Message):
    """Обработать ответ админа на выбор пользователя для отладки."""
    admin_id = str(message.chat.id)
    state = debug_setup_state.get(admin_id)
    if not state:
        return False

    choice = message.text.strip() if message.text else ""

    if choice == "0":
        debug_targets[admin_id] = None
        debug_setup_state.pop(admin_id, None)
        await message.answer(
            "✅ Режим отладки включен для <b>всех</b> пользователей.\n"
            "Используйте /undebug, чтобы выключить.",
            parse_mode="HTML",
        )
        return True

    target_uid = state.get("idx_map", {}).get(choice)
    if not target_uid:
        await message.answer(
            "❌ Не понял выбор. Отправьте номер из списка или /debug заново."
        )
        return True

    debug_targets[admin_id] = target_uid
    debug_setup_state.pop(admin_id, None)

    creds = gmail_credentials.get(target_uid, {})
    uname = creds.get("username", "unknown")
    await message.answer(
        f"✅ Режим отладки включен.\n👀 Мониторю: @{uname} (<code>{creds.get('email', '')}</code>)\n\n"
        "Используйте /undebug, чтобы выключить.",
        parse_mode="HTML",
    )
    return True


async def handle_undebug(message: types.Message):
    """Выключить режим отладки для этого админа."""
    if not is_admin(message.from_user.username.lower()):
        await message.answer("⛔ Команда доступна только администратору.")
        return

    admin_id = str(message.chat.id)
    was_active = admin_id in debug_targets
    debug_setup_state.pop(admin_id, None)
    debug_targets.pop(admin_id, None)

    if was_active:
        await message.answer("✅ Режим отладки выключен.")
    else:
        await message.answer("ℹ️ Режим отладки и так был выключен.")


async def start_gmail_setup(message: types.Message):
    """Запустить процесс настройки Gmail учетных данных."""
    user_id = str(message.chat.id)
    user_setup_state[user_id] = {"step": 1, "username": message.from_user.username}
    save_json(USER_SETUP_STATE_FILE, user_setup_state)

    await message.answer(
        "📧 <b>Настройка Gmail</b>\n\n"
        "Введите ваш Gmail-адрес (например: your.email@gmail.com):",
        parse_mode="HTML",
    )


async def process_setup_step(message: types.Message, username: str):
    """Обработать шаги настройки Gmail."""
    user_id = str(message.chat.id)
    state = user_setup_state.get(user_id, {})
    step = state.get("step", 1)

    if step == 1:
        # Первый шаг: вводим email
        email_account = message.text.strip() if message.text else ""
        if "@gmail.com" not in email_account:
            await message.answer(
                "❌ Пожалуйста, используйте Gmail адрес (должен содержать @gmail.com)"
            )
            return

        user_setup_state[user_id] = {"step": 2, "email": email_account, "username": username}
        save_json(USER_SETUP_STATE_FILE, user_setup_state)

        await message.answer(
            f"✅ Email сохранён: <code>{email_account}</code>\n\n"
            "Теперь введите <b>пароль приложения Gmail</b>.\n\n"
            "Как получить пароль приложения:\n"
            "1. Откройте <a href='https://myaccount.google.com/security'>Google Account Security</a>\n"
            "2. Включите двухфакторную аутентификацию (если еще не включена)\n"
            "3. Вернитесь в Security и найдите 'App passwords'\n"
            "4. Выберите приложение 'Mail' и устройство 'Windows Computer' (или любое)\n"
            "5. Google сгенерирует пароль из 16 символов\n"
            "6. Скопируйте этот пароль (без пробелов) и отправьте его боту",
            parse_mode="HTML",
        )

    elif step == 2:
        # Второй шаг: вводим пароль приложения
        app_password = message.text.strip().replace(" ", "") if message.text else ""
        email_account = state.get("email", "")

        # Тестируем подключение
        await message.answer("⏳ Проверяю учетные данные...")

        ok, error = await test_gmail_connection(email_account, app_password)

        if ok:
            # Сохраняем учетные данные
            async with data_lock:
                gmail_credentials[user_id] = {
                    "email": email_account,
                    "password": app_password,
                    "username": username,
                }
            save_json(GMAIL_CREDENTIALS_FILE, gmail_credentials)

            # Удаляем из состояния настройки
            user_setup_state.pop(user_id, None)
            save_json(USER_SETUP_STATE_FILE, user_setup_state)

            await message.answer(
                f"✅ <b>Настройка завершена!</b>\n\n"
                f"📧 Почта: <code>{email_account}</code>\n\n"
                "Бот начнет проверять вашу почту и отправлять уведомления всем подписчикам.\n\n"
                "Команды:\n"
                "/start - подписаться на уведомления\n"
                "/stop - отписаться\n"
                "/setup - изменить учетные данные\n"
                "/ping - проверить подключение к почте\n"
                "/status - статус бота (только админы)",
                parse_mode="HTML",
            )
            logger.info(f"[SETUP] Пользователь @{username} ({email_account}) успешно настроен")

            # Запускаем мониторинг для нового пользователя
            await start_monitoring_for_user(
                user_id, email_account, app_password, username, bot
            )

        else:
            await message.answer(
                f"❌ <b>Ошибка подключения:</b>\n\n"
                f"<code>{error}</code>\n\n"
                "Проверьте:\n"
                "✓ Правильность email и пароля\n"
                "✓ Включен ли IMAP в Gmail (Настройки → Пересылка и POP/IMAP)\n"
                "✓ Используете ли вы именно пароль приложения, а не обычный пароль\n\n"
                "Попробуем заново. Введите email:",
                parse_mode="HTML",
            )
            user_setup_state[user_id] = {"step": 1, "username": username}
            save_json(USER_SETUP_STATE_FILE, user_setup_state)
# ------------------- COMMAND HANDLERS -------------------

@dp.message_handler(commands=["setup"])
async def setup_handler(message: types.Message):
    """Обработчик команды /setup."""
    await start_gmail_setup(message)


@dp.message_handler(commands=["start"])
async def start_handler(message: types.Message):
    """Обработчик команды /start."""
    username = message.from_user.username.lower() if message.from_user.username else ""

    if not has_start_access(username):
        await message.answer(
            "⛔ Доступ к подписке ограничен. Обратитесь к администратору, "
            "чтобы вам выдали доступ.",
        )
        return

    async with data_lock:
        subscribers.add(message.chat.id)

    save_json(SUBSCRIBERS_FILE, list(subscribers))

    await message.answer(
        "✅ Вы подписаны на уведомления о новых темах форума\n\n"
        "Команды:\n"
        "/setup - настроить свою почту\n"
        "/stop - отписаться\n"
        "/ping - проверить подключение к почте\n"
        "/status - статус бота (только админы)",
        parse_mode="HTML",
    )


@dp.message_handler(commands=["ping"])
async def ping_handler(message: types.Message):
    """Обработчик команды /ping."""
    if not has_start_access(message.from_user.username.lower()):
        await message.answer(
            "⛔ Доступ ограничен. Обратитесь к администратору, чтобы вам выдали доступ.",
        )
        return
    await handle_ping(message)


@dp.message_handler(commands=["stop"])
async def stop_handler(message: types.Message):
    """Обработчик команды /stop."""
    async with data_lock:
        subscribers.discard(message.chat.id)
    save_json(SUBSCRIBERS_FILE, list(subscribers))
    await message.answer("❌ Вы отписаны от уведомлений")


@dp.message_handler(commands=["status"])
async def status_handler(message: types.Message):
    """Обработчик команды /status."""
    await handle_status(message)


@dp.message_handler(commands=["debug"])
async def debug_handler(message: types.Message):
    """Обработчик команды /debug."""
    await handle_debug(message)


@dp.message_handler(commands=["undebug"])
async def undebug_handler(message: types.Message):
    """Обработчик команды /undebug."""
    await handle_undebug(message)


@dp.message_handler(commands=["sell"])
async def sell_handler(message: types.Message):
    """Обработчик команды /sell."""
    if not is_admin(message.from_user.username.lower()):
        await message.answer("⛔ Команда доступна только администратору.")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer("Используйте: /sell @username")
        return

    target = parts[1].strip().lstrip("@").lower()
    async with data_lock:
        if target in allowed_users:
            await message.answer(f"Пользователь @{target} уже имеет доступ к /start.")
        else:
            allowed_users.add(target)
            save_json(ALLOWED_FILE, list(allowed_users))
            await message.answer(f"✅ Пользователю @{target} выдан доступ к /start.")


# ------------------- TEXT HANDLERS (для настройки и debug) -------------------

@dp.message_handler(lambda message: not message.text or not message.text.startswith("/"))
async def process_text_input(message: types.Message):
    """Обработка текстовых сообщений (не команд) для настройки и debug."""
    user_id = str(message.chat.id)

    # Если админ выбирает пользователя для режима отладки
    if user_id in debug_setup_state:
        if await process_debug_selection(message):
            return

    # Если пользователь находится в процессе настройки
    if user_id in user_setup_state:
        username = message.from_user.username or "unknown"
        await process_setup_step(message, username.lower())
        return


@dp.message_handler(content_types=types.ContentType.ANY)
async def echo_handler(message: types.Message):
    """Обработчик всех остальных сообщений."""
    if message.content_type == types.ContentType.TEXT:
        if message.text.startswith("/"):
            # Неизвестная команда
            await message.answer("Неизвестная команда. Используйте /start для справки.")
    else:
        # Игнорируем非 текстовые сообщения
        pass
# ------------------- LIFE CYCLE HOOKS -------------------

@dp.on_startup()
async def on_startup(dp):
    """Запускается при старте бота."""
    logger.info("[BOT] Загрузка сохраненных данных...")
    await load_all_data()

    # Запускаем мониторинг для всех настроенных пользователей
    logger.info("[BOT] Запуск мониторинга для настроенных пользователей...")
    for user_id, creds in gmail_credentials.items():
        await start_monitoring_for_user(
            user_id,
            creds["email"],
            creds["password"],
            creds.get("username", "unknown"),
            bot,
        )

    logger.info("[BOT] Бот успешно запущен и готов к работе")
    logger.info("=" * 60)
    logger.info("🤖 Форум-мониторинг бот запущен (асинхронная версия)")
    logger.info("=" * 60)
    logger.info("Администраторы могут использовать /status для просмотра информации")
    logger.info("Новые пользователи должны выполнить /setup для настройки Gmail")
    logger.info("=" * 60)


@dp.on_shutdown()
async def on_shutdown(dp):
    """Запускается при остановке бота."""
    logger.info("[BOT] Остановка всех задач мониторинга...")
    for user_id, task in monitoring_tasks.items():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    logger.info("[BOT] Все задачи остановлены")


# ------------------- MAIN -------------------

async def main():
    """Основная функция запуска."""
    # Запускаем поллинг
    try:
        await dp.start_polling()
    except KeyboardInterrupt:
        logger.info("[BOT] Остановка по Ctrl+C")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("[BOT] Остановка завершена")
