"""
Мониторинг Gmail (IMAP) на предмет писем о новых темах форума
и рассылка уведомлений подписчикам Telegram-бота.

ВЕРСИЯ С ПОДДЕРЖКОЙ МНОГОПОЛЬЗОВАТЕЛЬСКОЙ НАСТРОЙКИ:
Каждый пользователь сам настраивает свои учетные данные Gmail через команду /setup

Скрипт полностью самодостаточен: при первом запуске сам ставит
недостающие библиотеки (requests, beautifulsoup4), поэтому для
хостинга достаточно одного этого файла.

Что делает скрипт:
1. При команде /setup пользователь вводит свой Gmail и пароль приложения
2. Раз в POLL_INTERVAL секунд проверяет ВСЕ папки почты всех пользователей,
   ищет письма с темой, содержащей SUBJECT_FILTER
3. Из найденного письма достаёт заголовок темы и ссылку,
   спрятанную за кнопкой "Посмотреть эту тему"
4. Рассылает эти данные подписчикам
5. Параллельно слушает Telegram:
     /setup      - настроить/изменить учетные данные Gmail
     /start      - подписаться на рассылку
     /stop       - отписаться
     /ping       - проверить подключение к своей почте (доступно всем с доступом)
     /status     - показать статус бота (только для админов)
     /sell @user - (только для админов) выдать пользователю @user доступ
     /debug      - (только для админов) включить режим отладки: бот присылает
                   в этот чат все ответы по запросам и уведомляет о каждой
                   проверке почты (раз в POLL_INTERVAL сек). После команды
                   нужно выбрать, за каким настроенным пользователем следить
                   (или выбрать "0" — следить за всеми)
     /undebug    - (только для админов) выключить режим отладки

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


for _pip_name, _import_name in [("requests", "requests"), ("beautifulsoup4", "bs4")]:
    _ensure_package(_pip_name, _import_name)

import imaplib
import email
from email.header import decode_header
import json
import os
import time
import threading
import datetime
import requests
from bs4 import BeautifulSoup

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

# ------------------- СОСТОЯНИЕ БОТА (для /status) -------------------

START_TIME = datetime.datetime.now()
LAST_CHECK_TIME = None
LAST_CHECK_OK = None
LAST_CHECK_ERROR = ""
LAST_NOTIFICATION_SUBJECT = ""
LAST_NOTIFICATION_TIME = None

state_lock = threading.Lock()

# ------------------- ХРАНИЛИЩЕ -------------------

def load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default
    return default


def save_json(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[ERROR] Не удалось сохранить {path}: {e}")


subscribers = set(load_json(SUBSCRIBERS_FILE, []))
processed_ids = set(load_json(PROCESSED_FILE, []))
allowed_users = set(u.lower() for u in load_json(ALLOWED_FILE, []))

# Хранилище учетных данных: {user_id: {"email": "...", "password": "...", "username": "..."}}
gmail_credentials = load_json(GMAIL_CREDENTIALS_FILE, {})

# Состояние настройки пользователей: {user_id: {"step": 1 или 2, "email": "..."}}
user_setup_state = load_json(USER_SETUP_STATE_FILE, {})

# Состояние UID-синхронизации по папкам: {user_id: {folder: {"uidvalidity": int, "last_uid": int}}}
folder_state = load_json(FOLDER_STATE_FILE, {})
folder_state_lock = threading.Lock()

# ------------------- РЕЖИМ ОТЛАДКИ (/debug, /undebug) -------------------

# Кто из админов сейчас в режиме отладки и за каким пользователем следит.
# {admin_chat_id (str): target_user_id (str) или None (значит "все пользователи")}
debug_targets = {}

# Состояние выбора пользователя после команды /debug (пока админ не ответил номером).
# {admin_chat_id (str): {"idx_map": {"1": user_id, "2": user_id, ...}}}
debug_setup_state = {}


def get_debug_admins(target_user_id):
    """Возвращает список chat_id админов, которые сейчас отлаживают проверку
    почты для данного target_user_id (или включили отладку для всех)."""
    result = []
    for admin_id, tgt in debug_targets.items():
        if tgt is None or tgt == target_user_id:
            try:
                result.append(int(admin_id))
            except (TypeError, ValueError):
                continue
    return result


def notify_debug(debug_admins, text):
    for aid in debug_admins:
        send_telegram_message(text, chat_id=aid)

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


def get_all_folders(imap):
    status, folders = imap.list()
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


def test_gmail_connection(email_account, app_password):
    """Быстрая проверка, что логин по IMAP проходит."""
    try:
        imap = imaplib.IMAP4_SSL(IMAP_SERVER)
        imap.login(email_account, app_password)
        imap.logout()
        return True, ""
    except Exception as e:
        return False, str(e)


def check_mail_for_user(user_id, email_account, app_password, username):
    """Проверяет почту для конкретного пользователя."""
    global LAST_CHECK_TIME, LAST_CHECK_OK, LAST_CHECK_ERROR
    global LAST_NOTIFICATION_SUBJECT, LAST_NOTIFICATION_TIME

    debug_admins = get_debug_admins(user_id)
    check_started_at = datetime.datetime.now()

    if debug_admins:
        notify_debug(
            debug_admins,
            f"🔍 [DEBUG {check_started_at.strftime('%H:%M:%S')}] Начата проверка почты "
            f"@{username} (<code>{email_account}</code>)",
        )

    try:
        imap = imaplib.IMAP4_SSL(IMAP_SERVER)
        imap.login(email_account, app_password)
    except Exception as e:
        print(f"[IMAP] Ошибка подключения для {username} ({email_account}): {e}")
        with state_lock:
            LAST_CHECK_TIME = datetime.datetime.now()
            LAST_CHECK_OK = False
            LAST_CHECK_ERROR = str(e)
        if debug_admins:
            notify_debug(
                debug_admins,
                f"❌ [DEBUG] Ошибка подключения к почте @{username} (<code>{email_account}</code>):\n<code>{e}</code>",
            )
        return

    folders = get_all_folders(imap)

    if debug_admins:
        notify_debug(debug_admins, f"📂 [DEBUG] Найдено папок: {len(folders)} → {', '.join(folders) if folders else '—'}")

    debug_total_scanned = 0
    debug_total_matched = 0

    user_folder_state = folder_state.setdefault(user_id, {})

    for folder in folders:
        try:
            status, _ = imap.select(f'"{folder}"', readonly=True)
            if status != "OK":
                if debug_admins:
                    notify_debug(debug_admins, f"⚠️ [DEBUG] Папка «{folder}»: select() → {status}")
                continue
        except Exception as e:
            if debug_admins:
                notify_debug(debug_admins, f"⚠️ [DEBUG] Папка «{folder}»: ошибка select() → {e}")
            continue

        # UIDVALIDITY приходит как untagged-ответ на SELECT. Если он изменился
        # (или папку видим впервые), все ранее сохранённые UID для этой папки
        # становятся не валидны — сервер вправе перевыдать их заново.
        try:
            uidval_raw = imap.untagged_responses.get("UIDVALIDITY")
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
            # Впервые видим папку (или сменился UIDVALIDITY): делаем один
            # полный SEARCH ALL, но проверяем только последние FOLDER_SCAN_LIMIT
            # писем, как и раньше — дальше переходим на инкрементальный UID-поиск.
            try:
                status, data = imap.uid("search", None, "ALL")
            except Exception as e:
                if debug_admins:
                    notify_debug(debug_admins, f"⚠️ [DEBUG] Папка «{folder}»: ошибка uid search(ALL) → {e}")
                continue
            if status != "OK" or not data or not data[0]:
                if debug_admins:
                    notify_debug(debug_admins, f"📁 [DEBUG] Папка «{folder}»: писем нет (первичная инициализация)")
                user_folder_state[folder] = {"uidvalidity": current_uidvalidity, "last_uid": 0}
                continue

            all_uids = sorted(int(x) for x in data[0].split())
            uids_to_check = all_uids[-FOLDER_SCAN_LIMIT:]
            max_uid_in_folder = all_uids[-1]

            if debug_admins:
                notify_debug(
                    debug_admins,
                    f"📁 [DEBUG] Папка «{folder}»: первичная инициализация (UIDVALIDITY={current_uidvalidity}), "
                    f"всего писем {len(all_uids)}, будет проверено последних {len(uids_to_check)}",
                )
        else:
            last_uid = saved.get("last_uid", 0)
            try:
                status, data = imap.uid("search", None, f"{last_uid + 1}:*")
            except Exception as e:
                if debug_admins:
                    notify_debug(debug_admins, f"⚠️ [DEBUG] Папка «{folder}»: ошибка uid search(инкремент) → {e}")
                continue

            candidate_uids = []
            if status == "OK" and data and data[0]:
                candidate_uids = [int(x) for x in data[0].split()]

            # Некоторые серверы на диапазон "N:*", где N больше максимального
            # UID в папке, по спецификации всё равно возвращают последний UID.
            # Отфильтровываем всё, что не строго больше last_uid.
            uids_to_check = sorted(u for u in candidate_uids if u > last_uid)
            max_uid_in_folder = max(uids_to_check) if uids_to_check else last_uid

            if debug_admins:
                notify_debug(
                    debug_admins,
                    f"📁 [DEBUG] Папка «{folder}»: инкрементальная проверка от UID {last_uid + 1}, "
                    f"новых писем: {len(uids_to_check)}",
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
                # Сначала лёгкий запрос: только заголовки (тема + Message-ID),
                # без тела письма и вложений. Это на порядки быстрее, чем
                # качать письмо целиком для каждого сообщения на каждом цикле.
                status, header_data = imap.uid(
                    "fetch", uid_str, "(BODY.PEEK[HEADER.FIELDS (SUBJECT MESSAGE-ID)])"
                )
                if status != "OK" or not header_data or header_data[0] is None:
                    if debug_admins:
                        notify_debug(debug_admins, f"⚠️ [DEBUG] fetch header(UID {uid_str}) → {status}, пусто")
                    continue

                header_bytes = header_data[0][1]
                header_msg = email.message_from_bytes(header_bytes)

                message_id = f"{user_id}-{header_msg.get('Message-ID') or f'{folder}-UID{uid_str}'}"
                subject = decode_mime_words(header_msg.get("Subject", ""))
                debug_total_scanned += 1

                if message_id in processed_ids:
                    if debug_admins:
                        notify_debug(debug_admins, f"↩️ [DEBUG] «{subject}» — уже обработано ранее, пропуск")
                    continue

                if SUBJECT_FILTER.lower() not in subject.lower():
                    if debug_admins:
                        notify_debug(debug_admins, f"— [DEBUG] «{subject}» — тема не подходит под фильтр")
                    processed_ids.add(message_id)
                    continue

                debug_total_matched += 1
                if debug_admins:
                    notify_debug(debug_admins, f"✅ [DEBUG] «{subject}» — совпадение с фильтром!")

                # Только теперь, для реально нового и подходящего письма,
                # качаем его целиком, чтобы достать ссылку из HTML.
                status, msg_data = imap.uid("fetch", uid_str, "(RFC822)")
                if status != "OK" or not msg_data or msg_data[0] is None:
                    if debug_admins:
                        notify_debug(debug_admins, f"⚠️ [DEBUG] fetch full(UID {uid_str}) → {status}, пусто")
                    continue

                raw_email = msg_data[0][1]
                msg = email.message_from_bytes(raw_email)

                html_body, _ = get_email_body(msg)
                link = extract_topic_link(html_body)

                if debug_admins:
                    notify_debug(debug_admins, f"🔗 [DEBUG] Ссылка из письма: {link or '—'}")

                text = f"📢 {subject}\n\n👤 <b>От:</b> @{username}"
                if link:
                    text += f"\n🔗 {link}"

                # Уведомление уходит только владельцу этой почты (по его chat_id),
                # а не всем подписчикам бота.
                try:
                    owner_chat_id = int(user_id)
                except (TypeError, ValueError):
                    owner_chat_id = None

                if owner_chat_id is not None and owner_chat_id in subscribers:
                    send_telegram_message(text, chat_id=owner_chat_id)

                processed_ids.add(message_id)

                with state_lock:
                    LAST_NOTIFICATION_SUBJECT = subject
                    LAST_NOTIFICATION_TIME = datetime.datetime.now()

                print(f"[MAIL] ({username}) Отправлено уведомление: {subject}")

            except Exception as e:
                print(f"[MAIL] ({username}) Ошибка обработки письма: {e}")
                continue

        user_folder_state[folder] = {
            "uidvalidity": current_uidvalidity,
            "last_uid": max(max_uid_in_folder, saved.get("last_uid", 0) if saved else 0),
        }

    try:
        imap.logout()
    except Exception:
        pass

    save_json(PROCESSED_FILE, list(processed_ids))
    with folder_state_lock:
        save_json(FOLDER_STATE_FILE, folder_state)

    with state_lock:
        LAST_CHECK_TIME = datetime.datetime.now()
        LAST_CHECK_OK = True
        LAST_CHECK_ERROR = ""

    if debug_admins:
        duration = (datetime.datetime.now() - check_started_at).total_seconds()
        notify_debug(
            debug_admins,
            f"🏁 [DEBUG] Проверка @{username} завершена за {duration:.2f}с. "
            f"Просмотрено писем: {debug_total_scanned}, совпадений: {debug_total_matched}",
        )

    total_duration = (datetime.datetime.now() - check_started_at).total_seconds()
    if total_duration > POLL_INTERVAL:
        print(
            f"[WARN] Проверка почты @{username} заняла {total_duration:.1f}с, "
            f"что больше POLL_INTERVAL ({POLL_INTERVAL}с). "
            "Реальный интервал проверки этого пользователя растягивается."
        )


def mail_monitor_thread():
    """Фоновый поток, проверяющий почту всех пользователей."""
    print("Мониторинг почты запущен. Ожидание новых писем...")
    while True:
        # Проверяем почту для каждого настроенного пользователя
        for user_id_str, creds in gmail_credentials.items():
            try:
                check_mail_for_user(
                    user_id_str,
                    creds["email"],
                    creds["password"],
                    creds.get("username", "unknown")
                )
            except Exception as e:
                print(f"[ERROR] Ошибка при проверке почты пользователя {user_id_str}: {e}")

        time.sleep(POLL_INTERVAL)

# ------------------- TELEGRAM -------------------

def is_admin(username):
    """username ожидается уже в нижнем регистре, без '@'."""
    return bool(username) and username in ADMIN_USERNAMES


def has_start_access(username):
    """Доступ к /start есть у админов и у тех, кому его выдали через /sell."""
    return is_admin(username) or (bool(username) and username in allowed_users)


def send_telegram_message(text, chat_id=None, parse_mode="HTML"):
    targets = [chat_id] if chat_id else list(subscribers)
    if not targets:
        return
    for cid in targets:
        try:
            requests.post(
                f"{TG_API}/sendMessage",
                json={"chat_id": cid, "text": text, "parse_mode": parse_mode},
                timeout=10,
            )
        except Exception as e:
            print(f"[TG] Ошибка отправки в {cid}: {e}")


def format_uptime():
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


def handle_status(chat_id):
    """Показать статус бота (для администраторов)."""
    with state_lock:
        last_check = LAST_CHECK_TIME
        last_check_ok = LAST_CHECK_OK
        last_check_error = LAST_CHECK_ERROR
        last_notif_subj = LAST_NOTIFICATION_SUBJECT
        last_notif_time = LAST_NOTIFICATION_TIME

    configured_users = len(gmail_credentials)

    lines = ["🤖 <b>Статус бота</b>"]
    lines.append(f"⏱ Аптайм: <code>{format_uptime()}</code>")
    lines.append(f"👥 Подписчиков: <code>{len(subscribers)}</code>")
    lines.append(f"📧 Настроено почтовых аккаунтов: <code>{configured_users}</code>")

    if last_check:
        status_txt = "✅ успешно" if last_check_ok else f"❌ ошибка ({last_check_error})"
        lines.append(f"📬 Последняя проверка: <code>{last_check.strftime('%Y-%m-%d %H:%M:%S')}</code> — {status_txt}")
    else:
        lines.append("📬 Последняя проверка: ещё не выполнялась")

    if last_notif_time:
        lines.append(f"🔔 Последнее уведомление: «<code>{last_notif_subj}</code>» (<code>{last_notif_time.strftime('%Y-%m-%d %H:%M:%S')}</code>)")
    else:
        lines.append("🔔 Уведомлений пока не было")

    send_telegram_message("\n".join(lines), chat_id=chat_id)


def handle_ping(chat_id, username):
    """Проверить, работает ли подключение к почте текущего пользователя."""
    user_id = str(chat_id)
    creds = gmail_credentials.get(user_id)

    if not creds:
        send_telegram_message(
            "⚠️ У вас ещё не настроена почта.\nИспользуйте /setup, чтобы подключить Gmail.",
            chat_id=chat_id,
        )
        return

    send_telegram_message("⏳ Проверяю подключение к вашей почте...", chat_id=chat_id)

    ok, error = test_gmail_connection(creds["email"], creds["password"])

    if ok:
        send_telegram_message(
            f"✅ <b>Почта подключена и работает</b>\n📧 <code>{creds['email']}</code>",
            chat_id=chat_id,
        )
    else:
        send_telegram_message(
            f"❌ <b>Не удалось подключиться к почте</b>\n"
            f"📧 <code>{creds['email']}</code>\n\n"
            f"<code>{error}</code>\n\n"
            "Возможно, изменился пароль приложения. Используйте /setup, чтобы обновить учетные данные.",
            chat_id=chat_id,
        )


def handle_debug(chat_id):
    """Начать настройку режима отладки: показать список пользователей на выбор."""
    admin_id = str(chat_id)

    if not gmail_credentials:
        send_telegram_message(
            "⚠️ Пока нет ни одного пользователя с настроенной почтой (/setup).",
            chat_id=chat_id,
        )
        return

    idx_map = {}
    lines = ["🔍 <b>Режим отладки</b>", "Выберите, за кем следить — отправьте номер:", "", "0. Все пользователи"]
    for i, (uid, creds) in enumerate(gmail_credentials.items(), start=1):
        uname = creds.get("username", "unknown")
        lines.append(f"{i}. @{uname} (<code>{creds.get('email', '')}</code>)")
        idx_map[str(i)] = uid

    debug_setup_state[admin_id] = {"idx_map": idx_map}
    send_telegram_message("\n".join(lines), chat_id=chat_id)


def process_debug_selection(chat_id, text):
    """Обработать ответ админа на выбор пользователя для отладки."""
    admin_id = str(chat_id)
    state = debug_setup_state.get(admin_id)
    if not state:
        return

    choice = text.strip()

    if choice == "0":
        debug_targets[admin_id] = None
        debug_setup_state.pop(admin_id, None)
        send_telegram_message(
            "✅ Режим отладки включен для <b>всех</b> пользователей.\n"
            "Используйте /undebug, чтобы выключить.",
            chat_id=chat_id,
        )
        return

    target_uid = state.get("idx_map", {}).get(choice)
    if not target_uid:
        send_telegram_message(
            "❌ Не понял выбор. Отправьте номер из списка или /debug заново.",
            chat_id=chat_id,
        )
        return

    debug_targets[admin_id] = target_uid
    debug_setup_state.pop(admin_id, None)

    creds = gmail_credentials.get(target_uid, {})
    uname = creds.get("username", "unknown")
    send_telegram_message(
        f"✅ Режим отладки включен.\n👀 Мониторю: @{uname} (<code>{creds.get('email', '')}</code>)\n\n"
        "Используйте /undebug, чтобы выключить.",
        chat_id=chat_id,
    )


def handle_undebug(chat_id):
    """Выключить режим отладки для этого админа."""
    admin_id = str(chat_id)
    was_active = admin_id in debug_targets
    debug_setup_state.pop(admin_id, None)
    debug_targets.pop(admin_id, None)

    if was_active:
        send_telegram_message("✅ Режим отладки выключен.", chat_id=chat_id)
    else:
        send_telegram_message("ℹ️ Режим отладки и так был выключен.", chat_id=chat_id)


def start_gmail_setup(chat_id, username):
    """Запустить процесс настройки Gmail учетных данных."""
    user_id = str(chat_id)
    user_setup_state[user_id] = {"step": 1, "username": username}
    save_json(USER_SETUP_STATE_FILE, user_setup_state)

    send_telegram_message(
        "📧 <b>Настройка Gmail</b>\n\n"
        "Введите ваш Gmail-адрес (например: your.email@gmail.com):",
        chat_id=chat_id
    )


def process_setup_step(chat_id, text, username):
    """Обработать шаги настройки Gmail."""
    user_id = str(chat_id)
    state = user_setup_state.get(user_id, {})
    step = state.get("step", 1)

    if step == 1:
        # Первый шаг: вводим email
        email_account = text.strip()
        if "@gmail.com" not in email_account:
            send_telegram_message(
                "❌ Пожалуйста, используйте Gmail адрес (должен содержать @gmail.com)",
                chat_id=chat_id
            )
            return

        user_setup_state[user_id] = {"step": 2, "email": email_account, "username": username}
        save_json(USER_SETUP_STATE_FILE, user_setup_state)

        send_telegram_message(
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
            parse_mode="HTML"
        )

    elif step == 2:
        # Второй шаг: вводим пароль приложения
        app_password = text.strip().replace(" ", "")
        email_account = state.get("email", "")

        # Тестируем подключение
        send_telegram_message("⏳ Проверяю учетные данные...", chat_id=chat_id)

        ok, error = test_gmail_connection(email_account, app_password)

        if ok:
            # Сохраняем учетные данные
            gmail_credentials[user_id] = {
                "email": email_account,
                "password": app_password,
                "username": username
            }
            save_json(GMAIL_CREDENTIALS_FILE, gmail_credentials)

            # Удаляем из состояния настройки
            user_setup_state.pop(user_id, None)
            save_json(USER_SETUP_STATE_FILE, user_setup_state)

            send_telegram_message(
                f"✅ <b>Настройка завершена!</b>\n\n"
                f"📧 Почта: <code>{email_account}</code>\n\n"
                "Бот начнет проверять вашу почту и отправлять уведомления всем подписчикам.\n\n"
                "Команды:\n"
                "/start - подписаться на уведомления\n"
                "/stop - отписаться\n"
                "/setup - изменить учетные данные\n"
                "/ping - проверить подключение к почте\n"
                "/status - статус бота (только админы)",
                chat_id=chat_id
            )
            print(f"[SETUP] Пользователь @{username} ({email_account}) успешно настроен")

        else:
            send_telegram_message(
                f"❌ <b>Ошибка подключения:</b>\n\n"
                f"<code>{error}</code>\n\n"
                "Проверьте:\n"
                "✓ Правильность email и пароля\n"
                "✓ Включен ли IMAP в Gmail (Настройки → Пересылка и POP/IMAP)\n"
                "✓ Используете ли вы именно пароль приложения, а не обычный пароль\n\n"
                "Попробуем заново. Введите email:",
                chat_id=chat_id
            )
            user_setup_state[user_id] = {"step": 1, "username": username}
            save_json(USER_SETUP_STATE_FILE, user_setup_state)


def telegram_polling():
    offset = None
    while True:
        try:
            params = {"timeout": 30}
            if offset:
                params["offset"] = offset
            resp = requests.get(f"{TG_API}/getUpdates", params=params, timeout=35)
            updates = resp.json().get("result", [])

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

                # Если админ выбирает пользователя для режима отладки
                if user_id in debug_setup_state and not text.startswith("/"):
                    process_debug_selection(chat_id, text)
                    continue

                # Если пользователь находится в процессе настройки
                if user_id in user_setup_state and not text.startswith("/"):
                    process_setup_step(chat_id, text, username)
                    continue

                # Обработка команд
                if text == "/setup":
                    start_gmail_setup(chat_id, username)

                elif text == "/start":
                    if not has_start_access(username):
                        send_telegram_message(
                            "⛔ Доступ к подписке ограничен. Обратитесь к администратору, "
                            "чтобы вам выдали доступ.",
                            chat_id=chat_id,
                        )
                        continue
                    if chat_id not in subscribers:
                        subscribers.add(chat_id)
                        save_json(SUBSCRIBERS_FILE, list(subscribers))
                    send_telegram_message(
                        "✅ Вы подписаны на уведомления о новых темах форума\n\n"
                        "Команды:\n"
                        "/setup - настроить свою почту\n"
                        "/stop - отписаться\n"
                        "/ping - проверить подключение к почте\n"
                        "/status - статус бота (только админы)",
                        chat_id=chat_id,
                    )

                elif text == "/ping":
                    if not has_start_access(username):
                        send_telegram_message(
                            "⛔ Доступ ограничен. Обратитесь к администратору, чтобы вам выдали доступ.",
                            chat_id=chat_id,
                        )
                        continue
                    handle_ping(chat_id, username)

                elif text == "/stop":
                    if chat_id in subscribers:
                        subscribers.discard(chat_id)
                        save_json(SUBSCRIBERS_FILE, list(subscribers))
                    send_telegram_message("❌ Вы отписаны от уведомлений", chat_id=chat_id)

                elif text == "/status":
                    if not is_admin(username):
                        send_telegram_message("⛔ Команда доступна только администратору.", chat_id=chat_id)
                        continue
                    handle_status(chat_id)

                elif text == "/debug":
                    if not is_admin(username):
                        send_telegram_message("⛔ Команда доступна только администратору.", chat_id=chat_id)
                        continue
                    handle_debug(chat_id)

                elif text == "/undebug":
                    if not is_admin(username):
                        send_telegram_message("⛔ Команда доступна только администратору.", chat_id=chat_id)
                        continue
                    handle_undebug(chat_id)

                elif text.startswith("/sell"):
                    if not is_admin(username):
                        send_telegram_message("⛔ Команда доступна только администратору.", chat_id=chat_id)
                        continue
                    parts = text.split(maxsplit=1)
                    if len(parts) < 2 or not parts[1].strip():
                        send_telegram_message("Используйте: /sell @username", chat_id=chat_id)
                        continue
                    target = parts[1].strip().lstrip("@").lower()
                    if target in allowed_users:
                        send_telegram_message(f"Пользователь @{target} уже имеет доступ к /start.", chat_id=chat_id)
                    else:
                        allowed_users.add(target)
                        save_json(ALLOWED_FILE, list(allowed_users))
                        send_telegram_message(f"✅ Пользователю @{target} выдан доступ к /start.", chat_id=chat_id)

        except Exception as e:
            print(f"[TG] Ошибка polling: {e}")
            time.sleep(5)

# ------------------- ЗАПУСК -------------------

def main():
    # Запускаем поток Telegram polling
    tg_thread = threading.Thread(target=telegram_polling, daemon=True)
    tg_thread.start()

    # Запускаем поток проверки почты
    mail_thread = threading.Thread(target=mail_monitor_thread, daemon=True)
    mail_thread.start()

    print("=" * 60)
    print("🤖 Форум-мониторинг бот запущен")
    print("=" * 60)
    print("Администраторы могут использовать /status для просмотра информации")
    print("Новые пользователи должны выполнить /setup для настройки Gmail")
    print("=" * 60)

    # Главный поток просто ждет
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Бот остановлен")


if __name__ == "__main__":
    main()
