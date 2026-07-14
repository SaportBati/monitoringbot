"""
Мониторинг Gmail (IMAP) на предмет писем о новых темах форума
и рассылка уведомлений подписчикам Telegram-бота.

Скрипт полностью самодостаточен: при первом запуске сам ставит
недостающие библиотеки (requests, beautifulsoup4), поэтому для
хостинга достаточно одного этого файла.

Что делает скрипт:
1. Раз в POLL_INTERVAL секунд подключается к Gmail по IMAP,
   проходит ВСЕ папки (включая Спам), ищет письма с темой,
   содержащей SUBJECT_FILTER.
2. Из найденного письма достаёт заголовок темы и ссылку,
   спрятанную за кнопкой "Посмотреть эту тему".
3. Рассылает эти данные всем подписчикам Telegram-бота.
4. Параллельно слушает Telegram:
     /start - подписаться на рассылку
     /stop  - отписаться
     /ping  - статус бота (аптайм, время последней проверки почты,
              подключение к Gmail, число подписчиков)

Перед запуском:
- впишите свой Gmail-адрес в EMAIL_ACCOUNT ниже
- в Gmail должен быть включён доступ по IMAP
  (Настройки -> Пересылка и POP/IMAP -> Включить IMAP)
- APP_PASSWORD — это пароль приложения (app password), не обычный пароль
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
EMAIL_ACCOUNT = "grebenkinmatveyvyceslacovi2007@gmail.com"          # <-- укажите свою почту
APP_PASSWORD = "ltdc girf btdu ihzs"           # пароль приложения Gmail
BOT_TOKEN = "8808314870:AAHmQRtoaxcJXGQr1EdOBlHzIro20RzhFPw"

POLL_INTERVAL = 30              # как часто проверять почту, секунд
SUBJECT_FILTER = "новая тема в отслеживаемом форуме"
FOLDER_SCAN_LIMIT = 30           # сколько последних писем в каждой папке проверять

TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

DATA_DIR = "data"
SUBSCRIBERS_FILE = os.path.join(DATA_DIR, "subscribers.json")
PROCESSED_FILE = os.path.join(DATA_DIR, "processed.json")

os.makedirs(DATA_DIR, exist_ok=True)

# ------------------- СОСТОЯНИЕ БОТА (для /ping) -------------------

START_TIME = datetime.datetime.now()
LAST_CHECK_TIME = None          # когда последний раз проверяли почту
LAST_CHECK_OK = None            # успешно ли прошло подключение при последней проверке
LAST_CHECK_ERROR = ""           # текст последней ошибки, если была
LAST_NOTIFICATION_SUBJECT = ""  # тема последнего отправленного уведомления
LAST_NOTIFICATION_TIME = None

state_lock = threading.Lock()

# ------------------- ХРАНИЛИЩЕ -------------------

def load_json(path, default):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


subscribers = set(load_json(SUBSCRIBERS_FILE, []))
processed_ids = set(load_json(PROCESSED_FILE, []))

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


def test_gmail_connection():
    """Быстрая проверка, что логин по IMAP проходит. Используется для /ping."""
    try:
        imap = imaplib.IMAP4_SSL(IMAP_SERVER)
        imap.login(EMAIL_ACCOUNT, APP_PASSWORD)
        imap.logout()
        return True, ""
    except Exception as e:
        return False, str(e)


def check_mail():
    global LAST_CHECK_TIME, LAST_CHECK_OK, LAST_CHECK_ERROR
    global LAST_NOTIFICATION_SUBJECT, LAST_NOTIFICATION_TIME

    try:
        imap = imaplib.IMAP4_SSL(IMAP_SERVER)
        imap.login(EMAIL_ACCOUNT, APP_PASSWORD)
    except Exception as e:
        print(f"[IMAP] Ошибка подключения: {e}")
        with state_lock:
            LAST_CHECK_TIME = datetime.datetime.now()
            LAST_CHECK_OK = False
            LAST_CHECK_ERROR = str(e)
        return

    folders = get_all_folders(imap)

    for folder in folders:
        try:
            status, _ = imap.select(f'"{folder}"', readonly=True)
            if status != "OK":
                continue
        except Exception:
            continue

        try:
            status, data = imap.search(None, "ALL")
        except Exception:
            continue
        if status != "OK" or not data or not data[0]:
            continue

        ids = data[0].split()

        for eid in ids[-FOLDER_SCAN_LIMIT:]:
            try:
                status, msg_data = imap.fetch(eid, "(RFC822)")
                if status != "OK" or not msg_data or msg_data[0] is None:
                    continue

                raw_email = msg_data[0][1]
                msg = email.message_from_bytes(raw_email)

                message_id = msg.get("Message-ID") or f"{folder}-{eid.decode()}"
                if message_id in processed_ids:
                    continue

                subject = decode_mime_words(msg.get("Subject", ""))

                if SUBJECT_FILTER.lower() not in subject.lower():
                    processed_ids.add(message_id)
                    continue

                html_body, _ = get_email_body(msg)
                link = extract_topic_link(html_body)

                text = f"📢 {subject}"
                if link:
                    text += f"\n{link}"

                send_telegram_message(text)
                processed_ids.add(message_id)

                with state_lock:
                    LAST_NOTIFICATION_SUBJECT = subject
                    LAST_NOTIFICATION_TIME = datetime.datetime.now()

                print(f"[MAIL] Отправлено уведомление: {subject}")

            except Exception as e:
                print(f"[MAIL] Ошибка обработки письма: {e}")
                continue

    try:
        imap.logout()
    except Exception:
        pass

    save_json(PROCESSED_FILE, list(processed_ids))

    with state_lock:
        LAST_CHECK_TIME = datetime.datetime.now()
        LAST_CHECK_OK = True
        LAST_CHECK_ERROR = ""

# ------------------- TELEGRAM -------------------

def send_telegram_message(text, chat_id=None):
    targets = [chat_id] if chat_id else list(subscribers)
    if not targets:
        return
    for cid in targets:
        try:
            requests.post(
                f"{TG_API}/sendMessage",
                json={"chat_id": cid, "text": text},
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


def handle_ping(chat_id):
    gmail_ok, gmail_error = test_gmail_connection()

    with state_lock:
        last_check = LAST_CHECK_TIME
        last_check_ok = LAST_CHECK_OK
        last_check_error = LAST_CHECK_ERROR
        last_notif_subj = LAST_NOTIFICATION_SUBJECT
        last_notif_time = LAST_NOTIFICATION_TIME

    lines = ["🤖 Статус бота"]
    lines.append(f"Аптайм: {format_uptime()}")
    lines.append(f"Подписчиков: {len(subscribers)}")

    if gmail_ok:
        lines.append("Gmail: ✅ подключение проходит")
    else:
        lines.append(f"Gmail: ❌ ошибка подключения ({gmail_error})")

    if last_check:
        status_txt = "успешно" if last_check_ok else f"ошибка ({last_check_error})"
        lines.append(f"Последняя проверка почты: {last_check.strftime('%Y-%m-%d %H:%M:%S')} — {status_txt}")
    else:
        lines.append("Последняя проверка почты: ещё не выполнялась")

    if last_notif_time:
        lines.append(f"Последнее уведомление: «{last_notif_subj}» ({last_notif_time.strftime('%Y-%m-%d %H:%M:%S')})")
    else:
        lines.append("Уведомлений пока не было")

    send_telegram_message("\n".join(lines), chat_id=chat_id)


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
                text = message.get("text", "")

                if text == "/start":
                    if chat_id not in subscribers:
                        subscribers.add(chat_id)
                        save_json(SUBSCRIBERS_FILE, list(subscribers))
                    send_telegram_message(
                        "Вы подписаны на уведомления о новых темах форума ✅\nЧтобы отписаться — /stop\nПроверить статус бота — /ping",
                        chat_id=chat_id,
                    )
                elif text == "/stop":
                    if chat_id in subscribers:
                        subscribers.discard(chat_id)
                        save_json(SUBSCRIBERS_FILE, list(subscribers))
                    send_telegram_message("Вы отписаны от уведомлений ❌", chat_id=chat_id)
                elif text == "/ping":
                    handle_ping(chat_id)

        except Exception as e:
            print(f"[TG] Ошибка polling: {e}")
            time.sleep(5)

# ------------------- ЗАПУСК -------------------

def main():
    t = threading.Thread(target=telegram_polling, daemon=True)
    t.start()

    print("Мониторинг почты запущен. Ожидание новых писем...")
    while True:
        check_mail()
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
