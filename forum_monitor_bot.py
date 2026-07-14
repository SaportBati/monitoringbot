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
     /status     - показать статус бота (только для админов)
     /sell @user - (только для админов) выдать пользователю @user доступ

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

POLL_INTERVAL = 30              # как часто проверять почту, секунд
SUBJECT_FILTER = "новая тема в отслеживаемом форуме"
FOLDER_SCAN_LIMIT = 30           # сколько последних писем в каждой папке проверять

TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

DATA_DIR = "data"
SUBSCRIBERS_FILE = os.path.join(DATA_DIR, "subscribers.json")
PROCESSED_FILE = os.path.join(DATA_DIR, "processed.json")
ALLOWED_FILE = os.path.join(DATA_DIR, "allowed_users.json")
GMAIL_CREDENTIALS_FILE = os.path.join(DATA_DIR, "gmail_credentials.json")
USER_SETUP_STATE_FILE = os.path.join(DATA_DIR, "user_setup_state.json")

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

    try:
        imap = imaplib.IMAP4_SSL(IMAP_SERVER)
        imap.login(email_account, app_password)
    except Exception as e:
        print(f"[IMAP] Ошибка подключения для {username} ({email_account}): {e}")
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

                message_id = f"{user_id}-{msg.get('Message-ID') or f'{folder}-{eid.decode()}'}"
                if message_id in processed_ids:
                    continue

                subject = decode_mime_words(msg.get("Subject", ""))

                if SUBJECT_FILTER.lower() not in subject.lower():
                    processed_ids.add(message_id)
                    continue

                html_body, _ = get_email_body(msg)
                link = extract_topic_link(html_body)

                text = f"📢 {subject}\n\n👤 <b>От:</b> @{username}"
                if link:
                    text += f"\n🔗 {link}"

                send_telegram_message(text)
                processed_ids.add(message_id)

                with state_lock:
                    LAST_NOTIFICATION_SUBJECT = subject
                    LAST_NOTIFICATION_TIME = datetime.datetime.now()

                print(f"[MAIL] ({username}) Отправлено уведомление: {subject}")

            except Exception as e:
                print(f"[MAIL] ({username}) Ошибка обработки письма: {e}")
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
                        "/status - статус бота (только админы)",
                        chat_id=chat_id,
                    )

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
