import vk_api
import time
from datetime import datetime
import threading
import json
import os
import logging
import asyncio
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram import Update, Bot
from telegram.constants import ParseMode
import re

# Настройки логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


# ГЛОБАЛЬНЫЕ НАСТРОЙКИ
TG_BOT_TOKEN = "8256738732:AAFNvz4ZO5822qNYJvYnu3kV7nbWcxRNRfQ"
ADMIN_CHAT_ID = 1078972723 # Ваш Telegram ID

USERS_CONFIG = {
    "USER1": {
        "TOKEN": "vk1.a.jbwK6GFMSysGLmhUvcUuvU2E5l3Fe8C1BgsD3QjgFN-hzCK22EPwFDzGEAprjQ0UZDjmZB2a_UUMUwI0aq5TzFz22-FgEbRQDvwu35Ij7MiGpQaXTdKip-ubgThRkBfcmH89Ewg4qg8H_wp0OClS6fZQm2lCo_jdejXl9frYe40BZFrpFoFMLMyCxN_kyj2ypmiWombbLhZ6Bpxy1v5vDQ",
        "ALLOWED_CONFS": [
            2000000038, 2000000042, 2000000079,
            2000000043, 2000000085,
            2000000070,
            2000000078, 2000000071, 2000000037,
            2000000004, 2000000039, 2000000090
        ],
        "AUTO_REPLY_ENABLED": True,
        "AUTO_REPLY_CONF": 2000000040,
        "TRIGGER_KEYWORDS": ["/offjail", "/gunban", "/offban", "/mute", "v_mute", "/offwarn", "/fmute"],
        "ALLOWED_USER_IDS": [178936944, 539424038, 662975714, 90142259, 579184633, 408471417],
    },
    "USER2": {
        "TOKEN": "vk1.a.kEIwbZeJL7UHs4gJQhq6_FoMbRBIj8i8cjprPPDtunaMn3LI42x0cFeGmg_9E0PSYoxebvCI30KyY6s5AKwZ9gMITyJX4-5G7N9LC9o5KC48ra2C6Hay81MMl33wrrN86OOGHcMRVlBD2-RvgAtTglsUiW1zBNW33OnzXKbdOAzfcJROTyUOH8WKtMB9g7KzsfT_B8I9Hg4AP0pkhQw3tA",
        "ALLOWED_CONFS": [2000000074],
        "AUTO_REPLY_ENABLED": False,
        "AUTO_REPLY_CONF": None,
        "TRIGGER_KEYWORDS": [],
        "ALLOWED_USER_IDS": [],
    }
}

CHECK_INTERVAL = 1
REPLY_COOLDOWN = 1800
MESSAGE_MAX_AGE = 10

# ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ
bots_running = {}
logs = {}
logs_lock = threading.Lock()
auto_reply_status = {}
vk_sessions = {}
vk_api_objects = {}
last_command_times = {}

# ========== ЛОГИ ==========
def log_message(user_key, message_dict):
    with logs_lock:
        if user_key not in logs:
            logs[user_key] = []
        logs[user_key].append(message_dict)
        os.makedirs("logs", exist_ok=True)
        filename = f"logs/{user_key}_logs.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(logs[user_key], f, ensure_ascii=False, indent=2)

def log_info(user_key, text):
    print(f"[{user_key}] {text}")

# Эта функция теперь отправляет логи через отдельный цикл событий, чтобы не блокировать VK ботов
def telegram_send_log_blocking(text):
    asyncio.run(telegram_send_log(text))

async def telegram_send_log(text):
    try:
        bot_tg = Bot(token=TG_BOT_TOKEN)
        await bot_tg.send_message(chat_id=ADMIN_CHAT_ID, text=text, parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"Ошибка отправки лога в Telegram: {e}")


# ====== VK BOT ======
def run_vk_bot(user_key, config):
    TOKEN = config["TOKEN"]
    ALLOWED_CONFS = config["ALLOWED_CONFS"]
    AUTO_REPLY_CONF = config["AUTO_REPLY_CONF"]
    TRIGGER_KEYWORDS = config["TRIGGER_KEYWORDS"]
    ALLOWED_USER_IDS = config["ALLOWED_USER_IDS"]

    replied_messages = set()
    last_reply_times = {}

    if user_key not in last_command_times:
      last_command_times[user_key] = {}

    try:
        vk_session = vk_api.VkApi(token=TOKEN)
        vk_api_obj = vk_session.get_api()
        vk_sessions[user_key] = vk_session
        vk_api_objects[user_key] = vk_api_obj

        user_info = vk_api_obj.users.get()[0]
        log_info(user_key, f"Запущен бот для {user_info['first_name']} {user_info['last_name']} (id={user_info['id']})")
    except Exception as e:
        log_info(user_key, f"Ошибка подключения к VK: {e}")
        bots_running[user_key] = False
        return

    bots_running[user_key] = True
    
    while bots_running.get(user_key, False):
        try:
            convs = vk_api_objects[user_key].messages.getConversations(filter='unread', count=50)
            if convs["count"] > 0:
                log_info(user_key, f"Непрочитанных диалогов: {convs['count']}")

                for conv in convs["items"]:
                    peer_id = conv["conversation"]["peer"]["id"]
                    unread_count = conv["conversation"].get("unread_count", 0)
                    last_msg = conv.get("last_message", {})
                    msg_id = last_msg.get("id")
                    from_id = last_msg.get("from_id")
                    text = last_msg.get("text", "")
                    date_msg = last_msg.get("date", 0)

                    log_dict = {
                        "peer_id": peer_id,
                        "msg_id": msg_id,
                        "from_id": from_id,
                        "text": text,
                        "date": date_msg,
                        "unread_count": unread_count,
                    }
                    log_message(user_key, log_dict)

                    if peer_id in ALLOWED_CONFS and unread_count > 0:
                        vk_api_objects[user_key].messages.markAsRead(peer_id=peer_id)
                        log_info(user_key, f"Отметил {peer_id} как прочитанный")

                    if auto_reply_status.get(user_key, config["AUTO_REPLY_ENABLED"]) and AUTO_REPLY_CONF == peer_id:
                        current_time = time.time()
                        if text:
                            txt_low = text.lower()
                            for keyword in TRIGGER_KEYWORDS:
                                match = re.search(fr"^{re.escape(keyword)}\s+([\s\S]+?)\s+(\d+)\s+([\s\S]+)", txt_low)
                                if match and from_id in ALLOWED_USER_IDS:
                                    last_reply = last_command_times[user_key].get(from_id, 0)
                                    if current_time - last_reply >= REPLY_COOLDOWN:
                                        try:
                                            vk_api_objects[user_key].messages.send(peer_id=peer_id, message="Выдам", random_id=0)
                                            last_command_times[user_key][from_id] = current_time

                                            telegram_send_log_blocking(
                                                f"<b>✅ Автоответ сработал!</b>\n"
                                                f"<b>Пользователь:</b> [id{from_id}|ссылка]\n"
                                                f"<b>Команда:</b> {keyword}\n"
                                                f"<b>Сообщение:</b> {text}"
                                            )
                                            log_info(user_key, f"Автоответ отправлен пользователю {from_id} в {peer_id}")
                                        except Exception as e:
                                            log_info(user_key, f"Ошибка отправки автоответа: {e}")
                                    else:
                                        remain = int(REPLY_COOLDOWN - (current_time - last_reply))
                                        log_info(user_key, f"Кулдаун автоответа для {from_id}, осталось {remain} сек")
                                        telegram_send_log_blocking(
                                            f"<b>⚠️ Кулдаун автоответа</b>\n"
                                            f"<b>Пользователь:</b> [id{from_id}|ссылка]\n"
                                            f"<b>Осталось:</b> {remain} сек\n"
                                            f"<b>Команда:</b> {keyword}"
                                        )
                                    break
                                elif keyword in txt_low:
                                    telegram_send_log_blocking(
                                        f"<b>❌ Некорректная форма команды</b>\n"
                                        f"<b>Пользователь:</b> [id{from_id}|ссылка]\n"
                                        f"<b>Сообщение:</b> {text}"
                                    )
                                    log_info(user_key, f"Некорректная форма команды от {from_id} в {peer_id}")
            time.sleep(CHECK_INTERVAL)

        except Exception as e:
            log_info(user_key, f"Ошибка в цикле: {e}")
            time.sleep(5)


# ======== TELEGRAM BOT =======
async def check_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if str(user_id) != str(ADMIN_CHAT_ID):
        await update.message.reply_text("🚫 У вас нет доступа к управлению ботом.")
        return False
    return True

# ======== КОМАНДЫ ========
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context): return
    text = (
        "Привет! Управление VK ботами.\n"
        "Доступные команды:\n"
        "/status - Статус всех ботов\n"
        "/users - Список VK пользователей\n"
        "/dialogs USER - Список диалогов пользователя\n"
        "/read USER PEER [PAGE] - Просмотр сообщений диалога (страница по 20 сообщений)\n"
        "/send USER PEER TEXT - Отправить сообщение в диалог\n"
        "/lastmsgs USER PEER [COUNT] - Последние N сообщений\n"
        "/autorespond USER ON/OFF - Вкл/выкл автоответ\n"
        "/stop USER - Остановить VK бота\n"
        "/startbot USER - Запустить VK бота\n"
        "/userinfo USER - Инфо о VK пользователе\n"
        "/download USER PEER - Выгрузить историю диалога в JSON\n"
        "/allowedusers USER - Список разрешённых VK ID\n"
        "/allowedconfs USER - Список разрешённых бесед\n"
        "/addalloweduser USER VKID - Добавить разрешённого пользователя\n"
        "/removealloweduser USER VKID - Убрать разрешённого пользователя\n"
        "/addallowedconf USER PEER - Добавить разрешённый чат\n"
        "/removeallowedconf USER PEER - Убрать разрешённый чат\n"
    )
    await update.message.reply_text(text)

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context): return
    msg = ""
    for user_key in USERS_CONFIG.keys():
        running = bots_running.get(user_key, False)
        autoreply = auto_reply_status.get(user_key, USERS_CONFIG[user_key]["AUTO_REPLY_ENABLED"])
        msg += f"{user_key}: Запущен: {running}, Автоответ: {autoreply}\n"
    await update.message.reply_text(msg)

async def cmd_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context): return
    lines = []
    for user_key, cfg in USERS_CONFIG.items():
        vk_obj = vk_api_objects.get(user_key)
        if vk_obj:
            try:
                user_info = vk_obj.users.get()[0]
                name = f"{user_info.get('first_name')} {user_info.get('last_name')}"
            except:
                name = "Ошибка получения имени"
        else:
            name = "Бот не запущен"
        lines.append(f"{user_key} — {name}")
    await update.message.reply_text("\n".join(lines))

async def cmd_dialogs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context): return
    args = context.args
    if len(args) != 1:
        await update.message.reply_text("Использование: /dialogs USER")
        return
    user_key = args[0].upper()
    if user_key not in USERS_CONFIG:
        await update.message.reply_text("Пользователь не найден.")
        return

    vk_obj = vk_api_objects.get(user_key)
    if not vk_obj:
        await update.message.reply_text("Бот не запущен.")
        return

    try:
        convs = vk_obj.messages.getConversations(count=50)
        if convs["count"] == 0:
            await update.message.reply_text("Нет бесед.")
            return

        lines = []
        for conv in convs["items"]:
            peer_id = conv["conversation"]["peer"]["id"]
            title = conv["conversation"].get("chat_settings", {}).get("title")
            if not title:
                if peer_id > 2000000000:
                    title = f"Беседа {peer_id}"
                else:
                    try:
                        u = vk_obj.users.get(user_ids=peer_id)[0]
                        title = f"{u.get('first_name')} {u.get('last_name')} (id:{peer_id})"
                    except:
                        title = f"Пользователь {peer_id}"
            lines.append(f"{peer_id} — {title}")
        await update.message.reply_text("\n".join(lines))

    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

async def cmd_read(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context):
        return
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("Использование: /read USER PEER [PAGE]")
        return

    user_key = args[0].upper()
    peer_id = args[1]
    page = int(args[2]) if len(args) > 2 else 1

    if user_key not in USERS_CONFIG:
        await update.message.reply_text("Пользователь не найден.")
        return
    try:
        peer_id = int(peer_id)
    except:
        await update.message.reply_text("peer_id должен быть числом.")
        return
    if page < 1:
        page = 1

    vk_obj = vk_api_objects.get(user_key)
    if not vk_obj:
        await update.message.reply_text("Бот не запущен.")
        return

    count = 20
    offset = (page - 1) * count
    try:
        msgs = vk_obj.messages.getHistory(peer_id=peer_id, count=count, offset=offset)
        items = msgs.get("items", [])
        if not items:
            await update.message.reply_text("Сообщений нет.")
            return

        user_ids = list({m.get("from_id") for m in items if m.get("from_id") > 0})

        users_info = {}
        if user_ids:
            response = vk_obj.users.get(user_ids=','.join(map(str, user_ids)))
            for u in response:
                users_info[u['id']] = f"{u['first_name']} {u['last_name']}"

        text_lines = []
        for m in items:
            dt = m.get("date")
            dt_str = datetime.fromtimestamp(dt).strftime('%Y-%m-%d %H:%M:%S')
            from_id = m.get("from_id")
            text = m.get("text", "")
            out = m.get("out")
            direction = "->" if out == 1 else "<-"
            name = users_info.get(from_id, "Unknown")

            text_lines.append(f"[{dt_str}] {from_id} ({name}) {direction}: {text}")

        reply_text = f"Сообщения страницы {page} (peer_id={peer_id}):\n" + "\n".join(text_lines)
        await update.message.reply_text(reply_text)
    except Exception as e:
        await update.message.reply_text(f"Ошибка получения истории: {e}")

async def cmd_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context):
        return
    args = context.args
    if len(args) < 3:
        await update.message.reply_text("Использование: /send USER PEER TEXT")
        return

    user_key = args[0].upper()
    try:
        peer_id = int(args[1])
    except:
        await update.message.reply_text("PEER должен быть числом.")
        return

    text = " ".join(args[2:])
    if user_key not in USERS_CONFIG:
        await update.message.reply_text("Пользователь не найден.")
        return

    vk_obj = vk_api_objects.get(user_key)
    if not vk_obj:
        await update.message.reply_text("Бот не запущен.")
        return

    try:
        vk_obj.messages.send(peer_id=peer_id, message=text, random_id=0)
        await update.message.reply_text(f"Сообщение отправлено в {peer_id} от {user_key}.")
    except Exception as e:
        await update.message.reply_text(f"Ошибка отправки сообщения: {e}")

async def cmd_lastmsgs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context): return
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("Использование: /lastmsgs USER PEER [COUNT]")
        return
    user_key = args[0].upper()
    peer_id = args[1]
    count = int(args[2]) if len(args) > 2 else 20

    if user_key not in USERS_CONFIG:
        await update.message.reply_text("Пользователь не найден.")
        return
    try:
        peer_id = int(peer_id)
    except:
        await update.message.reply_text("peer_id должен быть числом.")
        return
    if count < 1 or count > 100:
        count = 20

    vk_obj = vk_api_objects.get(user_key)
    if not vk_obj:
        await update.message.reply_text("Бот не запущен.")
        return

    try:
        msgs = vk_obj.messages.getHistory(peer_id=peer_id, count=count)
        items = msgs.get("items", [])
        if not items:
            await update.message.reply_text("Сообщений нет.")
            return

        text_lines = []
        for m in reversed(items):
            from_id = m.get("from_id")
            out = m.get("out")
            text = m.get("text", "")
            direction = "->" if out == 1 else "<-"
            text_lines.append(f"[{from_id} {direction}] {text}")

        reply_text = f"Последние {len(text_lines)} сообщений (peer_id={peer_id}):\n" + "\n".join(text_lines)
        await update.message.reply_text(reply_text)
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

async def cmd_autorespond(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context): return
    args = context.args
    if len(args) != 2:
        await update.message.reply_text("Использование: /autorespond USER ON|OFF")
        return
    user_key = args[0].upper()
    action = args[1].lower()

    if user_key not in USERS_CONFIG:
        await update.message.reply_text("Пользователь не найден.")
        return
    if action not in ("on", "off"):
        await update.message.reply_text("Используйте ON или OFF")
        return

    auto_reply_status[user_key] = (action == "on")
    await update.message.reply_text(f"Автоответ для {user_key} {'включен' if action == 'on' else 'выключен'}.")

async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context): return
    args = context.args
    if len(args) != 1:
        await update.message.reply_text("Использование: /stop USER")
        return
    user_key = args[0].upper()
    if user_key not in USERS_CONFIG:
        await update.message.reply_text("Пользователь не найден.")
        return
    bots_running[user_key] = False
    await update.message.reply_text(f"Остановка бота {user_key} запрошена.")

async def cmd_startbot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context): return
    args = context.args
    if len(args) != 1:
        await update.message.reply_text("Использование: /startbot USER")
        return
    user_key = args[0].upper()
    if user_key not in USERS_CONFIG:
        await update.message.reply_text("Пользователь не найден.")
        return
    if bots_running.get(user_key):
        await update.message.reply_text(f"Бот {user_key} уже запущен.")
        return

    t = threading.Thread(target=run_vk_bot, args=(user_key, USERS_CONFIG[user_key]), daemon=True)
    t.start()
    await update.message.reply_text(f"Бот {user_key} запущен.")

async def cmd_userinfo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context): return
    args = context.args
    if len(args) != 1:
        await update.message.reply_text("Использование: /userinfo USERID")
        return
    try:
        vk_id = int(args[0])
    except:
        await update.message.reply_text("Неверный VK ID.")
        return

    for user_key, vk_obj in vk_api_objects.items():
        try:
            info = vk_obj.users.get(user_ids=vk_id)[0]
            name = f"{info.get('first_name')} {info.get('last_name')}"
            await update.message.reply_text(f"Пользователь VK {vk_id}:\n{name}")
            return
        except:
            continue
    await update.message.reply_text("Пользователь не найден или ошибка VK API.")

async def cmd_download(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context): return
    args = context.args
    if len(args) != 2:
        await update.message.reply_text("Использование: /download USER PEER")
        return
    user_key = args[0].upper()
    try:
        peer_id = int(args[1])
    except:
        await update.message.reply_text("peer_id должен быть числом.")
        return
    filename = f"logs/{user_key}_logs.json"
    if not os.path.exists(filename):
        await update.message.reply_text("Логи не найдены.")
        return
    with open(filename, "r", encoding="utf-8") as f:
        data = json.load(f)
    filtered = [m for m in data if m.get("peer_id") == peer_id]
    if not filtered:
        await update.message.reply_text("Сообщений для этого диалога нет.")
        return
    out_filename = f"logs/{user_key}_{peer_id}_history.json"
    with open(out_filename, "w", encoding="utf-8") as f:
        json.dump(filtered, f, ensure_ascii=False, indent=2)
    with open(out_filename, "rb") as f:
        await update.message.reply_document(f, filename=f"{user_key}_{peer_id}_history.json")

async def cmd_allowedusers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context): return
    args = context.args
    if len(args) != 1:
        await update.message.reply_text("Использование: /allowedusers USER")
        return
    user_key = args[0].upper()
    if user_key not in USERS_CONFIG:
        await update.message.reply_text("Пользователь не найден.")
        return
    allowed = USERS_CONFIG[user_key]["ALLOWED_USER_IDS"]
    if not allowed:
        await update.message.reply_text("Список пуст.")
        return
    await update.message.reply_text("\n".join(str(u) for u in allowed))

async def cmd_allowedconfs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context): return
    args = context.args
    if len(args) != 1:
        await update.message.reply_text("Использование: /allowedconfs USER")
        return
    user_key = args[0].upper()
    if user_key not in USERS_CONFIG:
        await update.message.reply_text("Пользователь не найден.")
        return
    allowed = USERS_CONFIG[user_key]["ALLOWED_CONFS"]
    if not allowed:
        await update.message.reply_text("Список пуст.")
        return
    await update.message.reply_text("\n".join(str(c) for c in allowed))

async def cmd_addalloweduser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context): return
    args = context.args
    if len(args) != 2:
        await update.message.reply_text("Использование: /addalloweduser USER VKID")
        return
    user_key = args[0].upper()
    try:
        vkid = int(args[1])
    except:
        await update.message.reply_text("VKID должен быть числом.")
        return
    if user_key not in USERS_CONFIG:
        await update.message.reply_text("Пользователь не найден.")
        return
    if vkid in USERS_CONFIG[user_key]["ALLOWED_USER_IDS"]:
        await update.message.reply_text("Уже в списке.")
        return
    USERS_CONFIG[user_key]["ALLOWED_USER_IDS"].append(vkid)
    await update.message.reply_text(f"Добавлен {vkid} в разрешённые пользователи {user_key}.")

async def cmd_removealloweduser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context): return
    args = context.args
    if len(args) != 2:
        await update.message.reply_text("Использование: /removealloweduser USER VKID")
        return
    user_key = args[0].upper()
    try:
        vkid = int(args[1])
    except:
        await update.message.reply_text("VKID должен быть числом.")
        return
    if user_key not in USERS_CONFIG:
        await update.message.reply_text("Пользователь не найден.")
        return
    if vkid not in USERS_CONFIG[user_key]["ALLOWED_USER_IDS"]:
        await update.message.reply_text("В списке нет такого пользователя.")
        return
    USERS_CONFIG[user_key]["ALLOWED_USER_IDS"].remove(vkid)
    await update.message.reply_text(f"Удалён {vkid} из разрешённых пользователей {user_key}.")

async def cmd_addallowedconf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context): return
    args = context.args
    if len(args) != 2:
        await update.message.reply_text("Использование: /addallowedconf USER PEER")
        return
    user_key = args[0].upper()
    try:
        peer_id = int(args[1])
    except:
        await update.message.reply_text("peer_id должен быть числом.")
        return
    if user_key not in USERS_CONFIG:
        await update.message.reply_text("Пользователь не найден.")
        return
    if peer_id in USERS_CONFIG[user_key]["ALLOWED_CONFS"]:
        await update.message.reply_text("Уже в списке.")
        return
    USERS_CONFIG[user_key]["ALLOWED_CONFS"].append(peer_id)
    await update.message.reply_text(f"Добавлен {peer_id} в разрешённые беседы {user_key}.")

async def cmd_removeallowedconf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context): return
    args = context.args
    if len(args) != 2:
        await update.message.reply_text("Использование: /removeallowedconf USER PEER")
        return
    user_key = args[0].upper()
    try:
        peer_id = int(args[1])
    except:
        await update.message.reply_text("peer_id должен быть числом.")
        return
    if user_key not in USERS_CONFIG:
        await update.message.reply_text("Пользователь не найден.")
        return
    if peer_id not in USERS_CONFIG[user_key]["ALLOWED_CONFS"]:
        await update.message.reply_text("В списке нет такого чата.")
        return
    USERS_CONFIG[user_key]["ALLOWED_CONFS"].remove(peer_id)
    await update.message.reply_text(f"Удалён {peer_id} из разрешённых бесед {user_key}.")


# ============ ЗАПУСК ===============
def main():
    # Запускаем VK ботов в отдельных потоках, так как они блокирующие
    for user_key, config in USERS_CONFIG.items():
        thread = threading.Thread(target=run_vk_bot, args=(user_key, config), daemon=True)
        thread.start()

    # Запускаем Telegram-бота в основном потоке с помощью Application.run_polling
    application = Application.builder().token(TG_BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("users", cmd_users))
    application.add_handler(CommandHandler("dialogs", cmd_dialogs))
    application.add_handler(CommandHandler("read", cmd_read))
    application.add_handler(CommandHandler("send", cmd_send))
    application.add_handler(CommandHandler("lastmsgs", cmd_lastmsgs))
    application.add_handler(CommandHandler("autorespond", cmd_autorespond))
    application.add_handler(CommandHandler("stop", cmd_stop))
    application.add_handler(CommandHandler("startbot", cmd_startbot))
    application.add_handler(CommandHandler("userinfo", cmd_userinfo))
    application.add_handler(CommandHandler("download", cmd_download))
    application.add_handler(CommandHandler("allowedusers", cmd_allowedusers))
    application.add_handler(CommandHandler("allowedconfs", cmd_allowedconfs))
    application.add_handler(CommandHandler("addalloweduser", cmd_addalloweduser))
    application.add_handler(CommandHandler("removealloweduser", cmd_removealloweduser))
    application.add_handler(CommandHandler("addallowedconf", cmd_addallowedconf))
    application.add_handler(CommandHandler("removeallowedconf", cmd_removeallowedconf))

    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()