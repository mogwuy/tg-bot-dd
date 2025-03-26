import logging
import json
import sqlite3
from collections import defaultdict
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

# Настройка логирования:
# Определяем формат логов и уровень важности сообщений (INFO и выше)
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# Подключение к базе данных SQLite:
# Создаём соединение с файлом базы данных "bot_db.sqlite". Параметр check_same_thread=False позволяет использовать соединение в разных потоках.
conn = sqlite3.connect("bot_db.sqlite", check_same_thread=False)
# Переключаем режим журнала (WAL) для улучшения производительности и надежности
conn.execute("PRAGMA journal_mode = WAL")
cursor = conn.cursor()

# Попытка добавить новый столбец "hidden" в таблицу breakdowns, если он отсутствует.
# Если столбец уже существует, возникает исключение, которое мы игнорируем.
try:
    cursor.execute("ALTER TABLE breakdowns ADD COLUMN hidden INTEGER DEFAULT 0")
    conn.commit()
except Exception:
    pass

# Создание таблиц, если они ещё не созданы:
# Таблица "users" для хранения информации о пользователях бота.
# Таблица "admins" для хранения информации об администраторах.
# Таблица "orders" для хранения заказов пользователей.
# Таблица "breakdowns" для хранения доступных разбивок (наборов товаров).
# Таблица "items" для хранения информации о товарах, входящих в разбивки.
# Таблица "messages" для хранения сообщений пользователей.
# Таблица "breakdown_instances" для хранения экземпляров разбивок и их статусов.
cursor.executescript("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    phone_number TEXT
);

CREATE TABLE IF NOT EXISTS admins (
    user_id INTEGER PRIMARY KEY,
    username TEXT
);

CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    breakdown_name TEXT NOT NULL,
    items TEXT,
    total_amount REAL,
    instance_id INTEGER,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
);

CREATE TABLE IF NOT EXISTS breakdowns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    hidden INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    breakdown_name TEXT NOT NULL,
    item_name TEXT NOT NULL,
    price REAL NOT NULL,
    FOREIGN KEY(breakdown_name) REFERENCES breakdowns(name)
);

CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    message TEXT NOT NULL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS breakdown_instances (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    breakdown_name TEXT NOT NULL,
    status TEXT DEFAULT 'open'
);
""")
conn.commit()

def save_user(user) -> None:
    """
    Сохраняет пользователя в таблице users, если его там еще нет.
    При отсутствии username используется значение "Без имени".
    """
    cursor.execute(
        "INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)",
        (user.id, user.username or "Без имени")
    )
    conn.commit()

def is_admin(user_id: int) -> bool:
    """
    Проверяет, является ли пользователь администратором.
    Если user_id равен заданному числу (жестко закодированному), возвращает True.
    Иначе выполняет запрос в таблицу admins.
    Сюда можно добавить перманентного администратора.
    """
    if user_id == 1244636103:
        return True
    cursor.execute("SELECT 1 FROM admins WHERE user_id = ?", (user_id,))
    return cursor.fetchone() is not None

# Функция start - обрабатывает команду /start и выводит главное меню.
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Сохраняем данные пользователя
    save_user(update.message.from_user)
    user_id = update.message.from_user.id
    # Формируем клавиатуру главного меню с кнопками для различных опций
    keyboard = [
        [InlineKeyboardButton("📂 Актуальные Разбивки", callback_data="actual_breakdowns")],
        [InlineKeyboardButton("🛒 Купить с ТаоБао", callback_data="buy_from_taobao")],
        [InlineKeyboardButton("👤 Личный Кабинет", callback_data="personal_account")]
    ]
    # Если пользователь является администратором, добавляем кнопку для администрирования
    if is_admin(user_id):
        keyboard.append([InlineKeyboardButton("⚙️ Администрирование", callback_data="admin_panel")])
    await update.message.reply_text("Привет! Выберите опцию:", reply_markup=InlineKeyboardMarkup(keyboard))

# Функция button - обработчик нажатий на inline-кнопки.
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    # Сохраняем данные пользователя, инициировавшего CallbackQuery
    save_user(query.from_user)
    # Отправляем ответ, чтобы убрать "часики" на кнопке
    await query.answer()
    data = query.data
    logger.info("Callback data: %s", data)

    # Обработка запроса на показ актуальных разбивок
    if data == "actual_breakdowns":
        # Извлекаем все разбивки, где hidden = 0 (не скрыты)
        cursor.execute("SELECT name FROM breakdowns WHERE hidden = 0")
        breakdowns = cursor.fetchall()
        if breakdowns:
            # Формируем клавиатуру с кнопками для каждой разбивки
            keyboard = [[InlineKeyboardButton(b[0], callback_data=f"breakdown_{b[0]}")] for b in breakdowns]
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")])
            await query.edit_message_text("Выберите разбивку:", reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
            await query.edit_message_text("🚫 Нет доступных разбивок.", reply_markup=InlineKeyboardMarkup(keyboard))

    # Обработка выбора конкретной разбивки
    elif data.startswith("breakdown_"):
        # Извлекаем название разбивки из данных callback
        breakdown_name = data.split("_", 1)[1]
        context.user_data["current_breakdown"] = breakdown_name
        # Получаем товары для выбранной разбивки
        cursor.execute("SELECT item_name, price FROM items WHERE breakdown_name=?", (breakdown_name,))
        items = cursor.fetchall()
        if items:
            # Инициализируем множество выбранных товаров
            context.user_data["selected_items"] = set()
            # Формируем кнопки для выбора товаров. Если товар выбран, перед именем ставится галочка.
            keyboard = [
                [InlineKeyboardButton(
                    f"{'✅ ' if i[0] in context.user_data.get('selected_items', set()) else ''}{i[0]} - {i[1]} руб.",
                    callback_data=f"toggle_item_{i[0]}"
                )] for i in items
            ]
            keyboard.append([InlineKeyboardButton("✅ Готово", callback_data="finish_selection")])
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="actual_breakdowns")])
            await query.edit_message_text("Выберите товары:", reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="actual_breakdowns")]]
            await query.edit_message_text("🚫 В этой разбивке пока нет товаров.", reply_markup=InlineKeyboardMarkup(keyboard))

    # Переключение выбора товара (добавление/удаление из выбранных)
    elif data.startswith("toggle_item_"):
        item_name = data.split("_", 2)[2]
        if item_name in context.user_data.get("selected_items", set()):
            context.user_data["selected_items"].remove(item_name)
        else:
            context.user_data["selected_items"].add(item_name)
        # Обновляем меню товаров с учётом изменений в выборе
        await show_items_menu(query, context)

    # Завершение выбора товаров и оформление заказа
    elif data == "finish_selection":
        if context.user_data.get("selected_items"):
            selected_items = context.user_data["selected_items"]
            breakdown_name = context.user_data["current_breakdown"]
            user_id = query.from_user.id

            # Обеспечиваем, что пользователь есть в таблице users
            cursor.execute("INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)",
                           (user_id, query.from_user.username or "Без имени"))

            items_details = []
            total = 0.0
            # Обрабатываем каждый выбранный товар: получаем цену и суммируем итоговую стоимость
            for item_name in selected_items:
                cursor.execute("SELECT price FROM items WHERE breakdown_name = ? AND item_name = ?",
                               (breakdown_name, item_name))
                result = cursor.fetchone()
                if result:
                    price = result[0]
                    total += price
                    items_details.append({"name": item_name, "price": price})
            # Преобразуем детали заказа в JSON для хранения
            items_json = json.dumps(items_details, ensure_ascii=False)
            # Проверяем наличие открытого экземпляра разбивки
            cursor.execute("SELECT id FROM breakdown_instances WHERE breakdown_name = ? AND status = 'open' LIMIT 1",
                           (breakdown_name,))
            row = cursor.fetchone()
            if row:
                instance_id = row[0]
            else:
                # Если открытого экземпляра нет, создаём новый
                cursor.execute("INSERT INTO breakdown_instances (breakdown_name, status) VALUES (?, 'open')",
                               (breakdown_name,))
                instance_id = cursor.lastrowid
            conn.commit()

            # Проверяем, не были ли уже выбраны данные товары другими пользователями
            unavailable = []
            for item_name in selected_items:
                cursor.execute("SELECT COUNT(*) FROM orders WHERE instance_id = ? AND breakdown_name = ? AND items LIKE ?",
                               (instance_id, breakdown_name, f'%"{item_name}"%'))
                if cursor.fetchone()[0] > 0:
                    unavailable.append(item_name)
            if unavailable:
                message_text = f"❌ Товары {', '.join(unavailable)} уже выбраны. Обновите выбор."
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="actual_breakdowns")]]
                await query.edit_message_text(message_text, reply_markup=InlineKeyboardMarkup(keyboard))
                context.user_data.pop("selected_items", None)
                return

            # Сохраняем заказ в таблице orders
            cursor.execute("INSERT INTO orders (user_id, breakdown_name, items, total_amount, instance_id) VALUES (?, ?, ?, ?, ?)",
                           (user_id, breakdown_name, items_json, total, instance_id))
            conn.commit()

            # Получаем все товары разбивки и собираем список уже занятых позиций
            cursor.execute("SELECT item_name FROM items WHERE breakdown_name = ?", (breakdown_name,))
            all_items = {r[0] for r in cursor.fetchall()}
            cursor.execute("SELECT items FROM orders WHERE instance_id = ?", (instance_id,))
            taken_items = set()
            for order in cursor.fetchall():
                try:
                    for it in json.loads(order[0]):
                        taken_items.add(it['name'])
                except Exception as e:
                    logger.error("❌ Ошибка парсинга JSON: %s", e)
            # Если все позиции разбивки заняты, обновляем статус экземпляра на 'complete'
            if all_items == taken_items:
                cursor.execute("UPDATE breakdown_instances SET status = 'complete' WHERE id = ?", (instance_id,))
                conn.commit()
                # Отправляем уведомление всем пользователям, сделавшим заказ в этом экземпляре
                cursor.execute("SELECT user_id, items, total_amount FROM orders WHERE instance_id = ?", (instance_id,))
                orders_details = cursor.fetchall()
                for user_id, items_json, order_total in orders_details:
                    try:
                        order_items = json.loads(items_json)
                        items_text = "\n".join([f"▪ {it['name']} - {it['price']} руб." for it in order_items])
                    except Exception as e:
                        logger.error("❌ Ошибка парсинга JSON: %s", e)
                        items_text = "🚫 Ошибка отображения"
                    notification_message = (
                        f"✅ Сет разбит!\nРазбивка: {breakdown_name}\nЭкземпляр: {instance_id}\n\n"
                        f"Ваш заказ:\n{items_text}\nСумма: {order_total} руб."
                    )
                    try:
                        await context.bot.send_message(chat_id=user_id, text=notification_message)
                    except Exception as e:
                        logger.error("❌ Ошибка отправки уведомления пользователю %s: %s", user_id, e)

            # Формируем сообщение с деталями заказа для пользователя
            items_list = "\n".join([f"  - {item['name']}: {item['price']} руб." for item in items_details])
            message_text = (
                f"✅ Вы выбрали в разбивке '{breakdown_name}':\n{items_list}\n💰 Общая сумма: {total} руб.\nЭкземпляр: {instance_id}"
            )
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
            await query.edit_message_text(message_text, reply_markup=InlineKeyboardMarkup(keyboard))
            # Очищаем выбранные товары из пользовательских данных
            context.user_data.pop("selected_items", None)
        else:
            # Если ни один товар не выбран, выводим соответствующее сообщение
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
            await query.edit_message_text("🚫 Вы не выбрали ни одного товара.", reply_markup=InlineKeyboardMarkup(keyboard))

    # Обработка запроса "Личный Кабинет"
    elif data == "personal_account":
        user_id = query.from_user.id
        # Получаем заказы пользователя из таблицы orders
        cursor.execute("SELECT breakdown_name, items, total_amount, instance_id FROM orders WHERE user_id = ?", (user_id,))
        orders = cursor.fetchall()
        if not orders:
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
            await query.edit_message_text("🚫 У вас нет активных заказов.", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        total_all = 0.0
        message_lines = ["📁 Ваши заказы:\n"]
        # Формируем список заказов с деталями каждого заказа
        for breakdown_name, items_json, total, instance_id in orders:
            line = f"🔹 Разбивка: {breakdown_name}"
            if instance_id:
                cursor.execute("SELECT status FROM breakdown_instances WHERE id = ?", (instance_id,))
                status = cursor.fetchone()
                if status and status[0] == "complete":
                    line += " (✅Сет разбит)"
            message_lines.append(line)
            try:
                items = json.loads(items_json)
                for item in items:
                    message_lines.append(f"    ▪ {item['name']} - {item['price']} руб.")
            except:
                message_lines.append("    (🚫 ошибка отображения товаров)")
            message_lines.append(f"    Итого: {total} руб.\n")
            total_all += total
        message_lines.append(f"💳 Общая сумма: {total_all} руб.")
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        await query.edit_message_text("\n".join(message_lines), reply_markup=InlineKeyboardMarkup(keyboard))

    # Обработка административного меню
    elif data == "admin_panel":
        keyboard = [
            [InlineKeyboardButton("📂 Разбивки", callback_data="breakdowns_menu")],
            [InlineKeyboardButton("💬 Последние Сообщения", callback_data="view_messages")],
            [InlineKeyboardButton("📊 Отчет", callback_data="instance_users_menu")],
            [InlineKeyboardButton("👤 Управление администраторами", callback_data="admin_management")],
            [InlineKeyboardButton("👥 Показать Пользователей", callback_data="show_users")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
        ]
        await query.edit_message_text("⚙️ Администрирование:", reply_markup=InlineKeyboardMarkup(keyboard))

    # Меню управления разбивками (добавление, скрытие, удаление)
    elif data == "breakdowns_menu":
        keyboard = [
            [InlineKeyboardButton("➕ Добавить Разбивку", callback_data="add_breakdown")],
            [InlineKeyboardButton("➕ Добавить Позицию", callback_data="add_item")],
            [InlineKeyboardButton("🙈 Скрыть Разбивку", callback_data="hide_breakdown_menu")],
            [InlineKeyboardButton("❌ Удалить Разбивку", callback_data="delete_breakdown_menu")],
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")]
        ]
        await query.edit_message_text("📂 Разбивки:", reply_markup=InlineKeyboardMarkup(keyboard))

    # Запрос на добавление новой разбивки:
    elif data == "add_breakdown":
        await query.edit_message_text("➕ Введите название разбивки:")
        # Флаг, сигнализирующий, что бот ожидает ввод названия новой разбивки
        context.user_data["awaiting_breakdown_name"] = True

    # Запрос на добавление нового товара:
    elif data == "add_item":
        # Извлекаем все доступные (не скрытые) разбивки
        cursor.execute("SELECT name FROM breakdowns WHERE hidden = 0")
        breakdowns = cursor.fetchall()
        if breakdowns:
            keyboard = [[InlineKeyboardButton(b[0], callback_data=f"select_breakdown_{b[0]}")] for b in breakdowns]
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="breakdowns_menu")])
            await query.edit_message_text("➕ Выберите разбивку для добавления позиции:", reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="breakdowns_menu")]]
            await query.edit_message_text("🚫 Нет доступных разбивок.", reply_markup=InlineKeyboardMarkup(keyboard))

    # Выбор разбивки для добавления товара:
    elif data.startswith("select_breakdown_"):
        breakdown_name = data.split("_", 2)[2]
        context.user_data["breakdown_name"] = breakdown_name
        await query.edit_message_text("➕ Введите название товара:")
        # Флаг, сигнализирующий, что бот ожидает ввод названия товара
        context.user_data["awaiting_item_name"] = True

    # Меню удаления разбивок:
    elif data == "delete_breakdown_menu":
        cursor.execute("SELECT name, hidden FROM breakdowns")
        breakdowns = cursor.fetchall()
        if breakdowns:
            keyboard = []
            for name, hidden in breakdowns:
                # Отмечаем, если разбивка уже скрыта
                button_text = f"❌ Удалить {'(скрытая) ' if hidden else ''}{name}"
                keyboard.append([InlineKeyboardButton(button_text, callback_data=f"delete_breakdown_{name}")])
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="breakdowns_menu")])
            await query.edit_message_text("❌ Выберите разбивку для удаления:", reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="breakdowns_menu")]]
            await query.edit_message_text("🚫 Нет доступных разбивок для удаления.", reply_markup=InlineKeyboardMarkup(keyboard))

    # Удаление выбранной разбивки и связанных с ней данных:
    elif data.startswith("delete_breakdown_"):
        breakdown_name = data.split("delete_breakdown_", 1)[1]
        cursor.execute("DELETE FROM breakdowns WHERE name = ?", (breakdown_name,))
        cursor.execute("DELETE FROM items WHERE breakdown_name = ?", (breakdown_name,))
        cursor.execute("DELETE FROM orders WHERE breakdown_name = ?", (breakdown_name,))
        cursor.execute("DELETE FROM breakdown_instances WHERE breakdown_name = ?", (breakdown_name,))
        conn.commit()
        await query.edit_message_text(f"✅ Разбивка '{breakdown_name}' и связанные данные удалены.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="breakdowns_menu")]]))

    # Меню скрытия разбивок:
    elif data == "hide_breakdown_menu":
        cursor.execute("SELECT name FROM breakdowns WHERE hidden = 0")
        breakdowns = cursor.fetchall()
        if breakdowns:
            keyboard = [[InlineKeyboardButton(f"🙈 Скрыть {b[0]}", callback_data=f"hide_breakdown_{b[0]}")] for b in breakdowns]
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="breakdowns_menu")])
            await query.edit_message_text("🙈 Выберите разбивку для скрытия:", reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="breakdowns_menu")]]
            await query.edit_message_text("🚫 Нет доступных разбивок для скрытия.", reply_markup=InlineKeyboardMarkup(keyboard))

    # Выполнение скрытия выбранной разбивки:
    elif data.startswith("hide_breakdown_"):
        breakdown_name = data[len("hide_breakdown_"):]
        cursor.execute("UPDATE breakdowns SET hidden = 1 WHERE name = ?", (breakdown_name,))
        conn.commit()
        await query.edit_message_text(f"✅ Разбивка '{breakdown_name}' скрыта.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="breakdowns_menu")]]))

    # Меню отчетов по экземплярам разбивок:
    elif data == "instance_users_menu":
        keyboard = [
            [InlineKeyboardButton("📈 Разбитые разбивки", callback_data="view_full_splits")],
            [InlineKeyboardButton("📋 Все позиции пользователей", callback_data="view_all_positions")],
            [InlineKeyboardButton("🧾 Чек пользователей", callback_data="view_user_checks")],
            [InlineKeyboardButton("❌ Удалить позицию пользователя", callback_data="delete_position_menu")],
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")]
        ]
        await query.edit_message_text("📊 Отчет:", reply_markup=InlineKeyboardMarkup(keyboard))

    # Отчет по полностью разбитым наборам:
    elif data == "view_full_splits":
        cursor.execute("""
            SELECT bi.id, bi.breakdown_name, bi.status, o.items, u.username
            FROM breakdown_instances bi
            JOIN orders o ON bi.id = o.instance_id
            JOIN users u ON o.user_id = u.user_id
            WHERE bi.status = 'complete'
        """)
        rows = cursor.fetchall()
        if rows:
            grouped = defaultdict(list)
            instance_info = {}
            # Группируем заказы по экземпляру разбивки
            for instance_id, breakdown_name, status, items_json, username in rows:
                grouped[instance_id].append((username, items_json))
                instance_info[instance_id] = (breakdown_name, status)
            text_lines = []
            # Формируем текст отчета для каждого экземпляра
            for instance_id in sorted(grouped.keys()):
                breakdown_name, status = instance_info[instance_id]
                text_lines.append(f"Экземпляр: {instance_id}\nРазбивка: {breakdown_name} (Статус: {status})")
                for username, items_json in grouped[instance_id]:
                    try:
                        order_items = json.loads(items_json)
                        items_text = ", ".join([f"{it['name']} - {it['price']} руб." for it in order_items])
                    except Exception as e:
                        logger.error("❌ Ошибка парсинга JSON: %s", e)
                        items_text = "🚫 Ошибка отображения"
                    text_lines.append(f"Заказ от @{username}: {items_text}")
                text_lines.append("-" * 40)
            text = "\n".join(text_lines)
        else:
            text = "🚫 Нет разбивок, где все позиции заняты."
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="instance_users_menu")]]
        await query.edit_message_text(text=f"<pre>{text}</pre>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))

    # Отчет по всем позициям товаров для каждого экземпляра:
    elif data == "view_all_positions":
        cursor.execute("SELECT id, breakdown_name FROM breakdown_instances")
        instances = cursor.fetchall()
        all_rows = []
        # Для каждого экземпляра получаем товары разбивки и статусы позиций (занято/свободно)
        for instance_id, breakdown_name in instances:
            cursor.execute("SELECT item_name, price FROM items WHERE breakdown_name = ?", (breakdown_name,))
            items_list = cursor.fetchall()
            cursor.execute("SELECT o.user_id, o.items FROM orders o WHERE o.instance_id = ?", (instance_id,))
            orders = cursor.fetchall()
            taken = {}
            # Определяем, какие товары уже взяты, и кем
            for user_id, items_json in orders:
                try:
                    order_items = json.loads(items_json)
                    cursor.execute("SELECT username FROM users WHERE user_id = ?", (user_id,))
                    res = cursor.fetchone()
                    taken_username = "@" + (res[0] if res else str(user_id))
                    for it in order_items:
                        taken[it['name']] = taken_username
                except Exception as e:
                    logger.error("❌ Ошибка парсинга JSON: %s", e)
            # Формируем список всех позиций с их статусом
            for item_name, price in items_list:
                status = taken.get(item_name, "Свободно")
                all_rows.append((instance_id, breakdown_name, item_name, price, status))
        if all_rows:
            grouped = defaultdict(list)
            for instance_id, breakdown_name, item_name, price, status in all_rows:
                grouped[instance_id].append((breakdown_name, item_name, price, status))
            lines = ["Экземпляр | Разбивка | Позиция | Цена | Статус"]
            for instance_id in sorted(grouped.keys()):
                breakdown_name = grouped[instance_id][0][0]
                for row in grouped[instance_id]:
                    _, item_name, price, status = row
                    lines.append(f"{instance_id} | {breakdown_name} | {item_name} | {price} | {status}")
                lines.append("_" * 40)
            text = "\n".join(lines)
        else:
            text = "🚫 Нет данных о позициях."
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="instance_users_menu")]]
        await query.edit_message_text(text=f"<pre>{text}</pre>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))

    # Отчет по чекам пользователей:
    elif data == "view_user_checks":
        cursor.execute("""
            SELECT o.breakdown_name, o.items, o.total_amount, u.username
            FROM orders o
            JOIN users u ON o.user_id = u.user_id
            WHERE o.instance_id IN (SELECT id FROM breakdown_instances WHERE status = 'complete')
        """)
        orders_data = cursor.fetchall()
        if orders_data:
            grouped = defaultdict(list)
            # Группируем заказы по пользователям
            for breakdown_name, items_json, order_total, username in orders_data:
                grouped[username].append((breakdown_name, items_json, order_total))
            lines = []
            for username, orders_list in grouped.items():
                lines.append(f"==== Заказы от @{username} ====")
                for breakdown_name, items_json, order_total in orders_list:
                    try:
                        order_items = json.loads(items_json)
                        items_text = "\n".join([f"▪ {it['name']} - {it['price']} руб." for it in order_items])
                    except Exception as e:
                        logger.error("❌ Ошибка парсинга JSON: %s", e)
                        items_text = "🚫 Ошибка отображения"
                    lines.append(f"Разбивка: {breakdown_name}\n{items_text}\nСумма: {order_total} руб.\n")
                lines.append("-" * 40)
            text = "\n".join(lines)
        else:
            text = "🚫 Нет чеков для пользователей."
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="instance_users_menu")]]
        await query.edit_message_text(text=text, reply_markup=InlineKeyboardMarkup(keyboard))

    # Меню управления администраторами:
    elif data == "admin_management":
        keyboard = [
            [InlineKeyboardButton("👤➕ Добавить администратора", callback_data="add_admin")],
            [InlineKeyboardButton("👤❌ Удалить администратора", callback_data="delete_admin_menu")],
            [InlineKeyboardButton("👤📊 Показать администраторов", callback_data="show_admins")],
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")]
        ]
        await query.edit_message_text("👤 Управление администраторами:", reply_markup=InlineKeyboardMarkup(keyboard))

    # Добавление администратора:
    elif data == "add_admin":
        # Запрашиваем ID нового администратора с возможностью возврата назад
        await query.edit_message_text("➕ Введите ID нового администратора:", 
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="admin_management")]]))
        context.user_data["awaiting_admin"] = True

    # Меню удаления администратора:
    elif data == "delete_admin_menu":
        cursor.execute("SELECT user_id, username FROM admins")
        admins = cursor.fetchall()
        if admins:
            keyboard = [[InlineKeyboardButton(f"👤❌ Удалить {username} (ID:{user_id})", callback_data=f"delete_admin_{user_id}")]
                        for user_id, username in admins]
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_management")])
            await query.edit_message_text("❌ Выберите администратора для удаления:", reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_management")]]
            await query.edit_message_text("🚫 Нет дополнительных администраторов для удаления.", reply_markup=InlineKeyboardMarkup(keyboard))

    # Удаление администратора:
    elif data.startswith("delete_admin_"):
        admin_id = int(data.split("delete_admin_")[1])
        cursor.execute("DELETE FROM admins WHERE user_id = ?", (admin_id,))
        conn.commit()
        await query.edit_message_text(f"✅ Администратор с ID {admin_id} удалён.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="admin_management")]]))

    # Показ списка администраторов:
    elif data == "show_admins":
        cursor.execute("SELECT user_id, username FROM admins")
        admins = cursor.fetchall()
        if admins:
            text_lines = [f"@{username} (ID:{user_id})" for user_id, username in admins]
            text = "\n".join(text_lines)
        else:
            text = "🚫 Нет администраторов."
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_management")]]
        await query.edit_message_text(f"👤 Администраторы:\n\n{text}", reply_markup=InlineKeyboardMarkup(keyboard))

    # Показ списка пользователей:
    elif data == "show_users":
        cursor.execute("SELECT user_id, username FROM users")
        users = cursor.fetchall()
        if users:
            lines = [f"ID: {uid} - @{username}" for uid, username in users]
            text = "\n".join(lines)
        else:
            text = "🚫 Нет пользователей."
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")]]
        await query.edit_message_text(text=f"👥 Пользователи:\n\n{text}", reply_markup=InlineKeyboardMarkup(keyboard))

    # Меню для удаления позиции в заказе пользователя:
    elif data == "delete_position_menu":
        cursor.execute("SELECT order_id, user_id, breakdown_name, items FROM orders")
        orders = cursor.fetchall()
        if orders:
            keyboard = []
            for order in orders:
                order_id, user_id, breakdown_name, items_json = order
                try:
                    items_list = json.loads(items_json)
                except Exception as e:
                    logger.error("❌ Ошибка парсинга JSON: %s", e)
                    continue
                if not items_list:
                    continue
                cursor.execute("SELECT username FROM users WHERE user_id = ?", (user_id,))
                res = cursor.fetchone()
                username = res[0] if res else str(user_id)
                button_text = f"Заказ #{order_id}: {breakdown_name} - @{username}"
                keyboard.append([InlineKeyboardButton(button_text, callback_data=f"select_order_{order_id}")])
            if not keyboard:
                keyboard = [[InlineKeyboardButton("🚫 Нет заказов для удаления позиций", callback_data="admin_panel")]]
            else:
                keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="instance_users_menu")])
            await query.edit_message_text("❌ Выберите заказ для удаления позиции пользователя:", reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="instance_users_menu")]]
            await query.edit_message_text("🚫 Нет заказов для удаления позиций.", reply_markup=InlineKeyboardMarkup(keyboard))

    # Выбор конкретного заказа для удаления позиции:
    elif data.startswith("select_order_"):
        order_id_str = data.split("select_order_", 1)[1]
        try:
            order_id = int(order_id_str)
        except:
            await query.edit_message_text("🚫 Некорректный номер заказа.",
                                          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="delete_position_menu")]]))
            return
        cursor.execute("SELECT items FROM orders WHERE order_id = ?", (order_id,))
        result = cursor.fetchone()
        if result is None:
            await query.edit_message_text("🚫 Заказ не найден.",
                                          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="delete_position_menu")]]))
            return
        try:
            items_list = json.loads(result[0])
        except Exception as e:
            logger.error("❌ Ошибка парсинга JSON: %s", e)
            items_list = []
        if not items_list:
            await query.edit_message_text("🚫 В этом заказе нет позиций для удаления.",
                                          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="delete_position_menu")]]))
            return
        keyboard = []
        # Создаём кнопки для удаления каждой позиции в заказе
        for item in items_list:
            item_name = item.get("name")
            price = item.get("price")
            button_text = f"❌ Удалить {item_name} ({price} руб.)"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"delete_item_{order_id}_{item_name}")])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="delete_position_menu")])
        await query.edit_message_text(f"Заказ #{order_id}. Выберите позицию для удаления:", reply_markup=InlineKeyboardMarkup(keyboard))

    # Удаление выбранной позиции в заказе:
    elif data.startswith("delete_item_"):
        parts = data.split("_", 3)
        if len(parts) < 4:
            await query.edit_message_text("🚫 Некорректные данные для удаления.",
                                          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="delete_position_menu")]]))
            return
        try:
            order_id = int(parts[2])
        except:
            await query.edit_message_text("🚫 Некорректный номер заказа.",
                                          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="delete_position_menu")]]))
            return
        item_name = parts[3]
        cursor.execute("SELECT items, total_amount, breakdown_name, instance_id FROM orders WHERE order_id = ?", (order_id,))
        order_data = cursor.fetchone()
        if not order_data:
            await query.edit_message_text("🚫 Заказ не найден.",
                                          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="delete_position_menu")]]))
            return
        items_json, total_amount, breakdown_name, instance_id = order_data
        try:
            items_list = json.loads(items_json)
        except Exception as e:
            logger.error("❌ Ошибка парсинга JSON: %s", e)
            items_list = []
        new_items = []
        removed = False
        removed_price = 0
        # Проходим по списку товаров и исключаем выбранную для удаления позицию
        for item in items_list:
            if not removed and item.get("name") == item_name:
                removed = True
                removed_price = item.get("price", 0)
            else:
                new_items.append(item)
        if not removed:
            await query.edit_message_text("🚫 Позиция не найдена в заказе.",
                                          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="delete_position_menu")]]))
            return
        new_total = total_amount - removed_price
        # Если после удаления товаров заказ пустой, удаляем заказ целиком
        if not new_items:
            cursor.execute("DELETE FROM orders WHERE order_id = ?", (order_id,))
            message_text = f"✅ Позиция '{item_name}' удалена, заказ #{order_id} удалён."
        else:
            new_items_json = json.dumps(new_items, ensure_ascii=False)
            cursor.execute("UPDATE orders SET items = ?, total_amount = ? WHERE order_id = ?", (new_items_json, new_total, order_id))
            message_text = f"✅ Позиция '{item_name}' удалена из заказа #{order_id}. Новый итог: {new_total} руб."
        # Если заказ принадлежит экземпляру разбивки, изменяем его статус на "open"
        if instance_id is not None:
            cursor.execute("UPDATE breakdown_instances SET status = 'open' WHERE id = ?", (instance_id,))
        conn.commit()
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="delete_position_menu")]]
        await query.edit_message_text(message_text, reply_markup=InlineKeyboardMarkup(keyboard))

    # Просмотр последних сообщений пользователей:
    elif data == "view_messages":
        cursor.execute("""
            SELECT messages.id, COALESCE(users.username, 'Неизвестно') as username, messages.message, messages.timestamp 
            FROM messages 
            LEFT JOIN users ON messages.user_id = users.user_id 
            ORDER BY messages.timestamp DESC 
            LIMIT 10
        """)
        msgs = cursor.fetchall()
        if msgs:
            text_lines = []
            keyboard = []
            # Формируем текст и кнопки для удаления каждого сообщения
            for msg_id, username, message_text, timestamp in msgs:
                text_lines.append(f"ID:{msg_id} | @{username}\n{message_text}\n🕒 {timestamp}")
                keyboard.append([InlineKeyboardButton(f"❌ Удалить ID:{msg_id}", callback_data=f"delete_message_{msg_id}")])
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")])
            full_text = "\n\n".join(text_lines)
            await query.edit_message_text(f"💬 Последние сообщения:\n\n{full_text}", reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")]]
            await query.edit_message_text("🚫 Нет сообщений.", reply_markup=InlineKeyboardMarkup(keyboard))

    # Удаление выбранного сообщения:
    elif data.startswith("delete_message_"):
        msg_id = data.split("delete_message_")[1]
        cursor.execute("DELETE FROM messages WHERE id = ?", (msg_id,))
        conn.commit()
        await query.edit_message_text(f"✅ Сообщение с ID {msg_id} удалено.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="view_messages")]]))

    # Обработка запроса "Купить с ТаоБао":
    elif data == "buy_from_taobao":
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        await query.edit_message_text("🛒 Введите ссылку на товар с ТаоБао:", reply_markup=InlineKeyboardMarkup(keyboard))
        # Устанавливаем флаг, чтобы в будущем обработать текстовое сообщение как ссылку с ТаоБао
        context.user_data["awaiting_taobao_message"] = True

    # Возврат к главному меню:
    elif data == "back_to_main":
        user_id = query.from_user.id
        keyboard = [
            [InlineKeyboardButton("📂 Актуальные Разбивки", callback_data="actual_breakdowns")],
            [InlineKeyboardButton("🛒 Купить с ТаоБао", callback_data="buy_from_taobao")],
            [InlineKeyboardButton("👤 Личный Кабинет", callback_data="personal_account")]
        ]
        if is_admin(user_id):
            keyboard.append([InlineKeyboardButton("⚙️ Администрирование", callback_data="admin_panel")])
        await query.edit_message_text("Привет! Выберите опцию:", reply_markup=InlineKeyboardMarkup(keyboard))

    # Обработка неизвестных команд:
    else:
        await query.edit_message_text("🚫 Неизвестная команда. Попробуйте снова.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]))

# Функция для обновления меню выбора товаров (вызывается после изменения выбранного товара)
async def show_items_menu(query, context):
    breakdown_name = context.user_data.get("current_breakdown")
    cursor.execute("SELECT item_name, price FROM items WHERE breakdown_name=?", (breakdown_name,))
    items = cursor.fetchall()
    if items:
        keyboard = [
            [InlineKeyboardButton(f"{'✅ ' if i[0] in context.user_data.get('selected_items', set()) else ''}{i[0]} - {i[1]} руб.",
                                  callback_data=f"toggle_item_{i[0]}")]
            for i in items
        ]
        keyboard.append([InlineKeyboardButton("✅ Готово", callback_data="finish_selection")])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="actual_breakdowns")])
        await query.edit_message_text("Выберите товары:", reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="actual_breakdowns")]]
        await query.edit_message_text("🚫 В этой разбивке пока нет товаров.", reply_markup=InlineKeyboardMarkup(keyboard))

# Функция для обработки текстовых сообщений от пользователя, объединяющая разные случаи ввода
async def handle_combined_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Сохраняем пользователя
    save_user(update.message.from_user)
    # Обработка ввода названия новой разбивки
    if context.user_data.get("awaiting_breakdown_name"):
        breakdown_name = update.message.text
        try:
            cursor.execute("INSERT INTO breakdowns (name) VALUES (?)", (breakdown_name,))
            conn.commit()
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="breakdowns_menu")]]
            await update.message.reply_text(f"✅ Разбивка '{breakdown_name}' добавлена", reply_markup=InlineKeyboardMarkup(keyboard))
        except sqlite3.IntegrityError:
            await update.message.reply_text("❌ Такая разбивка уже существует")
        context.user_data.clear()

    # Обработка ввода названия товара для выбранной разбивки
    elif context.user_data.get("awaiting_item_name"):
        context.user_data["item_name"] = update.message.text
        await update.message.reply_text("➕ Введите цену товара:")
        context.user_data["awaiting_item_price"] = True
        context.user_data["awaiting_item_name"] = False

    # Обработка ввода цены товара и добавление товара в базу данных
    elif context.user_data.get("awaiting_item_price"):
        try:
            price = float(update.message.text.replace(",", "."))
            cursor.execute("INSERT INTO items (breakdown_name, item_name, price) VALUES (?, ?, ?)",
                           (context.user_data["breakdown_name"], context.user_data["item_name"], price))
            conn.commit()
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="breakdowns_menu")]]
            await update.message.reply_text("✅ Товар успешно добавлен", reply_markup=InlineKeyboardMarkup(keyboard))
        except ValueError:
            await update.message.reply_text("❌ Некорректная цена")
        context.user_data.clear()

    # Обработка ссылки с ТаоБао от пользователя
    elif context.user_data.get("awaiting_taobao_message"):
        user_id = update.message.from_user.id
        username = update.message.from_user.username or "Без имени"
        # Сохраняем сообщение в таблице messages
        cursor.execute("INSERT INTO messages (user_id, message) VALUES (?, ?)", (user_id, update.message.text))
        conn.commit()
        # Обеспечиваем наличие пользователя в таблице users
        cursor.execute("INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)",
                       (user_id, update.message.from_user.username or "Без имени"))
        # Извлекаем список администраторов для уведомления
        cursor.execute("SELECT user_id FROM admins")
        admin_ids = cursor.fetchall()
        if not admin_ids:
            admin_ids = [(1244636103,)]
        # Отправляем уведомление каждому администратору о новом сообщении
        for admin in admin_ids:
            try:
                await context.bot.send_message(chat_id=admin[0], text=f"📨 Новое сообщение от @{username}:\n{update.message.text}")
            except Exception as e:
                logger.error("❌ Ошибка отправки уведомления администратору %s: %s", admin[0], e)
        # Добавляем кнопку для возврата в главное меню
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]])
        await update.message.reply_text("✅ Ваше сообщение отправлено", reply_markup=keyboard)
        context.user_data.clear()

    # Обработка ввода ID нового администратора
    elif context.user_data.get("awaiting_admin"):
        try:
            new_admin_id = int(update.message.text.strip())
            chat = await context.bot.get_chat(new_admin_id)
            new_admin_name = chat.first_name or chat.username or "Без имени"
            cursor.execute("INSERT OR IGNORE INTO admins (user_id, username) VALUES (?, ?)", (new_admin_id, new_admin_name))
            conn.commit()
            await update.message.reply_text(f"✅ Администратор {new_admin_name} (ID: {new_admin_id}) добавлен")
        except Exception as e:
            logger.error("❌ Ошибка добавления администратора: %s", e)
            await update.message.reply_text("❌ Ошибка добавления администратора. Проверьте ввод ID")
        context.user_data.clear()

# Функция main - инициализация и запуск бота
def main() -> None:
    # Создаём приложение Telegram Bot с заданным токеном
    application = Application.builder().token("ТОКЕН БОТА").build()
    # Регистрируем обработчики команд и сообщений
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_combined_input))
    # Запускаем бота в режиме опроса (polling)
    application.run_polling()

# Точка входа в приложение
if __name__ == "__main__":
    main()
