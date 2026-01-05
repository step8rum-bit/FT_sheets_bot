import telebot
from telebot import types
import gspread
import datetime
import time, requests
import sys
import os
import logging
import json

# =================================================================================
# --- НАСТРОЙКИ: ЗАПОЛНИТЕ ЭТИ ПОЛЯ СВОИМИ ДАННЫМИ ---
# =================================================================================

# --- БЕЗОПАСНАЯ ЗАГРУЗКА ТОКЕНА ---
# Токен будет загружен из переменных окружения на сервере
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
if not TELEGRAM_TOKEN:
    # Эта строка нужна для локальной отладки, если переменная не найдена
    logging.critical("КРИТИЧЕСКАЯ ОШИБКА: Переменная окружения TELEGRAM_TOKEN не установлена. Бот не может быть запущен.")
    sys.exit("Переменная TELEGRAM_TOKEN не найдена.")

# =================================================================================
# --- УПРАВЛЕНИЕ ДОСТУПОМ: ДОБАВЬТЕ СЮДА ВАШИХ ПРОДАВЦОВ ---
# =================================================================================
# Чтобы узнать ID, каждый продавец должен написать боту @userinfobot
ALLOWED_SELLERS = {
    139085263: "Румянцев", # Замените на реальный ID и ФИО
    7313811517: "FT Metropolis", # Замените на реальный ID и ФИО
}

# Укажите точные названия ваших Google Таблиц
# Старые таблицы:
OLD_PROMO_SHEET_NAME = "Таблица розыгрыш офлайн-магазин"
OLD_BONUSES_SHEET_NAME = "Гугл форма для списания и начисления бонусов"
OLD_LEADS_SHEET_NAME = "Гугл форма для лидов"

# Новая объединенная таблица:
NEW_MAIN_SHEET_NAME = 'Новая общая база'

# Название файла с ключами доступа, который вы скачали из Google Cloud
GOOGLE_CREDENTIALS_FILE = 'credentials.json'

# =================================================================================
# --- ИНИЦИАЛИЗАЦИЯ БОТА И ПОДКЛЮЧЕНИЕ К ТАБЛИЦАМ ---
# =================================================================================

# --- НАСТРОЙКА ЛОГИРОВАНИЯ В ФАЙЛ ---
# Определяем путь к лог-файлу в той же папке, где лежит скрипт
script_path = os.path.dirname(os.path.realpath(sys.argv[0]))
log_file_path = os.path.join(script_path, 'bot.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, encoding='utf-8'),
        logging.StreamHandler(sys.stdout) # Возвращаем вывод в консоль для отладки на сервере
    ]
)

# --- ПРОВЕРКА ВЕРСИИ GSPREAD ---
# Этот блок можно будет убрать после успешного запуска
logging.info(f"Путь к интерпретатору Python: {sys.executable}")
logging.info(f"Версия gspread, которую видит скрипт: {gspread.__version__}")
# --- КОНЕЦ ПРОВЕРКИ ---

# Хранилище для временных данных пользователя в процессе диалога
user_data = {}

# --- ИНИЦИАЛИЗАЦИЯ БОТА ---
# Объект бота должен быть создан здесь, до определения обработчиков
bot = telebot.TeleBot(TELEGRAM_TOKEN)

# Глобальные переменные для таблиц, чтобы они были доступны во всей программе
old_promo_sheet = None
old_bonuses_sheet = None
old_leads_sheet = None
new_customers_sheet = None
new_transactions_sheet = None
new_promo_sheet = None

# =================================================================================
# --- ГЛАВНОЕ МЕНЮ И СТАРТ ---
# =================================================================================

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    """Отправляет приветственное сообщение и главное меню, проверяя ID продавца."""
    chat_id = message.chat.id
    user_id = message.from_user.id

    # Проверяем, есть ли ID пользователя в списке разрешенных
    if user_id in ALLOWED_SELLERS:
        seller_name = ALLOWED_SELLERS[user_id]
        if chat_id not in user_data:
            user_data[chat_id] = {}
        user_data[chat_id]['seller_name'] = seller_name
        bot.send_message(chat_id, f"Здравствуйте, {seller_name}!")
        show_main_menu(chat_id)
    else:
        # Если ID нет в списке, доступ запрещен
        bot.send_message(chat_id, "❌ Доступ запрещен. Пожалуйста, обратитесь к администратору.")
        logging.warning(f"Попытка несанкционированного доступа от пользователя с ID: {user_id} ({message.from_user.username})")

@bot.message_handler(commands=['cancel'])
def cancel_operation(message):
    """Отменяет текущую операцию и возвращает в главное меню."""
    chat_id = message.chat.id
    user_id = message.from_user.id

    # Сбрасываем диалог, но восстанавливаем имя продавца из списка разрешенных
    user_data[chat_id] = {}
    if user_id in ALLOWED_SELLERS:
        user_data[chat_id]['seller_name'] = ALLOWED_SELLERS[user_id]
    
    bot.send_message(chat_id, "Действие отменено. Вы возвращены в главное меню.")
    show_main_menu(chat_id)

def show_main_menu(chat_id):
    """Показывает главное меню с кнопками."""
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    btn1 = types.KeyboardButton("🛒 списать баллы")
    btn2 = types.KeyboardButton("✅ Зарегистрировать в акции")
    btn3 = types.KeyboardButton("📝 Добавить лида (Эл. гарантия)")
    # Добавляем каждую кнопку на отдельную строку для вертикального меню
    markup.add(btn1)
    markup.add(btn2)
    markup.add(btn3)
    bot.send_message(chat_id, "Здравствуйте! Выберите действие:", reply_markup=markup)

def format_phone_number(phone):
    """Форматирует номер телефона в +7 (XXX) XXX-XX-XX."""
    if not phone or not isinstance(phone, str):
        return "" # Возвращаем пустую строку, если на вход пришло не строковое значение
    # Убираем все, кроме цифр
    clean_phone = ''.join(filter(str.isdigit, phone))
    # Если номер начинается с 7 или 8 и имеет 11 цифр, убираем первую
    if len(clean_phone) == 11 and (clean_phone.startswith('7') or clean_phone.startswith('8')):
        clean_phone = clean_phone[1:]
    # Если у нас 10 цифр, форматируем
    if len(clean_phone) == 10:
        return f"'+7 ({clean_phone[0:3]}) {clean_phone[3:6]}-{clean_phone[6:8]}-{clean_phone[8:10]}"
    return phone # Возвращаем как есть, если формат не стандартный

def normalize_phone(phone):
    """Приводит номер к единому 10-значному формату для поиска и хранения."""
    if not phone or not isinstance(phone, str):
        return ""
    clean_phone = ''.join(filter(str.isdigit, phone))
    if len(clean_phone) == 11 and (clean_phone.startswith('7') or clean_phone.startswith('8')):
        return clean_phone[1:]
    if len(clean_phone) == 10:
        return clean_phone
    return clean_phone # Возвращаем "как есть", если формат совсем нестандартный

# =================================================================================
# --- СЦЕНАРИЙ 1: ПРОВЕСТИ ТРАНЗАКЦИЮ ---
# =================================================================================

@bot.message_handler(func=lambda message: message.text == "🛒 списать баллы")
def handle_transaction(message):
    chat_id = message.chat.id
    # Очищаем данные предыдущей операции, сохраняя ФИО продавца
    seller_name = user_data.get(chat_id, {}).get('seller_name', 'Не указано')
    user_data[chat_id] = {'seller_name': seller_name}

    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    markup.add(types.KeyboardButton("Отмена"))
    msg = bot.send_message(chat_id, "Введите номер телефона клиента (например, 79123456789):", reply_markup=markup)
    bot.register_next_step_handler(msg, process_phone_step, "transaction")

def process_phone_step(message, flow_type):
    """Ищет клиента по номеру телефона в новой базе."""
    chat_id = message.chat.id
    phone_number = message.text.strip()

    # Обработка отмены
    if phone_number == "Отмена":
        cancel_operation(message)
        return

    if not (phone_number.isdigit() and len(phone_number) > 9):
        msg = bot.send_message(chat_id, "❌ Неверный формат. Введите номер телефона, состоящий только из цифр. Попробуйте снова:")
        bot.register_next_step_handler(msg, process_phone_step, flow_type)
        return

    # Нормализуем номер для поиска и сохранения в основную базу
    normalized_phone = normalize_phone(phone_number)
    user_data[chat_id]['phone'] = normalized_phone
    
    try:
        # Ищем клиента в НАШЕЙ НОВОЙ базе `Клиенты` по нормализованному номеру
        cell = new_customers_sheet.find(normalized_phone, in_column=3) # Колонка C - 'Телефон'
        
        if cell:
            customer_row = new_customers_sheet.row_values(cell.row)
            user_data[chat_id]['customer_id'] = customer_row[0]
            user_data[chat_id]['full_name'] = customer_row[1]
            user_data[chat_id]['email'] = customer_row[4]
            bot.send_message(chat_id, f"✅ Клиент найден: {customer_row[1]}.")
            
            # В зависимости от выбранного сценария, переходим к следующему шагу
            if flow_type == "transaction":
                ask_for_total_amount(chat_id)
            elif flow_type == "promo":
                finalize_promo_registration(message)
            elif flow_type == "lead":
                finalize_lead_addition(message)
        else:
            bot.send_message(chat_id, "👤 Новый клиент. Начинаем регистрацию.")
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
            markup.add(types.KeyboardButton("Отмена"))
            msg = bot.send_message(chat_id, "Введите ФИО клиента:", reply_markup=markup)
            bot.register_next_step_handler(msg, process_new_customer_name_step, flow_type)
            
    except Exception as e:
        logging.error(f"Ошибка при поиске клиента (chat_id: {chat_id}): {e}")
        bot.send_message(chat_id, "Произошла ошибка при поиске клиента. Попробуйте позже.")
        send_welcome(message)

def process_new_customer_name_step(message, flow_type):
    chat_id = message.chat.id
    user_data[chat_id]['full_name'] = message.text.strip()
    if user_data[chat_id]['full_name'] == "Отмена":
        cancel_operation(message)
        return
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    markup.add(types.KeyboardButton("Отмена"))
    msg = bot.send_message(chat_id, "Введите email клиента:", reply_markup=markup)
    bot.register_next_step_handler(msg, process_new_customer_email_step, flow_type)

def process_new_customer_email_step(message, flow_type):
    chat_id = message.chat.id
    user_data[chat_id]['email'] = message.text.strip()
    if user_data[chat_id]['email'] == "Отмена":
        cancel_operation(message)
        return
    
    try:
        # Добавляем нового клиента ТОЛЬКО в новую таблицу `Клиенты`
        # Надежная генерация ID: находим максимальный существующий и прибавляем 1
        all_ids = new_customers_sheet.col_values(1)
        # Фильтруем пустые строки и заголовок
        numeric_ids = [int(id_val) for id_val in all_ids if id_val and id_val.isdigit()]
        max_id = max(numeric_ids) if numeric_ids else 999 # Начинаем с 1000, если база пуста
        new_customer_id = max_id + 1
        
        new_row = [
            new_customer_id,
            user_data[chat_id]['full_name'],
            user_data[chat_id]['phone'],
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            user_data[chat_id]['email']
        ]
        new_customers_sheet.append_row(new_row, value_input_option='USER_ENTERED')
        user_data[chat_id]['customer_id'] = new_customer_id
        bot.send_message(chat_id, "✅ Новый клиент успешно зарегистрирован!")
        
        # Переходим к нужному шагу в зависимости от сценария
        if flow_type == "transaction":
            ask_for_total_amount(chat_id)
        elif flow_type == "promo":
            finalize_promo_registration(message)
        elif flow_type == "lead":
            finalize_lead_addition(message)
            
    except Exception as e:
        logging.error(f"Ошибка при добавлении клиента (chat_id: {chat_id}): {e}")
        bot.send_message(chat_id, "Ошибка при добавлении клиента. Попробуйте позже.")
        send_welcome(message)

def ask_for_total_amount(chat_id):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    markup.add(types.KeyboardButton("Отмена"))
    msg = bot.send_message(chat_id, "Введите полную сумму чека (до бонусов), например: 1500.50", reply_markup=markup)
    bot.register_next_step_handler(msg, process_total_amount_step)

def process_total_amount_step(message):
    chat_id = message.chat.id
    if message.text == "Отмена":
        cancel_operation(message)
        return
    try:
        user_data[chat_id]['total_before_bonus'] = float(message.text.replace(',', '.'))
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        markup.add(types.KeyboardButton("Отмена"))
        msg = bot.send_message(chat_id, "Введите сумму списанных бонусов:", reply_markup=markup)
        bot.register_next_step_handler(msg, process_bonus_spent_step)
    except ValueError:
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        markup.add(types.KeyboardButton("Отмена"))
        msg = bot.send_message(chat_id, "❌ Неверный формат. Пожалуйста, введите число. Попробуйте еще раз.", reply_markup=markup)
        bot.register_next_step_handler(msg, process_total_amount_step)

def process_bonus_spent_step(message):
    chat_id = message.chat.id
    if message.text == "Отмена":
        cancel_operation(message)
        return
    try:
        user_data[chat_id]['bonus_spent'] = float(message.text.replace(',', '.'))
        # Рассчитываем итоговую сумму
        total = user_data[chat_id]['total_before_bonus'] - user_data[chat_id]['bonus_spent']
        user_data[chat_id]['final_total'] = total
        bot.send_message(chat_id, f"Итоговая сумма: {total}")
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        markup.add(types.KeyboardButton("Отмена"))
        msg = bot.send_message(chat_id, "Введите номер чека:", reply_markup=markup)
        bot.register_next_step_handler(msg, process_check_number_step)
    except ValueError:
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        markup.add(types.KeyboardButton("Отмена"))
        msg = bot.send_message(chat_id, "❌ Неверный формат. Пожалуйста, введите число. Попробуйте еще раз.", reply_markup=markup)
        bot.register_next_step_handler(msg, process_bonus_spent_step)

def process_check_number_step(message):
    chat_id = message.chat.id
    if message.text == "Отмена":
        cancel_operation(message)
        return
    user_data[chat_id]['check_number'] = message.text.strip()
    
    # Запрашиваем источник с помощью кнопок
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    markup.add("шел по ТЦ", "увидел в ТГ", "увидел в ВК", "таргет. реклама")
    markup.add("email", "маркетплейсы", "друг рассказал", "рекомендация эксперта")
    markup.add("ютуб", "другое", "Отмена")
    msg = bot.send_message(chat_id, "Откуда клиент узнал о нас?", reply_markup=markup)
    bot.register_next_step_handler(msg, process_source_step)

def process_source_step(message):
    """Обрабатывает выбор источника информации."""
    chat_id = message.chat.id
    source = message.text.strip()
    if source == "Отмена":
        cancel_operation(message)
        return

    if source.lower() == 'другое':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        markup.add(types.KeyboardButton("Отмена"))
        msg = bot.send_message(chat_id, "Пожалуйста, укажите источник:", reply_markup=markup)
        bot.register_next_step_handler(msg, finalize_transaction)
    else:
        user_data[chat_id]['source'] = source
        finalize_transaction(message, is_custom_source=False)

def finalize_transaction(message, is_custom_source=True):
    """Завершает транзакцию и записывает данные в обе системы."""
    chat_id = message.chat.id
    if message.text == "Отмена":
        cancel_operation(message)
        return
    if is_custom_source:
        user_data[chat_id]['source'] = message.text.strip()
    
    bot.send_message(chat_id, "⏳ Сохраняю данные... Пожалуйста, подождите.")

    try:
        # --- ЗАПИСЬ В СТАРУЮ ТАБЛИЦУ "БОНУСЫ" ---
        formatted_phone_for_bonus = format_phone_number(user_data[chat_id]['phone'])
        old_bonus_row = [
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            user_data[chat_id]['seller_name'],
            user_data[chat_id]['full_name'],
            formatted_phone_for_bonus,
            user_data[chat_id]['email'],
            user_data[chat_id]['total_before_bonus'],
            user_data[chat_id]['bonus_spent'],
            user_data[chat_id]['final_total'],
            user_data[chat_id]['check_number'],
            user_data[chat_id]['source'],
        ]
        old_bonuses_sheet.append_row(old_bonus_row, value_input_option='USER_ENTERED')

        # --- АВТОМАТИЧЕСКАЯ ЗАПИСЬ В ТАБЛИЦУ РОЗЫГРЫША ---
        # Разделяем ФИО на имя и фамилию для таблицы розыгрыша
        full_name_parts = user_data[chat_id].get('full_name', '').split()
        first_name = full_name_parts[1] if len(full_name_parts) > 1 else ""
        last_name = full_name_parts[0] if len(full_name_parts) > 0 else ""
        formatted_phone = format_phone_number(user_data[chat_id]['phone'])
        old_promo_row = [
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            first_name,
            last_name,
            user_data[chat_id]['email'],
            formatted_phone,
            "yes" # Согласие по умолчанию, т.к. клиент участвует в бонусной программе
        ]
        old_promo_sheet.append_row(old_promo_row, value_input_option='USER_ENTERED')
        
        # --- ЗАПИСЬ В НОВУЮ ТАБЛИЦУ "ТРАНЗАКЦИИ" ---
        # Надежная генерация ID
        all_ids = new_transactions_sheet.col_values(1)
        new_id = max([int(i) for i in all_ids if i.isdigit()] or [0]) + 1
        new_transaction_row = [
            new_id, # ID транзакции
            user_data[chat_id]['customer_id'], # ID клиента (связь)
            user_data[chat_id]['seller_name'],
            user_data[chat_id]['total_before_bonus'],
            user_data[chat_id]['bonus_spent'],
            user_data[chat_id]['final_total'],
            user_data[chat_id]['check_number'],
            user_data[chat_id]['source'],
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ]
        new_transactions_sheet.append_row(new_transaction_row, value_input_option='USER_ENTERED')

        # --- АВТОМАТИЧЕСКАЯ ЗАПИСЬ В НОВУЮ ТАБЛИЦУ "УЧАСТНИКИ АКЦИИ" ---
        # Надежная генерация ID
        all_ids_promo = new_promo_sheet.col_values(1)
        new_promo_id = max([int(i) for i in all_ids_promo if i.isdigit()] or [0]) + 1
        new_promo_row = [
            new_promo_id, # ID Записи
            user_data[chat_id]['customer_id'], # ID Клиента
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ]
        new_promo_sheet.append_row(new_promo_row, value_input_option='USER_ENTERED')
        
        bot.send_message(chat_id, "🎉 Транзакция успешно сохранена в обеих системах!")
        logging.info(f"Транзакция для клиента {user_data[chat_id]['full_name']} сохранена.")

    except Exception as e:
        logging.error(f"Ошибка при записи транзакции (chat_id: {chat_id}): {e}")
        bot.send_message(chat_id, "❌ Произошла критическая ошибка при записи данных. Обратитесь к администратору.")
    finally:
        send_welcome(message)

# =================================================================================
# --- СЦЕНАРИЙ 2: РЕГИСТРАЦИЯ В АКЦИИ ---
# =================================================================================

@bot.message_handler(func=lambda message: message.text == "✅ Зарегистрировать в акции")
def handle_promo_registration(message):
    chat_id = message.chat.id
    # Очищаем данные предыдущей операции, сохраняя ФИО продавца
    seller_name = user_data.get(chat_id, {}).get('seller_name', 'Не указано')
    user_data[chat_id] = {'seller_name': seller_name}
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    markup.add(types.KeyboardButton("Отмена"))
    msg = bot.send_message(chat_id, "Введите номер телефона клиента для регистрации в акции:", reply_markup=markup)
    bot.register_next_step_handler(msg, process_phone_step, "promo")

def finalize_promo_registration(message):
    """Завершает регистрацию в акции и записывает данные."""
    chat_id = message.chat.id
    bot.send_message(chat_id, "⏳ Регистрирую клиента в акции...")
    
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    markup.add("Да", "Нет", "Отмена")
    msg = bot.send_message(chat_id, "Клиент дал согласие на обработку данных и получение рекламы?", reply_markup=markup)
    bot.register_next_step_handler(msg, process_promo_consent_step)

def process_promo_consent_step(message):
    chat_id = message.chat.id
    if message.text == "Отмена":
        cancel_operation(message)
        return

    consent = message.text.strip().lower()
    user_data[chat_id]['consent'] = "yes" if consent == "да" else "no"
    
    try:
        # --- ЗАПИСЬ В СТАРУЮ ТАБЛИЦУ "АКЦИЯ" ---
        full_name_parts = user_data[chat_id].get('full_name', '').split()
        first_name = full_name_parts[1] if len(full_name_parts) > 1 else ""
        last_name = full_name_parts[0] if len(full_name_parts) > 0 else ""
        formatted_phone = format_phone_number(user_data[chat_id]['phone'])

        old_promo_row = [
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            first_name,
            last_name,
            user_data[chat_id]['email'],
            formatted_phone,
            user_data[chat_id]['consent']
        ]
        old_promo_sheet.append_row(old_promo_row, value_input_option='USER_ENTERED')

        # --- ЗАПИСЬ В НОВУЮ ТАБЛИЦУ "УЧАСТНИКИ АКЦИИ" ---
        # Надежная генерация ID
        all_ids = new_promo_sheet.col_values(1)
        new_id = max([int(i) for i in all_ids if i.isdigit()] or [0]) + 1
        new_promo_row = [
            new_id, # ID Записи
            user_data[chat_id]['customer_id'], # ID Клиента
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ]
        new_promo_sheet.append_row(new_promo_row, value_input_option='USER_ENTERED')

        bot.send_message(chat_id, "🎉 Клиент успешно зарегистрирован в акции!")
        logging.info(f"Клиент {user_data[chat_id]['full_name']} зарегистрирован в акции.")

    except Exception as e:
        logging.error(f"Ошибка при регистрации в акции (chat_id: {chat_id}): {e}")
        bot.send_message(chat_id, "❌ Ошибка при регистрации. Обратитесь к администратору.")
    finally:
        send_welcome(message)

# =================================================================================
# --- СЦЕНАРИЙ 3: ДОБАВИТЬ ЛИДА (ЭЛ. ГАРАНТИЯ) ---
# =================================================================================

@bot.message_handler(func=lambda message: message.text == "📝 Добавить лида (Эл. гарантия)")
def handle_lead_addition(message):
    chat_id = message.chat.id
    # Очищаем данные предыдущей операции, сохраняя ФИО продавца
    seller_name = user_data.get(chat_id, {}).get('seller_name', 'Не указано')
    user_data[chat_id] = {'seller_name': seller_name}
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    markup.add(types.KeyboardButton("Отмена"))
    msg = bot.send_message(chat_id, "Введите номер телефона клиента для оформления электронной гарантии:", reply_markup=markup)
    bot.register_next_step_handler(msg, process_phone_step, "lead")

def finalize_lead_addition(message):
    """Завершает добавление лида."""
    chat_id = message.chat.id
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    markup.add(types.KeyboardButton("Отмена"))
    msg = bot.send_message(chat_id, "Введите номер чека:", reply_markup=markup)
    bot.register_next_step_handler(msg, process_lead_check_number_step)

def process_lead_check_number_step(message):
    chat_id = message.chat.id
    if message.text == "Отмена":
        cancel_operation(message)
        return
    user_data[chat_id]['check_number'] = message.text.strip()
    
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    markup.add("Спешил", "Не захотел", "Другое", "Отмена")
    msg = bot.send_message(chat_id, "Почему клиент не захотел списывать баллы/участвовать в акции?", reply_markup=markup)
    bot.register_next_step_handler(msg, process_lead_reason_step)

def process_lead_reason_step(message):
    chat_id = message.chat.id
    reason = message.text.strip()
    if reason == "Отмена":
        cancel_operation(message)
        return

    if reason.lower() == 'другое':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        markup.add(types.KeyboardButton("Отмена"))
        msg = bot.send_message(chat_id, "Укажите причину:", reply_markup=markup)
        bot.register_next_step_handler(msg, save_lead_data)
    else:
        user_data[chat_id]['reason'] = reason
        save_lead_data(message, is_custom_reason=False)

def save_lead_data(message, is_custom_reason=True):
    chat_id = message.chat.id
    if message.text == "Отмена":
        cancel_operation(message)
        return
    if is_custom_reason:
        user_data[chat_id]['reason'] = message.text.strip()

    bot.send_message(chat_id, "⏳ Сохраняю данные клиента...")

    try:
        # --- ЗАПИСЬ В СТАРУЮ ТАБЛИЦУ "ЛИДЫ" ---
        formatted_phone_for_lead = format_phone_number(user_data[chat_id]['phone'])
        old_lead_row = [
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            user_data[chat_id]['seller_name'],
            user_data[chat_id]['full_name'],
            formatted_phone_for_lead,
            user_data[chat_id]['email'],
            user_data[chat_id]['check_number'],
            user_data[chat_id]['reason']
        ]
        old_leads_sheet.append_row(old_lead_row, value_input_option='USER_ENTERED')

        # В новой системе этот клиент уже добавлен в общую таблицу "Клиенты",
        # поэтому отдельной записи в "Лиды" не требуется.
        
        bot.send_message(chat_id, "🎉 Данные клиента (лида) успешно сохранены!")
        logging.info(f"Клиент {user_data[chat_id]['full_name']} добавлен как лид.")

    except Exception as e:
        logging.error(f"Ошибка при добавлении лида (chat_id: {chat_id}): {e}")
        bot.send_message(chat_id, "❌ Ошибка при сохранении лида. Обратитесь к администратору.")
    finally:
        send_welcome(message)

# =================================================================================
# --- ЗАПУСК БОТА ---
# =================================================================================

def run_bot():
    """Основная функция для запуска бота и инициализации таблиц."""
    global old_promo_sheet, old_bonuses_sheet, old_leads_sheet, new_customers_sheet, new_transactions_sheet, new_promo_sheet
    
    # --- Устойчивая инициализация с несколькими попытками ---
    MAX_INIT_ATTEMPTS = 3
    for attempt in range(1, MAX_INIT_ATTEMPTS + 1):
        # --- Устойчивая инициализация с несколькими попытками ---
        try:
            logging.info(f"Попытка инициализации #{attempt}...")
            
            # --- Безопасная загрузка ключей на сервере ---
            google_creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
            if google_creds_json:
                gc = gspread.service_account_from_dict(json.loads(google_creds_json, strict=False))
            else:
                gc = gspread.service_account(filename=GOOGLE_CREDENTIALS_FILE)

            logging.info("Успешное подключение к Google API.")
            old_promo_sheet = gc.open(OLD_PROMO_SHEET_NAME).sheet1
            old_bonuses_sheet = gc.open(OLD_BONUSES_SHEET_NAME).sheet1
            old_leads_sheet = gc.open(OLD_LEADS_SHEET_NAME).sheet1
            spreadsheet_new = gc.open(NEW_MAIN_SHEET_NAME)
            new_customers_sheet = spreadsheet_new.worksheet("Клиенты")
            new_transactions_sheet = spreadsheet_new.worksheet("Транзакции")
            new_promo_sheet = spreadsheet_new.worksheet("Участники акции")
            logging.info("Все таблицы и листы успешно загружены!")
            break
        except Exception as e:
            logging.error(f"ОШИБКА при инициализации (попытка {attempt}/{MAX_INIT_ATTEMPTS}): {e}")
            if attempt == MAX_INIT_ATTEMPTS:
                logging.error("КРИТИЧЕСКАЯ ОШИБКА: Не удалось инициализировать таблицы. Бот не будет запущен.")
                return
            time.sleep(10)

    logging.info("Бот запускается...")
    while True:
        try:
            bot.polling(none_stop=True, interval=3, timeout=20)
        except requests.exceptions.ConnectionError as e:
            logging.error(f"СЕТЕВАЯ ОШИБКА: {e}. Перезапуск polling через 15 секунд...")
            time.sleep(15)
        except Exception as e:
            logging.error(f"КРИТИЧЕСКАЯ ОШИБКА в цикле polling: {e}", exc_info=True)
            logging.info("Перезапуск через 15 секунд...")
            time.sleep(15)

if __name__ == '__main__':
    run_bot()
