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
            # --- Безопасная загрузка ключей на сервере ---
            google_creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
            if google_creds_json:
                gc = gspread.service_account_from_dict(json.loads(google_creds_json, strict=False))
            else:
                gc = gspread.service_account(filename=GOOGLE_CREDENTIALS_FILE)


