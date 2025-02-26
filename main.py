from flask import Flask, render_template_string, jsonify, request
import requests
import json
import httpx
import time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
import os

app = Flask(__name__)

# URL для получения товаров из 1С
url = os.getenv('ONE_URL')

# Данные для доступа к Shopify API
shopify_store_url = os.getenv('SHOPIFY_STORE_URL')
access_token = os.getenv('SHOPIFY_ACCESS_TOKEN')

# Функция для извлечения корректных JSON-объектов с поддержкой вложенности
def extract_valid_json(content):
    decoder = json.JSONDecoder()
    idx = 0
    valid_data = []
    while idx < len(content):
        try:
            obj, idx = decoder.raw_decode(content, idx)
            valid_data.append(obj)
        except json.JSONDecodeError:
            idx += 1  # Пропускаем некорректные символы
    return valid_data

# Очистка содержимого JSON
def clean_json_content(content):
    # Удаляем лишние обратные слеши перед запятыми
    return content.replace('\\",', '",')


def clean_price(amount):
    try:
        print(f"🔍 Исходная цена: {amount} | Тип: {type(amount)}")  # Лог перед обработкой

        # Удаляем пробелы, заменяем запятые на точки
        amount_cleaned = str(amount).replace('\u00A0', '').replace(' ', '').replace(',', '.')

        # Проверяем, содержит ли строка число с точкой
        if '.' in amount_cleaned:
            price = round(float(amount_cleaned), 2)  # Если есть точка, преобразуем в float
        else:
            price = int(amount_cleaned)  # Если точек нет — это целое число

        print(f"✅ Очищенная цена: {price} | Тип: {type(price)}")  # Лог после обработки
        return price
    except (ValueError, TypeError) as e:
        print(f"❌ Ошибка в clean_price(): {e} | Исходное значение: {amount}")  # Лог ошибки
        return 0.0  # Если ошибка — возвращаем 0.0


def clean_quantity(quantity):
    try:
        print(f"🔍 Исходное количество: {quantity} | Тип: {type(quantity)}")  # Лог перед обработкой

        if not quantity:  # Проверяем, не None ли значение или пустая строка
            return 0

        # Удаляем пробелы (включая неразрывные), заменяем запятые на точки
        quantity_cleaned = str(quantity).replace('\u00A0', '').replace(' ', '').replace(',', '.')

        # Если число с точкой — округляем и переводим в int
        if '.' in quantity_cleaned:
            quantity_int = int(float(quantity_cleaned))  # float → int, чтобы убрать .0
        else:
            quantity_int = int(quantity_cleaned)  # Уже целое число

        print(f"✅ Очищенное количество: {quantity_int} | Тип: {type(quantity_int)}")  # Лог после обработки
        return quantity_int
    except (ValueError, TypeError) as e:
        print(f"❌ Ошибка в clean_quantity(): {e} | Исходное значение: {quantity}")  # Лог ошибки
        return 0  # Если ошибка — возвращаем 0


# Получаем список товаров из 1С
def fetch_products():
    try:
        with httpx.Client(http2=True, verify=False, timeout=10) as client:
            response = client.get(url)
            print(f"Статус ответа от 1С: {response.status_code}")

            if response.status_code == 200:
                content = response.content.decode('utf-8-sig').strip()
                content = clean_json_content(content)
                try:
                    products = json.loads(content)
                    return products
                except json.JSONDecodeError as e:
                    print(f"Ошибка декодирования JSON: {e}")
                    # Пытаемся извлечь корректные JSON-объекты
                    return extract_valid_json(content)
            else:
                print(f"Не удалось получить товары из 1С. Код статуса: {response.status_code}")
                return None
    except Exception as e:
        print(f"Ошибка при запросе к 1С: {e}")
        return None

# Получаем все товары из Shopify (с пагинацией)
def fetch_all_shopify_products():
    shopify_url = f"{shopify_store_url}/admin/api/2024-01/products.json?fields=id,variants,status&limit=250"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token
    }
    all_products = []
    page_info = None

    while True:
        params = {"limit": 250}
        if page_info:
            params["page_info"] = page_info

        # Задержка перед запросом к Shopify, чтобы избежать 429
        time.sleep(0.6)

        response = requests.get(shopify_url, headers=headers, params=params)
        if response.status_code == 200:
            products = response.json().get('products', [])
            all_products.extend(products)
            # Проверяем заголовок "Link" на наличие следующей страницы
            link_header = response.headers.get("Link")
            if link_header and 'rel="next"' in link_header:
                page_info = link_header.split('page_info=')[1].split('>')[0]
            else:
                break
        else:
            print(f"Ошибка при получении товаров из Shopify. Код: {response.status_code}")
            break

    return all_products


def send_request_with_retry(url, method='GET', headers=None, json_data=None, max_retries=5):
    retries = 0
    while retries < max_retries:
        response = None
        try:
            if method == 'GET':
                response = requests.get(url, headers=headers)
            elif method == 'POST':
                response = requests.post(url, headers=headers, json=json_data)
            elif method == 'PUT':
                response = requests.put(url, headers=headers, json=json_data)

            if response.status_code == 429:  # Too Many Requests
                retry_after = response.headers.get("Retry-After", 5)  # Берем Retry-After, иначе 5 сек
                print(f"⚠️ Ошибка 429, ждем {retry_after} секунд... Попытка {retries + 1} из {max_retries}")
                time.sleep(int(float(retry_after)))  # Исправлено: теперь Retry-After безопасно преобразуется
                retries += 1
            else:
                print(f"✅ Запрос успешно выполнен после {retries} повторных попыток. Код: {response.status_code}")
                return response  # Возвращаем успешный ответ
        except Exception as e:
            print(f"❌ Ошибка при выполнении запроса: {e} | Попытка {retries + 1} из {max_retries}")
            time.sleep(2 ** retries)  # Экспоненциальная задержка перед следующим повтором
            retries += 1

    print(f"❗ Достигнут лимит повторных попыток ({max_retries}). Запрос не выполнен.")
    return response  # Возвращаем последний ответ, если достигнут лимит попыток


def update_shopify_variant(variant_id, inventory_item_id, new_price, new_quantity):
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token
    }

    # Обновляем цену
    update_variant_url = f"{shopify_store_url}/admin/api/2024-01/variants/{variant_id}.json"
    variant_data = {"variant": {"id": variant_id, "price": new_price}}
    response = send_request_with_retry(update_variant_url, method='PUT', headers=headers, json_data=variant_data)
    if response and response.status_code == 200:
        print(f"✅ Цена варианта {variant_id} обновлена.")
    else:
        print(
            f"❌ Ошибка при обновлении цены варианта {variant_id}: {response.status_code if response else 'Нет ответа'}")

    # Обновляем количество через Inventory API
    update_inventory_url = f"{shopify_store_url}/admin/api/2024-01/inventory_levels/set.json"
    inventory_data = {"location_id": 73379741896, "inventory_item_id": inventory_item_id, "available": new_quantity}
    response = send_request_with_retry(update_inventory_url, method='POST', headers=headers, json_data=inventory_data)
    if response and response.status_code == 200:
        print(f"✅ Количество варианта {variant_id} обновлено.")
    else:
        print(
            f"❌ Ошибка при обновлении количества варианта {variant_id}: {response.status_code if response else 'Нет ответа'}")


# Преобразуем данные из 1С для Shopify
def transform_to_shopify_format(product):
    # Проверяем, что у нас словарь
    if not isinstance(product, dict):
        print(f"Пропускаем некорректный формат товара: {product}")
        return None

    # Ищем цену типа "ТОВ"
    price_info = next((p for p in product.get('price', []) if p.get('type_price') == 'ТОВ'), None)
    if not price_info:
        return None  # Если нет цены "ТОВ", не публикуем товар

    # Очищаем цену и умножаем на 1.2
    price_float = float(clean_price(price_info['amount'])) * 1.2
    # Очищаем количество
    quantity_int = clean_quantity(product.get('quantity', '0'))

    # Формируем структуру товара для Shopify
    shopify_product = {
        "product": {
            "title": product['name'],
            "vendor": "MIXOpro.Ukraine",
            "tags": "1C Sync",
            "handle": product['name'].replace(" ", "-").lower(),
            "status": "active",  # По условию, если цена есть, делаем активным
            "variants": [
                {
                    "sku": product['id'],
                    "price": f"{price_float:.2f}",
                    "option1": "Default Title",
                    # Включаем управление остатками "shopify"
                    "inventory_management": "shopify",
                    "inventory_quantity": quantity_int,
                    "requires_shipping": True
                }
            ],
            "options": [
                {
                    "name": "Title",
                    "position": 1,
                    "values": [
                        "Default Title"
                    ]
                }
            ]
        }
    }
    return shopify_product

# Отправляем (или обновляем) товар в Shopify
def send_to_shopify(shopify_product, existing_products):
    sku = shopify_product['product']['variants'][0]['sku']
    new_price = shopify_product['product']['variants'][0]['price']
    new_quantity = shopify_product['product']['variants'][0]['inventory_quantity']

    # Ищем товар с таким SKU в уже существующих
    existing_product = next((p for p in existing_products if any(v['sku'] == sku for v in p['variants'])), None)

    if existing_product:
        print(f"Товар со SKU {sku} уже существует в Shopify. Обновляем...")
        variant = next(v for v in existing_product['variants'] if v['sku'] == sku)
        variant_id = variant['id']
        inventory_item_id = variant['inventory_item_id']  # Теперь передаем inventory_item_id

        # Обновляем цену и количество
        print(f"Старые данные: цена = {variant['price']}")
        print(f"Новые данные: цена = {new_price}, количество = {new_quantity}")
        update_shopify_variant(variant_id, inventory_item_id, new_price, new_quantity)
    else:
        print(f"Товар со SKU {sku} не найден в Shopify. Создаем новый продукт.")
        time.sleep(0.6)

        shopify_url = f"{shopify_store_url}/admin/api/2024-01/products.json"
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": access_token
        }
        response = requests.post(shopify_url, headers=headers, json=shopify_product)
        if response.status_code == 201:
            print("Товар успешно создан.")
        else:
            print(f"Ошибка при создании товара: {response.status_code}, {response.json()}")

# 🛠 Добавляем контекст приложения для APScheduler
def scheduled_sync():
    with app.app_context():  # Создаём контекст приложения для фоновой задачи
        print("🔄 Запускаем фоновую синхронизацию...")
        sync_products()

@app.route('/sync_products')
def sync_products():
    products = fetch_products()
    existing_products = fetch_all_shopify_products()

    if not products:
        return jsonify({'status': 'No products found or an error occurred.'})

    print(f"Найдено товаров в 1С: {len(products)}")
    for product in products:
        if not isinstance(product, dict):
            print(f"Пропущен некорректный формат товара: {product}")
            continue  # Пропускаем некорректный формат товара

        shopify_product = transform_to_shopify_format(product)
        if shopify_product:
            send_to_shopify(shopify_product, existing_products)
        else:
            print(f"Пропущен товар без цены 'ТОВ': {product.get('id', 'Неизвестный ID')}")

    print("✅ Фоновая синхронизация завершена!")
    return jsonify({'status': 'finished'})

@app.route('/')
def index():
    return "The scheduler is running. Check your console for output."

# 🔄 Запускаем APScheduler с потоком
executors = {
    'default': ThreadPoolExecutor(20)
}

scheduler = BackgroundScheduler(executors=executors)
scheduler.add_job(func=scheduled_sync, trigger='interval', minutes=3)  # Используем `scheduled_sync`
scheduler.start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)