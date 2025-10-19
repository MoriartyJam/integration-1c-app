from flask import Flask, render_template_string, jsonify, request
import requests
import json
import httpx
import time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime, timedelta, timezone
import os

app = Flask(__name__)

# ================== НАЛАШТУВАННЯ ==================
# URL для отримання товарів з 1С
url = os.getenv('ONE_URL')

# Дані для доступу до Shopify API
shopify_store_url = os.getenv('SHOPIFY_STORE_URL')
access_token = os.getenv('SHOPIFY_ACCESS_TOKEN')

# Базовий інтервал автосинхронізації (хв)
SCHEDULE_MINUTES = 180
JOB_ID = "sync_job"

# ================== УТИЛІТИ ==================
def extract_valid_json(content):
    """Спроба витягнути зламаний JSON послідовно."""
    decoder = json.JSONDecoder()
    idx = 0
    valid_data = []
    while idx < len(content):
        try:
            obj, idx = decoder.raw_decode(content, idx)
            valid_data.append(obj)
        except json.JSONDecodeError:
            idx += 1
    return valid_data

def clean_json_content(content):
    # Підчищаємо зайві слеші
    return content.replace('\\",', '",')

def clean_price(amount):
    try:
        print(f"🔍 Вхідна ціна: {amount} | Тип: {type(amount)}")
        amount_cleaned = str(amount).replace('\u00A0', '').replace(' ', '').replace(',', '.')
        if '.' in amount_cleaned:
            price = round(float(amount_cleaned), 2)
        else:
            price = int(amount_cleaned)
        print(f"✅ Очищена ціна: {price} | Тип: {type(price)}")
        return price
    except (ValueError, TypeError) as e:
        print(f"❌ clean_price(): {e} | Вхід: {amount}")
        return 0.0

def clean_quantity(quantity):
    try:
        print(f"🔍 Вхідна к-сть: {quantity} | Тип: {type(quantity)}")
        if not quantity:
            return 0
        quantity_cleaned = str(quantity).replace('\u00A0', '').replace(' ', '').replace(',', '.')
        if '.' in quantity_cleaned:
            quantity_int = int(float(quantity_cleaned))
        else:
            quantity_int = int(quantity_cleaned)
        print(f"✅ Очищена к-сть: {quantity_int} | Тип: {type(quantity_int)}")
        return quantity_int
    except (ValueError, TypeError) as e:
        print(f"❌ clean_quantity(): {e} | Вхід: {quantity}")
        return 0

# ================== 1C ==================
def fetch_products():
    try:
        # http2=True вимагає пакет h2; якщо його нема — або встанови `pip install 'httpx[http2]'`, або прибери http2=True
        with httpx.Client(http2=True, verify=False, timeout=20) as client:
            response = client.get(url)
            print(f"Статус 1С: {response.status_code}")

            if response.status_code == 200:
                content = response.content.decode('utf-8-sig').strip()
                content = clean_json_content(content)
                try:
                    products = json.loads(content)
                    return products
                except json.JSONDecodeError as e:
                    print(f"JSONDecodeError: {e}")
                    return extract_valid_json(content)
            else:
                print(f"Не вдалося отримати товари з 1С. Код: {response.status_code}")
                return None
    except Exception as e:
        print(f"Помилка запиту до 1С: {e}")
        return None

# ================== Shopify ==================
def fetch_all_shopify_products():
    base_url = f"{shopify_store_url}/admin/api/2024-01/products.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token
    }
    all_products = []
    params = {
        "limit": 250,
        "fields": "id,handle,variants,status"
    }
    next_url = base_url

    while next_url:
        time.sleep(0.6)  # rate limit
        response = requests.get(next_url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json().get('products', [])
            all_products.extend(data)
            link_header = response.headers.get("Link")
            if link_header and 'rel="next"' in link_header:
                parts = link_header.split(",")
                next_link = next((p for p in parts if 'rel="next"' in p), None)
                if next_link:
                    next_url = next_link[next_link.find("<") + 1:next_link.find(">")]
                    params = None
                else:
                    break
            else:
                break
        else:
            print(f"❌ Помилка отримання товарів з Shopify: {response.status_code}")
            break

    print(f"📦 Отримано товарів з Shopify: {len(all_products)}")
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

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After", 5)
                print(f"⚠️ 429 | чекаємо {retry_after} с... Спроба {retries + 1}/{max_retries}")
                time.sleep(int(float(retry_after)))
                retries += 1
            else:
                print(f"✅ Успіх після {retries} ретраїв. Код: {response.status_code}")
                return response
        except Exception as e:
            print(f"❌ Помилка запиту: {e} | Спроба {retries + 1}/{max_retries}")
            time.sleep(2 ** retries)
            retries += 1

    print(f"❗ Досягнуто ліміт ретраїв ({max_retries}).")
    return response

def update_shopify_variant(variant_id, inventory_item_id, new_price, new_quantity):
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token
    }

    # Оновлюємо ціну
    update_variant_url = f"{shopify_store_url}/admin/api/2024-01/variants/{variant_id}.json"
    variant_data = {"variant": {"id": variant_id, "price": new_price}}
    response = send_request_with_retry(update_variant_url, method='PUT', headers=headers, json_data=variant_data)
    if response and response.status_code == 200:
        print(f"✅ Ціну варіанта {variant_id} оновлено.")
    else:
        print(f"❌ Помилка оновлення ціни {variant_id}: {response.status_code if response else 'нема відповіді'}")

    # Оновлюємо кількість через Inventory API
    update_inventory_url = f"{shopify_store_url}/admin/api/2024-01/inventory_levels/set.json"
    # ⚠️ ПІДСТАВ СВІЙ location_id
    inventory_data = {"location_id": 73379741896, "inventory_item_id": inventory_item_id, "available": new_quantity}
    response = send_request_with_retry(update_inventory_url, method='POST', headers=headers, json_data=inventory_data)
    if response and response.status_code == 200:
        print(f"✅ Кількість варіанта {variant_id} оновлено.")
    else:
        print(f"❌ Помилка оновлення кількості {variant_id}: {response.status_code if response else 'нема відповіді'}")

def transform_to_shopify_format(product):
    """Мапінг товару з 1С у формат створення продукту Shopify."""
    if not isinstance(product, dict):
        print(f"Пропуск — некоректний формат: {product}")
        return None

    price_info = next((p for p in product.get('price', []) if p.get('type_price') == 'ТОВ'), None)
    if not price_info:
        return None  # без ціни ТОВ не публікуємо

    price_float = float(clean_price(price_info['amount'])) * 1.2
    quantity_int = clean_quantity(product.get('quantity', '0'))

    shopify_product = {
        "product": {
            "title": product['name'],
            "vendor": "MIXOpro.Ukraine",
            "tags": "1C Sync",
            "handle": product['name'].replace(" ", "-").lower(),
            "status": "active",
            "variants": [
                {
                    "sku": product['id'],
                    "price": f"{price_float:.2f}",
                    "option1": "Default Title",
                    "inventory_management": "shopify",
                    "inventory_quantity": quantity_int,
                    "requires_shipping": True
                }
            ],
            "options": [
                {"name": "Title", "position": 1, "values": ["Default Title"]}
            ]
        }
    }
    return shopify_product

def send_to_shopify(shopify_product, existing_products, all_skus, all_handles):
    sku = shopify_product['product']['variants'][0]['sku']
    new_price = shopify_product['product']['variants'][0]['price']
    new_quantity = shopify_product['product']['variants'][0]['inventory_quantity']
    handle = shopify_product['product']['handle']

    print(f"🔍 SKU: {sku} | Handle: {handle}")

    # SKU вже існує — оновлюємо
    if sku in all_skus:
        existing_product = next((p for p in existing_products if any(v['sku'] == sku for v in p['variants'])), None)
        if existing_product:
            print(f"🔁 SKU {sku} існує. Оновлюємо варіант...")
            variant = next(v for v in existing_product['variants'] if v['sku'] == sku)
            update_shopify_variant(variant['id'], variant['inventory_item_id'], new_price, new_quantity)
        return

    # handle вже є — не створюємо
    if handle in all_handles:
        print(f"⚠️ Пропуск — handle вже існує: {handle}")
        return

    # Створюємо товар
    print(f"🆕 Створення товару SKU {sku}, handle '{handle}'")
    time.sleep(0.6)
    shopify_url = f"{shopify_store_url}/admin/api/2024-01/products.json"
    headers = {"Content-Type": "application/json", "X-Shopify-Access-Token": access_token}
    response = requests.post(shopify_url, headers=headers, json=shopify_product)
    if response.status_code == 201:
        new_product = response.json()['product']
        print(f"✅ Створено: handle={new_product['handle']}")
        existing_products.append(new_product)
        all_skus.add(new_product['variants'][0]['sku'])
        all_handles.add(new_product['handle'])
    else:
        print(f"❌ Помилка створення: {response.status_code}, {response.json()}")

# ================== ЛОГІКА СИНХРОНІЗАЦІЇ ==================
last_run_time = None  # для статусу

def scheduled_sync():
    """Фонова синхронізація + фіксація часу запуску."""
    global last_run_time
    with app.app_context():
        print("🔄 Запуск фонової синхронізації...")
        last_run_time = datetime.now(timezone.utc)  # 👈 без deprecated utcnow()
        _ = sync_products()
        print("✅ Фонову синхронізацію завершено.")

@app.route('/sync_products')
def sync_products():
    products = fetch_products()
    existing_products = fetch_all_shopify_products()

    if not products:
        return jsonify({'status': 'No products found or an error occurred.'})

    all_skus = {v['sku'] for p in existing_products for v in p.get('variants', [])}
    all_handles = {p.get('handle') for p in existing_products}

    print(f"Знайдено товарів в 1С: {len(products)}")
    for product in products:
        if not isinstance(product, dict):
            print(f"Пропуск некоректного запису: {product}")
            continue

        shopify_product = transform_to_shopify_format(product)
        if shopify_product:
            send_to_shopify(shopify_product, existing_products, all_skus, all_handles)
        else:
            print(f"Пропуск без 'ТОВ' ціни: {product.get('id', 'невідомий ID')}")

    return jsonify({'status': 'finished'})

# ================== ВЕБ-ІНТЕРФЕЙС (UA, MIXOpro.Ukraine) ==================
INDEX_HTML = """
<!doctype html>
<html lang="uk">
<head>
  <meta charset="utf-8">
  <title>MIXOpro.Ukraine — 1С ↔ Shopify Sync</title>
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <meta name="theme-color" content="#0b0f19">
  <style>
    :root {
      --bg: #0b0f19;
      --card: #111827;
      --muted: #9CA3AF;
      --text: #E5E7EB;
      --accent: #38bdf8;
      --border: #1f2937;
      --danger: #ef4444;
      --shadow: 0 10px 30px rgba(0,0,0,.35);
      --radius: 16px;
    }
    * { box-sizing: border-box; }
    html, body { height: 100%; }
    body {
      margin: 0;
      background: radial-gradient(1200px 800px at 100% -20%, #0b1224 0%, #0b0f19 55%), var(--bg);
      color: var(--text);
      font-family: Inter, -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
      -webkit-font-smoothing: antialiased;
      -moz-osx-font-smoothing: grayscale;
      padding-top: env(safe-area-inset-top);
      padding-left: env(safe-area-inset-left);
      padding-right: env(safe-area-inset-right);
    }
    header {
      position: sticky; top: 0; z-index: 50;
      backdrop-filter: blur(10px);
      background: rgba(11,15,25,.6);
      border-bottom: 1px solid var(--border);
    }
    .wrap {
      max-width: 900px;
      margin: 0 auto;
      padding: 18px;
    }
    .brand { display: flex; align-items: center; gap: 12px; font-weight: 800; letter-spacing: .3px; }
    .brand .dot {
      width: 12px; height: 12px; border-radius: 999px;
      background: linear-gradient(45deg,var(--accent),#60a5fa);
      box-shadow: 0 0 24px rgba(56,189,248,.6);
      flex: 0 0 12px;
    }
    .card {
      background: linear-gradient(180deg, rgba(17,24,39,.9), rgba(17,24,39,.7));
      border: 1px solid var(--border);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
    }
    .inner { padding: 18px; }
    h1 { font-size: 22px; margin: 0 0 8px; line-height: 1.2; }
    p.muted { color: var(--muted); margin: 0; }
    .actions {
      display: flex; flex-wrap: wrap; gap: 10px; margin-top: 14px;
    }
    button {
      appearance: none; border: 0; border-radius: 12px; padding: 14px 16px;
      font-weight: 700; cursor: pointer; font-size: 16px; line-height: 1.1;
      -webkit-tap-highlight-color: transparent;
      transition: transform .06s ease, opacity .2s ease, box-shadow .2s ease;
      touch-action: manipulation;
    }
    button:active { transform: translateY(1px) scale(.99); }
    .btn-primary { background: linear-gradient(45deg, var(--accent), #60a5fa); color: #001018; }
    .btn-ghost { background: #101623; color: var(--text); border: 1px solid var(--border); }
    .kpis { display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; margin-top: 14px; }
    .kpi { background: #0e1524; border: 1px solid var(--border); border-radius: 12px; padding: 12px; }
    .kpi .label { color: var(--muted); font-size: 12px; }
    .kpi .value { font-size: 18px; font-weight: 800; margin-top: 4px; }
    .mono {
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
      white-space: pre; overflow-x: auto; /* 👈 на мобилках можно скроллить лог */
      margin-top: 14px; padding-bottom: 4px;
    }
    footer { margin: 36px 0; text-align: center; color: var(--muted); padding-bottom: env(safe-area-inset-bottom); }

    /* ====== Мобильная адаптация ====== */
    @media (max-width: 640px) {
      .wrap { padding: 12px; }
      .inner { padding: 14px; }
      h1 { font-size: 20px; }
      .actions { gap: 8px; }
      .actions button {
        flex: 1 1 100%; /* кнопки — во всю ширину */
      }
      .kpis {
        grid-template-columns: 1fr; /* KPI — одной колонкой */
        gap: 8px;
      }
      .kpi { padding: 10px; }
      .kpi .value { font-size: 16px; }
      .brand { gap: 10px; font-size: 15px; }
      .card { border-radius: 14px; }
      button { padding: 13px 14px; font-size: 15px; } /* чуть компактнее */
    }

    /* Поддержка системной темы (если захочешь светлую) */
    @media (prefers-color-scheme: light) {
      :root {
        --bg: #f3f4f6;
        --card: #ffffff;
        --text: #0b0f19;
        --muted: #4b5563;
        --border: #e5e7eb;
      }
      body { background: var(--bg); }
      .card { background: #fff; }
      .btn-ghost { background: #fff; }
    }
  </style>
</head>
<body>
  <header>
    <div class="wrap" style="display:flex;align-items:center;justify-content:space-between;">
      <div class="brand">
        <span class="dot"></span>
        <div>MIXOpro.Ukraine • <small>1С ↔ Shopify Sync</small></div>
      </div>
    </div>
  </header>

  <main class="wrap">
    <section class="card">
      <div class="inner">
        <h1>Керування синхронізацією</h1>
        <p class="muted">Запускайте оновлення вручну. Автосинхронізація працює кожні {{ schedule_minutes }} хв.</p>

        <div class="actions">
          <button id="run" class="btn-primary">🚀 Запустити зараз</button>
          <button id="refresh" class="btn-ghost">🔄 Оновити статус</button>
        </div>

        <div class="kpis">
          <div class="kpi">
            <div class="label">Базовий інтервал</div>
            <div class="value">{{ schedule_minutes }} хв</div>
          </div>
          <div class="kpi">
            <div class="label">Останній запуск</div>
            <div class="value"><span id="kpi-last">—</span></div>
          </div>
        </div>

        <div id="status" class="mono" aria-live="polite">Завантажую статус…</div>
      </div>
    </section>

    <footer>© {{ year }} MIXOpro.Ukraine — Синхронізація 1С ↔ Shopify</footer>
  </main>

  <script>
    const elStatus = document.getElementById('status');
    const elKpiLast = document.getElementById('kpi-last');

    function fmt(ts) { return ts ? new Date(ts).toLocaleString() : '—'; }

    async function loadStatus() {
      const r = await fetch('/status');
      const j = await r.json();
      elStatus.textContent =
        `Задача планувальника: ${j.job_exists ? 'активна' : 'відсутня'}\nОстанній запуск: ${fmt(j.last_run_time)}`;
      elKpiLast.textContent = fmt(j.last_run_time);
    }

    document.getElementById('refresh').addEventListener('click', loadStatus);

    document.getElementById('run').addEventListener('click', async () => {
      const btn = document.getElementById('run');
      btn.disabled = true; btn.textContent = '⏳ Запуск...';
      try {
        const r = await fetch('/run_sync', { method: 'POST' });
        const j = await r.json();
        alert(j.message);
      } catch (e) {
        alert('Помилка під час запуску');
      } finally {
        btn.disabled = false; btn.textContent = '🚀 Запустити зараз';
        loadStatus();
      }
    });

    loadStatus();
  </script>
</body>
</html>
"""

@app.route("/")
def index():
    return render_template_string(INDEX_HTML, schedule_minutes=SCHEDULE_MINUTES, year=datetime.utcnow().year)

@app.route("/status")
def status():
    job = scheduler.get_job(JOB_ID)
    next_run = job.next_run_time if job else None
    return jsonify({
        "last_run_time": last_run_time.isoformat() if last_run_time else None,  # 👈 tz-aware ISO
        "next_run_time": next_run.astimezone(timezone.utc).isoformat() if next_run else None,
        "job_exists": bool(job)
    })

@app.route("/run_sync", methods=["POST"])
def run_sync():
    """Ручний запуск синхронізації + зсув наступного автозапуску на повний інтервал."""
    scheduled_sync()

    # 👇 Надійний спосіб: пересоздаємо тригер зі start_date = now + interval
    next_start = datetime.now(timezone.utc) + timedelta(minutes=SCHEDULE_MINUTES)
    new_trigger = IntervalTrigger(
        minutes=SCHEDULE_MINUTES,
        start_date=next_start,
        timezone=timezone.utc,
    )

    job = scheduler.get_job(JOB_ID)
    if job:
        job.reschedule(new_trigger)
        msg = f"Синхронізацію виконано. Наступний автозапуск через {SCHEDULE_MINUTES} хв."
    else:
        scheduler.add_job(func=scheduled_sync, trigger=new_trigger, id=JOB_ID, replace_existing=True)
        msg = "Синхронізацію виконано. Задачу планувальника створено заново."
    return jsonify({"ok": True, "message": msg})

# ================== APSCHEDULER ==================
executors = {'default': ThreadPoolExecutor(20)}
scheduler = BackgroundScheduler(
    executors=executors,
    timezone=timezone.utc,
    job_defaults={"coalesce": False, "misfire_grace_time": 300},
)

initial_trigger = IntervalTrigger(
    minutes=SCHEDULE_MINUTES,
    start_date=datetime.now(timezone.utc) + timedelta(minutes=SCHEDULE_MINUTES),
    timezone=timezone.utc,
)
scheduler.add_job(func=scheduled_sync, trigger=initial_trigger, id=JOB_ID, replace_existing=True)
scheduler.start()

# ================== ENTRYPOINT ==================
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=False)