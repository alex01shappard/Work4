import sqlite3
import json
import os

# Функция для создания базы данных
def initialize_database():
    output_dir = "output_data/4"
    os.makedirs(output_dir, exist_ok=True)
    db_path = os.path.join(output_dir, 'products.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Создаем таблицу товаров
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            price REAL,
            quantity INTEGER,
            category TEXT,
            from_city TEXT,
            is_available BOOLEAN,
            views INTEGER,
            update_count INTEGER DEFAULT 0
        )
    ''')

    conn.commit()
    return conn

# Функция для загрузки данных о товарах из JSON файла
def load_product_data(conn, product_file):
    cursor = conn.cursor()

    with open(product_file, 'r', encoding='utf-8') as f:
        products = json.load(f)

    for product in products:
        cursor.execute('''
            INSERT OR IGNORE INTO products (name, price, quantity, category, from_city, is_available, views)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            product.get('name'),
            product.get('price'),
            product.get('quantity'),
            product.get('category'),
            product.get('fromCity'),
            product.get('isAvailable'),
            product.get('views')
        ))

    conn.commit()

# Функция для обработки обновлений из текстового файла
def apply_updates(conn, updates_file):
    cursor = conn.cursor()
    missing_products = []

    with open(updates_file, 'r', encoding='utf-8') as f:
        updates = f.read().strip().split('=====\n')

    for update in updates:
        lines = update.strip().split('\n')
        data = {}
        for line in lines:
            if '::' in line:
                key, value = line.split('::', 1)
                data[key] = value
        
        name = data.get('name')
        method = data.get('method')
        param = data.get('param')

        if not name or not method:
            print(f"Skipping invalid update: {data}")
            continue

        try:
            cursor.execute("SELECT id, price, quantity FROM products WHERE name = ?", (name,))
            product = cursor.fetchone()

            if not product:
                missing_products.append(name)
                continue

            product_id, price, quantity = product

            if method == 'price_abs':
                new_price = max(0, price + float(param))
                cursor.execute("UPDATE products SET price = ?, update_count = update_count + 1 WHERE id = ?", (new_price, product_id))

            elif method == 'price_percent':
                new_price = max(0, price * (1 + float(param)))
                cursor.execute("UPDATE products SET price = ?, update_count = update_count + 1 WHERE id = ?", (new_price, product_id))

            elif method == 'quantity_add':
                new_quantity = quantity + int(param)
                cursor.execute("UPDATE products SET quantity = ?, update_count = update_count + 1 WHERE id = ?", (new_quantity, product_id))

            elif method == 'quantity_sub':
                new_quantity = max(0, quantity - int(param))
                cursor.execute("UPDATE products SET quantity = ?, update_count = update_count + 1 WHERE id = ?", (new_quantity, product_id))

            elif method == 'remove':
                cursor.execute("DELETE FROM products WHERE id = ?", (product_id,))

            elif method == 'available':
                cursor.execute("UPDATE products SET is_available = ?, update_count = update_count + 1 WHERE id = ?", (param.lower() == 'true', product_id))

            conn.commit()

        except Exception as e:
            print(f"Error applying update for {name}: {e}")
    
    # Сохранение отсутствующих товаров в файл
    output_dir = "output_data/4"
    missing_products_path = os.path.join(output_dir, 'missing_products.json')
    with open(missing_products_path, 'w', encoding='utf-8') as f:
        json.dump(missing_products, f, indent=4)


# Функция для выполнения запросов
def execute_queries(conn):
    cursor = conn.cursor()

    output_dir = "output_data/4"
    os.makedirs(output_dir, exist_ok=True)

    # Топ-10 самых обновляемых товаров
    top_products_path = os.path.join(output_dir, 'top_products.json')
    with open(top_products_path, 'w', encoding='utf-8') as f:
        cursor.execute("SELECT name, update_count FROM products ORDER BY update_count DESC LIMIT 10")
        top_products = [{"name": row[0], "update_count": row[1]} for row in cursor.fetchall()]
        json.dump(top_products, f, indent=4)

    # Анализ цен товаров
    price_analysis_path = os.path.join(output_dir, 'price_analysis.json')
    with open(price_analysis_path, 'w', encoding='utf-8') as f:
        cursor.execute('''
            SELECT category, SUM(price), MIN(price), MAX(price), AVG(price), COUNT(*)
            FROM products
            GROUP BY category
        ''')
        price_analysis = [
            {
                "category": row[0],
                "total_price": row[1],
                "min_price": row[2],
                "max_price": row[3],
                "average_price": row[4],
                "product_count": row[5]
            } for row in cursor.fetchall()
        ]
        json.dump(price_analysis, f, indent=4)

    # Анализ остатков товаров
    quantity_analysis_path = os.path.join(output_dir, 'quantity_analysis.json')
    with open(quantity_analysis_path, 'w', encoding='utf-8') as f:
        cursor.execute('''
            SELECT category, SUM(quantity), MIN(quantity), MAX(quantity), AVG(quantity)
            FROM products
            GROUP BY category
        ''')
        quantity_analysis = [
            {
                "category": row[0],
                "total_quantity": row[1],
                "min_quantity": row[2],
                "max_quantity": row[3],
                "average_quantity": row[4]
            } for row in cursor.fetchall()
        ]
        json.dump(quantity_analysis, f, indent=4)

    # Произвольный запрос
    custom_query_path = os.path.join(output_dir, 'custom_query.json')
    with open(custom_query_path, 'w', encoding='utf-8') as f:
        cursor.execute("SELECT name, price FROM products WHERE price > 1000")
        custom_query_results = [{"name": row[0], "price": row[1]} for row in cursor.fetchall()]
        json.dump(custom_query_results, f, indent=4)

if __name__ == "__main__":
    product_file = "data/4/_product_data.json"
    updates_file = "data/4/_update_data.text"

    # Инициализация базы данных
    conn = initialize_database()

    # Загрузка данных о товарах
    load_product_data(conn, product_file)

    # Применение обновлений
    apply_updates(conn, updates_file)

    # Выполнение запросов
    execute_queries(conn)

    conn.close()
