import sqlite3
import json
import os

# Подключение к SQLite и создание базы данных
db_name = "books.db"
conn = sqlite3.connect(db_name)
cursor = conn.cursor()

# Очистка существующих таблиц (если нужно избежать дублирования)
cursor.execute("DROP TABLE IF EXISTS books;")
cursor.execute("DROP TABLE IF EXISTS subitems;")

# Создание таблицы на основе структуры данных item.json
table_creation_query = """
CREATE TABLE IF NOT EXISTS books (
    title TEXT,
    author TEXT,
    genre TEXT,
    pages INTEGER,
    published_year INTEGER,
    isbn TEXT,
    rating REAL,
    views INTEGER
);
"""
cursor.execute(table_creation_query)

# Загрузка данных из item.json
file_path = 'data/1-2/item.json'
with open(file_path, 'r', encoding='utf-8') as file:
    books_data = json.load(file)

# Вставка данных в таблицу
insert_query = """
INSERT INTO books (title, author, genre, pages, published_year, isbn, rating, views)
VALUES (?, ?, ?, ?, ?, ?, ?, ?);
"""
for book in books_data:
    cursor.execute(insert_query, (
        book['title'], book['author'], book['genre'], book['pages'], 
        book['published_year'], book['isbn'], book['rating'], book['views']
    ))

conn.commit()

# Создание директории для выходных данных
output_dir_1 = "output_data/1"
os.makedirs(output_dir_1, exist_ok=True)

# Функция для выполнения заданий первого задания
VAR = 72
N = VAR + 10

# 1. Вывод первых (VAR+10) отсортированных строк
cursor.execute(f"SELECT * FROM books ORDER BY views DESC LIMIT {N}")
sorted_books = cursor.fetchall()
with open(os.path.join(output_dir_1, 'sorted_books.json'), 'w', encoding='utf-8') as file:
    json.dump(sorted_books, file, ensure_ascii=False, indent=4)

# 2. Вычисления для числового поля (views)
cursor.execute("SELECT SUM(views), MIN(views), MAX(views), AVG(views) FROM books")
stats = cursor.fetchone()

# 3. Частотность категориального поля (genre)
cursor.execute("SELECT genre, COUNT(*) FROM books GROUP BY genre")
genre_frequency = cursor.fetchall()

# 4. Фильтр и сортировка по критериям (например, rating > 3.0, сортировка по pages DESC)
cursor.execute(f"SELECT * FROM books WHERE rating > 3.0 ORDER BY pages DESC LIMIT {N}")
filtered_books = cursor.fetchall()
with open(os.path.join(output_dir_1, 'filtered_books.json'), 'w', encoding='utf-8') as file:
    json.dump(filtered_books, file, ensure_ascii=False, indent=4)

# Вывод результатов
print("Статистика по числовому полю views:")
print(f"Сумма: {stats[0]}, Мин: {stats[1]}, Макс: {stats[2]}, Среднее: {stats[3]:.2f}")

print("Частотность жанров:")
for genre, count in genre_frequency:
    print(f"{genre}: {count}")

# Загрузка данных из файла subitem.text
subitem_file_path = 'data/1-2/subitem.text'
with open(subitem_file_path, 'r', encoding='utf-8') as file:
    subitem_data = file.read().strip().split('=====')

# Создание таблицы для subitem.text
table_creation_query_subitems = """
CREATE TABLE IF NOT EXISTS subitems (
    title TEXT,
    price INTEGER,
    place TEXT,
    date TEXT
);
"""
cursor.execute(table_creation_query_subitems)

# Вставка данных в таблицу subitems
insert_query_subitems = """
INSERT INTO subitems (title, price, place, date)
VALUES (?, ?, ?, ?);
"""
for entry in subitem_data:
    try:
        fields = dict(line.split('::') for line in entry.strip().split('\n') if '::' in line)
        cursor.execute(insert_query_subitems, (
            fields['title'], int(fields['price']), fields['place'], fields['date']
        ))
    except Exception as e:
        print(f"Ошибка обработки записи: {entry.strip()}\nОшибка: {e}")

conn.commit()

# Создание директории для выходных данных задания 2
output_dir_2 = "output_data/2"
os.makedirs(output_dir_2, exist_ok=True)

# Запрос 1: Найти книги из books, у которых есть соответствие в subitems по title
query1 = """
SELECT books.title, books.author, books.genre, subitems.price, subitems.place
FROM books
JOIN subitems ON books.title = subitems.title;
"""
cursor.execute(query1)
result1 = cursor.fetchall()
with open(os.path.join(output_dir_2, 'query1_results.json'), 'w', encoding='utf-8') as file:
    json.dump(result1, file, ensure_ascii=False, indent=4)

# Запрос 2: Найти книги с максимальной ценой из subitems
query2 = """
SELECT books.title, books.author, subitems.price
FROM books
JOIN subitems ON books.title = subitems.title
ORDER BY subitems.price DESC LIMIT 1;
"""
cursor.execute(query2)
result2 = cursor.fetchall()
with open(os.path.join(output_dir_2, 'query2_results.json'), 'w', encoding='utf-8') as file:
    json.dump(result2, file, ensure_ascii=False, indent=4)

# Запрос 3: Найти книги, которые продавались только онлайн
query3 = """
SELECT books.title, books.author
FROM books
JOIN subitems ON books.title = subitems.title
WHERE subitems.place = 'online';
"""
cursor.execute(query3)
result3 = cursor.fetchall()
with open(os.path.join(output_dir_2, 'query3_results.json'), 'w', encoding='utf-8') as file:
    json.dump(result3, file, ensure_ascii=False, indent=4)

# Закрытие соединения
conn.close()
