import sqlite3
import pandas as pd
import json

# Пути к файлам
file_pkl = "data/3/_part_1.pkl"
file_text = "data/3/_part_2.text"
output_folder = "output_data/3/"

# 1. Загрузка данных
# Файл _part_1.pkl
data_pkl = pd.read_pickle(file_pkl)
data_pkl = pd.DataFrame(data_pkl)  # Преобразуем список в DataFrame

# Файл _part_2.text
def parse_text_entry(entry):
    """Парсит записи из файла _part_2.text в словарь."""
    import re
    return {k: v for k, v in re.findall(r'(\w+)::([^:\n]+)', entry)}

with open(file_text, 'r', encoding='utf-8') as f:
    entries = [parse_text_entry(entry.strip()) for entry in f.read().split('=====') if entry.strip()]

data_text = pd.DataFrame(entries)

# Приведение данных к общему формату
data_pkl['duration_ms'] = pd.to_numeric(data_pkl['duration_ms'], errors='coerce')
data_pkl['year'] = pd.to_numeric(data_pkl['year'], errors='coerce')
data_pkl['tempo'] = pd.to_numeric(data_pkl['tempo'], errors='coerce')

data_text['duration_ms'] = pd.to_numeric(data_text['duration_ms'], errors='coerce')
data_text['year'] = pd.to_numeric(data_text['year'], errors='coerce')
data_text['tempo'] = pd.to_numeric(data_text['tempo'], errors='coerce')

# Объединение данных
common_columns = ['artist', 'song', 'duration_ms', 'year', 'tempo', 'genre']
merged_data = pd.concat([data_pkl[common_columns], data_text[common_columns]])

# 2. Создание базы данных и таблицы
conn = sqlite3.connect(f"{output_folder}/songs.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS songs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    artist TEXT,
    song TEXT,
    duration_ms INTEGER,
    year INTEGER,
    tempo REAL,
    genre TEXT
);
""")

# 3. Запись данных в таблицу
merged_data.to_sql('songs', conn, if_exists='replace', index=False)

# 4. Выполнение запросов
# Запрос 1: Первые VAR+10 строк в json
query1 = "SELECT * FROM songs ORDER BY year LIMIT 82;"
result1 = pd.read_sql(query1, conn)
result1.to_json(f"{output_folder}/query1.json", orient='records', force_ascii=False)

# Запрос 2: Сумма, мин, макс, среднее для duration_ms
query2 = """
SELECT SUM(duration_ms) AS sum_duration,
       MIN(duration_ms) AS min_duration,
       MAX(duration_ms) AS max_duration,
       AVG(duration_ms) AS avg_duration
FROM songs;
"""
result2 = pd.read_sql(query2, conn)
result2.to_csv(f"{output_folder}/query2.csv", index=False)

# Запрос 3: Частота встречаемости жанров
query3 = "SELECT genre, COUNT(*) AS frequency FROM songs GROUP BY genre ORDER BY frequency DESC;"
result3 = pd.read_sql(query3, conn)
result3.to_csv(f"{output_folder}/query3.csv", index=False)

# Запрос 4: Первые VAR+15 строк, отфильтрованные и отсортированные
query4 = "SELECT * FROM songs WHERE tempo > 100 ORDER BY tempo DESC LIMIT 87;"
result4 = pd.read_sql(query4, conn)
result4.to_json(f"{output_folder}/query4.json", orient='records', force_ascii=False)

# Закрытие соединения
conn.close()

print("Запросы выполнены и сохранены в output_data/3/")
