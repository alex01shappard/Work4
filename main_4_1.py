import msgpack
import json
import os

# Путь к файлу msgpack
input_file = 'data/4/_product_data.msgpack'  
output_file = 'data/4/_product_data.json'   

# Чтение и декодирование magpack файла
with open(input_file, 'rb') as f:
    data = msgpack.unpack(f, raw=False)

# Запись данных в json-файл
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)

print(f"Файл успешно преобразован в {output_file}")
