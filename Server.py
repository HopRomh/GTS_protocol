import socket
import mysql.connector
import csv
from egts import *
import time


class Egtspy:
    def __init__(self, data):
        self.data = data
    
    def parse_data(self):
        # Пример парсинга данных EGTS
        # Здесь предполагается, что данные приходят в формате, где speed можно извлечь.
        # В данном примере, мы просто возвращаем фиктивные данные
        return {
            'timestamp': 1627848167.123,
            'some_field': 'example_data',
            'speed': 50.5  # Пример значения скорости
        }

# Настройки подключения к MySQL
db_config = {
    'host': "localhost",
    'user': "root",
    'password': "251447",
    'database': "maha"
}

# Создание базы данных и таблицы (если не существует)
def initialize_database():
    try:
        with mysql.connector.connect(
            host="localhost",
            user="root",
            password="251447"
        ) as db:
            cursor = db.cursor()
            cursor.execute("CREATE DATABASE IF NOT EXISTS maha")
        
        with mysql.connector.connect(**db_config) as db:
            cursor = db.cursor()
            cursor.execute("USE maha")
            cursor.execute("DROP TABLE IF EXISTS NameTable")
            cursor.execute("""
            CREATE TABLE NameTable (
                id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp DOUBLE NOT NULL,
                data VARCHAR(255) NOT NULL,
                speed DOUBLE
            )
            """)
            db.commit()
            print("Таблица NameTable успешно создана.")
            
            # Проверка структуры таблицы
            cursor.execute("DESCRIBE NameTable")
            for row in cursor.fetchall():
                print(row)
    except mysql.connector.Error as err:
        print(f"Ошибка при инициализации базы данных: {err}")

initialize_database()

# Функция для сохранения данных в MySQL
def save_to_mysql(timestamp, data, speed):
    try:
        with mysql.connector.connect(**db_config) as db:
            cursor = db.cursor()
            query = "INSERT INTO NameTable (timestamp, data, speed) VALUES (%s, %s, %s)"
            cursor.execute(query, (timestamp, data, speed))
            db.commit()
            print(f"Данные сохранены в MySQL: {timestamp}, {data}, {speed}")
    except mysql.connector.Error as err:
        print(f"Ошибка при сохранении данных: {err}")



# Функция для чтения данных из CSV-файла
def read_csv_file(file_path):
    with open(file_path, newline='') as egts_packages:     #csvfile
        csv_reader = csv.reader(egts_packages)
        for row in csv_reader:
            data = ''.join(row)
            yield data



# Создание сокета для прослушивания порта
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(('0.0.0.0', 12349))  
server_socket.listen(5)

print("Сервер запущен и слушает порт 12349")

# Чтение и обработка данных из CSV-файла
# csv_file_path = 'egts_packages.csv'
# for raw_data in read_csv_file(csv_file_path):
#     print(f"Чтение данных из CSV: {raw_data}")
#     egts_parser = Egtspy(raw_data)
#     parsed_data = egts_parser.parse_data()
#     print(f"Парсинг данных: {parsed_data}")
#     save_to_mysql(parsed_data['timestamp'], parsed_data['some_field'], parsed_data['speed'])

# Обработка данных из сокета
while True:
    client_socket, addr = server_socket.accept()
    print(f"Подключен клиент: {addr}")
    
 
    packet = client_socket.recv(65535)
            
    if packet == b'':
        print(f"{addr[0]}:{addr[1]} => Disconnected")
        
        client_socket.close()
            
    parsed_packet = Egts(packet)
    for subrecord in parsed_packet.records[0].subrecords:
        if subrecord.type == 16:

            save_to_mysql(time.time(),f"{subrecord.lat}, {subrecord.long}" , subrecord.speed)

        
    client_socket.close()
