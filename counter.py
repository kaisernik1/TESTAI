import re
import csv
from datetime import datetime
import os

def parse_log_file(input_file, output_file):
    # Обновленное регулярное выражение для поиска timestamp и UID
    pattern = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}).*?Incoming request: UID (\d+)"
    
    try:
        print(f"Начинаем обработку файла: {input_file}")
        
        # Создаем CSV файл
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            # Записываем заголовки
            writer.writerow(['UID', 'Timestamp'])
            
            # Читаем и обрабатываем лог-файл
            with open(input_file, 'r', encoding='utf-8') as logfile:
                line_count = 0
                match_count = 0
                
                for line in logfile:
                    line_count += 1
                    if "Incoming request" in line:
                        print(f"Найдена строка с запросом: {line.strip()}")
                        match = re.search(pattern, line)
                        if match:
                            match_count += 1
                            timestamp = match.group(1)
                            uid = match.group(2)
                            print(f"Извлечено: UID={uid}, Timestamp={timestamp}")
                            writer.writerow([uid, timestamp])
                        else:
                            print(f"Не удалось извлечь данные из строки: {line.strip()}")
                
                print(f"\nСтатистика обработки:")
                print(f"Всего строк в файле: {line_count}")
                print(f"Найдено совпадений: {match_count}")
        
        if match_count > 0:
            print(f"CSV файл успешно создан: {output_file}")
        else:
            print("Внимание: CSV файл создан, но в нем только заголовки (не найдено совпадений)")
        
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")
        import traceback
        print(traceback.format_exc())

def main():
    # Путь к файлу логов
    log_file = "uploads/UID 122.txt"  # Измените на нужный путь
    # Путь для сохранения CSV
    output_file = "results.csv"
    
    if not os.path.exists(log_file):
        print(f"Ошибка: Файл {log_file} не найден")
        return
    
    parse_log_file(log_file, output_file)

if __name__ == "__main__":
    main()
