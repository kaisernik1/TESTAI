import re
import csv
from datetime import datetime
import os

def parse_log_file(input_file, output_file):
    # Регулярные выражения для поиска
    request_pattern = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}).*?Incoming request: UID (\d+)"
    scraping_pattern = r"Received scraping request: (\d+) videos for query '([^']+)'"
    topic_pattern = r"Random topic from list: (.*)"
    
    try:
        print(f"Начинаем обработку файла: {input_file}")
        
        # Создаем CSV файл
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            # Записываем заголовки
            writer.writerow(['UID', 'Timestamp', 'Status', 'Videos Count', 'Query', 'Random Topic'])
            
            # Читаем и обрабатываем лог-файл
            with open(input_file, 'r', encoding='utf-8') as logfile:
                lines = logfile.readlines()
                line_count = len(lines)
                match_count = 0
                
                for i in range(len(lines)):
                    line = lines[i]
                    if "Incoming request" in line:
                        print(f"Найдена строка с запросом: {line.strip()}")
                        match = re.search(request_pattern, line)
                        if match:
                            match_count += 1
                            timestamp = match.group(1)
                            uid = match.group(2)
                            
                            # Проверяем следующую строку на наличие статуса blacklist
                            status = "not_blacklisted"  # значение по умолчанию
                            videos_count = ""
                            query = ""
                            random_topic = ""
                            
                            if i + 1 < len(lines):
                                next_line = lines[i + 1]
                                if "Not Blacklisting" in next_line:
                                    status = "not_blacklisted"
                                    # Ищем строки с информацией о скрапинге и случайной теме
                                    for j in range(i + 2, min(i + 10, len(lines))):  # ищем в следующих 10 строках
                                        if "Received scraping request" in lines[j]:
                                            scraping_match = re.search(scraping_pattern, lines[j])
                                            if scraping_match:
                                                videos_count = scraping_match.group(1)
                                                query = scraping_match.group(2)
                                        elif "Random topic from list:" in lines[j]:
                                            topic_match = re.search(topic_pattern, lines[j])
                                            if topic_match:
                                                random_topic = topic_match.group(1).strip()
                                elif "Blacklisting" in next_line:
                                    status = "blacklisted"
                            
                            print(f"Извлечено: UID={uid}, Timestamp={timestamp}, Status={status}, Videos={videos_count}, Query={query}, Topic={random_topic}")
                            writer.writerow([uid, timestamp, status, videos_count, query, random_topic])
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
