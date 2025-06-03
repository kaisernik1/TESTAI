import re
import csv
from datetime import datetime
import os
import ast

def parse_log_file(input_file, output_file):
    # Регулярные выражения для поиска
    request_pattern = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}).*?Incoming request: UID (\d+)"
    scraping_pattern = r"Received scraping request: (\d+) videos for query '([^']+)'"
    topic_pattern = r"Random topic from list: (.*)"
    augmented_pattern = r"Augmented query: '.*?' -> '(.*?)(?<!\\)'\\"
    time_pattern = r"Time to (.*?): ([\d.]+)s"
    sorted_videos_pattern = r"Sorting videos by query relevance took.*?: \[(.*?)\]"
    
    try:
        print(f"Начинаем обработку файла: {input_file}")
        
        # Создаем CSV файл
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            # Записываем заголовки
            headers = [
                'UID', 'Timestamp', 'Status', 'Videos Count', 'Query', 'Random Topic', 
                'Augmented Query 1', 'Augmented Query 2',
                'Time to copy audio', 'Time to load and transform video',
                'Time to load and transform audio', 'Time to load and transform text',
                'Time to get ImageBind inputs', 'Time to get ImageBind embeddings'
            ]
            
            # Добавляем заголовки для видео ID, значений и флагов выбора
            for i in range(1, 13):  # Предполагаем максимум 12 видео
                headers.extend([f'Video ID {i}', f'Video Value {i}', f'Video Selected {i}'])
            
            writer.writerow(headers)
            
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
                            augmented_query1 = ""
                            augmented_query2 = ""
                            
                            # Инициализируем временные метрики
                            time_metrics = {
                                'copy audio': '',
                                'load and transform video': '',
                                'load and transform audio': '',
                                'load and transform text': '',
                                'get ImageBind inputs': '',
                                'get ImageBind embeddings': ''
                            }
                            
                            # Инициализируем список для хранения видео
                            video_data = []
                            selected_videos = []
                            
                            if i + 1 < len(lines):
                                next_line = lines[i + 1]
                                if "Not Blacklisting" in next_line:
                                    status = "not_blacklisted"
                                    # Ищем строки с информацией о скрапинге, теме и augmented queries
                                    # Ищем до следующей строки с Incoming request
                                    j = i + 2
                                    while j < len(lines) and "Incoming request" not in lines[j]:
                                        if "Received scraping request" in lines[j]:
                                            scraping_match = re.search(scraping_pattern, lines[j])
                                            if scraping_match:
                                                videos_count = scraping_match.group(1)
                                                query = scraping_match.group(2)
                                        elif "Random topic from list:" in lines[j]:
                                            topic_match = re.search(topic_pattern, lines[j])
                                            if topic_match:
                                                random_topic = topic_match.group(1).strip()
                                        elif "Augmented query:" in lines[j]:
                                            if not augmented_query1:
                                                aug_match = re.search(augmented_pattern, lines[j])
                                                if aug_match:
                                                    augmented_query1 = aug_match.group(1).strip()
                                            elif not augmented_query2:
                                                aug_match = re.search(augmented_pattern, lines[j])
                                                if aug_match:
                                                    augmented_query2 = aug_match.group(1).strip()
                                        elif "Time to" in lines[j]:
                                            time_match = re.search(time_pattern, lines[j])
                                            if time_match:
                                                metric_name = time_match.group(1)
                                                metric_value = time_match.group(2)
                                                if metric_name in time_metrics:
                                                    time_metrics[metric_name] = metric_value
                                        elif "Query relevance scores" in lines[j]:
                                            # Ищем массив видео через одну строку после Query relevance scores
                                            if j + 1 < len(lines):
                                                array_line = lines[j + 1].strip()
                                                # Убираем символ \ в конце строки, если он есть
                                                if array_line.endswith('\\'):
                                                    array_line = array_line[:-1]
                                                if "[" in array_line and "]" in array_line:
                                                    try:
                                                        video_list = ast.literal_eval(array_line)
                                                        if isinstance(video_list, list):
                                                            video_data = video_list
                                                            print(f"Найден массив видео: {video_data}")
                                                    except Exception as e:
                                                        print(f"Ошибка при парсинге массива видео: {e}")
                                                        print(f"Проблемная строка: {array_line}")
                                        elif "Sorting videos by query relevance took" in lines[j]:
                                            # Ищем список выбранных видео
                                            sorted_match = re.search(sorted_videos_pattern, lines[j])
                                            if sorted_match:
                                                selected_videos_str = sorted_match.group(1)
                                                selected_videos = [vid.strip("'") for vid in selected_videos_str.split(', ')]
                                                print(f"Найден список выбранных видео: {selected_videos}")
                                        j += 1
                                elif "Blacklisting" in next_line:
                                    status = "blacklisted"
                            
                            print(f"Извлечено: UID={uid}, Timestamp={timestamp}, Status={status}, Videos={videos_count}, Query={query}, Topic={random_topic}")
                            print(f"Augmented Query 1: {augmented_query1}")
                            print(f"Augmented Query 2: {augmented_query2}")
                            print("Временные метрики:", time_metrics)
                            print("Видео данные:", video_data)
                            print("Выбранные видео:", selected_videos)
                            
                            # Формируем строку для CSV
                            row = [
                                uid, timestamp, status, videos_count, query, random_topic,
                                augmented_query1, augmented_query2,
                                time_metrics['copy audio'],
                                time_metrics['load and transform video'],
                                time_metrics['load and transform audio'],
                                time_metrics['load and transform text'],
                                time_metrics['get ImageBind inputs'],
                                time_metrics['get ImageBind embeddings']
                            ]
                            
                            # Добавляем данные о видео и флаги выбора
                            for video_id, video_value in video_data:
                                row.extend([video_id, str(video_value), str(video_id in selected_videos).lower()])
                            
                            # Добавляем пустые значения, если видео меньше 12
                            while len(row) < len(headers):
                                row.extend(['', '', ''])
                            
                            writer.writerow(row)
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
