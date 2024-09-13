import os
import time
import logging
from collections import defaultdict
from multiprocessing import Process, Queue, Lock, cpu_count



# Налаштовуємо логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Функція для побудови таблиці зсувів (алгоритм Боєра-Мура)
def build_shift_table(pattern):
    table = {}
    length = len(pattern)
    for index, char in enumerate(pattern[:-1]):
        table[char] = length - index - 1
    table.setdefault(pattern[-1], length)
    return table

# Алгоритм Боєра-Мура для пошуку підрядка
def bm_search(file, patterns_list):
    # Функція для зчитування тексту з файлу
    def read_file(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()

    # Читаємо файл
    text = read_file(file)
    result_dict = defaultdict(list)

    for pattern in patterns_list:
        shift_table = build_shift_table(pattern)
        i = 0
        while i <= len(text) - len(pattern):
            j = len(pattern) - 1
            while j >= 0 and text[i + j] == pattern[j]:
                j -= 1
            if j < 0:
                # Підрядок знайдено
                result_dict[pattern].append(str(file))
                break
            i += shift_table.get(text[i + len(pattern) - 1], len(pattern))

    return result_dict

# Функція для пошуку ключових слів у файлах 
def search_keywords_in_files(files, keywords, queue, lock):
    results = defaultdict(list)
    for file_path in files:
        try:
            logging.info(f"Обробляємо файл: {file_path}")
            search_results = bm_search(file_path, keywords)
            for keyword, paths in search_results.items():
                results[keyword].extend(paths)
        except FileNotFoundError:
            logging.error(f"Файл не знайдено: {file_path}")
        except Exception as e:
            logging.error(f"Помилка при обробці файлу {file_path}: {str(e)}")
    
    with lock:
        queue.put(results)

# Функція для отримання списку файлів із директорії
def get_files_from_directory(directory, extension='.txt'):
    try:
        files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(extension)]
        logging.info(f"Знайдено {len(files)} файлів у директорії {directory}")
        return files
    except FileNotFoundError:
        logging.error(f"Директорія не знайдена: {directory}")
        return []
    except Exception as e:
        logging.error(f"Помилка при читанні директорії {directory}: {str(e)}")
        return []

# Функція для багатопроцесорної обробки файлів
def multiprocessing_search(files, keywords, num_processes=None):
    # Стартуємо таймер
    start_time = time.time()

    if num_processes is None:
        num_processes = cpu_count()
    logging.info(f"Кількість ядер процесора: {num_processes}")    
    
    # Розподіляємо файли між процесами
    chunk_size = len(files) // num_processes
    processes = []
    queue = Queue()
    lock = Lock()  

    for i in range(num_processes):
        start_index = i * chunk_size
        end_index = (i + 1) * chunk_size if i != num_processes - 1 else len(files)
        process_files = files[start_index:end_index]
        process = Process(target=search_keywords_in_files, args=(process_files, keywords, queue, lock))
        processes.append(process)
        process.start()

    # Очікуємо завершення всіх процесів
    for process in processes:
        process.join()

    # Збираємо результати з черги
    final_results = defaultdict(list)
    while not queue.empty():
        result = queue.get()
        for keyword, paths in result.items():
            final_results[keyword].extend(paths)
    
    # Виводимо час виконання
    logging.info(f"Час виконання: {time.time() - start_time} секунд")
    
    return final_results

# Основний блок програми
if __name__ == "__main__":
    # Вказуємо шлях до директорії та ключові слова
    directory = "./some_files"  
    keywords = ["summer", "large", "level", "fact"]

    # Отримуємо список файлів із директорії
    files = get_files_from_directory(directory)

    # Якщо файли знайдено, викликаємо багатопроцесорний пошук
    if files:
        results = multiprocessing_search(files, keywords, num_processes=None)

        # Виводимо результати
        for keyword, file_list in results.items():
            logging.info(f"Ключове слово '{keyword}' знайдено в файлах: {file_list}")
    else:
        logging.info("Файли не знайдено для обробки.")

