import threading
import os
import time
import logging
from collections import defaultdict

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

# Функція для пошуку ключових слів у файлах за допомогою Боєра-Мура
def search_keywords_in_files(files, keywords, result_dict, lock):
    for file_path in files:
        try:
            logging.info(f"Обробляємо файл: {file_path}")
            search_results = bm_search(file_path, keywords)
            with lock:  # Забезпечуємо безпечний доступ до словника результатів
                for keyword, paths in search_results.items():
                    result_dict[keyword].extend(paths)
        except FileNotFoundError:
            logging.error(f"Файл не знайдено: {file_path}")
        except Exception as e:
            logging.error(f"Помилка при обробці файлу {file_path}: {str(e)}")

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

# Основна функція для паралельної обробки файлів
def threads_search(files, keywords, num_threads):
    # Стартуємо таймер
    start_time = time.time()
    
    # Розподіляємо файли між потоками
    chunk_size = len(files) // num_threads
    threads = []
    result_dict = defaultdict(list)
    lock = threading.Lock()  # Створюємо лок для синхронізації доступу до результатів

    for i in range(num_threads):
        start_index = i * chunk_size
        end_index = (i + 1) * chunk_size if i != num_threads - 1 else len(files)
        thread_files = files[start_index:end_index]
        thread = threading.Thread(target=search_keywords_in_files, args=(thread_files, keywords, result_dict, lock))
        threads.append(thread)
        thread.start()

    # Очікуємо завершення всіх потоків
    for thread in threads:
        thread.join()

    # Виводимо час виконання
    logging.info(f"Час виконання: {time.time() - start_time} секунд")
    
    return result_dict

# Основний блок програми
if __name__ == "__main__":
    # Вказуємо шлях до директорії та ключові слова
    directory = "./some_files"  
    keywords = ["summer", "large", "level", "fact"]

    # Отримуємо список файлів із директорії
    files = get_files_from_directory(directory)

    # Якщо файли знайдено, викликаємо паралельний пошук
    if files:
        results = threads_search(files, keywords, num_threads=4)
        
        # Виводимо результати
        for keyword, file_list in results.items():
            logging.info(f"Ключове слово '{keyword}' знайдено в файлах: {file_list}")
            
    else:
        logging.info("Файли не знайдено для обробки.")
