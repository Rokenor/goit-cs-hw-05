import string
import requests
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

def get_text(url):
    ''' Завантажує текст із заданої url-адреси'''
    try:
        response = requests.get(url)
        response.raise_for_status()  # Перевірка на помилки HTTP
        return response.text
    except requests.RequestException as e:
        print(f"Помилка читання URL {url}: {e}")
        return None

def remove_punctuation(text):
    '''Функція для видалення знаків пунктуації'''
    return text.translate(str.maketrans("", "", string.punctuation))

def map_function(word):
    '''Функція мапінгу: повертає кортеж (слово, 1)'''
    return word, 1

def shuffle_function(mapped_values):
    '''Функція шафлінгу: групує однакові слова'''
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()

def reduce_function(key_values):
    '''Функція редукції: підсумовує значення для кожного слова'''
    key, values = key_values
    return key, sum(values)

def map_reduce(text, search_words=None):
    '''Основна функція MapReduce'''
    
    # Видалення знаків пунктуації та приведення до нижнього регістру
    text = remove_punctuation(text)
    text = text.lower()
    words = text.split()

    # Якщо задано список слів для пошуку, враховувати тільки ці слова
    if search_words:
        words = [word for word in words if word in search_words]

    print(f"Починаємо MapReduce для {len(words)} слів...")

    # Паралельний Мапінг
    with ThreadPoolExecutor() as executor:
        mapped_values = list(executor.map(map_function, words))

    # Shuffle
    shuffled_values = shuffle_function(mapped_values)

    # Паралельна Редукція
    with ThreadPoolExecutor() as executor:
        reduced_values = list(executor.map(reduce_function, shuffled_values))

    return dict(reduced_values)

def visualize_top_words(word_counts, top_n=10):
    '''Візуалізує топ-N найчастіше вживаних слів'''

    # Сортуємо словник за значеннями (частотою) у зворотному порядку
    sorted_words = sorted(word_counts.items(), key=lambda item: item[1], reverse=True)

    # Беремо лише топ-N слів
    top_words = sorted_words[:top_n]

    if not top_words:
        print("Немає даних для візуалізації")
        return
    
    # Виводимо топ-слова у термінал
    print(f"\n--- Топ {top_n} найчастіше вживаних слів ---")
    for word, count in top_words:
        print(f"{word:<15} : {count}") 
    print(f"{"-"*40}\n")
    
    # Розділяємо слова та їх частоти для графіка
    words = [item[0] for item in top_words]
    counts = [item[1] for item in top_words]

    # Створення графіка
    plt.figure(figsize=(10, 6))
    # Використовуємо горизонтальний barh, щоб слова краще читались
    plt.barh(words[::-1], counts[::-1], color='skyblue') 
    plt.xlabel('Частота використання')
    plt.ylabel('Слова')
    plt.title(f'Топ {top_n} найчастіше вживаних слів')
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    # URL тексту
    url = "https://gutenberg.net.au/ebooks01/0100021.txt"

    print(f"Завантаження тексту з {url}...")
    text = get_text(url)

    if text:
        # Виконання MapReduce на вхідному тексті
        result = map_reduce(text)

        print("Аналіз завершено. Будуємо графік...")
        # Візуалізація топ-10 слів
        visualize_top_words(result, top_n=10)
    else:
        print("Помилка: Не вдалося отримати вхідний текст.")