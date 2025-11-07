import asyncio
import argparse
import logging
import sys
from aiopath import AsyncPath
from typing import List

# Налаштування логування помилок
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

async def copy_file(file_path: AsyncPath, output_dir: AsyncPath):
    '''
    Асинхронно копіює файл у папку призначення, 
    створюючи підпапку на основі розширення файлу
    '''
    try:
        # Визначаємо розширення або ставимо 'no_extension'
        extension = file_path.suffix[1:].lower() if file_path.suffix else "no_extension"

        # Частково створюємо асинхронний шлях до цільової підпапки
        target_subdir = output_dir / extension

        # Асинхронно створюємо підпапку, якщо вона не існує
        await target_subdir.mkdir(exist_ok=True, parents=True)

        # Визначаємо повний шлях до нового файлу
        target_file_path = target_subdir / file_path.name

        # Асинхронно читаємо вміст файлу
        content = await file_path.read_bytes()

        # Асинхронно записуємо вміст у новий файл
        await target_file_path.write_bytes(content)

        logging.info(f"Скопійовано: {file_path.name} -> {target_subdir.name}/")

    except (IOError, OSError) as e:
        logging.error(f"Не вдалося скопіювати файл {file_path}: {e}")
    except Exception as e:
        logging.error(f" Неочікувана поведінка при роботі з файлом {file_path}: {e}")

async def read_folder(current_path: AsyncPath, output_dir: AsyncPath, task_list: List[asyncio.Task]):
    '''
    Рекурсивно читає всі файли у вихідній папці та її підпапках
    Для кожного знайденого файлу створює Task на копіювання
    '''
    try:
        # Асинхронно ітеруємо вміст поточної папки
        async for item in current_path.iterdir():
            if await item.is_dir():
                # Якщо це папка рекурсивно викликаємо себе
                await read_folder(item, output_dir, task_list)
            elif await item.is_file():
                # Якщо це файл, додаємо завдання на копіювання до списку
                task = asyncio.create_task(copy_file(item, output_dir))
                task_list.append(task)
                 
    except (IOError, OSError) as e:
        logging.error(f"Не вдалося прочитати папку {current_path}: {e}")

async def main():
    # Створення об'єкта ArgumentParser
    parser = argparse.ArgumentParser(
        description="Асинхронне сортування файлів за розширеннями"
    )

    # Додавання необхідних аргументів
    parser.add_argument(
        "-s", "--source",
        type=str,
        required=True,
        help="Вихідна папка (source folder)"
    )
    parser.add_argument(
        "-o", "--output",
        type=str,
        required=True,
        help="Папка призначення (output folder)"
    )

    args = parser.parse_args()

    # Ініціалізація асинхронних шляхів
    source_dir = AsyncPath(args.source)
    output_dir = AsyncPath(args.output)

    # Перевірка існування вихідної папки
    if not await source_dir.is_dir():
        logging.error(f"Вихідна папка не існує: {source_dir}")
        return
    
    logging.info(f"Початок сканування папки: {source_dir}")

    copy_tasks = []

    # Запуск асинхронної функції read_folder
    await read_folder(source_dir, output_dir, copy_tasks)

    if not copy_tasks:
        logging.info("Файлів для копіювання не знайдено")
        return
    
    logging.info(f"Знайдено {len(copy_tasks)} файлів. Початок асинхронного копіювання...")

    # Очікуємо завершення всіх завдань на копіювання
    await asyncio.gather(*copy_tasks)

    logging.info("Сортування файлів завершено")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Процес сортування перервано користувачем")