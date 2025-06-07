# 🧠 Async RQ Worker

Асинхронный Redis-воркер для обработки задач из очередей.
Используется для создания надёжного бэкенда в микросервисной 
архитектуре, поддерживает логирование и кастомную обработку задач.

---

## 🚀 Назначение

Этот воркер предназначен для:

- Получения задач из Redis-очередей (`blpop`)
- Асинхронной обработки данных через кастомную функцию
- Логирования и безопасного выполнения с повторными попытками

---

## 🧱 Архитектура

- **Redis** — очередь задач с приоритетом обработки.
- **FastAPI/DRF** — внешний API может класть задачи в Redis.
- **async_rq_worker** — обрабатывает задачи, вызывает кастомную функцию.
---
## 📌 Особенности

- Поддержка нескольких очередей
- Безопасная сериализация и десериализация задач
- Гибкая интеграция с кастомной логикой
- Обработка исключений и повторный запуск
- Лёгкая масштабируемость
---

## 📦 Установка

```bash
pip install -r requirements.txt
```

---

## 🧪 Пример использования

```python
import asyncio
from redis.asyncio import Redis
from init_worker import AsyncRQWorker

async def example_function(task_id: str, data: dict) -> None:
    print(f'Обработка задачи {task_id}: {data}')

worker = AsyncRQWorker(
    redis_client=Redis(host='localhost', port=6379, decode_responses=True),
    queue_names=['parsing_queue'],
    function=example_function,
)

asyncio.run(worker.run())
```

---

## ⚙️ Конфигурация

Очереди должны передаваться как список имён:

```python
queue_names=['parsing_queue']
```

Каждое имя будет преобразовано в `'parsing_queue:processing'`.

---

## 🧾 Логи

Логи на русском языке. Пример:

```
Получена задача: func(example_function) data: ('parsing_queue:processing', '{...}')
Передана задача: queue(parsing_queue:processing) task_id: abcd123456 data: {...}
```

---

## 🔧 Технологии

| Технология      | Версия         |
|-----------------|----------------|
| Python          | 3.10+          |
| Redis           | 6+             |
| redis-py        | ^5.x (async)   |
| Pydantic        | ^2.x           |

---

## 📂 Структура проекта

```
.
├── init_worker.py       # основной файл воркера
├── enums.py             # перечисления сообщений и статусов очередей
├── README.md
```

---

## 📥 Вставка задачи в очередь вручную

```python
await worker.set_task_in_queue(
    data={'account': '12345', 'utility': 'water'},
    queue_name='parsing_queue',
)
```

---