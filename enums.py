from enum import Enum


class QueueStatus(str, Enum):
    PROCESSING = ':processing'
    RESULT = ':result'
    FAILED = ':failed'
    RETRY = ':retry'


class WorkerMessage(str, Enum):
    TASK_RECEIVED = 'Получена задача: func({function}) data: {data}'
    TASK_SENT = 'Передана задача: queue({queue}) task_id: {task_id} data: {data}'
    WORKER_STARTED = '{worker} запущен.'
    NO_FUNCTION = 'Функция обработки не передана'
    JSON_DECODE_ERROR = 'Ошибка декодирования JSON: {error}'
    DATA_RECEIVED = 'Получена data: {data} -> func: {function}'
    WORKER_STOPPED = 'Worker остановлен вручную.'
    PROCESSING_ERROR = (
        'Ошибка во время обработки задачи: func: {function} error: {error}'
    )
    QUEUE_VALIDATION_ERROR = 'Ошибка валидации конфигурации очереди: {error}'
