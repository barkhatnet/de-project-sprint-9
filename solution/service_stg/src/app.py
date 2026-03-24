import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
from app_config import AppConfig
from stg_loader.repository.stg_repository import StgRepository
from stg_loader.stg_message_processor_job import StgMessageProcessor

app = Flask(__name__)

# Инициализируем конфиг
config = AppConfig()

# Заводим endpoint для проверки, поднялся ли сервис.
# Обратиться к нему можно будет GET-запросом по адресу localhost:5000/health.
# Если в ответе будет healthy - сервис поднялся и работает.
@app.get('/health')
def health():
    return 'healthy'


if __name__ == '__main__':
    # Устанавливаем уровень логгирования в Debug, чтобы иметь возможность просматривать отладочные логи.
    app.logger.setLevel(logging.DEBUG)

    # 1. Инициализируем зависимости, используя методы из config
    kafka_consumer = config.kafka_consumer()
    kafka_producer = config.kafka_producer()
    redis_client = config.redis_client()

    # 2. Создаем подключение к БД и репозиторий
    pg_db = config.pg_warehouse_db()
    stg_repository = StgRepository(pg_db)

    # 3. Инициализируем процессор сообщений, передавая объекты в конструктор
    proc = StgMessageProcessor(
        kafka_consumer,
        kafka_producer,
        redis_client,
        stg_repository,
        app.logger
    )

    # Запускаем процессор в бэкграунде.
    # BackgroundScheduler будет по расписанию вызывать функцию run нашего обработчика(MessageProcessor).
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        func=proc.run, 
        trigger="interval", 
        seconds=config.DEFAULT_JOB_INTERVAL)
    scheduler.start()

    # Запускаем Flask-приложение, чтобы сервис продолжал работу
    app.run(debug=True, host='0.0.0.0', use_reloader=False)
