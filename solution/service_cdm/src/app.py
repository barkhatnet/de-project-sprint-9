import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
from app_config import AppConfig
from cdm_loader.repository.cdm_repository import CdmRepository
from cdm_loader.cdm_message_processor_job import CdmMessageProcessor

app = Flask(__name__)

# Инициализируем конфиг
config = AppConfig()

# Заводим endpoint для проверки, поднялся ли сервис.
# Обратиться к нему можно будет GET-запросом по адресу localhost:5000/health.
# Если в ответе будет healthy - сервис поднялся и работает.
@app.get('/health')
def hello_world():
    return 'healthy'

if __name__ == '__main__':
    # Устанавливаем уровень логгирования в Debug, чтобы иметь возможность просматривать отладочные логи.
    app.logger.setLevel(logging.DEBUG)

    # 1. Инициализируем зависимости, используя методы из config
    kafka_consumer = config.kafka_consumer()

    # 2. Инициализируем репозиторий CDM, передавая подключение к базе данных
    pg_db = config.pg_warehouse_db()
    cdm_repository = CdmRepository(pg_db)

    # 3. Инициализируем процессор сообщений, передавая объекты в конструктор
    proc = CdmMessageProcessor(
        kafka_consumer, 
        cdm_repository,
        app.logger
    ) 

    # Запускаем процессор в фоновом режиме
    # BackgroundScheduler будет вызывать функцию "run" нашего CdmMessageProcessor по расписанию
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        func=proc.run, 
        trigger="interval", 
        seconds=config.DEFAULT_JOB_INTERVAL
    )
    scheduler.start()

    # Запускаем Flask-приложение, чтобы сервис продолжал работу
    app.run(debug=False, host='0.0.0.0', use_reloader=False)
