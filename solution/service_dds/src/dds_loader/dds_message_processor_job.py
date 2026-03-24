import json

from logging import Logger
from datetime import datetime, timezone
from lib.kafka_connect import KafkaConsumer, KafkaProducer 
from dds_loader.repository.dds_repository import DdsRepository


class DdsMessageProcessor:
    def __init__(self,
                 kafka_consumer: KafkaConsumer,
                 kafka_producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 logger: Logger) -> None:

        self._consumer = kafka_consumer
        self._producer = kafka_producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = 50

    def run(self) -> None:
        # Логируем начало обработки батча
        self._logger.info(f"{datetime.now(timezone.utc)}: START")

        for i in range(self._batch_size):
            msg = self._consumer.consume()

            if msg is None:
                self._logger.info('Сообщение не получено, выход из цикла батча')
                break

            # 1. Проверка структуры сообщения (object_id и object_type)
            if "object_id" not in msg or "object_type" not in msg:
                self._logger.warning('В сообщении отсутствует id или type, пропускаем')
                continue

            object_id = msg["object_id"]
            if msg["object_type"] != 'order':
                self._logger.info(f'Тип объекта {msg["object_type"]} не является "order", пропускаем')
                continue

            # 2. Извлечение payload
            payload = msg.get("payload")
            if not payload:
                self._logger.warning(f'Заказ {object_id} не содержит payload, пропускаем')
                continue

            # Подготовка метаданных источника (один раз для всех вставок)
            src_json = json.dumps({"service": "stg", "object_id": object_id})

            # 3. Извлечение данных пользователя и ресторана (безопасно через get)
            user = payload.get("user", {})
            user_id = user.get("id")
            restaurant = payload.get("restaurant", {})
            restaurant_id = restaurant.get("id")
            products = payload.get("products", [])

            if not all([user_id, restaurant_id, products]):
                self._logger.warning(f'Неполные данные в заказе {object_id}, пропускаем')
                continue

            # 4. Сохранение данных в DDS (Hubs & Satellites)
            try:
                # Добавляем пользователя
                self._dds_repository.user_add(user_id, user.get("name"), user.get("login"), src_json)
                
                # Добавляем ресторан
                self._dds_repository.restaurant_add(restaurant_id, restaurant.get("name"), src_json)
                
                # Получаем список продуктов, которые уже были в этом заказе (для корректного обновления CDM)
                # Важно вызвать это ДО сохранения новых данных о заказе
                loaded_earlier_products = self._dds_repository.order_products_get(object_id, self._logger)

                # Добавляем заказ
                self._dds_repository.order_add(
                    object_id, payload.get("date"), payload.get("status"), 
                    payload.get("cost"), payload.get("payment"), user_id, src_json
                )
            except Exception as e:
                self._logger.error(f'Ошибка при записи базовых сущностей DDS для заказа {object_id}: {e}')
                continue

            # 5. Обработка продуктов заказа
            payload_products_add = []
            order_status = payload.get("status")

            for product in products:
                try:
                    # Добавляем категорию и получаем её ID (если логика репозитория это предполагает)
                    category_name = product.get("category")
                    cat_id = self._dds_repository.category_add(category_name, src_json)
                    
                    # Добавляем сам продукт и связи
                    self._dds_repository.product_add(
                        object_id, product.get("id"), category_name, 
                        product.get("name"), restaurant_id, src_json
                    )

                    # Если заказ завершен, готовим данные для отправки в CDM (витрины)
                    if order_status == 'CLOSED':
                        payload_products_add.append({
                            'product_id': product.get("id"),
                            'product_name': product.get("name"),
                            'category_id': str(cat_id),
                            'category_name': category_name,
                            'user_id': user_id
                        })
                except Exception as e:
                    self._logger.error(f'Ошибка при обработке продукта в заказе {object_id}: {e}')

            # 6. Формирование и отправка сообщения для следующего слоя (CDM)
            payload_out = {
                "id": object_id,
                "products_remove_cdm": loaded_earlier_products,
                "products_add_cdm": payload_products_add
            }

            msg_out = {
                "object_id": object_id,
                "object_type": 'order',
                "payload": payload_out
            }

            try:
                self._producer.produce(msg_out)
            except Exception as e:   
                self._logger.error(f'Ошибка отправки в Kafka для заказа {object_id}: {e}')

        # Логируем завершение обработки батча
        self._logger.info(f"{datetime.now(timezone.utc)}: FINISH")