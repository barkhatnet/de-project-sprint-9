import json

from datetime import datetime, timezone
from typing import Any, Dict
from logging import Logger
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient
from stg_loader.repository.stg_repository import StgRepository


class StgMessageProcessor:
    def __init__(self,
                 kafka_consumer: KafkaConsumer,
                 kafka_producer: KafkaProducer,
                 redis_client: RedisClient,
                 stg_repository: StgRepository,
                 logger: Logger) -> None:
        
        self._consumer = kafka_consumer
        self._producer = kafka_producer
        self._redis = redis_client
        self._stg_repository = stg_repository
        self._logger = logger
        self._batch_size = 50

    def run(self) -> None:
        # Логируем начало обработки батча
        self._logger.info(f"{datetime.now(timezone.utc)}: START")

        for i in range(self._batch_size):
            msg = self._consumer.consume()
            
            if msg is None:
                break

            if msg.get("object_type") != 'order':
                continue

            try:
                object_id = msg.get("object_id")
                object_type = msg.get("object_type")
                payload = msg.get("payload", {})

                if not object_id or not payload:
                    self._logger.warning("Сообщение содержит пустой id или payload, пропускаем")
                    continue

                # Сохраняем в STG с корректной меткой времени
                self._stg_repository.order_events_insert(
                    object_id, 
                    object_type, 
                    datetime.now(timezone.utc), 
                    json.dumps(payload)
                )

                # Обогащение данными из Redis
                user_id = payload.get("user", {}).get("id")
                restaurant_id = payload.get("restaurant", {}).get("id")

                if not user_id or not restaurant_id:
                    continue

                raw_user = self._redis.get(user_id)
                raw_restaurant = self._redis.get(restaurant_id)

                if not raw_user or not raw_restaurant:
                    continue

                redis_user = json.loads(raw_user) if isinstance(raw_user, (str, bytes)) else raw_user
                redis_restaurant = json.loads(raw_restaurant) if isinstance(raw_restaurant, (str, bytes)) else raw_restaurant

                # Сопоставление продуктов
                order_products = payload.get("order_items", [])
                for product in order_products:
                    product["category"] = "Unknown"
                    for menu_item in redis_restaurant.get("menu", []):
                        if menu_item.get("_id") == product.get("id"):
                            product["category"] = menu_item.get("category")
                            break

                # Формируем и отправляем в DDS
                msg_out = {
                    "object_id": object_id,
                    "object_type": object_type,
                    "payload": {
                        "id": object_id,
                        "date": payload.get("date"),
                        "cost": payload.get("cost"),
                        "payment": payload.get("payment"),
                        "status": payload.get("final_status"),
                        "user": {
                            "id": user_id,
                            "name": redis_user.get("name"),
                            "login": redis_user.get("login")
                        },
                        "restaurant": {
                            "id": restaurant_id,
                            "name": redis_restaurant.get("name")
                        },
                        "products": order_products
                    }
                }

                self._producer.produce(msg_out)

            except Exception as e:
                self._logger.error(f"Критическая ошибка при обработке сообщения {msg.get('object_id')}: {e}")
                continue

        self._logger.info(f"{datetime.now(timezone.utc)}: FINISH")