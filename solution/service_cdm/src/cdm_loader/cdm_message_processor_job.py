from datetime import datetime, timezone
from logging import Logger
from uuid import UUID
from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 kafka_consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 logger: Logger) -> None:

        self._consumer = kafka_consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = 50

    def run(self) -> None:
        # Логируем начало обработки батча
        self._logger.info(f"{datetime.now(timezone.utc)}: START")

        for i in range(self._batch_size):
            msg = self._consumer.consume()

            if msg is None:
                self._logger.info('Сообщения не получены')
                break

            # 1. Проверка наличия ID и типа объекта
            if "object_id" not in msg:
                self._logger.info('В сообщении отсутствует "object_id", пропускаем')
                continue

            object_id = msg["object_id"]
            if msg.get("object_type") != 'order':
                self._logger.info(f'Тип объекта не "order", а {msg.get("object_type")}, пропускаем')
                continue

            payload = msg.get("payload")
            if not payload:
                continue

            # --- БЛОК ДОБАВЛЕНИЯ ДАННЫХ В CDM ---
            # Используем set, чтобы обновлять счетчик категории только один раз за заказ
            added_categories = set()

            for product in payload.get("products_add_cdm", []):
                # Обновляем информацию о продуктах пользователя в витрине
                try:
                    self._cdm_repository.user_product_info_add(
                                product["user_id"], 
                                product["product_id"],
                                product["product_name"])
                except Exception as e:
                    self._logger.error(f'Ошибка добавления продукта {product.get("product_id")} в CDM: {e}')
                    
                # Обновляем информацию о категориях пользователя, если еще не добавлена в этом батче
                if product["category_name"] not in added_categories:
                    try:
                        self._cdm_repository.user_category_info_add(
                            product["user_id"],
                            product["category_id"], 
                            product["category_name"])    
                    
                        added_categories.add(product["category_name"])
                    except Exception as e:
                        self._logger.error(f'Ошибка добавления категории {product.get("category_id")} в CDM: {e}')

            # --- БЛОК УДАЛЕНИЯ/ОТКАТА ДАННЫХ В CDM ---
            # Нужно для корректного пересчета витрин при обновлении статуса заказа
            removed_categories = set()

            for product in payload.get("products_remove_cdm", []):
                # Удаляем/уменьшаем счетчик по продукту
                try:
                    self._cdm_repository.user_product_info_remove(
                            product["user_id"], 
                            product["product_id"])
                except Exception as e:
                    self._logger.error(f'Ошибка удаления продукта {product.get("product_id")} из CDM: {e}')

                # Удаляем/уменьшаем счетчик по категории
                if product["category_name"] not in removed_categories:
                    try:
                        self._cdm_repository.user_category_info_remove(
                                product["user_id"],
                                product["category_id"])    
                        
                        removed_categories.add(product["category_name"])
                    except Exception as e:
                        self._logger.error(f'Ошибка удаления категории {product.get("category_id")} из CDM: {e}')
           
        self._logger.info(f"{datetime.now(timezone.utc)}: FINISH")