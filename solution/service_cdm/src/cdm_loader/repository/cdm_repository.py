import uuid

from datetime import datetime
from typing import Any, Dict, List
from lib.pg import PgConnect


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def user_product_info_add(self, user_id: str, product_id: str, product_name: str) -> None:
        # Добавляет запись или инкрементирует счетчик. Генерируем UUID через MD5.
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO cdm.user_product_counters(user_id, product_id, product_name, order_cnt)
                    VALUES (
                        MD5(%(user_id)s::VARCHAR)::UUID, 
                        MD5(%(product_id)s::VARCHAR)::UUID, 
                        %(product_name)s::VARCHAR, 
                        1
                    )
                    ON CONFLICT (user_id, product_id) DO UPDATE SET
                        order_cnt = cdm.user_product_counters.order_cnt + 1,
                        product_name = EXCLUDED.product_name;
                    """,
                    {'user_id': user_id, 'product_id': product_id, 'product_name': product_name}
                )

    def user_category_info_add(self, user_id: str, category_id: str, category_name: str) -> None:
        # Добавляет запись или инкрементирует счетчик категории.
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO cdm.user_category_counters(user_id, category_id, category_name, order_cnt)
                    VALUES (
                        MD5(%(user_id)s::VARCHAR)::UUID, 
                        MD5(%(category_id)s::VARCHAR)::UUID, 
                        %(category_name)s::VARCHAR, 
                        1
                    )
                    ON CONFLICT (user_id, category_id) DO UPDATE SET
                        order_cnt = cdm.user_category_counters.order_cnt + 1,
                        category_name = EXCLUDED.category_name;
                    """,
                    {'user_id': user_id, 'category_id': category_id, 'category_name': category_name}
                )

    def user_product_info_remove(self, user_id: str, product_id: str) -> None:
        # Уменьшает счетчик продукта. Используем ту же логику MD5 для поиска.
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE cdm.user_product_counters
                    SET order_cnt = GREATEST(order_cnt - 1, 0)
                    WHERE user_id = MD5(%(user_id)s::VARCHAR)::UUID 
                      AND product_id = MD5(%(product_id)s::VARCHAR)::UUID
                    RETURNING id;
                    """,
                    {'user_id': user_id, 'product_id': product_id}
                )
                assert len(cur.fetchall()) > 0
                
    def user_category_info_remove(self, user_id: str, category_id: str) -> None:   
        # Уменьшает счетчик категории.
        with self._db.connection() as conn:               
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE cdm.user_category_counters
                    SET order_cnt = GREATEST(order_cnt - 1, 0)
                    WHERE user_id = MD5(%(user_id)s::VARCHAR)::UUID 
                      AND category_id = MD5(%(category_id)s::VARCHAR)::UUID
                    RETURNING id;
                    """,
                    {'user_id': user_id, 'category_id': category_id}
                )
                assert len(cur.fetchall()) > 0

    def total_counters_get(self) -> tuple:
        # Получение агрегатов для проверки.
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 
                        (SELECT COALESCE(SUM(ORDER_CNT), 0) FROM cdm.user_product_counters) AS PRODUCT_CNT,
                        (SELECT COALESCE(SUM(ORDER_CNT), 0) FROM cdm.user_category_counters) AS CATEGORY_CNT;
                    """
                )
                row = cur.fetchone()
        return row if row else (0, 0)