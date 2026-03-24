import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List
from logging import Logger

from lib.pg import PgConnect
from pydantic import BaseModel


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def order_products_get(self, order_id: str, logger: Logger) -> list:
        # Получаем список продуктов, ранее загруженных в DDS для этого заказа
        products = list()
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 
                        h_product.product_id, 
                        h_category.h_category_pk, 
                        h_category.category_name, 
                        h_user.user_id, 
                        s_order_status.status
                    FROM dds.h_order AS h_order
                    JOIN dds.l_order_product AS l_order_product ON l_order_product.h_order_pk = h_order.h_order_pk
                    JOIN dds.h_product AS h_product ON l_order_product.h_product_pk = h_product.h_product_pk
                    JOIN dds.l_product_category AS l_product_category ON l_product_category.h_product_pk = h_product.h_product_pk
                    JOIN dds.h_category AS h_category ON h_category.h_category_pk = l_product_category.h_category_pk
                    JOIN dds.l_order_user AS l_order_user ON l_order_user.h_order_pk = h_order.h_order_pk
                    JOIN dds.h_user AS h_user ON h_user.h_user_pk = l_order_user.h_user_pk
                    JOIN dds.s_order_status AS s_order_status ON s_order_status.h_order_pk = h_order.h_order_pk
                    WHERE h_order.order_id = %(order_id)s::VARCHAR 
                      AND s_order_status.status = 'CLOSED';
                    """,
                    {'order_id': order_id}
                )
                
                for row in cur.fetchall():
                    products.append({
                        "product_id": row[0],
                        "category_id": str(row[1]),
                        "category_name": row[2],
                        "user_id": row[3]
                    })
        return products

    def user_add(self, user_id: str, user_name: str, user_login: str, src: str) -> None:
        # Добавляем пользователя в хаб и его данные в сателлит
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_user (h_user_pk, user_id, load_dt, load_src)
                    VALUES (MD5(%(user_id)s::VARCHAR)::UUID, %(user_id)s::VARCHAR, NOW(), %(load_src)s)
                    ON CONFLICT (h_user_pk) DO NOTHING;
                    """,
                    {'user_id': user_id, 'load_src': src}
                )
                cur.execute(
                    """
                    INSERT INTO dds.s_user_names (h_user_pk, load_dt, username, userlogin, load_src, hk_user_names_hashdiff)
                    VALUES (
                        MD5(%(user_id)s::VARCHAR)::UUID, 
                        NOW(), 
                        %(username)s::VARCHAR, 
                        %(userlogin)s::VARCHAR, 
                        %(load_src)s,
                        MD5(%(user_id)s::VARCHAR || %(username)s || %(userlogin)s)::UUID
                    ) 
                    ON CONFLICT (hk_user_names_hashdiff) DO NOTHING;
                    """,
                    {'user_id': user_id, 'username': user_name, 'userlogin': user_login, 'load_src': src}
                )

    def restaurant_add(self, restaurant_id: str, restaurant_name: str, src: str) -> None:
        # Добавляем ресторан в хаб и сателлит имен
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src)
                    VALUES (MD5(%(restaurant_id)s::VARCHAR)::UUID, %(restaurant_id)s::VARCHAR, NOW(), %(load_src)s)
                    ON CONFLICT (h_restaurant_pk) DO NOTHING;
                    """,
                    {'restaurant_id': restaurant_id, 'load_src': src}
                )
                cur.execute(
                    """
                    INSERT INTO dds.s_restaurant_names (h_restaurant_pk, load_dt, name, load_src, hk_restaurant_names_hashdiff)
                    VALUES (
                        MD5(%(restaurant_id)s::VARCHAR)::UUID, 
                        NOW(), 
                        %(name)s::VARCHAR, 
                        %(load_src)s,
                        MD5(%(restaurant_id)s::VARCHAR || %(name)s)::UUID
                    ) 
                    ON CONFLICT (hk_restaurant_names_hashdiff) DO NOTHING;
                    """,
                    {'restaurant_id': restaurant_id, 'name': restaurant_name, 'load_src': src}
                )
                
    def category_add(self, category_name: str, src: str) -> Any:
        # Добавляем категорию и возвращаем её хэш-ключ (PK)
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_category (h_category_pk, category_name, load_dt, load_src)
                    VALUES (MD5(%(category_name)s::VARCHAR)::UUID, %(category_name)s::VARCHAR, NOW(), %(load_src)s)
                    ON CONFLICT (h_category_pk) DO UPDATE SET category_name = EXCLUDED.category_name
                    RETURNING h_category_pk;
                    """,
                    {'category_name': category_name, 'load_src': src}
                )
                res = cur.fetchone()
        return res[0] if res else 'not initialized'

    def order_add(self, order_id: str, order_dt: str, order_status: str, cost: float, payment: float, user_id: str, src: str) -> None:
        # Добавляем заказ, статус и связь с пользователем
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                # Хаб заказа
                cur.execute(
                    """
                    INSERT INTO dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src)
                    VALUES (MD5(%(order_id)s::VARCHAR)::UUID, %(order_id)s::VARCHAR, %(order_dt)s, NOW(), %(load_src)s)
                    ON CONFLICT (h_order_pk) DO UPDATE SET order_dt = EXCLUDED.order_dt;
                    """,
                    {'order_id': order_id, 'order_dt': order_dt, 'load_src': src}
                )
                # Линк Заказ-Пользователь
                cur.execute(
                    """
                    INSERT INTO dds.l_order_user (l_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
                    VALUES (
                        MD5(%(order_id)s::VARCHAR || %(user_id)s::VARCHAR)::UUID, 
                        MD5(%(order_id)s::VARCHAR)::UUID, 
                        MD5(%(user_id)s::VARCHAR)::UUID, 
                        NOW(), 
                        %(load_src)s
                    ) ON CONFLICT (l_order_user_pk) DO NOTHING;
                    """,
                    {'order_id': order_id, 'user_id': user_id, 'load_src': src}
                )
                # Сателлит статуса заказа
                cur.execute(
                    """
                    INSERT INTO dds.s_order_status (h_order_pk, load_dt, status, load_src, hk_order_status_hashdiff)
                    VALUES (
                        MD5(%(order_id)s::VARCHAR)::UUID, 
                        NOW(), 
                        %(status)s::VARCHAR, 
                        %(load_src)s, 
                        MD5(%(order_id)s::VARCHAR || %(status)s)::UUID
                    ) ON CONFLICT (hk_order_status_hashdiff) DO NOTHING;
                    """,
                    {'order_id': order_id, 'status': order_status, 'load_src': src}
                )

    def product_add(self, order_id: str, product_id: str, category_name: str, product_name: str, restaurant_id: str, src: str) -> None:
        # Добавляем продукт и все необходимые линки (Data Vault)
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                # Хаб продукта
                cur.execute(
                    """
                    INSERT INTO dds.h_product (h_product_pk, product_id, load_dt, load_src)
                    VALUES (MD5(%(product_id)s::VARCHAR)::UUID, %(product_id)s::VARCHAR, NOW(), %(load_src)s)
                    ON CONFLICT (h_product_pk) DO NOTHING;
                    """,
                    {'product_id': product_id, 'load_src': src}
                )
                # Сателлит названия продукта
                cur.execute(
                    """
                    INSERT INTO dds.s_product_names (h_product_pk, load_dt, name, load_src, hk_product_names_hashdiff)
                    VALUES (
                        MD5(%(product_id)s::VARCHAR)::UUID, 
                        NOW(), 
                        %(product_name)s::VARCHAR, 
                        %(load_src)s, 
                        MD5(%(product_id)s::VARCHAR || %(product_name)s::VARCHAR)::UUID
                    ) ON CONFLICT (hk_product_names_hashdiff) DO NOTHING;
                    """,
                    {'product_id': product_id, 'product_name': product_name, 'load_src': src}
                )
                # Линк Продукт-Категория
                cur.execute(
                    """
                    INSERT INTO dds.l_product_category (l_product_category_pk, h_product_pk, h_category_pk, load_dt, load_src)
                    VALUES (
                        MD5(%(product_id)s::VARCHAR || %(cat)s::VARCHAR)::UUID, 
                        MD5(%(product_id)s::VARCHAR)::UUID, 
                        MD5(%(cat)s::VARCHAR)::UUID, 
                        NOW(), 
                        %(load_src)s
                    ) ON CONFLICT (l_product_category_pk) DO NOTHING;
                    """,
                    {'product_id': product_id, 'cat': category_name, 'load_src': src}
                )
                # Линк Продукт-Ресторан
                cur.execute(
                    """
                    INSERT INTO dds.l_product_restaurant (l_product_restaurant_pk, h_product_pk, h_restaurant_pk, load_dt, load_src)
                    VALUES (
                        MD5(%(product_id)s::VARCHAR || %(restaurant_id)s::VARCHAR)::UUID, 
                        MD5(%(product_id)s::VARCHAR)::UUID, 
                        MD5(%(restaurant_id)s::VARCHAR)::UUID, 
                        NOW(), 
                        %(load_src)s
                    ) ON CONFLICT (l_product_restaurant_pk) DO NOTHING;
                    """,
                    {'product_id': product_id, 'restaurant_id': restaurant_id, 'load_src': src}
                )
                # Линк Заказ-Продукт
                cur.execute(
                    """
                    INSERT INTO dds.l_order_product (l_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
                    VALUES (
                        MD5(%(order_id)s::VARCHAR || %(product_id)s::VARCHAR)::UUID, 
                        MD5(%(order_id)s::VARCHAR)::UUID, 
                        MD5(%(product_id)s::VARCHAR)::UUID, 
                        NOW(), 
                        %(load_src)s
                    ) ON CONFLICT (l_order_product_pk) DO NOTHING;
                    """,
                    {'order_id': order_id, 'product_id': product_id, 'load_src': src}
                )