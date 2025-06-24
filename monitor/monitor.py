import time
from datetime import datetime
from .telegram_notifier import TelegramNotifier
from .marzban_api import MarzbanAPI
from .config import Config, Responses
import redis
import logging


class NodeMonitor:
    def __init__(self):
        # Основные компоненты
        self.api = MarzbanAPI()
        self.notifier = TelegramNotifier()
        self.redis = redis.Redis(
            host=Config.REDIS_HOST,
            port=Config.REDIS_PORT,
            db=Config.REDIS_DB,
            password=Config.REDIS_PASSWORD,
        )

        # Префиксы для ключей Redis
        self.node_status_key_prefix = "node_status:"
        self.node_disconnect_time_prefix = "node_disconnect_time:"

        # Параметры задержек и попыток
        self.sleep_interval = 30
        self.reconnect_attempts = 3
        self.reconnect_delay = 5
        self.node_check_delay = 10
        self.master_name = Config.MARZBAN_BASE_URL

    def get_node_status_key(self, node_id):
        return f"{self.node_status_key_prefix}{node_id}"

    def get_node_disconnect_time_key(self, node_id):
        return f"{self.node_disconnect_time_prefix}{node_id}"

    def log_node_info(self, node):
        logging.info(f"--- Узел: {node.get('name', 'Неизвестный узел')} ---")
        logging.info(f"ID: {node.get('id', 'Неизвестно')}")
        logging.info(f"Адрес: {node.get('address', 'IP не указан')}")
        logging.info(f"Порт: {node.get('port', 'Неизвестно')}")
        logging.info(f"Статус: {node.get('status', 'Неизвестно')}")
        logging.info(f"Сообщение: {node.get('message', 'Ошибка не указана')}")

    def monitor(self):
        self.notifier.send_message(
            Responses.get_message(
                "MONITOR_START",
                master_name=master_name
            ),
            parse_mode="HTML",
        )
        while True:
            try:
                logging.info("Начало мониторинга узлов...")
                start_time = time.time()

                try:
                    nodes = self.api.get_nodes()
                    elapsed_time = time.time() - start_time
                    logging.info(f"Запрос узлов выполнен за {elapsed_time:.2f} секунд")
                    logging.info(f"Получено {len(nodes)} узлов для мониторинга.")
                except TimeoutError:
                    logging.error("Timeout while retrieving nodes")
                    self.notifier.send_message(
                        Responses.get_message(
                            "ERROR_MONITOR_FAILURE",
                            error_message="Timeout while retrieving nodes",
                            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        ),
                        parse_mode="HTML",
                    )
                    time.sleep(self.sleep_interval)  # Задержка перед следующей попыткой
                    continue
                except Exception as e:
                    logging.error(f"Ошибка при получении узлов: {e}")
                    self.notifier.send_message(
                        Responses.get_message(
                            "ERROR_MONITOR_FAILURE",
                            error_message=str(e),
                            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        ),
                        parse_mode="HTML",
                    )
                    time.sleep(self.sleep_interval)
                    continue

                for node in nodes:
                    self.log_node_info(node)

                    node_id = node["id"]
                    node_name = node["name"]
                    node_ip = node.get("address", "IP не указан")
                    node_message = node.get("message", "Ошибка не указана")
                    node_redis_key = self.get_node_status_key(node_id)
                    node_disconnect_time_key = self.get_node_disconnect_time_key(
                        node_id
                    )

                    try:
                        node_status = self.api.get_node(node_id)
                    except TimeoutError:
                        logging.error(
                            f"Timeout while retrieving status for node {node_name}"
                        )
                        continue
                    except Exception as e:
                        logging.error(
                            f"Ошибка при получении статуса узла {node_name}: {e}"
                        )
                        continue

                    current_status = node_status.get("status", "unknown")
                    logging.info(f"Статус узла {node_name}: {current_status}")

                    # Если узел восстановился
                    if (
                        self.redis.get(node_redis_key) == b"disconnected"
                        and current_status == "connected"
                    ):
                        disconnect_time = self.redis.get(node_disconnect_time_key)
                        if disconnect_time:
                            disconnect_time = float(disconnect_time)
                            reconnect_time = time.time()
                            downtime = reconnect_time - disconnect_time
                            downtime_minutes = round(downtime / 60, 2)
                            timestamp_reconnect = datetime.now().strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )
                            logging.info(
                                f"Узел {node_name} восстановлен через "
                                f"{downtime_minutes} минут."
                            )
                            self.notifier.send_message(
                                Responses.get_message(
                                    "SUCCESS_NODE_RECONNECTED",
                                    node_name=node_name,
                                    node_ip=node_ip,
                                    timestamp=timestamp_reconnect,
                                    downtime_minutes=downtime_minutes,
                                ),
                                parse_mode="HTML",
                            )
                        self.redis.delete(node_redis_key)
                        self.redis.delete(node_disconnect_time_key)

                    # Если узел отключен
                    if current_status not in ["connected", "disabled"]:
                        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        logging.warning(
                            f"Узел {node_name} ({node_ip}) отключен. "
                            f"Попытка переподключения в {timestamp}..."
                        )

                        if self.redis.get(node_redis_key) != b"disconnected":
                            self.notifier.send_message(
                                Responses.get_message(
                                    "ERROR_NODE_DISCONNECTED",
                                    node_name=node_name,
                                    node_ip=node_ip,
                                    error_message=node_message,
                                    timestamp=timestamp,
                                ),
                                parse_mode="HTML",
                            )
                            self.redis.set(node_disconnect_time_key, time.time())

                        # Попытка переподключения
                        for i in range(self.reconnect_attempts):
                            try:
                                self.api.reconnect_node(node_id)
                                time.sleep(self.node_check_delay)
                                node_status = self.api.get_node(node_id)

                                if node_status["status"] == "connected":
                                    timestamp_reconnect = datetime.now().strftime(
                                        "%Y-%m-%d %H:%M:%S"
                                    )
                                    logging.info(
                                        f"Узел {node_name} успешно переподключен в "
                                        f"{timestamp_reconnect} после {i + 1} попыток."
                                    )
                                    self.notifier.send_message(
                                        Responses.get_message(
                                            "SUCCESS_NODE_RECONNECTED_AFTER_ATTEMPTS",
                                            node_name=node_name,
                                            node_ip=node_ip,
                                            timestamp=timestamp_reconnect,
                                            attempts=i + 1,
                                        ),
                                        parse_mode="HTML",
                                    )
                                    self.redis.delete(node_redis_key)
                                    self.redis.delete(node_disconnect_time_key)
                                    break
                            except Exception as e:
                                logging.error(
                                    f"Ошибка при переподключении узла {node_name}: {e}"
                                )
                                time.sleep(self.reconnect_delay)

                        else:
                            # Если все попытки неудачны
                            timestamp_failure = datetime.now().strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )
                            logging.error(
                                f"Не удалось переподключить узел {node_name} "
                                f"в {timestamp_failure}."
                            )
                            self.notifier.send_message(
                                Responses.get_message(
                                    "ERROR_NODE_RECONNECT_FAILED",
                                    node_name=node_name,
                                    node_ip=node_ip,
                                    error_message=node_message,
                                    attempts=self.reconnect_attempts,
                                    timestamp=timestamp_failure,
                                ),
                                parse_mode="HTML",
                            )
                            self.redis.set(node_redis_key, "disconnected")

                time.sleep(self.sleep_interval)

            except Exception as e:
                timestamp_error = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                logging.error(f"Ошибка при мониторинге узлов: {e} в {timestamp_error}")
                self.notifier.send_message(
                    Responses.get_message(
                        "ERROR_MONITOR_FAILURE",
                        error_message=str(e),
                        timestamp=timestamp_error,
                    ),
                    parse_mode="HTML",
                )
