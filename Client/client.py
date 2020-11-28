from client_error import ClientError
import socket
import time


class Client:

    """Класс представляет собой клиент для отправки на сервер, запроса с сервера и обработки метрик"""

    def __init__(self, host: str, port: int, timeout: int = None):
        """Создаем соединение с сервером"""
        self._sock = socket.create_connection((host, port), timeout=timeout)

    @staticmethod
    def parsing(data_str: str) -> dict:
        """Разбираем ответ от сервера и возвращаем словарь с полученными данными"""
        data_dict = {}
        for metric in data_str.lstrip('ok').lstrip().rstrip().splitlines():
            metric_name, metric_value, timestamp = metric.split()
            if metric_name not in data_dict:
                data_dict[metric_name] = [(int(timestamp), float(metric_value))]
            else:
                data_dict[metric_name].append((int(timestamp), float(metric_value)))
        return data_dict

    def get(self, metric_name: str) -> dict:
        """Запрос данных с сервера и отправка их на обработку"""
        self._sock.sendall(f'get {metric_name}\n'.encode('utf8'))
        data = self._sock.recv(1024).decode('utf8')
        if data.startswith('ok'):
            return Client.parsing(data)
        else:
            raise ClientError('wrong command')

    def put(self, metric_name: str, metric_value: float, timestamp: int = int(time.time())) -> None:
        """Отправка данных на сервер"""
        self._sock.sendall(f'put {metric_name} {metric_value} {timestamp}\n'.encode('utf8'))
        data = self._sock.recv(1024).decode()
        if data.startswith('error'):
            raise ClientError('wrong command')

    def close(self) -> None:
        self._sock.close()
