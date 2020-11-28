import asyncio
import sqlite3


class ClientServerProtocol(asyncio.Protocol):
    """Класс, представляющий из себя протокол обмена данными с сервером"""
    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        resp = process_data(data.decode())
        self.transport.write(resp.encode())


def process_data(data: str) -> str:
    """Обработка данных, полученных от клиента и формирование ответа"""
    global cursor

    # Сохраняем данные в базе данных
    if data.startswith('put'):
        try:
            metric_name, metric_value, metric_time = data.lstrip('put').split()
            metric_value = float(metric_value)
            metric_time = int(metric_time)

            # Не может быть одинаковых метрик в одно и то же время
            # Удаляем прошлую запись и записываем новые данные
            cursor.execute('delete from metric where metric_name=? and metric_time=?', (metric_name, metric_time))
            cursor.execute('insert into metric(metric_name, metric_value, metric_time) values (?, ?, ?)',
                           (metric_name, metric_value, metric_time))
            return 'ok\n\n'
        except (TypeError, ValueError):
            return 'error\nwrong command\n\n'

    # Получаем данные из базы данных
    elif data.startswith('get'):
        metric_name = data.lstrip('get ').rstrip('\n')

        # Имя метрики не может состоять из нескольких слов
        if ' ' in metric_name:
            return 'error\nwrong command\n\n'

        # Читаем всю базу данных и формируем ответ
        if metric_name == '*':
            data_result = 'ok'
            for metric in cursor.execute('select metric_name, metric_value, metric_time from metric'):
                data_result += '\n' + ' '.join(map(str, metric))
            return f'{data_result}\n\n'

        # Читаем запрашиваемые клиентом данные и формируем ответ
        else:
            data_result = 'ok'
            for metric in cursor.execute(
                    'select metric_name, metric_value, metric_time from metric where metric_name=?', (metric_name)):
                data_result += '\n' + ' '.join(map(str, metric))
            return f'{data_result}\n\n'
    else:
        # Неверный формат запроса
        return 'error\nwrong command\n\n'


# Создаем базу данных в оперативной памяти
# При желании :memory: можно заменить, чтобы база находилась на диске
connection = sqlite3.connect(':memory:')
cursor = connection.cursor()
cursor.execute('create table metric(metric_name, metric_value, metric_time)')

loop = asyncio.get_event_loop()
core = loop.create_server(ClientServerProtocol, '127.0.0.1', 8888)

server = loop.run_until_complete(core)

try:
    loop.run_forever()
except KeyboardInterrupt:
    pass

connection.close()
server.close()
loop.run_until_complete(server.wait_closed())
loop.close()
