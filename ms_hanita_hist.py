"""
Микросервис по сбору статистики входов/выходов пользователей Hanita.
Принимает сообщения типа:
    {
        "user_id"  : user_id,
        "action"   : "enter"/"quit",
        "timestamp": ...
    }
Заносит в БД запись:
ID  user_id     action      time
1   355         "enter"     34523.23322
2   52223       "quit"      34623.34652
...
"""
import asyncio
import json
import os
import sqlite3
import time

HOST = "127.0.0.1"
PORT = 5555
PATH_TO_DB = os.path.join(os.path.dirname(__name__), "hist.db")


class Storage:
    """ Класс для хранения истории входа/выхода пользователей """

    def __init__(self):
        self.conn = sqlite3.connect(PATH_TO_DB)
        self.cursor = self.conn.cursor()
        self.setup()

    def setup(self):
        """ Создаем таблицу БД """
        self.cursor.executescript("""
            CREATE TABLE IF NOT EXISTS history(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                action TEXT CHECK(action IN ("enter", "quit")) NOT NULL,
                time REAL
            );
        """)
        self.conn.commit()

    def put(self, user_id, action, timestamp):
        """ Положить в БД """
        cmd = "INSERT INTO history(user_id, action, time) VALUES (?, ?, ?);"
        self.cursor.execute(cmd, (user_id, action, timestamp))
        self.conn.commit()

    def get(self, user_id):
        """ Достать из БД """
        pass

    def close(self):
        self.conn.close()
        self.conn = None


class MsHistProtocol(asyncio.Protocol):
    def __init__(self):
        super().__init__()
        self._storage = Storage()

    async def handle(self, msg):
        self._storage.put(msg["user_id"], msg["action"], msg["timestamp"])
        print(*[key + ": " + str(msg[key]) for key in msg])

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        decoded_data = data.decode()
        msg = None
        try:
            msg = json.loads(decoded_data)
        except json.JSONDecodeError as err:
            print("bad json:", err)

        if msg:
            if {"user_id", "action", "timestamp"} <= set(msg):
                asyncio.ensure_future(self.handle(msg))
            else:
                print("Неверный запрос")


def main():
    print("Start at {}:{}".format(HOST, PORT))

    stor = Storage()

    loop = asyncio.get_event_loop()
    coro = loop.create_server(MsHistProtocol, HOST, PORT)
    server = loop.run_until_complete(coro)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print("\nQuiting...")

    server.close()
    stor.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


if __name__ == "__main__":
    main()
