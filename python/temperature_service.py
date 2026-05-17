"""(3) Consumidor + gRPC Web Service.

- Consome estatisticas do TOPIC_STATS (Kafka) em uma thread em background
  e armazena em SQLite.
- Expoe um servico gRPC para clientes consultarem ultima leitura,
  historico, sensores conhecidos e contagem total.
"""
from concurrent import futures
import json
import logging
import sqlite3
import threading

import grpc
from kafka import KafkaConsumer

import const
import TemperatureService_pb2 as pb
import TemperatureService_pb2_grpc as pb_grpc


# ============================================================
# Camada de persistencia (SQLite)
# ============================================================
class StatsDB:
    def __init__(self, path: str):
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute('''
            CREATE TABLE IF NOT EXISTS temperature_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_id TEXT NOT NULL,
                avg_temperature REAL NOT NULL,
                min_temperature REAL NOT NULL,
                max_temperature REAL NOT NULL,
                sample_count INTEGER NOT NULL,
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                computed_at TEXT NOT NULL
            )
        ''')
        self._conn.commit()

    def insert(self, s: dict):
        with self._lock:
            self._conn.execute(
                'INSERT INTO temperature_stats '
                '(sensor_id, avg_temperature, min_temperature, max_temperature, '
                ' sample_count, window_start, window_end, computed_at) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                (
                    s['sensor_id'],
                    float(s['avg_temperature']),
                    float(s['min_temperature']),
                    float(s['max_temperature']),
                    int(s['sample_count']),
                    s['window_start'],
                    s['window_end'],
                    s['computed_at'],
                ),
            )
            self._conn.commit()

    def latest(self):
        with self._lock:
            cur = self._conn.execute(
                'SELECT sensor_id, avg_temperature, min_temperature, max_temperature, '
                '       sample_count, window_start, window_end, computed_at '
                'FROM temperature_stats ORDER BY id DESC LIMIT 1'
            )
            return cur.fetchone()

    def latest_by_sensor(self, sensor_id: str):
        with self._lock:
            cur = self._conn.execute(
                'SELECT sensor_id, avg_temperature, min_temperature, max_temperature, '
                '       sample_count, window_start, window_end, computed_at '
                'FROM temperature_stats WHERE sensor_id = ? ORDER BY id DESC LIMIT 1',
                (sensor_id,),
            )
            return cur.fetchone()

    def history(self, sensor_id: str, limit: int):
        with self._lock:
            cur = self._conn.execute(
                'SELECT sensor_id, avg_temperature, min_temperature, max_temperature, '
                '       sample_count, window_start, window_end, computed_at '
                'FROM temperature_stats WHERE sensor_id = ? ORDER BY id DESC LIMIT ?',
                (sensor_id, limit),
            )
            return cur.fetchall()

    def list_sensors(self):
        with self._lock:
            cur = self._conn.execute(
                'SELECT DISTINCT sensor_id FROM temperature_stats ORDER BY sensor_id'
            )
            return [row[0] for row in cur.fetchall()]

    def count(self):
        with self._lock:
            cur = self._conn.execute('SELECT COUNT(*) FROM temperature_stats')
            return int(cur.fetchone()[0])


# ============================================================
# Consumidor Kafka (em thread)
# ============================================================
def kafka_consumer_loop(db: StatsDB):
    consumer = KafkaConsumer(
        const.TOPIC_STATS,
        bootstrap_servers=[const.BROKER_ADDR + ':' + const.BROKER_PORT],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest',
    )
    print(f'[service:kafka] consumindo {const.TOPIC_STATS}')
    for msg in consumer:
        try:
            db.insert(msg.value)
            print(f'[service:kafka] gravado: {msg.value}')
        except Exception as e:
            print(f'[service:kafka] erro ao gravar: {e}')


# ============================================================
# Servico gRPC
# ============================================================
def _row_to_stats(row):
    if row is None:
        return pb.TemperatureStats()
    return pb.TemperatureStats(
        sensor_id=row[0],
        avg_temperature=row[1],
        min_temperature=row[2],
        max_temperature=row[3],
        sample_count=row[4],
        window_start=row[5],
        window_end=row[6],
        computed_at=row[7],
    )


class TemperatureServer(pb_grpc.TemperatureServiceServicer):
    def __init__(self, db: StatsDB):
        self._db = db

    def GetLatestStats(self, request, context):
        return _row_to_stats(self._db.latest())

    def GetLatestStatsBySensor(self, request, context):
        return _row_to_stats(self._db.latest_by_sensor(request.sensor_id))

    def GetHistory(self, request, context):
        limit = request.limit if request.limit > 0 else 50
        rows = self._db.history(request.sensor_id, limit)
        result = pb.TemperatureStatsList()
        for r in rows:
            result.stats.append(_row_to_stats(r))
        return result

    def ListSensors(self, request, context):
        return pb.SensorList(sensor_ids=self._db.list_sensors())

    def CountStats(self, request, context):
        return pb.StatsCount(count=self._db.count())


def serve():
    db = StatsDB(const.DB_PATH)

    # Kafka consumer em thread separada
    t = threading.Thread(target=kafka_consumer_loop, args=(db,), daemon=True)
    t.start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb_grpc.add_TemperatureServiceServicer_to_server(TemperatureServer(db), server)
    server.add_insecure_port(const.GRPC_IP + ':' + const.GRPC_PORT)
    server.start()
    print(f'[service:grpc] ouvindo em {const.GRPC_IP}:{const.GRPC_PORT}')
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()
