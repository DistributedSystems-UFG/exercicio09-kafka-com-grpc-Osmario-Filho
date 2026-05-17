"""(2) Consumidor/Produtor: processa leituras e produz estatisticas.

Consome o TOPIC_RAW, mantem uma janela deslizante de 2 horas por sensor
e publica em TOPIC_STATS a media (alem de min/max e contagem) toda vez
que recebe uma nova leitura.
"""
from kafka import KafkaConsumer, KafkaProducer
from const import BROKER_ADDR, BROKER_PORT, TOPIC_RAW, TOPIC_STATS
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta
import json

WINDOW = timedelta(hours=2)


def parse_ts(s: str) -> datetime:
    # aceita ISO-8601 com 'Z' ou offset
    if s.endswith('Z'):
        s = s[:-1] + '+00:00'
    return datetime.fromisoformat(s)


def run():
    consumer = KafkaConsumer(
        TOPIC_RAW,
        bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest',
    )
    producer = KafkaProducer(
        bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )

    # janela deslizante de leituras por sensor: deque de (timestamp, temperature)
    windows = defaultdict(deque)

    print(f'[processor] consumindo {TOPIC_RAW} -> publicando estatisticas em {TOPIC_STATS}')
    for msg in consumer:
        event = msg.value
        sensor_id = event['sensor_id']
        temperature = float(event['temperature'])
        ts = parse_ts(event['timestamp'])

        w = windows[sensor_id]
        w.append((ts, temperature))

        # remove leituras fora da janela de 2h
        cutoff = ts - WINDOW
        while w and w[0][0] < cutoff:
            w.popleft()

        temps = [t for _, t in w]
        stats = {
            'sensor_id': sensor_id,
            'avg_temperature': round(sum(temps) / len(temps), 2),
            'min_temperature': round(min(temps), 2),
            'max_temperature': round(max(temps), 2),
            'sample_count': len(temps),
            'window_start': w[0][0].isoformat(),
            'window_end': ts.isoformat(),
            'computed_at': datetime.now(timezone.utc).isoformat(),
        }
        producer.send(TOPIC_STATS, value=stats)
        producer.flush()
        print(f'[processor] {sensor_id}: leitura={temperature} C  ->  {stats}')


if __name__ == '__main__':
    try:
        run()
    except KeyboardInterrupt:
        print('\n[processor] encerrado')
