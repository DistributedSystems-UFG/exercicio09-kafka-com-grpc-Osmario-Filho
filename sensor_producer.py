"""(1) Produtor: sensor simulado de temperatura.

Publica eventos no topico TOPIC_RAW somente quando a variacao em relacao
a ultima leitura enviada for maior que um limiar (variacao significativa).
"""
from kafka import KafkaProducer
from const import BROKER_ADDR, BROKER_PORT, TOPIC_RAW
import json
import random
import sys
import time
from datetime import datetime, timezone


def run(sensor_id: str, threshold: float = 0.5, interval: float = 1.0):
    producer = KafkaProducer(
        bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )

    current_temp = 22.0          # temperatura inicial
    last_sent_temp = None        # ultimo valor publicado

    print(f'[sensor:{sensor_id}] iniciado (threshold={threshold} C)')
    while True:
        # leitura simulada com pequenas variacoes
        current_temp += random.uniform(-0.8, 0.8)

        if last_sent_temp is None or abs(current_temp - last_sent_temp) >= threshold:
            event = {
                'sensor_id': sensor_id,
                'temperature': round(current_temp, 2),
                'timestamp': datetime.now(timezone.utc).isoformat(),
            }
            producer.send(TOPIC_RAW, value=event)
            producer.flush()
            print(f'[sensor:{sensor_id}] -> {event}')
            last_sent_temp = current_temp
        else:
            print(f'[sensor:{sensor_id}] leitura {current_temp:.2f} C nao publicada (variacao < {threshold})')

        time.sleep(interval)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python3 sensor_producer.py <sensor_id> [threshold] [interval_s]')
        sys.exit(1)
    sid = sys.argv[1]
    th = float(sys.argv[2]) if len(sys.argv) > 2 else 0.5
    iv = float(sys.argv[3]) if len(sys.argv) > 3 else 1.0
    try:
        run(sid, th, iv)
    except KeyboardInterrupt:
        print('\n[sensor] encerrado')
