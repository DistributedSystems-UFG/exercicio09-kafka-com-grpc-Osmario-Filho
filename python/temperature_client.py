"""(4) Cliente gRPC: consulta o servico para obter ultimas/historicas estatisticas."""
from __future__ import print_function
import logging
import sys

import grpc
import const
import TemperatureService_pb2 as pb
import TemperatureService_pb2_grpc as pb_grpc


def run(sensor_id: str = None, history_limit: int = 5):
    addr = const.CLIENT_IP + ':' + const.GRPC_PORT
    print(f'[client] conectando em {addr}')
    with grpc.insecure_channel(addr) as channel:
        stub = pb_grpc.TemperatureServiceStub(channel)

        # Lista sensores conhecidos
        sensors = stub.ListSensors(pb.EmptyMessage())
        print('Sensores conhecidos: ' + str(list(sensors.sensor_ids)))

        # Total de estatisticas armazenadas
        total = stub.CountStats(pb.EmptyMessage())
        print('Total de estatisticas armazenadas: ' + str(total.count))

        # Ultima estatistica geral
        latest = stub.GetLatestStats(pb.EmptyMessage())
        print('\nUltima estatistica registrada:')
        print(latest)

        # Se um sensor_id foi informado, faz consultas adicionais
        target = sensor_id or (list(sensors.sensor_ids)[0] if sensors.sensor_ids else None)
        if target is None:
            print('Nenhum sensor disponivel para consulta detalhada.')
            return

        print(f'\nUltima estatistica do sensor "{target}":')
        latest_s = stub.GetLatestStatsBySensor(pb.SensorID(sensor_id=target))
        print(latest_s)

        print(f'\nHistorico (ultimos {history_limit}) do sensor "{target}":')
        hist = stub.GetHistory(pb.HistoryRequest(sensor_id=target, limit=history_limit))
        for i, s in enumerate(hist.stats, 1):
            print(f'--- {i} ---')
            print(s)


if __name__ == '__main__':
    logging.basicConfig()
    sid = sys.argv[1] if len(sys.argv) > 1 else None
    lim = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    run(sid, lim)
