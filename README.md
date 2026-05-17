[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/A6uVSc3Y)

# Exercicio 9 - Kafka + gRPC

Sistema minimo que combina os dois paradigmas vistos nos exercicios anteriores:

- **pub-sub** (Kafka, ex7) para o fluxo de eventos do sensor
- **cliente-servidor** (gRPC, ex8) para consulta dos dados processados

## Arquitetura

```
(1) sensor_producer.py       (Kafka producer)
        |
        v   topic: temperature_readings
(2) stats_processor.py       (Kafka consumer + producer)
        |
        v   topic: temperature_stats
(3) temperature_service.py   (Kafka consumer + gRPC server + SQLite)
        ^
        |   gRPC
(4) temperature_client.py    (gRPC client)
```

### Cenario

- (1) **Sensor simulado** gera leituras de temperatura. Publica em `temperature_readings`
  *somente* quando a variacao em relacao a ultima leitura enviada e
  significativa (limiar configuravel, padrao 0.5 C).
- (2) **Processador** consome `temperature_readings`, mantem por sensor uma
  janela deslizante de 2 horas e publica em `temperature_stats` a media
  movel (alem de min/max e contagem).
- (3) **Servico** consome `temperature_stats`, persiste em SQLite e expoe
  endpoints gRPC para consulta.
- (4) **Cliente** chama o servico gRPC para obter a ultima estatistica,
  estatistica por sensor, historico, lista de sensores e contagem total.

## Endpoints gRPC

Definidos em `protos/TemperatureService.proto`:

| RPC                       | Request           | Response               |
| ------------------------- | ----------------- | ---------------------- |
| `GetLatestStats`          | `EmptyMessage`    | `TemperatureStats`     |
| `GetLatestStatsBySensor`  | `SensorID`        | `TemperatureStats`     |
| `GetHistory`              | `HistoryRequest`  | `TemperatureStatsList` |
| `ListSensors`             | `EmptyMessage`    | `SensorList`           |
| `CountStats`              | `EmptyMessage`    | `StatsCount`           |

## Como executar

### 0) Pre-requisitos

- Broker Kafka acessivel (ver instrucoes em `ex7`).
- Python 3 com as dependencias:

```
sudo apt update
sudo apt install python3-pip python3-venv
python3 -m venv myvenv
source myvenv/bin/activate
pip3 install kafka-python grpcio grpcio-tools
```

### 1) Configurar enderecos

- `const.py` (raiz) e `python/const.py`: ajustar `BROKER_ADDR` para o IP do
  broker Kafka.
- `python/const.py`: ajustar `CLIENT_IP` para o IP onde o servico gRPC
  estara rodando, e `GRPC_IP`/`GRPC_PORT` no servidor (use `0.0.0.0`
  para aceitar conexoes externas).

### 2) Compilar o `.proto`

```
cd python
python3 -m grpc_tools.protoc -I../protos \
    --python_out=. --grpc_python_out=. \
    ../protos/TemperatureService.proto
```

Isso gera `TemperatureService_pb2.py` e `TemperatureService_pb2_grpc.py`
dentro de `python/`.

### 3) Subir o servico (consumidor + gRPC)

Em um terminal (na pasta `python/`):

```
python3 temperature_service.py
```

Ele cria/usa `temperature_stats.db` (SQLite) no diretorio corrente.

### 4) Subir o processador

Em outro terminal (na raiz do projeto):

```
python3 stats_processor.py
```

### 5) Subir um (ou mais) sensor

Em outro(s) terminal(is) (na raiz do projeto):

```
python3 sensor_producer.py sensor-A
python3 sensor_producer.py sensor-B 0.3 0.5
```

Argumentos: `<sensor_id> [threshold C] [intervalo s]`.

### 6) Consultar via cliente gRPC

Em outro terminal (na pasta `python/`):

```
python3 temperature_client.py            # consulta geral
python3 temperature_client.py sensor-A 10  # historico do sensor-A (ate 10 entradas)
```

## Observacoes

- O sensor so publica quando ha variacao significativa - leituras
  estaveis nao geram trafego.
- A janela de 2 horas e calculada por sensor, com base nos timestamps
  recebidos pelo processador.
- A persistencia e feita em SQLite local para manter o exercicio simples;
  a interface gRPC ficaria identica caso fosse trocada por outro
  back-end.
