# Streaming

![Untitled](Streaming%206387d37183fd4761b615e3fa8a45ada2/Untitled.png)

# Introduction

Streaming은 Kafka로 부터 발행된 Message Queue 정보를 유실없이 안정적으로 저장하기 위한 목적을 가집니다. 

기존의 경우 NodeJS를 통해 구독, 파싱, 트랜잭션의 작업을 한번에 처리했습니다.  그나마 부담을 덜이기 위해 Interval을 적용하고 부하가 커질 수록 Interval 주기는 길어지기만 했습니다. 또한 일부 오류에 대해서 DB Connection을 정확히 끊어주지 못해 Max-Connection-Pool이 발생하곤 했습니다.

이를 개선하기 위해 Spark Streaming을 선택했습니다. 컴퓨터의 성능을 최대한으로 끌어들일 수 있고 Kafka-MQ처리에 보다 효과적인 속도와 안정성을 보여줍니다. 데이터 처리에 대한 튜닝을 보다 편하게 대응하도록 Spark Dashboard도 제공합니다.

본 시스템의 경우 PLC, BLE 각각의 파이프라인으로 구성됩니다. 개발자가 지정한 N 초 만큼의 카운트에 따라 ‘insert Interval Thread’ 가 동작합니다.  해당 동작에 따라 PostgreSQL에 Insert를 수행합니다.

N초 동안 Kafka MQ 정보가 data payload Queue에 쌓입니다. 개발자가 데이터가 정상적으로 수집되는지 외부 콘솔영역에서 UDP 통신으로 ‘REQ’ 메시지만 날려주면 Queue의 첫번째 인덱스 정보를 읽어 반환해주기도 합니다.

# Getting Started

## Step 1 — requirement

- `openjdk_version=11`
- `spark_version=3.0.3`
- `scala_version=2.12`
- `log4j_version=2.17.1`
- `hadoop_version=3.2.2`
- `postgres_version=42.3.1`

## Step 2 — build & deploy

- 설치 및 배포에 대해서 참고하세요 [Streaming](https://www.notion.so/Streaming-3b594c39e543490e8b93a902c8823d82)

# Topic Reference

## thingarxhub, plc

구독하여 수신된 아래 데이터를 DB에 적재하기전 파싱작업 및 필터링 합니다.

```json
{
  "key": null,
  "value": "{ \"body\": { \"counts\": 1, \"packets\": [ { \"packet\": { \"data\": { \"rssi\": -59, \"payload\": [ 162, 0.04, 0.03, 0.08, 0.0, 0.0, 0.0, 78 ], \"msgId\": 32769, \"time\": \"2021-12-08T10:31:01.686381Z\", \"mac\": \"F0:B5:D1:9F:24:D8\" }, \"received.time\": \"2021-12-08T14:17:08.305097800\", \"tag\": \"coupang/vib\" } } ] }, \"status\": { \"code\": \"0000\", \"message\": \"OK\" } }",
  "topic": "thingarxhub",
  "partition": 0,
  "offset": 685367,
  "timestamp": "2021-12-23 13:17:00",
  "timestampType": 0
}
```
