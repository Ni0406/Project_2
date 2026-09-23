import os
import json
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel
import redis
from confluent_kafka import Producer
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

app = FastAPI(title="Ingestion Service")

# Метрики для Prometheus (Часть 4 ТЗ)
REQUEST_COUNT = Counter("http_requests_total", "Total HTTP Requests", ["method", "endpoint", "status"])
LATENCY = Histogram("http_request_duration_seconds", "HTTP Request Duration", ["endpoint"])

# Конфигурация из переменных окружения (для K8s ConfigMap/Secret)
REDIS_HOST = os.getenv("REDIS_HOST", "valkey-redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "my-cluster-kafka-bootstrap:9092")

# Подключение к Valkey (Redis) и Kafka
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
kafka_producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})

class IncidentPayload(BaseModel):
    service_name: str
    message: str
    error_code: int

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/api/v1/incidents")
def report_incident(incident: IncidentPayload):
    with LATENCY.labels(endpoint="/api/v1/incidents").time():
        # 1. Rate Limiting на уровне сервиса через Valkey/Redis
        client_key = f"rate_limit:{incident.service_name}"
        current_reqs = r.incr(client_key)
        if current_reqs == 1:
            r.expire(client_key, 60) # Окно в 1 минуту
        
        if current_reqs > 100: # Лимит 100 запросов в минуту
            REQUEST_COUNT.labels(method="POST", endpoint="/api/v1/incidents", status="429").inc()
            raise HTTPException(status_code=429, detail="Rate limit exceeded")

        # 2. Отправка события в Kafka
        event = incident.model_dump()
        kafka_producer.produce(
            topic="incidents-raw",
            key=incident.service_name,
            value=json.dumps(event).encode('utf-8')
        )
        kafka_producer.flush(timeout=1.0)
        
        REQUEST_COUNT.labels(method="POST", endpoint="/api/v1/incidents", status="200").inc()
        return {"status": "accepted", "service": incident.service_name}