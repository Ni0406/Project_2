import os
import json
import time
import psycopg2
from confluent_kafka import Consumer, Producer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "my-cluster-kafka-bootstrap:9092")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_NAME = os.getenv("DB_NAME", "incidents_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "postgres")

# Инициализация таблицы в PostgreSQL
def init_db():
    while True:
        try:
            conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS incidents (
                        id SERIAL PRIMARY KEY,
                        service_name VARCHAR(100),
                        message TEXT,
                        error_code INT,
                        severity VARCHAR(20),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                conn.commit()
            conn.close()
            print("Connected to PostgreSQL successfully.")
            break
        except Exception as e:
            print(f"Waiting for DB... {e}")
            time.sleep(3)

init_db()

consumer = Consumer({
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'group.id': 'triage-group',
    'auto.offset.reset': 'earliest'
})
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})

consumer.subscribe(['incidents-raw'])

print("Triage Agent (System-1) listening on incidents-raw...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue

    data = json.loads(msg.value().decode('utf-8'))
    
    # System-1 быстрый детерминированный классификатор (TypeSafe / Rule-based)
    error_code = data.get("error_code", 0)
    if error_code >= 500:
        severity = "CRITICAL"
    elif error_code >= 400:
        severity = "WARNING"
    else:
        severity = "INFO"

    # Сохранение в Postgres
    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO incidents (service_name, message, error_code, severity) VALUES (%s, %s, %s, %s)",
            (data["service_name"], data["message"], error_code, severity)
        )
        conn.commit()
    conn.close()

    # Если сбой серьезный — перенаправляем на глубокий анализ агенту (System-2)
    if severity == "CRITICAL":
        data["severity"] = severity
        producer.produce('incidents-critical', key=data["service_name"], value=json.dumps(data).encode('utf-8'))
        producer.flush(1.0)
        print(f"[Triage] Escalate critical incident from {data['service_name']} to Analyzer")