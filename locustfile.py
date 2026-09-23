from locust import HttpUser, task, between
import random

class IncidentLoadTester(HttpUser):
    wait_time = between(0.05, 0.2)

    @task(3)
    def send_incident_event(self):
        """Шлем поток инцидентов, которые генерируют события в Kafka"""
        payload = {
            "service_name": random.choice(["payment-api", "auth-service", "order-backend"]),
            "message": "High CPU utilization detected over 90%",
            "error_code": 500
        }

        with self.client.post("/api/v1/incidents", json=payload, catch_response=True) as response:
            if response.status_code in [200, 429]:
                response.success()
            else:
                response.failure(f"Unexpected status: {response.status_code}")

    @task(1)
    def check_health(self):
        self.client.get("/healthz")