import os
import json
import threading
from typing import TypedDict
from confluent_kafka import Consumer
from langchain_community.llms import Ollama
from langgraph.graph import StateGraph, END
from fastapi import FastAPI, Response, HTTPException

app = FastAPI(title="Analyzer Diagnostic Service")

OLLAMA_HOST = os.getenv("OLLAMA_BASE_URL", "http://host.minikube.internal:11434")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "my-cluster-kafka-bootstrap:9092")

# Инициализируем LLM-движок Ollama
llm = Ollama(base_url=OLLAMA_HOST, model=os.getenv("LLM_MODEL", "qwen2.5:1.5b"), temperature=0.2)

# --- LangGraph определение агента ---
class AgentState(TypedDict):
    service: str
    message: str
    root_cause: str
    remediation_patch: str

def diagnose_node(state: AgentState):
    prompt = f"Analyze incident for service {state['service']}: '{state['message']}'. Provide 1-sentence technical root cause."
    try:
        res = llm.invoke(prompt)
    except Exception as e:
        res = f"Diagnostic fallback: Model unreachable ({e})"
    return {"root_cause": res.strip()}

def remediation_node(state: AgentState):
    prompt = f"Root cause: '{state['root_cause']}'. Propose a 1-line Kubernetes Helm/patch fix."
    try:
        res = llm.invoke(prompt)
    except Exception as e:
        res = "Increase replicaCount and memory limits in values.yaml"
    return {"remediation_patch": res.strip()}

workflow = StateGraph(AgentState)
workflow.add_node("diagnose", diagnose_node)
workflow.add_node("remediation", remediation_node)
workflow.set_entry_point("diagnose")
workflow.add_edge("diagnose", "remediation")
workflow.add_edge("remediation", END)
agent_app = workflow.compile()

# --- Фоновый Kafka Consumer ---
def start_kafka_consumer():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': 'analyzer-group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['incidents-critical'])
    print("Analyzer Agent (System-2) listening on incidents-critical...")
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        data = json.loads(msg.value().decode('utf-8'))
        print(f"[Analyzer] Running LangGraph RCA for: {data['service_name']}")
        result = agent_app.invoke({
            "service": data["service_name"],
            "message": data["message"],
            "root_cause": "",
            "remediation_patch": ""
        })
        print(f"[RCA Done] Root cause: {result['root_cause']}")
        print(f"[Remediation Patch]: {result['remediation_patch']}")

threading.Thread(target=start_kafka_consumer, daemon=True).start()

# Эндпоинт для проверки Istio Circuit Breaker (Задание 6.2)
fail_counter = 0
@app.get("/api/v1/diagnostic-status")
def diagnostic_status(trigger_fail: bool = False):
    global fail_counter
    if trigger_fail:
        fail_counter += 1
        # Имитируем падение для срабатывания Outlier Detection в Istio
        raise HTTPException(status_code=503, detail="Simulated service failure for Circuit Breaker test")
    return {"status": "operational", "processed_fails": fail_counter}