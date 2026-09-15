import asyncio
import json
from pathlib import Path
from threading import Thread

from confluent_kafka import Consumer
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from services.common import BOOTSTRAP, TOPIC

app = FastAPI(title="OceanPulse Control Room")
INDEX = Path(__file__).with_name("index.html").read_text()

clients: set[WebSocket] = set()
loop: asyncio.AbstractEventLoop | None = None

def kafka_reader():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "oceanpulse-web-v1",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([TOPIC])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            event = json.loads(msg.value())
            payload = {
                **event,
                "_partition": msg.partition(),
                "_offset": msg.offset(),
            }
            if loop:
                asyncio.run_coroutine_threadsafe(broadcast(payload), loop)
    finally:
        consumer.close()

async def broadcast(payload: dict):
    dead = []
    for ws in list(clients):
        try:
            await ws.send_json(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)

@app.on_event("startup")
async def startup():
    global loop
    loop = asyncio.get_running_loop()
    Thread(target=kafka_reader, daemon=True).start()

@app.get("/")
async def home():
    return HTMLResponse(INDEX)

@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "kafka_bootstrap": BOOTSTRAP,
        "topic": TOPIC,
        "websocket_clients": len(clients),
    }

@app.websocket("/ws/events")
async def websocket_events(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        clients.discard(ws)
