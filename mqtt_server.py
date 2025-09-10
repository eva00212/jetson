#!/usr/bin/env python3
# jetson_request_loop_dataonly.py
# - 젯슨: 각 노드에 시간(settime, epoch) 내려주기 → 5초마다 sample 요청
# - 데이터 토픽만 구독해 표준 JSON 수신/표시 + JSONL 파일 저장(일별 회전)

import json, time, signal, uuid, os
from datetime import datetime, timezone, timedelta
import paho.mqtt.client as mqtt

# ========= 필수 설정 (네 환경으로 수정) =========
BROKER = "192.168.10.1"     # 젯슨 AP IP
PORT   = 1883
PERIOD_SEC = 5              # sample 주기(초)
TIME_SYNC_SEC = 600         # settime 재동기화 주기(초, 10분)

# 노드/토픽 정의 (node1/node2 필요에 맞게 수정/추가)
NODES = {
    "node1": {
        "cmd":    "barn/node/node1/cmd",
        "rsp":    "barn/node/node1/rsp",
        "temp_t": "/barn/sensor/temp001/data",
        "humi_t": "/barn/sensor/humi001/data",
    },
    "node2": {
        "cmd":    "barn/node/node2/cmd",
        "rsp":    "barn/node/node2/rsp",
        "temp_t": "/barn/sensor/temp002/data",
        "humi_t": "/barn/sensor/humi002/data",
    }
}

# ========= 저장 설정 =========
DATA_DIR = "data"           # 저장 폴더
ROTATE_DAILY = True         # True면 data/YYYY-MM-DD.jsonl 로 저장
BASENAME = "data.jsonl"     # ROTATE_DAILY=False일 때 파일명

QOS = 1                     # 구독/퍼블리시 QoS (브로커/클라에 따라 조정)

# ========= 파일 저장 유틸 =========
def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)

def data_filepath():
    if ROTATE_DAILY:
        KST = timezone(timedelta(hours=9))
        today = datetime.now(KST).strftime("%Y-%m-%d")
        return os.path.join(DATA_DIR, f"{today}.jsonl")
    return os.path.join(DATA_DIR, BASENAME)

def append_jsonl(obj: dict):
    ensure_data_dir()
    with open(data_filepath(), "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False, separators=(',', ':')) + "\n")

# ========= MQTT =========
cli = mqtt.Client(client_id="jetson_dataonly")

def on_connect(client, userdata, flags, rc, properties=None):
    print(f"[CONNECT] rc={rc}")
    subs = []
    for conf in NODES.values():
        subs.append((conf["temp_t"], QOS))
        subs.append((conf["humi_t"], QOS))
        # 디버깅할 땐 응답도 보고 싶으면 주석 해제:
        # subs.append((conf["rsp"] + "/#", QOS))
    if subs:
        client.subscribe(subs)

def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8")
    try:
        obj = json.loads(payload)
        for k in ("device_id","type","value","unit","timestamp"):
            if k not in obj:
                raise ValueError(f"missing {k}")

        print(f"[DATA] {msg.topic} -> {payload}")

        # 수신시각도 보조로 기록
        KST = timezone(timedelta(hours=9))
        stored = {
            "device_id": obj["device_id"],
            "type": obj["type"],
            "value": obj["value"],
            "unit": obj["unit"],
            "timestamp": obj["timestamp"],                     # 노드가 찍은 시각
            "received_at": datetime.now(KST).isoformat(timespec="seconds")
        }
        append_jsonl(stored)

    except Exception as e:
        print(f"[WARN] non-conforming payload on {msg.topic}: {e} :: {payload}")

cli.on_connect = on_connect
cli.on_message = on_message

def pub(topic: str, obj: dict, retain=False, qos=QOS):
    cli.publish(topic, json.dumps(obj, ensure_ascii=False, separators=(',',':')), qos=qos, retain=retain)

# 초기 매핑(retain)
def send_setmap_all():
    for node, conf in NODES.items():
        payload = {"action": "setmap", "map": {"temp": conf["temp_t"], "hum": conf["humi_t"]}}
        print(f"[SETMAP] {node} -> {payload}")
        pub(conf["cmd"], payload, retain=True)

# 시간 동기화: 젯슨의 현재 UTC epoch 초 하달
def send_settime_all():
    epoch = int(time.time())
    for node, conf in NODES.items():
        payload = {"action": "settime", "epoch": epoch}
        print(f"[SETTIME] {node} -> {payload}")
        pub(conf["cmd"], payload, retain=False)

# 샘플 요청
def send_sample(node_name: str):
    conf = NODES[node_name]
    req_id = f"req-{uuid.uuid4().hex[:8]}"
    payload = {
        "action": "sample",
        "types": ["temperature","humidity"],
        "publish_to": [conf["temp_t"], conf["humi_t"]],
        "reply_to": conf["rsp"],
        "request_id": req_id
    }
    print(f"[SAMPLE] {node_name} -> {payload}")
    pub(conf["cmd"], payload, retain=False)
    return req_id

# 메인 루프
_running = True
def _stop(*_):
    global _running
    _running = False
signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)

def main():
    cli.connect(BROKER, PORT, keepalive=60)
    cli.loop_start()

    time.sleep(0.3)
    send_setmap_all()
    send_settime_all()  # 시작 1회 동기화

    print(f"[RUN] sampling every {PERIOD_SEC}s; time sync every {TIME_SYNC_SEC}s.")
    next_sample = time.time()
    next_timesync = time.time() + TIME_SYNC_SEC

    try:
        while _running:
            now = time.time()
            if now >= next_sample:
                for node in NODES.keys():
                    send_sample(node)
                next_sample = now + PERIOD_SEC
            if now >= next_timesync:
                send_settime_all()
                next_timesync = now + TIME_SYNC_SEC
            time.sleep(0.05)
    finally:
        print("[STOP] closing...")
        cli.loop_stop()
        cli.disconnect()

if __name__ == "__main__":
    main()
