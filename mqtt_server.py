#!/usr/bin/env python3
# 젯슨: setmap(retain) → settime(retain, 웜업 재전송) → 주기 샘플
# 데이터 토픽만 구독 + JSONL 저장(일별 회전)

import json, time, signal, uuid, os
from datetime import datetime, timezone, timedelta
import paho.mqtt.client as mqtt

# ===== 설정 =====
BROKER = "192.168.10.1"
PORT   = 1883
PERIOD_SEC = 5               # 샘플 주기
TIME_SYNC_SEC = 600          # 정기 settime 간격(초)
WARMUP_SEC = 30              # 시작 후 웜업 기간(초)
WARMUP_SETTIME_INTERVAL = 2  # 웜업 중 settime 재전송 간격(초)

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

# 저장 설정
DATA_DIR = "data"
ROTATE_DAILY = True
BASENAME = "data.jsonl"
QOS = 1

# ===== 저장 유틸 =====
def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)

def data_filepath():
    kst = timezone(timedelta(hours=9))
    if ROTATE_DAILY:
        today = datetime.now(kst).strftime("%Y-%m-%d")
        return os.path.join(DATA_DIR, f"{today}.jsonl")
    return os.path.join(DATA_DIR, BASENAME)

def append_jsonl(obj: dict):
    ensure_data_dir()
    with open(data_filepath(), "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False, separators=(',', ':')) + "\n")

# ===== MQTT =====
cli = mqtt.Client(client_id="jetson_dataonly")

def on_connect(client, userdata, flags, rc, properties=None):
    print(f"[CONNECT] rc={rc}")
    subs = []
    for conf in NODES.values():
        subs.append((conf["temp_t"], QOS))
        subs.append((conf["humi_t"], QOS))
        # 디버깅 원하면 응답도:
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

        kst = timezone(timedelta(hours=9))
        stored = {
            "device_id": obj["device_id"],
            "type": obj["type"],
            "value": obj["value"],
            "unit": obj["unit"],
            "timestamp": obj["timestamp"],                         # 노드 시각
            "received_at": datetime.now(kst).isoformat(timespec="seconds")
        }
        append_jsonl(stored)

    except Exception as e:
        print(f"[WARN] non-conforming payload on {msg.topic}: {e} :: {payload}")

cli.on_connect = on_connect
cli.on_message = on_message

def pub(topic: str, obj: dict, retain=False, qos=QOS):
    cli.publish(topic, json.dumps(obj, ensure_ascii=False, separators=(',',':')), qos=qos, retain=retain)

def send_setmap_all():
    for node, conf in NODES.items():
        payload = {"action":"setmap","map":{"temp":conf["temp_t"],"hum":conf["humi_t"]}}
        print(f"[SETMAP] {node} -> {payload}")
        pub(conf["cmd"], payload, retain=True)

def send_settime_all(retain=True):
    epoch = int(time.time())  # UTC epoch
    for node, conf in NODES.items():
        payload = {"action":"settime","epoch":epoch}
        print(f"[SETTIME] {node} -> {payload} (retain={retain})")
        pub(conf["cmd"], payload, retain=retain)

def send_sample(node_name: str):
    conf = NODES[node_name]
    req_id = f"req-{uuid.uuid4().hex[:8]}"
    payload = {
        "action":"sample",
        "types":["temperature","humidity"],
        "publish_to":[conf["temp_t"], conf["humi_t"]],
        "reply_to": conf["rsp"],
        "request_id": req_id
    }
    print(f"[SAMPLE] {node_name} -> {payload}")
    pub(conf["cmd"], payload, retain=False)
    return req_id

# ===== 메인 =====
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
    send_settime_all(retain=True)  # 시작 즉시 1회(보존)

    start = time.time()
    next_sample = time.time() + PERIOD_SEC
    next_timesync = time.time() + TIME_SYNC_SEC
    next_warmup = start  # 웜업 중 2초 간격으로 재전송

    print(f"[RUN] sample={PERIOD_SEC}s, time_sync={TIME_SYNC_SEC}s, warmup={WARMUP_SEC}s")
    try:
        while _running:
            now = time.time()

            # 웜업: 초기 WARMUP_SEC 동안 2초마다 settime 재전송
            if now - start <= WARMUP_SEC and now >= next_warmup:
                send_settime_all(retain=True)
                next_warmup = now + WARMUP_SETTIME_INTERVAL

            # 정기 time sync
            if now >= next_timesync:
                send_settime_all(retain=True)
                next_timesync = now + TIME_SYNC_SEC

            # 주기 샘플
            if now >= next_sample:
                for node in NODES.keys():
                    send_sample(node)
                next_sample = now + PERIOD_SEC

            time.sleep(0.05)
    finally:
        print("[STOP] closing...")
        cli.loop_stop()
        cli.disconnect()

if __name__ == "__main__":
    main()
