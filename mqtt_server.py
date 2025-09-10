#!/usr/bin/env python3
# jetson_request_loop_dataonly.py
# 젯슨: 5초마다 각 노드에 sample 요청 → 노드는 데이터 토픽으로만 JSON 발행
# 젯슨은 데이터 토픽만 구독해 표준 JSON을 수신/표시 + 파일 저장(JSONL, 일별 회전)

import json, time, signal, uuid, os
from datetime import datetime
import paho.mqtt.client as mqtt

# ========= 환경 설정 =========
BROKER = "192.168.10.1"
PORT   = 1883
QOS    = 1
PERIOD_SEC = 5  # 5초 주기

# 저장 설정
DATA_DIR = "data"            # 저장 폴더
ROTATE_DAILY = True          # True면 일별 파일(data/YYYY-MM-DD.jsonl)로 저장
BASENAME = "data.jsonl"      # ROTATE_DAILY=False일 때 파일명

# 노드/토픽 정의
NODES = {
    "node1": {
        "cmd":    "barn/node/node1/cmd",     # 명령 채널(제어용, 데이터 JSON 사용 안 함)
        "rsp":    "barn/node/node1/rsp",     # 응답 채널(제어/상태용, 데이터 JSON 사용 안 함)
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

# ========= 유틸: 파일 저장 =========
def ensure_data_dir():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)

def data_filepath():
    if ROTATE_DAILY:
        today = datetime.now().strftime("%Y-%m-%d")
        return os.path.join(DATA_DIR, f"{today}.jsonl")
    else:
        return os.path.join(DATA_DIR, BASENAME)

def append_jsonl(obj: dict):
    """
    obj: {"device_id","type","value","unit","timestamp"} 만 포함(그대로 저장)
    """
    ensure_data_dir()
    fp = data_filepath()
    with open(fp, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False, separators=(',', ':')) + "\n")

# ========= MQTT =========
cli = mqtt.Client(client_id="jetson_dataonly")

def on_connect(client, userdata, flags, rc, properties=None):
    print(f"[CONNECT] rc={rc}")
    # 데이터 토픽만 구독 (온도/습도)
    subs = []
    for conf in NODES.values():
        subs.append((conf["temp_t"], QOS))
        subs.append((conf["humi_t"], QOS))
    client.subscribe(subs)

def on_message(client, userdata, msg):
    # 데이터 토픽으로부터만 들어옴(표준 JSON이라고 가정)
    payload = msg.payload.decode("utf-8")
    try:
        obj = json.loads(payload)

        # 필드 검증 (요구한 5개만 체크)
        for k in ("device_id","type","value","unit","timestamp"):
            if k not in obj:
                raise ValueError(f"missing {k}")

        # 콘솔 출력
        print(f"[DATA] {msg.topic} -> {payload}")

        # 파일 저장(JSONL)
        # 필요한 필드만 저장(여분 키가 와도 무시하고 5개만 추출)
        stored = {
            "device_id": obj["device_id"],
            "type": obj["type"],
            "value": obj["value"],
            "unit": obj["unit"],
            "timestamp": obj["timestamp"]
        }
        append_jsonl(stored)

    except Exception as e:
        print(f"[WARN] non-conforming payload on {msg.topic}: {e} :: {payload}")

cli.on_connect = on_connect
cli.on_message = on_message

def pub(topic: str, obj: dict, retain=False, qos=QOS):
    cli.publish(topic, json.dumps(obj, ensure_ascii=False, separators=(',',':')), qos=qos, retain=retain)

# ======== 초기 매핑: 데이터 토픽만 지정(retain) ========
def send_setmap_all():
    # 노드가 내부적으로 이 매핑을 사용해 데이터 토픽으로만 JSON을 발행하도록 함
    for node, conf in NODES.items():
        payload = {
            "action": "setmap",
            "map": {
                "temp": conf["temp_t"],
                "hum":  conf["humi_t"]
            }
        }
        print(f"[SETMAP] {node} -> {payload}")
        pub(conf["cmd"], payload, retain=True)

# ======== 1회 요청: 데이터 토픽으로만 발행하도록 sample ========
def send_sample(node_name: str):
    conf = NODES[node_name]
    req_id = f"req-{uuid.uuid4().hex[:8]}"
    # 노드 합의:
    # - 이 명령을 받으면 센서를 읽고, 'publish_to'의 토픽에만 표준 JSON을 발행
    # - rsp는 선택: 처리상태 텍스트나 간단 JSON(자유형)으로 응답 가능(데이터 JSON 금지)
    payload = {
        "action": "sample",
        "types": ["temperature","humidity"],                 # 무엇을 측정할지
        "publish_to": [conf["temp_t"], conf["humi_t"]],      # 데이터 토픽(표준 JSON만 발행)
        "reply_to": conf["rsp"],                             # 상태 확인용(내용 자유)
        "request_id": req_id
    }
    print(f"[SAMPLE] {node_name} -> {payload}")
    pub(conf["cmd"], payload, retain=False)
    return req_id

# ======== 메인 루프 ========
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
    send_setmap_all()  # 데이터 토픽 매핑 고정(retain)

    print(f"[RUN] periodic sample every {PERIOD_SEC}s (data topics only).")
    next_tick = time.time()
    try:
        while _running:
            now = time.time()
            if now >= next_tick:
                for node in NODES.keys():
                    send_sample(node)
                next_tick = now + PERIOD_SEC
            time.sleep(0.05)
    finally:
        print("[STOP] closing...")
        cli.loop_stop()
        cli.disconnect()

if __name__ == "__main__":
    main()
