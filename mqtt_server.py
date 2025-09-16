#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
jetson_request_and_store.py  (MQTT v3.1.1)
- 5초마다 /barn/ctrl/ftm01/cmd 로 {"cmd":"sample","corr":...} 요청
- /barn/raw/ftm01/1 수신 → 값 ÷10 → 표준 토픽 발행 + /home/user/sensor_data/YYYY-MM-DD.jsonl 저장
"""

import os, json, uuid, time, threading
from datetime import datetime, timezone, timedelta
import paho.mqtt.client as mqtt

BROKER      = "192.168.4.1"
CTRL_TOPIC  = "/barn/ctrl/ftm01/cmd"
ACK_TOPIC   = "/barn/ctrl/ftm01/ack"
RAW_TOPIC   = "/barn/raw/ftm01/1"

TOPIC_TEMP  = "/barn/sensor/temp001/data"
TOPIC_HUM   = "/barn/sensor/hum001/data"

SAVE_DIR    = "/home/user/sensor_data"
os.makedirs(SAVE_DIR, exist_ok=True)

POLL_SEC    = 5  # 5초마다 샘플 요청

# ---- 파일 경로 (KST 날짜별) ----
_cur_date=None; _cur_path=None
def kst_now(): return datetime.now(timezone(timedelta(hours=9)))
def path_today():
    global _cur_date, _cur_path
    today = kst_now().strftime("%Y-%m-%d")
    if today != _cur_date:
        _cur_date=today
        _cur_path=os.path.join(SAVE_DIR, f"{today}.jsonl")
        if not os.path.exists(_cur_path):
            open(_cur_path,"w").close()
            print("[INFO] New file:", _cur_path)
    return _cur_path

# ---- MQTT (v3.1.1) ----
c = mqtt.Client(protocol=mqtt.MQTTv311)

def publish_standard(device_id, typ, value, unit, ts_iso):
    rec = {"Device_id": device_id, "Type": typ, "Value": value, "Unit": unit, "Timestamp": ts_iso}
    payload = json.dumps(rec, ensure_ascii=False)
    topic = TOPIC_TEMP if typ=="temperature" else TOPIC_HUM
    c.publish(topic, payload, qos=1)
    with open(path_today(),"a",encoding="utf-8") as f:
        f.write(payload+"\n")

def on_connect(client, userdata, flags, rc):
    print("[MQTT] connected:", rc)
    client.subscribe(RAW_TOPIC, qos=1)
    client.subscribe(ACK_TOPIC, qos=1)

def on_message(client, userdata, msg):
    t = msg.topic
    if t == RAW_TOPIC:
        try:
            raw = json.loads(msg.payload.decode("utf-8"))
        except:
            return
        if not raw.get("ok"):   # 읽기 실패면 스킵
            return
        hum_raw  = raw.get("hum_raw")
        temp_raw = raw.get("temp_raw")
        ts_iso   = raw.get("ts") or datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        if hum_raw is not None:
            hum = float(hum_raw)/10.0
            publish_standard("hum001","humidity", hum, "%", ts_iso)
        if temp_raw is not None:
            temp = float(temp_raw)/10.0
            publish_standard("temp001","temperature", temp, "C", ts_iso)

    elif t == ACK_TOPIC:
        # 필요하면 corr 매칭/로그 추가
        pass

def request_loop():
    while True:
        corr = uuid.uuid4().hex[:8]
        msg = {"cmd":"sample","corr":corr,"args":{}}
        c.publish(CTRL_TOPIC, json.dumps(msg, ensure_ascii=False), qos=1)
        # print("[TX]", CTRL_TOPIC, msg)
        time.sleep(POLL_SEC)

if __name__=="__main__":
    c.on_connect = on_connect
    c.on_message = on_message
    c.connect(BROKER, 1883, 30)

    # RAW 저장 경로 안내
    print("[INFO] Writing to:", SAVE_DIR)

    # 요청 스레드 시작(5초마다 sample 요청)
    th = threading.Thread(target=request_loop, daemon=True)
    th.start()

    c.loop_forever()
