#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
5초마다 아두이노에 sample 요청 → RAW 수신 → 계산 → 표준토픽 발행 + 파일저장
저장 위치: /home/user/sensor_data/YYYY-MM-DD.jsonl
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

POLL_SEC    = 5  # 5초마다 요청

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
            print(f"[INFO] 📁 새 파일 생성: {_cur_path}")
    return _cur_path

c = mqtt.Client(protocol=mqtt.MQTTv311)

def publish_standard(device_id, typ, value, unit, ts_iso):
    rec = {"Device_id": device_id, "Type": typ, "Value": value, "Unit": unit, "Timestamp": ts_iso}
    payload = json.dumps(rec, ensure_ascii=False)
    topic = TOPIC_TEMP if typ=="temperature" else TOPIC_HUM
    c.publish(topic, payload, qos=1)
    with open(path_today(),"a",encoding="utf-8") as f:
        f.write(payload+"\n")

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ MQTT connected")
        print(f"[INFO] 저장 위치: {SAVE_DIR}")
    else:
        print(f"⚠️ MQTT 연결 실패 rc={rc}")
    client.subscribe(RAW_TOPIC, qos=1)
    client.subscribe(ACK_TOPIC, qos=1)

def on_message(client, userdata, msg):
    if msg.topic == RAW_TOPIC:
        try:
            raw = json.loads(msg.payload.decode("utf-8"))
        except:
            print("⚠️ RAW 메시지 JSON 파싱 실패")
            return
        if not raw.get("ok"):
            print("⚠️ RAW 수신 실패 (ok=false)")
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

        print(f"📥 RAW 수신 → 저장 완료 | 온도={temp_raw/10.0}°C 습도={hum_raw/10.0}%")

def request_loop():
    while True:
        corr = uuid.uuid4().hex[:8]
        msg = {"cmd":"sample","corr":corr,"args":{}}
        c.publish(CTRL_TOPIC, json.dumps(msg, ensure_ascii=False), qos=1)
        print(f"📤 sample 요청 전송 (corr={corr})")
        time.sleep(POLL_SEC)

if __name__=="__main__":
    c.on_connect = on_connect
    c.on_message = on_message
    c.connect(BROKER, 1883, 30)

    th = threading.Thread(target=request_loop, daemon=True)
    th.start()

    c.loop_forever()
