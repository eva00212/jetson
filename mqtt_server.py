#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
/barn/raw/ftm01/1 → (계산) → /barn/sensor/temp001/data, /hum001/data 발행
+ 5초마다 저장, 경로: /home/user/sensor_data/YYYY-MM-DD.jsonl
"""

import os, json, time
from datetime import datetime, timezone, timedelta
import paho.mqtt.client as mqtt

BROKER = "192.168.4.1"
RAW_TOPIC = "/barn/raw/ftm01/1"
TOPIC_TEMP = "/barn/sensor/temp001/data"
TOPIC_HUM  = "/barn/sensor/hum001/data"

SAVE_DIR = "/home/user/sensor_data"
os.makedirs(SAVE_DIR, exist_ok=True)

def kst_now(): return datetime.now(timezone(timedelta(hours=9)))
_cur_date=None; _cur_path=None
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

c = mqtt.Client(protocol=mqtt.MQTTv5)

def publish_standard(device_id, typ, value, unit, ts_iso):
    rec = {"Device_id": device_id, "Type": typ, "Value": value, "Unit": unit, "Timestamp": ts_iso}
    payload = json.dumps(rec, ensure_ascii=False)
    topic = TOPIC_TEMP if typ=="temperature" else TOPIC_HUM
    c.publish(topic, payload, qos=1)
    with open(path_today(),"a",encoding="utf-8") as f:
        f.write(payload+"\n")

def on_connect(client, userdata, flags, rc, props=None):
    print("[MQTT] connected:", rc)
    client.subscribe(RAW_TOPIC, qos=1)

def on_message(client, userdata, msg):
    try:
        raw = json.loads(msg.payload.decode("utf-8"))
    except:
        return
    if not raw.get("ok"):
        return
    hum_raw = raw.get("hum_raw")
    temp_raw = raw.get("temp_raw")
    ts_iso  = raw.get("ts") or datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    if hum_raw is not None:
        hum = float(hum_raw)/10.0
        publish_standard("hum001", "humidity", hum, "%", ts_iso)
    if temp_raw is not None:
        temp = float(temp_raw)/10.0
        publish_standard("temp001", "temperature", temp, "C", ts_iso)

if __name__=="__main__":
    c.on_connect = on_connect
    c.on_message = on_message
    c.connect(BROKER, 1883, 30)
    print("[INFO] Writing to:", SAVE_DIR)
    c.loop_start()

    # 5초마다 강제 flush 느낌의 대기 (안전한 주기 제어)
    while True:
        time.sleep(5)
