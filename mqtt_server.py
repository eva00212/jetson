#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
jetson_raw_to_standard.py
- /barn/raw/ftm01/1 구독 → 원시값 파싱 → (÷10) → 표준 토픽 발행 + 파일 저장
- 파일: ~/YYYY-MM-DD.jsonl (KST 날짜 기준)
"""
import os, json
from datetime import datetime, timezone, timedelta
import paho.mqtt.client as mqtt

BROKER = "192.168.4.1"
RAW_TOPIC = "/barn/raw/ftm01/1"
TOPIC_TEMP = "/barn/sensor/temp001/data"
TOPIC_HUM  = "/barn/sensor/hum001/data"

def kst_now(): return datetime.now(timezone(timedelta(hours=9)))
_cur_date=None; _cur_path=None
def path_today():
    global _cur_date, _cur_path
    today = kst_now().strftime("%Y-%m-%d")
    if today != _cur_date:
        _cur_date=today
        _cur_path=os.path.expanduser(f"~/{today}.jsonl")
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
    with open(path_today(),"a",encoding="utf-8") as f: f.write(payload+"\n")

def on_connect(client, userdata, flags, rc, props=None):
    print("[MQTT] connected:", rc)
    client.subscribe(RAW_TOPIC, qos=1)

def on_message(client, userdata, msg):
    try:
        raw = json.loads(msg.payload.decode("utf-8"))
    except:
        return
    if not raw.get("ok"):
        # 필요시 에러 카운터/로그 처리
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
    print("[INFO] Running. Writing to:", path_today())
    c.loop_forever()
