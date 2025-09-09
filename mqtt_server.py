# mqtt_server.py
import paho.mqtt.client as mqtt
import os, datetime

BROKER = "localhost"                  # Mosquitto 브로커 주소
TOPIC  = "uplink/sht31/lines"         # Jetson이 퍼블리시하는 토픽
SAVE_DIR = r"C:/Users/YJ/Downloads"             # 저장 폴더

os.makedirs(SAVE_DIR, exist_ok=True)

def get_daily_path():
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    return os.path.join(SAVE_DIR, f"sht31_{today_str}.jsonl")

def on_connect(client, userdata, flags, rc, properties=None):
    print("Connected to broker, rc=", rc)
    client.subscribe(TOPIC, qos=1)

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8", errors="ignore").strip()
        path = get_daily_path()
        with open(path, "a", encoding="utf-8") as f:
            f.write(payload + "\n")
        print(f"[SAVED] {path} <= {payload}")
    except Exception as e:
        print("Error saving:", e)

def main():
    client = mqtt.Client(client_id="windows-jsonl-saver")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER, 1883, keepalive=60)
    client.loop_forever()

if __name__ == "__main__":
    main()
