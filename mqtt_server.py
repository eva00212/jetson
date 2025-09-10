#include <WiFi.h>
#include <PubSubClient.h>      // QoS 0만 지원. QoS1 원하면 AsyncMqttClient 등 사용.
#include <ArduinoJson.h>
#include <Preferences.h>
#include <Adafruit_SHT31.h>
#include <time.h>

// ======== 환경설정 ========
// 젯슨 AP SSID/PASS (젯슨 AP에 붙는 경우)
const char* WIFI_SSID = "JETSON_AP";
const char* WIFI_PASS = "your_ap_password";

// Mosquitto 브로커 (젯슨 AP IP)
const char* MQTT_HOST = "192.168.10.1";
const uint16_t MQTT_PORT = 1883;

// 노드ID (젯슨 스크립트의 NODES 키와 일치)
const char* NODE_ID = "node1";

// 명령/응답 토픽(응답은 상태용. 데이터 JSON은 '절대' rsp에 쓰지 않음)
String TOPIC_CMD = String("barn/node/") + NODE_ID + "/cmd";
String TOPIC_RSP = String("barn/node/") + NODE_ID + "/rsp";

// 기본 데이터 토픽(SETMAP 이전 기본값; 이후 NVS에 저장/복원)
String TOPIC_TEMP = "/barn/sensor/temp001/data";
String TOPIC_HUMI = "/barn/sensor/humi001/data";

// device_id 고정(원하는 규칙으로 바꾸세요)
const char* DEVICE_ID_TEMP = "temp001";
const char* DEVICE_ID_HUMI = "humi001";

// ======== 전역 ========
WiFiClient wifi;
PubSubClient mqtt(wifi);
Preferences prefs;
Adafruit_SHT31 sht31 = Adafruit_SHT31();

// ======== NTP (선택) ========
void setupTime() {
  // 젯슨이 인터넷 NAT을 제공하지 않으면 외부 NTP가 안될 수 있음.
  // 그 경우 timestamp는 로컬 시간 기준 or 공란 처리.
  configTzTime("KST-9", "pool.ntp.org", "time.google.com");
}

String nowISO8601() {
  struct tm timeinfo;
  if (!getLocalTime(&timeinfo, 2000)) { // 2초 대기
    // NTP 미동기화 시 fallback (필요시 ""로 두거나 임시값)
    return "1970-01-01T00:00:00Z";
  }
  char buf[40];
  strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S%z", &timeinfo);
  // %z는 +0900 형태 → +09:00 형태로 바꾸기
  String s(buf);
  if (s.length() >= 5) s = s.substring(0, s.length()-2) + ":" + s.substring(s.length()-2);
  return s;
}

// ======== WiFi ========
void ensureWiFi() {
  if (WiFi.status() == WL_CONNECTED) return;
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS);
  Serial.printf("[WiFi] connecting to %s ...\n", WIFI_SSID);
  while (WiFi.status() != WL_CONNECTED) { delay(500); Serial.print("."); }
  Serial.printf("\n[WiFi] connected. IP=%s\n", WiFi.localIP().toString().c_str());
}

// ======== MQTT ========
void onMqttMessage(char* topic, byte* payload, unsigned int len) {
  // cmd만 구독
  StaticJsonDocument<512> doc;
  DeserializationError err = deserializeJson(doc, payload, len);
  if (err) {
    Serial.printf("[CMD] JSON parse error: %s\n", err.c_str());
    return;
  }
  const char* action = doc["action"] | "";
  if (strcmp(action,"setmap")==0) {
    const char* t = doc["map"]["temp"] | nullptr;
    const char* h = doc["map"]["hum"]  | nullptr;
    if (t) TOPIC_TEMP = String(t);
    if (h) TOPIC_HUMI = String(h);
    // NVS 저장
    prefs.begin("topics", false);
    if (t) prefs.putString("temp", TOPIC_TEMP);
    if (h) prefs.putString("hum",  TOPIC_HUMI);
    prefs.end();
    Serial.printf("[SETMAP] temp=%s, hum=%s\n", TOPIC_TEMP.c_str(), TOPIC_HUMI.c_str());

    // 상태 응답(데이터 JSON 아님)
    StaticJsonDocument<128> rsp;
    rsp["action"] = "setmap";
    rsp["status"] = "ok";
    char buf[128]; size_t n = serializeJson(rsp, buf);
    mqtt.publish((TOPIC_RSP + "/setmap").c_str(), buf, false); // retained=false, QoS0
  }
  else if (strcmp(action,"sample")==0) {
    // 측정할 타입 & 발행할 토픽(순서대로 매핑)
    JsonArray types = doc["types"].as<JsonArray>();
    JsonArray pubs  = doc["publish_to"].as<JsonArray>();

    int count = min(types.size(), pubs.size());
    for (int i=0; i<count; ++i) {
      const char* typ = types[i] | "";
      const char* pub = pubs[i]  | "";
      if (!typ || !pub || strlen(pub)==0) continue;

      if (strcmp(typ,"temperature")==0) {
        float t = sht31.readTemperature(); // C
        publishSensorJSON(pub, DEVICE_ID_TEMP, "temperature", t, "C");
      } else if (strcmp(typ,"humidity")==0) {
        float h = sht31.readHumidity();    // %RH
        publishSensorJSON(pub, DEVICE_ID_HUMI, "humidity", h, "%RH");
      } // 필요시 CO2/VOC 등 추가
    }

    // 상태 응답(데이터 JSON 금지)
    const char* reqid = doc["request_id"] | "";
    if (reqid && strlen(reqid)>0) {
      String to = String(doc["reply_to"] | TOPIC_RSP.c_str());
      StaticJsonDocument<192> rsp;
      rsp["action"] = "sample";
      rsp["status"] = "ok";
      rsp["request_id"] = reqid;
      char buf[192]; size_t n = serializeJson(rsp, buf);
      mqtt.publish((to + "/" + reqid).c_str(), buf, false);
    }
  }
}

void ensureMqtt() {
  if (mqtt.connected()) return;
  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setCallback(onMqttMessage);
  Serial.printf("[MQTT] connecting %s:%d ...\n", MQTT_HOST, MQTT_PORT);
  while (!mqtt.connected()) {
    if (mqtt.connect(NODE_ID)) {
      Serial.println("[MQTT] connected");
      mqtt.subscribe(TOPIC_CMD.c_str()); // cmd만 구독
      break;
    } else {
      Serial.printf("[MQTT] failed rc=%d, retry in 2s\n", mqtt.state());
      delay(2000);
    }
  }
}

// ======== 센서 초기화 ========
bool setupSHT31() {
  if (!sht31.begin(0x44)) { // 0x45 모델이면 주소 바꾸세요
    Serial.println("[SHT31] not found!");
    return false;
  }
  sht31.heater(false);
  return true;
}

// ======== 데이터 발행(JSON 규격 고정) ========
void publishSensorJSON(const char* topic, const char* device_id,
                       const char* type, float value, const char* unit) {
  StaticJsonDocument<192> doc;
  doc["device_id"] = device_id;
  doc["type"]      = type;
  doc["value"]     = value;     // NaN이면 수신측에서 걸러질 수 있음
  doc["unit"]      = unit;
  doc["timestamp"] = nowISO8601();

  char buf[256];
  size_t n = serializeJson(doc, buf, sizeof(buf));
  bool ok = mqtt.publish(topic, buf, false);  // retain=false, (PubSubClient는 QoS0 고정)
  Serial.printf("[PUB] %s -> %s (%s)\n", topic, buf, ok?"ok":"fail");
}

// ======== 설정 복원 ========
void restoreTopics() {
  prefs.begin("topics", true);
  String t = prefs.getString("temp", TOPIC_TEMP);
  String h = prefs.getString("hum",  TOPIC_HUMI);
  prefs.end();
  TOPIC_TEMP = t;
  TOPIC_HUMI = h;
  Serial.printf("[RESTORE] temp=%s, hum=%s\n", TOPIC_TEMP.c_str(), TOPIC_HUMI.c_str());
}

// ======== Arduino 표준 ========
void setup() {
  Serial.begin(115200);
  delay(100);

  restoreTopics();
  ensureWiFi();
  setupTime();     // NTP 시도(안되면 1970 타임스탬프 fallback)
  setupSHT31();
  ensureMqtt();
}

void loop() {
  ensureWiFi();
  ensureMqtt();
  mqtt.loop();
  delay(10);
}
