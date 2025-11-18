from kafka import KafkaProducer
import time
import json
import numpy as np
import random

DIRECCIONES = ["N", "NO", "O", "SO", "S", "SE", "E", "NE"]

TOPIC = "22217" # Mi carné

def generar_sensor():
    temp = np.random.normal(25, 10)
    temp = round(min(max(temp, 0), 110), 2)

    hum = np.random.normal(60, 15)
    hum = int(min(max(hum, 0), 100))

    wind = random.choice(DIRECCIONES)

    return {
        "temperatura": temp,
        "humedad": hum,
        "direccion_viento": wind
    }

producer = KafkaProducer(
    bootstrap_servers="iot.redesuvg.cloud:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Producer en ejecución...")

while True:
    data = generar_sensor()
    print("Enviando:", data)
    producer.send(TOPIC, data)
    producer.flush()

    time.sleep(random.randint(15, 30))
