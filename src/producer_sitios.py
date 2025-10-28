from kafka import KafkaProducer
import csv
import json
import time

# Inicializar el productor
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Ruta al archivo CSV
with open('/home/vboxuser/stscl.csv', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for i, row in enumerate(reader):
        # Enviar el mensaje a Kafka
        producer.send('sitios_lorica', value=row)
        print(f"✅ [{i + 1}] Enviado: {row['Nombre'][:50]}...")
        time.sleep(2)# 2 segundos entre envios para ver mejor el streaming

producer.flush()
print("✔️ Envío de datos completado. Todos los patrimonios enviados a kafka.")



