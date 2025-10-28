Sistema de Análisis de Patrimonio Cultural - Lorica

Descripción

Sistema de análisis en tiempo real para clasificación automática de 50 bienes patrimoniales de Santa Cruz de Lorica usando Apache Spark Streaming y Kafka. El sistema procesa datos patrimoniales, los clasifica por prioridad de conservación y genera reportes ejecutivos automáticos.

Objetivo

Clasificar y priorizar automáticamente el patrimonio cultural de Lorica para:

Identificar prioridades de conservación
Clasificar por tipo, propiedad y zona
Recomendar usos turísticos óptimos
Generar reportes ejecutivos automáticos

Arquitectura del Sistema

[CSV Patrimonial] → [Kafka Producer] → [Kafka Topic] → [Spark Streaming] → [Análisis Inteligente]
                                                              ↓
                                      [Reportes Ejecutivos] + [Detalle Operativo]
Enlaces del conjunto de datos

Fuente: https://www.datos.gov.co/d/ic3a-dm3e

Instrucciones de Ejecución

Prerrequisitos

Apache Kafka 2.13-3.7.2
Apache Spark 3.5.0
Python 3 con librerías: kafka-python, pyspark

Ejecución Paso a Paso

1.Iniciar Infraestructura Kafka

Terminal 1 - Zookeeper:

cd kafka_2.13-3.7.2
bin/zookeeper-server-start.sh config/zookeeper.properties

Terminal 2 - Kafka Server:

cd kafka_2.13-3.7.2  
bin/kafka-server-start.sh config/server.properties

2. Ejecutar Generador de Datos

python src/producer_sitios.py

Verás: "Enviado: Casa de Rosa María..." - enviando 50 patrimonios*

3. Ejecutar Análisis en Tiempo Real

spark-submit src/spark_streaming_stscl.py

Verás batches procesándose con resultados en tiempo real

Resultados del Análisis

Resumen Ejecutivo (Para Gerentes)

CATEGORÍA                          | PROPIEDAD | ZONA  | TOTAL_BIENES
-----------------------------------|-----------|-------|-------------
Patrimonio cultural material inmueble | Pública   | URBANO | 14
Patrimonio cultural material inmueble | Privada   | URBANO | 12  
Áreas protegidas                      | Pública   | RURAL  | 2
Patrimonio Cultural Mueble           | Pública   | URBANO | 9

Detalle Operativo (Para Técnicos)

BIEN CULTURAL               | PRIORIDAD               | USO RECOMENDADO
---------------------------|-------------------------|-------------------
Casa de Rosa María         | MEDIA - Intervención    | RESIDENCIAL
Cementerio central         | ALTA - Acceso directo   | FUNERARIO/CULTURAL
Ciénaga de Bañó           | ALTA - Acceso directo   | ECOTURISMO
Parque Simón Bolívar       | ALTA - Acceso directo   | ESPACIO PÚBLICO

Sistema de Priorización

ALTA - Acceso directo: Bienes públicos para acceso inmediato
MEDIA - Intervención necesaria: Patrimonios materiales que requieren mantenimiento
BAJA - Monitoreo: Bienes que solo necesitan vigilancia

Estructura del Proyecto

proyecto-spark-patrimonio-lorica/
├──  src/                           # Códigos fuente
│   ├── producer_sitios.py          # Generador Kafka - envía datos
│   └── spark_streaming_stscl.py    # Spark Streaming - analiza datos
├── data/                            # Dataset original  
│   └── stscl.csv                   # 50 bienes patrimoniales
└── docs/                           # Documentación
    └── README.md                   # Este archivo


Autor

Jhonatan Jair Sibaja Perez
Universidad Nacional Abierta y a Distancia (UNAD)

Licencia

Este proyecto es con fines educativos como parte de [Big Data].
