import random
import json
import uuid
import time
from datetime import datetime, timedelta, UTC
from kafka import KafkaProducer

# =====================================================
# Azure Event Hub Configuration (Kafka Compatible)
# =====================================================
EVENTHUBS_NAMESPACE = "<<NAMESPACE_HOST_NAME>>"
EVENT_HUB_NAME = "<<EVENT_HUB_NAME>>"

CONNECTION_STRING = "<<NAMESPACE_CONNECTION_STRING>>"

# =====================================================
# Kafka Producer Configuration
# =====================================================
producer = KafkaProducer(
    bootstrap_servers=[f"{EVENTHUBS_NAMESPACE}:9093"],
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username="$ConnectionString",
    sasl_plain_password=CONNECTION_STRING,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),

    # Azure Event Hub tuning
    request_timeout_ms=60000,
    api_version_auto_timeout_ms=60000,
    metadata_max_age_ms=60000,
    retries=5,
    linger_ms=10
)

# =====================================================
# Static Reference Data
# =====================================================
departments = [
    "Emergency", "Surgery", "ICU",
    "Pediatrics", "Maternity", "Oncology", "Cardiology"
]

genders = ["Male", "Female"]

# =====================================================
# Helper: Inject Dirty Data
# =====================================================
def inject_dirty_data(record):
    # 5% invalid age
    if random.random() < 0.05:
        record["age"] = random.randint(101, 150)

    # 5% future admission time
    if random.random() < 0.05:
        record["admission_time"] = (
            datetime.now(UTC) + timedelta(hours=random.randint(1, 72))
        ).isoformat()

    return record

# =====================================================
# Event Generator
# =====================================================
def generate_patient_event():
    admission_time = datetime.now(UTC) - timedelta(hours=random.randint(0, 72))
    discharge_time = admission_time + timedelta(hours=random.randint(1, 72))

    event = {
        "patient_id": str(uuid.uuid4()),
        "gender": random.choice(genders),
        "age": random.randint(1, 100),
        "department": random.choice(departments),
        "admission_time": admission_time.isoformat(),
        "discharge_time": discharge_time.isoformat(),
        "bed_id": random.randint(1, 500),
        "hospital_id": random.randint(1, 7)
    }

    return inject_dirty_data(event)

# =====================================================
# Continuous Producer Loop
# =====================================================
print("Patient Flow Generator started...")

while True:
    event = generate_patient_event()

    try:
        producer.send(EVENT_HUB_NAME, value=event)
        producer.flush()
        print("Sent event:", event["patient_id"])
    except Exception as e:
        print(" Kafka send error:", e)

    time.sleep(2)

# iam adding these line to what changes in the git hub to see