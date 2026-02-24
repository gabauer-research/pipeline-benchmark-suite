# -*- coding: utf-8 -*-
"""
Generiert valide XML-Dateien entsprechend dem XSD-Schema
für Performance-Tests
@author: cgaba
"""
import os
from datetime import datetime, timedelta
import random

OUTPUT_DIR = "../xml_pool/"

GERAETE = ["Sensor_A", "Sensor_B", "Sensor_C", "Device_X", "Device_Y"]
OPERATORS = ["Alice", "Bob", "Charlie", "Diana", "Eve"]
PARAMETERS = ["Temperature", "Pressure", "Humidity", "Voltage", "Current"]
PUMPEN = ["Pumpe_1", "Pumpe_2", "Pumpe_3", "Pumpe_Alpha", "Pumpe_Beta"]


def generate_xml(measurement_id, geraet, operator, parameter, timestamp):
    """Generiert eine valide XML-Datei entsprechend dem XSD-Schema."""
    
    # Zufällige Messwerte
    druck = round(random.uniform(1.0, 10.0), 2)
    temperatur = round(random.uniform(-50.0, 150.0), 2)
    frequenz = round(random.uniform(50.0, 60.0), 2)
    pumpe = random.choice(PUMPEN)
    
    # Zufällige Anzahl Sensoren (2-5)
    num_sensors = random.randint(2, 5)
    sensoren_xml = ""
    for i in range(num_sensors):
        sensor_id = f"S{i+1:03d}"
        sensor_wert = round(random.uniform(0.0, 100.0), 2)
        sensoren_xml += f"""                        <sensor>
                            <id>{sensor_id}</id>
                            <wert>{sensor_wert}</wert>
                        </sensor>
"""
    
    xml_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<measurement>
    <metadata>
        <measurement_id>{measurement_id}</measurement_id>
        <timestamp>{timestamp}</timestamp>
        <operator>{operator}</operator>
        <geraet>{geraet}</geraet>
        <parameter>{parameter}</parameter>
    </metadata>
    <data>
        <druck>{druck}</druck>
        <temperatur>{temperatur}</temperatur>
        <frequenz>{frequenz}</frequenz>
        <pumpe>{pumpe}</pumpe>
        <sensoren>
{sensoren_xml}        </sensoren>
    </data>
</measurement>"""
    
    return xml_content


def generate_dataset(num_files=1000):
    """
    Generiert ein Dataset mit unterschiedlichen validen XMLs.
    
    Args:
        num_files: Anzahl zu generierender XML-Dateien
    """
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print(f"Generiere {num_files} XSD-konforme XML-Dateien...")
    
    base_time = datetime(2024, 1, 1, 10, 0, 0)
    
    for i in range(num_files):
        measurement_id = f"M{i+1:06d}"
        geraet = random.choice(GERAETE)
        operator = random.choice(OPERATORS)
        parameter = random.choice(PARAMETERS)
        timestamp = (base_time + timedelta(minutes=i)).isoformat()
        
        xml_content = generate_xml(measurement_id, geraet, operator, parameter, timestamp)
        
        filename = f"measurement_{i+1:06d}.xml"
        filepath = os.path.join(OUTPUT_DIR, filename)
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(xml_content)
        
        if (i + 1) % 100 == 0:
            print(f"  {i + 1}/{num_files} erstellt...")
    
    print(f"\n✓ {num_files} XSD-konforme XML-Dateien erstellt in: {OUTPUT_DIR}")


if __name__ == "__main__":
    # Generiere 1000 unterschiedliche XMLs
    generate_dataset(1000)