#!/bin/bash

cd ..
source .mqtt_qos/bin/activate
cd mqtt_qos_code/
python src/server.py