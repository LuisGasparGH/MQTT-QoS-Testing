#!/bin/bash

# Run docker: docker run -dit --network host ubuntu
# Rename docker: docker rename random_name ubuntu_docker
# Copy setup file to docker: docker cp docker_setup.sh ubuntu_docker:/docker_setup.sh
# Enter docker: docker exec -it ubuntu_docker bash
apt-get update -y
apt install ubuntu-minimal python3-pip net-tools mosquitto mosquitto-clients nano git htop -y
git clone https://github.com/LuisGasparGH/MQTT-QoS-Testing.git
