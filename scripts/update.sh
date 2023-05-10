#! /bin/bash
sudo ifconfig wlan0 up
sudo ifconfig eth0 down
git fetch
git pull
sudo ifconfig eth0 up
sudo ifconfig wlan0 down