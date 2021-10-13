#!/bin/bash
set -e
wget -S -T 10 -t 5 https://kaiko-delivery-qfinlab-polimi.s3.amazonaws.com/scripts/requirements.txt
sudo yum install -y python3-devel.x86_64
sudo pip3 install -r requirements.txt