#!/bin/bash

sudo pip3 install -U PyAthenaJDBC
sudo pip3 install -U boto3
sudo pip3 install pg8000
sudo pip3 install flatten-json
sudo pip3 install watchtower
sudo wget -P /usr/lib/spark/jars/ https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/0.14.0/spark-xml_2.12-0.14.0.jar
