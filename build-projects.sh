#!/bin/bash -e

cd collector && mvn clean && mvn package && cd ..
cd hadoop-worker && mvn clean && mvn package && cd ..
cd schemamanager && mvn clean && mvn package && cd ..
cd user-activities && mvn clean && mvn package && cd ..