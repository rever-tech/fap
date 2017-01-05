#!/usr/bin/env bash
rsync -avuz dist root@172.16.100.1:/zserver/rever-projects/fap/schemamanager/ &&
rsync -avuz cmd root@172.16.100.1:/zserver/rever-projects/fap/schemamanager/ &&
rsync -avuz runservice root@172.16.100.1:/zserver/rever-projects/fap/schemamanager/ &&
rsync -avuz rundocker root@172.16.100.1:/zserver/rever-projects/fap/schemamanager/ &&
ssh root@172.16.100.1 "/zserver/rever-projects/fap/schemamanager/rundocker restart development -p 172.16.100.1:10117:10117 -p 172.16.100.1:10118:10118" || { echo "runservice failed"; exit 1; } &&
ssh root@172.16.100.1 "tail -100 /zserver/rever-projects/fap/schemamanager/logs/service.log"
