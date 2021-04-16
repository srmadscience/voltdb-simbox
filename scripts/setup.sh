#!/bin/sh

cd /home/ubuntu
. ./.profile

cd voltdb-simbox/scripts

sqlcmd --servers=vdb1 < ../ddl/create_db.sql
