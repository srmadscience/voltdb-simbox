#!/bin/sh

. $HOME/.profile

java ${JVMOPTS} -jar ../jars/voltdb-simbox-client.jar `cat $HOME/.vdbhostnames` 500000 90 7200 500 15

