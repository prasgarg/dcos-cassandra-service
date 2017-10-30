#!/bin/bash

#MESOS_BUILD_PATH=${PWD}

if [ -z "$MESOS_BUILD_PATH" ]; then
       echo " Please set MESOS_BUILD_PATH environment variable above"
       exit 1
fi

MASTER_IP=127.0.0.2
MY_IP_1=127.0.0.3
MY_IP_2=127.0.0.4
MY_IP_3=127.0.0.5
BASEDIR=/private/tmp/mesos
# 9000 port is required because it is reuired by the cassandra executor as mentioned in cassandra-scheduler/src/dist/conf/scheduler.yml
# EXECUTOR_API_PORT
${MESOS_BUILD_PATH}/mesos-1.0.3/build/bin/mesos-master.sh --ip=${MASTER_IP} --work_dir=${BASEDIR}/master --zk=zk://$MASTER_IP:2181/mesos --quorum=1 &
${MESOS_BUILD_PATH}/mesos-1.0.3/build/bin/mesos-slave.sh --master=${MASTER_IP}:5050 --ip=${MY_IP_1} --work_dir=${BASEDIR}/slave1 --resources='ports:[31000-32000,7000-7001,51751-51751,8080-8090,9010-9040,9042-9042,9160-9160,22100-22100,7100-7400,1414-1414]' &
${MESOS_BUILD_PATH}/mesos-1.0.3/build/bin/mesos-slave.sh --master=${MASTER_IP}:5050 --ip=${MY_IP_2} --work_dir=${BASEDIR}/slave2 --resources='ports:[31000-32000,7000-7001,51751-51751,8080-8090,9010-9040,9042-9042,9160-9160,22100-22100,7100-7400,1414-1414]' &
${MESOS_BUILD_PATH}/mesos-1.0.3/build/bin/mesos-slave.sh --master=${MASTER_IP}:5050 --ip=${MY_IP_3} --work_dir=${BASEDIR}/slave3 --resources='ports:[31000-32000,7000-7001,51751-51751,8080-8090,9010-9040,9042-9042,9160-9160,22100-22100,7100-7400,1414-1414]' &
