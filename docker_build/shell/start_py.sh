#!/bin/sh

pid=`ps -ef|grep "python3 -u /codes/test2.py"| grep -v "grep"|awk '{print $2}'`

if [ "$pid" != "" ]
then
        echo "test2.py already run, stop it first"
        kill -9 ${pid}
fi

echo "starting now..."

nohup python3 -u /codes/test2.py > test.out 2>&1 &

pid=`ps -ef|grep "python3 -u /codes/test2.py"| grep -v "grep"|awk '{print $2}'`

echo ${pid} > pid.out
echo "test2.py started at pid: "${pid}
