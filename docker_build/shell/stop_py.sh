#!/bin/sh

pid=`ps -ef|grep "python3 -u /codes/test2.py"| grep -v "grep"|awk '{print $2}'`

if [ "$pid" != "" ]
then
        kill -9 ${pid}
        echo "stop test2.py complete"
else
        echo "test2.py is not run, there's no need to stop it"
fi
