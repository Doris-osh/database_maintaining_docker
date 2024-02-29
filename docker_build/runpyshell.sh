SCRIPT_DIR=$(dirname "$0")

nohup python3 -u ${SCRIPT_DIR}/test2.py > test.out 2>&1 &

pid=`ps -ef|grep "python3 -u ${SCRIPT_DIR}/test2.py"| grep -v "grep"|awk '{print $2}'`

echo ${pid} > pid.out

echo "test2.py started at pid: "${pid}
