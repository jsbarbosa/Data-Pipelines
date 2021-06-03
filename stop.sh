rm *.err *.log *.out *.pid

PIDS=$(ps aux | grep airflow | awk '{print $2}')

echo "$PIDS"

kill -9 $PIDS
