cat > start-cluster.sh << 'EOF'
#!/bin/bash
set -e

CP="out:lib/*"
HEAP="-Xmx4g -Xms1g"
KVS_PORT=8000
FLAME_PORT=9000

mkdir -p logs

echo "=== Starting KVS Coordinator ==="
java $HEAP -cp "$CP" cis5550.kvs.Coordinator $KVS_PORT > logs/kvs-coord.log 2>&1 &
echo $! > logs/kvs-coord.pid
sleep 3

echo "=== Starting KVS Workers ==="
for i in 1 2 3 4 5; do
  port=$((8000 + i))
  java $HEAP -cp "$CP" cis5550.kvs.Worker $port worker$i localhost:$KVS_PORT > logs/kvs-w$i.log 2>&1 &
  echo $! > logs/kvs-w$i.pid
done
sleep 3

echo "=== Starting Flame Coordinator ==="
java $HEAP -cp "$CP" cis5550.flame.Coordinator $FLAME_PORT localhost:$KVS_PORT > logs/flame-coord.log 2>&1 &
echo $! > logs/flame-coord.pid
sleep 3

echo "=== Starting Flame Workers ==="
for i in 1 2 3 4 5; do
  port=$((9000 + i))
  java $HEAP -cp "$CP" cis5550.flame.Worker $port localhost:$FLAME_PORT > logs/flame-w$i.log 2>&1 &
  echo $! > logs/flame-w$i.pid
done
sleep 5

echo "=== Cluster started ==="
echo "KVS Coord:   http://localhost:$KVS_PORT/"
echo "Flame Coord: http://localhost:$FLAME_PORT/"
echo "Logs in: logs/"
EOF
chmod +x start-cluster.sh
./start-cluster.sh