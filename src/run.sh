#!/bin/bash
set -e

KVS_COORD_PORT=8000
FLAME_COORD_PORT=9000

echo "=== Stopping any running services ==="
pkill -f cis5550 2>/dev/null || true
sleep 2

echo "=== Compiling ==="
rm -rf bin
mkdir -p bin
javac -d bin $(find cis5550 -name "*.java")
echo "Compile OK."

echo "=== Packaging indexer JAR ==="
(cd bin && jar cf ../indexer.jar cis5550/indexer/Indexer.class cis5550/external/PorterStemmer.class)
mkdir -p logs
mkdir -p worker1

echo "=== Starting KVS Coordinator ==="
java -Xmx256m -cp bin cis5550.kvs.Coordinator $KVS_COORD_PORT > logs/kvs-coord.log 2>&1 &
sleep 3

echo "=== Starting KVS Worker (1x) ==="
java -Xmx2500m -cp bin cis5550.kvs.Worker 8001 worker1 localhost:$KVS_COORD_PORT > logs/kvs-worker1.log 2>&1 &
sleep 3

echo "=== Starting Flame Coordinator ==="
java -Xmx256m -cp bin cis5550.flame.Coordinator $FLAME_COORD_PORT localhost:$KVS_COORD_PORT > logs/flame-coord.log 2>&1 &
sleep 3

echo "=== Starting Flame Workers (2x) ==="
java -Xmx1500m -cp bin cis5550.flame.Worker 9001 localhost:$FLAME_COORD_PORT > logs/flame-worker1.log 2>&1 &
sleep 1
java -Xmx1500m -cp bin cis5550.flame.Worker 9002 localhost:$FLAME_COORD_PORT > logs/flame-worker2.log 2>&1 &
sleep 5

echo ""
echo "=== All services started ==="
echo "KVS:   http://localhost:$KVS_COORD_PORT/"
echo "Flame: http://localhost:$FLAME_COORD_PORT/"
echo ""
echo "To submit the indexer:"
echo "  java -cp bin cis5550.flame.FlameSubmit localhost:$FLAME_COORD_PORT indexer.jar cis5550.indexer.Indexer"