@echo off
setlocal EnableDelayedExpansion

REM ===========================================================================
REM Local crawler launcher for a single Windows host.
REM Hardware target: Intel i7-14700 (20 cores / 28 threads), 32 GB RAM.
REM
REM Topology (mirrors deploy-ec2.sh, scaled onto one box):
REM   1 KVS Coordinator     (port 8000)
REM   1 Flame Coordinator   (port 9000)
REM   4 KVS Workers         (ports 8001..8004, dirs worker1..worker4)
REM   4 Flame Workers       (ports 9001..9004)
REM
REM Heap budget on 32 GB (leaves ~6 GB for OS + page cache + browser/IDE):
REM   KVS coord       : Xmx512m              ~0.5g
REM   Flame coord     : Xmx512m              ~0.5g
REM   KVS worker  x 4 : Xmx4g  each        ->  16g
REM   Flame worker x 4: Xmx2g  each        ->   8g
REM   Tools (ConfigLoader/FlameSubmit, transient): Xmx512m
REM   --------------------------------------------------
REM   Sum Xmx ~ 26g; initial commit ~7g.
REM
REM Why these sizes:
REM   * KVS worker holds crawled pages in an in-memory ConcurrentHashMap (the
REM     `tables` field). With 4 shards x 4g, the cluster can cache ~16g of
REM     pt-crawl data in RAM before GC pressure matters. Anything beyond that
REM     is still safe (data is also written to disk under workerN/), it just
REM     gets slower.
REM   * Flame worker runs the per-URL crawl lambda on a 32-thread pool. Worst
REM     case: 32 fetches x ~5 MB MAX_CONTENT_SIZE = ~160 MB peak, plus HTTP
REM     buffers and Loom/native overhead -> 2g is comfortable.
REM   * Coordinators just route worker registrations + dispatch; 512m is plenty.
REM
REM Concurrency knobs (passed as -D system properties):
REM   flame.worker.concurrency      -> per-Flame-worker thread pool (HTTP fetch)
REM   kvs.worker.replicaConcurrency -> per-KVS-worker replica-forward pool
REM   webserver.numWorkers          -> inbound HTTP request-handler pool
REM Defaults match deploy-ec2.sh's defaults so behavior matches EC2.
REM ===========================================================================

set CP=lib\*

REM === Per-role heap settings ===
set HEAP_KVS_COORD=-Xmx512m -Xms256m
set HEAP_FLAME_COORD=-Xmx512m -Xms256m
set HEAP_KVS_WORKER=-Xmx4g -Xms1g
set HEAP_FLAME_WORKER=-Xmx2g -Xms512m
set HEAP_TOOL=-Xmx512m -Xms128m

REM === Concurrency tunables (matches deploy-ec2.sh defaults) ===
set FLAME_CONCURRENCY=32
set KVS_REPLICA_CONCURRENCY=32
set WEBSERVER_NUM_WORKERS=100

set TUNE_COORD=-Dwebserver.numWorkers=%WEBSERVER_NUM_WORKERS%
set TUNE_KVS_WORKER=-Dkvs.worker.replicaConcurrency=%KVS_REPLICA_CONCURRENCY% -Dwebserver.numWorkers=%WEBSERVER_NUM_WORKERS%
set TUNE_FLAME_WORKER=-Dflame.worker.concurrency=%FLAME_CONCURRENCY% -Dwebserver.numWorkers=%WEBSERVER_NUM_WORKERS%

REM === Cluster ports ===
set KVS_COORD_PORT=8000
set FLAME_COORD_PORT=9000

REM === Kill any existing cis5550 Java processes ===
echo === Stopping any running services ===
powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name = 'java.exe'\" | Where-Object { $_.CommandLine -like '*cis5550*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }" >nul 2>&1
timeout /t 2 /nobreak >nul

REM === Clean old build ===
if exist out rmdir /s /q out
if exist lib rmdir /s /q lib
mkdir out
mkdir lib

REM === Clean old KVS state (worker5 cleanup is defensive for prior 5-worker runs) ===
if exist worker1 rmdir /s /q worker1
if exist worker2 rmdir /s /q worker2
if exist worker3 rmdir /s /q worker3
if exist worker4 rmdir /s /q worker4
if exist worker5 rmdir /s /q worker5
mkdir worker1
mkdir worker2
mkdir worker3
mkdir worker4

REM === Compile ===
echo === Compiling ===
javac -cp "%CP%" -d out --source-path src ^
    src\cis5550\kvs\*.java ^
    src\cis5550\webserver\*.java ^
    src\cis5550\flame\*.java ^
    src\cis5550\generic\*.java ^
    src\cis5550\tools\*.java ^
    src\cis5550\crawler\*.java
if errorlevel 1 (
    echo Compile FAILED.
    exit /b 1
)
jar cf lib\kvs.jar -C out cis5550/kvs -C out cis5550/tools
jar cf lib\webserver.jar -C out cis5550/webserver -C out cis5550/tools
jar cf lib\flame.jar -C out cis5550/flame -C out cis5550/tools
jar cf lib\crawler.jar -C out cis5550/crawler -C out cis5550/tools
echo Compile OK.

REM === Start KVS Coordinator ===
echo === Starting KVS Coordinator (port %KVS_COORD_PORT%) ===
start "KVS-Coord" java %HEAP_KVS_COORD% %TUNE_COORD% -cp "out;lib\*" cis5550.kvs.Coordinator %KVS_COORD_PORT%
timeout /t 3 /nobreak >nul

REM === Start KVS Workers ===
echo === Starting 4 KVS Workers ===
start "KVS-W1" java %HEAP_KVS_WORKER% %TUNE_KVS_WORKER% -cp "out;lib\*" cis5550.kvs.Worker 8001 worker1 localhost:%KVS_COORD_PORT%
start "KVS-W2" java %HEAP_KVS_WORKER% %TUNE_KVS_WORKER% -cp "out;lib\*" cis5550.kvs.Worker 8002 worker2 localhost:%KVS_COORD_PORT%
start "KVS-W3" java %HEAP_KVS_WORKER% %TUNE_KVS_WORKER% -cp "out;lib\*" cis5550.kvs.Worker 8003 worker3 localhost:%KVS_COORD_PORT%
start "KVS-W4" java %HEAP_KVS_WORKER% %TUNE_KVS_WORKER% -cp "out;lib\*" cis5550.kvs.Worker 8004 worker4 localhost:%KVS_COORD_PORT%
timeout /t 3 /nobreak >nul

REM === Start Flame Coordinator ===
echo === Starting Flame Coordinator (port %FLAME_COORD_PORT%) ===
start "Flame-Coord" java %HEAP_FLAME_COORD% %TUNE_COORD% -cp "out;lib\*" cis5550.flame.Coordinator %FLAME_COORD_PORT% localhost:%KVS_COORD_PORT%
timeout /t 3 /nobreak >nul

REM === Start Flame Workers ===
echo === Starting 4 Flame Workers ===
start "Flame-W1" java %HEAP_FLAME_WORKER% %TUNE_FLAME_WORKER% -cp "out;lib\*" cis5550.flame.Worker 9001 localhost:%FLAME_COORD_PORT%
start "Flame-W2" java %HEAP_FLAME_WORKER% %TUNE_FLAME_WORKER% -cp "out;lib\*" cis5550.flame.Worker 9002 localhost:%FLAME_COORD_PORT%
start "Flame-W3" java %HEAP_FLAME_WORKER% %TUNE_FLAME_WORKER% -cp "out;lib\*" cis5550.flame.Worker 9003 localhost:%FLAME_COORD_PORT%
start "Flame-W4" java %HEAP_FLAME_WORKER% %TUNE_FLAME_WORKER% -cp "out;lib\*" cis5550.flame.Worker 9004 localhost:%FLAME_COORD_PORT%

REM === Wait for Flame workers to register ===
echo === Waiting for Flame workers to register ===
set RETRIES=0
:wait_workers
timeout /t 2 /nobreak >nul
set /a RETRIES+=1
for /f %%i in ('powershell -NoProfile -Command "try { (Invoke-WebRequest -Uri 'http://localhost:%FLAME_COORD_PORT%/workers' -TimeoutSec 2 -UseBasicParsing).Content.Split([char]10)[0].Trim() } catch { Write-Output '0' }"') do set WORKER_COUNT=%%i
if "%WORKER_COUNT%" GEQ "4" (
    echo %WORKER_COUNT% Flame workers registered.
    goto workers_ready
)
if %RETRIES% GEQ 15 (
    echo ERROR: Flame workers did not register after 30 seconds. Check the worker windows for errors.
    exit /b 1
)
echo   ...waiting (%RETRIES%/15, workers so far: %WORKER_COUNT%)
goto wait_workers
:workers_ready

REM === Load blacklist config into KVS ===
echo === Loading blacklist into KVS ===
java %HEAP_TOOL% -cp "out;lib\*" cis5550.tools.ConfigLoader localhost:%KVS_COORD_PORT% config\blacklist.txt pt-blacklist pattern
if errorlevel 1 (
    echo WARNING: Failed to load blacklist. Crawler will run without it.
)

REM === Submit crawler job ===
echo === Submitting crawler job ===
java %HEAP_TOOL% -cp "out;lib\*" cis5550.flame.FlameSubmit localhost:%FLAME_COORD_PORT% lib\crawler.jar cis5550.crawler.Crawler ^
    https://blogroll.org ^
    https://www.techmeme.com ^
    https://techcrunch.com ^
    https://www.britannica.com ^
    https://www.mit.edu ^
    pt-blacklist

echo.
echo === Done ===
echo KVS Coord:   http://localhost:%KVS_COORD_PORT%/
echo Flame Coord: http://localhost:%FLAME_COORD_PORT%/
echo Heap   : KVS-W=%HEAP_KVS_WORKER%, Flame-W=%HEAP_FLAME_WORKER%, Coords=%HEAP_KVS_COORD%
echo Tunings: flame.worker.concurrency=%FLAME_CONCURRENCY%, kvs.worker.replicaConcurrency=%KVS_REPLICA_CONCURRENCY%, webserver.numWorkers=%WEBSERVER_NUM_WORKERS%
