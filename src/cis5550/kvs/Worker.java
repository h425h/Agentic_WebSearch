package cis5550.kvs;

import cis5550.webserver.Server;
import cis5550.tools.Logger;
import cis5550.tools.KeyEncoder;
import cis5550.tools.HTTP;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.security.*;

public class Worker extends cis5550.generic.Worker implements KVS {

    static final Logger logger = Logger.getLogger(Worker.class);

    // table -> row key -> Row
    static ConcurrentHashMap<String, ConcurrentHashMap<String, Row>> tables = new ConcurrentHashMap<>();

    // version history per row
    static ConcurrentHashMap<String, ConcurrentHashMap<String, List<Row>>> versions = new ConcurrentHashMap<>();

    static String storageDir;
    static String myId;
    static String coordinatorAddr;

    // current worker list from coordinator: sorted list of [id, address]
    static List<String[]> workerList = new ArrayList<>();

    // bounded pool for replica forwarding so a PUT storm can't spawn unbounded threads/sockets.
    // Sized via -Dkvs.worker.replicaConcurrency; clamped to [1, 1024] to survive typos.
    // DiscardOldestPolicy is intentional: drops are repaired by the anti-entropy maintenance
    // thread (every 30s) via key/hash compare with neighbors.
    private static final int REPLICA_THREADS = clampPoolSize(
            Integer.getInteger("kvs.worker.replicaConcurrency", 32));
    static final ExecutorService replicaPool = new ThreadPoolExecutor(
            REPLICA_THREADS, REPLICA_THREADS, 60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(10_000),
            new ThreadPoolExecutor.DiscardOldestPolicy());

    private static int clampPoolSize(int n) {
        if (n < 1) return 1;
        if (n > 1024) return 1024;
        return n;
    }

    // Stripe lock for persistent-PUT read-modify-write. Replaces the old
    // `synchronized (table.intern())`, which serialized every write to a given
    // table through one monitor. With 64 stripes, writes to different rows
    // (even in the same table) run in parallel; same-row writes still serialize
    // (correct for atomic ifcolumn/equals semantics).
    private static final int LOCK_STRIPES = 64;
    private static final Object[] ROW_LOCKS = new Object[LOCK_STRIPES];
    static {
        for (int i = 0; i < LOCK_STRIPES; i++) ROW_LOCKS[i] = new Object();
    }

    private static Object stripeLock(String table, String rowKey) {
        int h = (table.hashCode() * 31) ^ rowKey.hashCode();
        return ROW_LOCKS[Math.floorMod(h, LOCK_STRIPES)];
    }

    // pt- prefix = persistent
    static boolean isPersistent(String table) {
        return table.startsWith("pt-");
    }

    static File tableDir(String table) {
        return new File(storageDir, table);
    }

    // EC1: encoded names >= 6 chars go in __xx subdir
    static File rowFile(String table, String key) {
        String encoded = KeyEncoder.encode(key);
        if (encoded.length() >= 6) {
            String subdir = "__" + encoded.substring(0, 2);
            return new File(new File(tableDir(table), subdir), encoded);
        }
        return new File(tableDir(table), encoded);
    }

    // write row to disk: temp file + atomic rename so concurrent readers
    // never see a partially-written/truncated file. Stripe lock in PUT
    // handler still serializes writers per row.
    static void writeToDisk(String table, Row row) throws IOException {
        File f = rowFile(table, row.key());
        File parent = f.getParentFile();
        parent.mkdirs();
        File tmp = new File(parent, f.getName() + ".tmp");
        try (FileOutputStream fos = new FileOutputStream(tmp)) {
            fos.write(row.toByteArray());
            fos.flush();
            fos.getFD().sync();
        }
        Files.move(tmp.toPath(), f.toPath(),
                   StandardCopyOption.ATOMIC_MOVE,
                   StandardCopyOption.REPLACE_EXISTING);
    }

    // read row from disk
    static Row readFromDisk(String table, String key) {
        File f = rowFile(table, key);
        if (!f.exists()) return null;
        try (FileInputStream fis = new FileInputStream(f)) {
            return Row.readFrom(fis);
        } catch (Exception e) {
            logger.error("error reading row: " + e.getMessage());
            return null;
        }
    }

    static boolean tableExists(String table) {
        if (isPersistent(table)) return tableDir(table).exists();
        return tables.containsKey(table);
    }

    // EC1: scan both regular files and __xx subdirs
    static List<String> getRowKeys(String table) {
        List<String> keys = new ArrayList<>();
        if (isPersistent(table)) {
            File dir = tableDir(table);
            if (!dir.exists()) return keys;
            File[] entries = dir.listFiles();
            if (entries == null) return keys;
            for (File f : entries) {
                if (f.isFile()) {
                    if (f.getName().endsWith(".tmp")) continue;
                    keys.add(KeyEncoder.decode(f.getName()));
                } else if (f.isDirectory() && f.getName().startsWith("__")) {
                    // EC1 subdir
                    File[] subfiles = f.listFiles();
                    if (subfiles != null) {
                        for (File sf : subfiles) {
                            if (sf.isFile() && !sf.getName().endsWith(".tmp")) {
                                keys.add(KeyEncoder.decode(sf.getName()));
                            }
                        }
                    }
                }
            }
        } else {
            ConcurrentHashMap<String, Row> t = tables.get(table);
            if (t != null) keys.addAll(t.keySet());
        }
        Collections.sort(keys);
        return keys;
    }

    // Lazy directory walk for scans. Avoids the O(N log N) list+sort that
    // getRowKeys does on every call. Range filter is applied inline so we
    // never materialize keys outside the partition's [startRow, endRowExclusive).
    // Persistent: streams entries via Files.newDirectoryStream (one getdents64
    // syscall path on Linux), descending into __xx EC1 subdirs lazily.
    // Non-persistent: iterates the in-memory map's keySet directly.
    static void streamRowKeys(String table, String startRow, String endRowExclusive,
                              java.util.function.Consumer<String> consumer) throws IOException {
        if (isPersistent(table)) {
            File dir = tableDir(table);
            if (!dir.exists()) return;
            try (java.nio.file.DirectoryStream<java.nio.file.Path> top =
                     java.nio.file.Files.newDirectoryStream(dir.toPath())) {
                for (java.nio.file.Path entry : top) {
                    String name = entry.getFileName().toString();
                    if (name.startsWith("__")) {
                        try (java.nio.file.DirectoryStream<java.nio.file.Path> sub =
                                 java.nio.file.Files.newDirectoryStream(entry)) {
                            for (java.nio.file.Path sf : sub) {
                                String sfName = sf.getFileName().toString();
                                if (sfName.endsWith(".tmp")) continue;
                                emitIfInRange(KeyEncoder.decode(sfName),
                                              startRow, endRowExclusive, consumer);
                            }
                        }
                    } else {
                        if (name.endsWith(".tmp")) continue;
                        emitIfInRange(KeyEncoder.decode(name), startRow, endRowExclusive, consumer);
                    }
                }
            }
        } else {
            ConcurrentHashMap<String, Row> t = tables.get(table);
            if (t == null) return;
            for (String key : t.keySet()) {
                emitIfInRange(key, startRow, endRowExclusive, consumer);
            }
        }
    }

    private static void emitIfInRange(String key, String start, String endExcl,
                                      java.util.function.Consumer<String> c) {
        if (start != null && key.compareTo(start) < 0) return;
        if (endExcl != null && key.compareTo(endExcl) >= 0) return;
        c.accept(key);
    }

    static List<String> getAllTables() {
        List<String> names = new ArrayList<>(tables.keySet());
        // pick up pt- dirs on disk
        File dir = new File(storageDir);
        if (dir.exists()) {
            File[] subdirs = dir.listFiles();
            if (subdirs != null) {
                for (File f : subdirs) {
                    if (f.isDirectory() && f.getName().startsWith("pt-") && !names.contains(f.getName())) {
                        names.add(f.getName());
                    }
                }
            }
        }
        Collections.sort(names);
        return names;
    }

    // EC1: delete including __xx subdirs
    static void deleteDir(File dir) {
        if (!dir.exists()) return;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    // delete subdir contents first
                    File[] subfiles = f.listFiles();
                    if (subfiles != null) for (File sf : subfiles) sf.delete();
                    f.delete();
                } else {
                    f.delete();
                }
            }
        }
        dir.delete();
    }

    // EC1: count files including in __xx subdirs
    static int countFilesInDir(File dir) {
        if (!dir.exists()) return 0;
        File[] entries = dir.listFiles();
        if (entries == null) return 0;
        int count = 0;
        for (File f : entries) {
            if (f.isFile()) {
                if (!f.getName().endsWith(".tmp")) count++;
            } else if (f.isDirectory() && f.getName().startsWith("__")) {
                File[] subfiles = f.listFiles();
                if (subfiles != null) {
                    for (File sf : subfiles) {
                        if (sf.isFile() && !sf.getName().endsWith(".tmp")) count++;
                    }
                }
            }
        }
        return count;
    }

    // EC2: download and update worker list from coordinator
    static void refreshWorkers() {
        try {
            String resp = new String(HTTP.doRequest("GET", "http://" + coordinatorAddr + "/workers", null).body());
            String[] lines = resp.trim().split("\n");
            int n = Integer.parseInt(lines[0].trim());
            List<String[]> updated = new ArrayList<>();
            for (int i = 1; i <= n && i < lines.length; i++) {
                String[] parts = lines[i].trim().split(",", 2);
                if (parts.length == 2) updated.add(new String[]{parts[0], parts[1]});
            }
            // sort by id
            updated.sort((a, b) -> a[0].compareTo(b[0]));
            synchronized (workerList) {
                workerList = updated;
            }
        } catch (Exception e) {
            logger.error("refresh workers failed: " + e.getMessage());
        }
    }

    // EC2: get the two workers with next-lower IDs (wrap around)
    static List<String> getReplicaTargets() {
        List<String[]> wl;
        synchronized (workerList) {
            wl = new ArrayList<>(workerList);
        }
        if (wl.size() <= 1) return new ArrayList<>();

        // find our index
        int myIdx = -1;
        for (int i = 0; i < wl.size(); i++) {
            if (wl.get(i)[0].equals(myId)) { myIdx = i; break; }
        }
        if (myIdx == -1) return new ArrayList<>();

        List<String> targets = new ArrayList<>();
        for (int i = 1; i <= 2 && i < wl.size(); i++) {
            int idx = (myIdx - i + wl.size()) % wl.size();
            targets.add(wl.get(idx)[1]); // address
        }
        return targets;
    }

    // EC2: forward PUT to a replica. `forward=0` prevents the replica from re-forwarding.
    static void forwardPut(String address, String table, String rowKey, String col, byte[] val) {
        try {
            String url = "http://" + address + "/data/" + table + "/" + rowKey + "/" + col + "?forward=0";
            HTTP.doRequest("PUT", url, val);
        } catch (Exception e) {
            logger.error("forward PUT failed to " + address + ": " + e.getMessage());
        }
    }

    // EC3: hash a row for comparison
    static String hashRow(Row row) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(row.toByteArray());
            byte[] hash = md.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            return "";
        }
    }

    // EC3: get the two workers with next-higher IDs (wrap around)
    static List<String[]> getNextHigherWorkers() {
        List<String[]> wl;
        synchronized (workerList) {
            wl = new ArrayList<>(workerList);
        }
        if (wl.size() <= 1) return new ArrayList<>();

        int myIdx = -1;
        for (int i = 0; i < wl.size(); i++) {
            if (wl.get(i)[0].equals(myId)) { myIdx = i; break; }
        }
        if (myIdx == -1) return new ArrayList<>();

        List<String[]> targets = new ArrayList<>();
        for (int i = 1; i <= 2 && i < wl.size(); i++) {
            int idx = (myIdx + i) % wl.size();
            targets.add(wl.get(idx));
        }
        return targets;
    }

    // EC3: get the key range this worker is responsible for
    // worker is responsible for keys >= its own ID and < next worker's ID
    static String[] getMyKeyRange() {
        List<String[]> wl;
        synchronized (workerList) {
            wl = new ArrayList<>(workerList);
        }
        if (wl.isEmpty()) return new String[]{null, null};

        int myIdx = -1;
        for (int i = 0; i < wl.size(); i++) {
            if (wl.get(i)[0].equals(myId)) { myIdx = i; break; }
        }
        if (myIdx == -1) return new String[]{null, null};

        String start = wl.get(myIdx)[0];
        String end = (myIdx + 1 < wl.size()) ? wl.get(myIdx + 1)[0] : null;
        return new String[]{start, end};
    }

    // --- KVS interface methods ---

    public void put(String tableName, String row, String column, byte[] value) throws FileNotFoundException, IOException {
        Row r = getRow(tableName, row);
        if (r == null) r = new Row(row);
        r.put(column, value);
        putRow(tableName, r);
    }

    public void putRow(String tableName, Row row) throws FileNotFoundException, IOException {
        if (isPersistent(tableName)) {
            writeToDisk(tableName, row);
        } else {
            tables.computeIfAbsent(tableName, k -> new ConcurrentHashMap<>()).put(row.key(), row);
        }
    }

    public Row getRow(String tableName, String row) throws FileNotFoundException, IOException {
        if (isPersistent(tableName)) return readFromDisk(tableName, row);
        ConcurrentHashMap<String, Row> t = tables.get(tableName);
        if (t == null) return null;
        return t.get(row);
    }

    public boolean existsRow(String tableName, String row) throws FileNotFoundException, IOException {
        return getRow(tableName, row) != null;
    }

    public byte[] get(String tableName, String row, String column) throws FileNotFoundException, IOException {
        Row r = getRow(tableName, row);
        if (r == null) return null;
        return r.getBytes(column);
    }

    public Iterator<Row> scan(String tableName, String startRow, String endRowExclusive) throws FileNotFoundException, IOException {
        // Lazy: collect only the filtered key list (small relative to table size,
        // bounded by the partition's [startRow, endRowExclusive) range), then
        // read each Row on demand via getRow. Row bodies (potentially 5 MB pages)
        // are no longer all resident at once, fixing the "scan materializes whole
        // table in heap" footgun in the local KVS interface.
        final String table = tableName;
        final List<String> keys = new ArrayList<>();
        streamRowKeys(table, startRow, endRowExclusive, keys::add);
        return new Iterator<Row>() {
            int idx = 0;
            Row peeked = null;

            private void advanceToNext() {
                while (peeked == null && idx < keys.size()) {
                    try {
                        peeked = getRow(table, keys.get(idx++));
                    } catch (IOException e) {
                        peeked = null;
                    }
                }
            }

            public boolean hasNext() {
                advanceToNext();
                return peeked != null;
            }

            public Row next() {
                advanceToNext();
                Row r = peeked;
                peeked = null;
                return r;
            }
        };
    }

    public int count(String tableName) throws FileNotFoundException, IOException {
        if (isPersistent(tableName)) {
            return countFilesInDir(tableDir(tableName));
        }
        ConcurrentHashMap<String, Row> t = tables.get(tableName);
        return t == null ? 0 : t.size();
    }

    public boolean rename(String oldTableName, String newTableName) throws IOException {
        if (!tableExists(oldTableName)) return false;
        if (tableExists(newTableName)) return false;
        if (isPersistent(oldTableName) != isPersistent(newTableName)) return false;

        if (isPersistent(oldTableName)) {
            Files.move(tableDir(oldTableName).toPath(), tableDir(newTableName).toPath());
        } else {
            ConcurrentHashMap<String, Row> t = tables.remove(oldTableName);
            if (t != null) tables.put(newTableName, t);
            ConcurrentHashMap<String, List<Row>> v = versions.remove(oldTableName);
            if (v != null) versions.put(newTableName, v);
        }
        return true;
    }

    public boolean delete(String tableName) throws IOException {
        if (isPersistent(tableName)) {
            deleteDir(tableDir(tableName));
        } else {
            tables.remove(tableName);
            versions.remove(tableName);
        }
        return true;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("usage: Worker <port> <storageDir> <coordinatorIp:port>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        storageDir = args[1];
        coordinatorAddr = args[2];

        // load or generate worker id
        File dir = new File(storageDir);
        dir.mkdirs();
        File idFile = new File(dir, "id");
        String id;
        if (idFile.exists()) {
            id = new String(java.nio.file.Files.readAllBytes(idFile.toPath())).trim();
        } else {
            StringBuilder sb = new StringBuilder();
            Random rand = new Random();
            for (int i = 0; i < 5; i++) sb.append((char)('a' + rand.nextInt(26)));
            id = sb.toString();
            try (PrintWriter pw = new PrintWriter(idFile)) { pw.print(id); }
        }

        myId = id;

        Worker worker = new Worker();

        logger.info("worker id: " + id + ", storage: " + storageDir);
        logger.info("kvs.worker.replicaConcurrency=" + REPLICA_THREADS
                + " (queue=10000, policy=DiscardOldest)");

        Server.port(port);
        startPingThread(coordinatorAddr, id, port);

        // EC2: refresh worker list every 5 seconds
        Thread refreshThread = new Thread(() -> {
            while (true) {
                try {
                    refreshWorkers();
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    logger.error("refresh error: " + e.getMessage());
                }
            }
        });
        refreshThread.setDaemon(true);
        refreshThread.start();

        // EC3: replica maintenance every 30 seconds
        Thread maintThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(30000);
                    List<String[]> neighbors = getNextHigherWorkers();
                    for (String[] neighbor : neighbors) {
                        String neighborAddr = neighbor[1];
                        String neighborId = neighbor[0];

                        // get their key range (what we should replicate from them)
                        // find the neighbor's next worker to determine their range end
                        List<String[]> wl;
                        synchronized (workerList) { wl = new ArrayList<>(workerList); }
                        int nIdx = -1;
                        for (int i = 0; i < wl.size(); i++) {
                            if (wl.get(i)[0].equals(neighborId)) { nIdx = i; break; }
                        }
                        if (nIdx == -1) continue;
                        String rangeStart = wl.get(nIdx)[0];
                        String rangeEnd = (nIdx + 1 < wl.size()) ? wl.get(nIdx + 1)[0] : null;

                        // get their table list
                        try {
                            String tablesResp = new String(HTTP.doRequest("GET", "http://" + neighborAddr + "/tables", null).body());
                            String[] theirTables = tablesResp.trim().split("\n");

                            for (String table : theirTables) {
                                table = table.trim();
                                if (table.isEmpty()) continue;

                                // get row keys + hashes from neighbor for their key range
                                String url = "http://" + neighborAddr + "/data/" + table + "?keyonly=true"
                                        + (rangeStart != null ? "&startRow=" + rangeStart : "")
                                        + (rangeEnd != null ? "&endRowExclusive=" + rangeEnd : "");
                                try {
                                    HTTP.Response hashResp = HTTP.doRequest("GET", url, null);
                                    if (hashResp.statusCode() != 200) continue;

                                    // parse key\thash lines
                                    String body = new String(hashResp.body());
                                    String[] hashLines = body.split("\n");
                                    for (String line : hashLines) {
                                        line = line.trim();
                                        if (line.isEmpty()) continue;
                                        String[] parts = line.split("\t", 2);
                                        if (parts.length != 2) continue;
                                        String rowKey = parts[0];
                                        String theirHash = parts[1];

                                        // check our local copy
                                        Row localRow = worker.getRow(table, rowKey);
                                        String localHash = localRow != null ? hashRow(localRow) : "";

                                        if (!theirHash.equals(localHash)) {
                                            // fetch the full row from neighbor
                                            HTTP.Response rowResp = HTTP.doRequest("GET",
                                                    "http://" + neighborAddr + "/data/" + table + "/" + rowKey, null);
                                            if (rowResp.statusCode() == 200) {
                                                Row fetched = Row.readFrom(new ByteArrayInputStream(rowResp.body()));
                                                if (fetched != null) worker.putRow(table, fetched);
                                            }
                                        }
                                    }
                                } catch (Exception e) {
                                    logger.error("maintenance fetch failed: " + e.getMessage());
                                }
                            }
                        } catch (Exception e) {
                            logger.error("maintenance table list failed: " + e.getMessage());
                        }
                    }
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    logger.error("maintenance error: " + e.getMessage());
                }
            }
        });
        maintThread.setDaemon(true);
        // Anti-entropy hashes EVERY row of EVERY table against neighbours every 30s. During a
        // big Flame batch job the ephemeral temp tables (job-*) hold multi-MB mega-rows that
        // change constantly, so every cycle re-MD5s and re-fetches megabytes of moving data and
        // never converges — pegging a core and starving the actual job. We don't simulate worker
        // failures here, so default it OFF; re-enable with -Dkvs.worker.antiEntropy=true.
        if (Boolean.parseBoolean(System.getProperty("kvs.worker.antiEntropy", "false"))) {
            maintThread.start();
        } else {
            logger.info("anti-entropy maintenance thread disabled (set -Dkvs.worker.antiEntropy=true to enable)");
        }

        // PUT /data/<T>/<R>/<C>
        Server.put("/data/:table/:row/:col", (req, res) -> {
            String table = req.params("table");
            String rowKey = req.params("row");
            String col = req.params("col");
            byte[] val = req.bodyAsBytes();
            if (val == null) val = new byte[0];

            String ifcol = req.queryParams("ifcolumn");
            String equals = req.queryParams("equals");

            if (isPersistent(table)) {
                synchronized (stripeLock(table, rowKey)) {
                    Row row = readFromDisk(table, rowKey);
                    if (row == null) row = new Row(rowKey);

                    if (ifcol != null && equals != null) {
                        String cur = row.get(ifcol);
                        if (!equals.equals(cur == null ? "" : cur)) return "FAIL";
                    }

                    row.put(col, val);
                    writeToDisk(table, row);
                }
            } else {
                tables.computeIfAbsent(table, k -> new ConcurrentHashMap<>());
                // versions.computeIfAbsent(table, k -> new ConcurrentHashMap<>());

                ConcurrentHashMap<String, Row> t = tables.get(table);
                // ConcurrentHashMap<String, List<Row>> vt = versions.get(table);
                boolean trackVersions = !table.startsWith("job-");
                if (trackVersions) {
                    versions.computeIfAbsent(table, k -> new ConcurrentHashMap<>()); 
                }

                synchronized (t) {
                    if (!t.containsKey(rowKey)) t.put(rowKey, new Row(rowKey));
                    Row row = t.get(rowKey);

                    if (ifcol != null && equals != null) {
                        byte[] cur = row.getBytes(ifcol);
                        String curStr = cur == null ? null : new String(cur);
                        if (!equals.equals(curStr)) return "FAIL";
                    }

                    row.put(col, val);

                    if (trackVersions) {
                        ConcurrentHashMap<String, List<Row>> vt = versions.get(table);
                        vt.computeIfAbsent(rowKey, k -> new ArrayList<>());
                        vt.get(rowKey).add(row.clone());
                        int ver = vt.get(rowKey).size();
                        res.header("Version", String.valueOf(ver));
                    }
                }
            }

            // EC2: forward PUT to replicas (fire and forget), unless this PUT is itself a forward
            if (!"0".equals(req.queryParams("forward"))) {
                final byte[] finalVal = val;
                List<String> targets = getReplicaTargets();
                for (String target : targets) {
                    replicaPool.submit(() -> forwardPut(target, table, rowKey, col, finalVal));
                }
            }

            return "OK";
        });

        // GET /data/<T>/<R>/<C>
        Server.get("/data/:table/:row/:col", (req, res) -> {
            String table = req.params("table");
            String rowKey = req.params("row");
            String col = req.params("col");
            String verParam = req.queryParams("version");

            Row row = null;
            int ver = -1;

            if (!isPersistent(table) && verParam != null) {
                ConcurrentHashMap<String, List<Row>> vt = versions.get(table);
                if (vt != null) {
                    List<Row> vlist = vt.get(rowKey);
                    if (vlist != null) {
                        int reqVer = Integer.parseInt(verParam);
                        if (reqVer >= 1 && reqVer <= vlist.size()) {
                            row = vlist.get(reqVer - 1);
                            ver = reqVer;
                        }
                    }
                }
            } else {
                row = worker.getRow(table, rowKey);
                if (row != null && !isPersistent(table)) {
                    ConcurrentHashMap<String, List<Row>> vt = versions.get(table);
                    if (vt != null) {
                        List<Row> vlist = vt.get(rowKey);
                        if (vlist != null) ver = vlist.size();
                    }
                }
            }

            if (row == null) { res.status(404, "Not Found"); return null; }
            byte[] val = row.getBytes(col);
            if (val == null) { res.status(404, "Not Found"); return null; }

            if (ver > 0) res.header("Version", String.valueOf(ver));
            res.bodyAsBytes(val);
            return null;
        });

        // GET /data/<T>/<R> - whole row
        Server.get("/data/:table/:row", (req, res) -> {
            String table = req.params("table");
            String rowKey = req.params("row");

            if (!tableExists(table)) { res.status(404, "Not Found"); return null; }
            Row row = worker.getRow(table, rowKey);
            if (row == null) { res.status(404, "Not Found"); return null; }

            res.bodyAsBytes(row.toByteArray());
            return null;
        });

        // HEAD /data/<T>/<R> - existence check only (used by KVSClient.existsRow).
        // For persistent tables this is a single stat() syscall; we never read the
        // row file, so the per-anchor existence check no longer ships up to a 5 MB
        // page body across the network just to be discarded.
        Server.head("/data/:table/:row", (req, res) -> {
            String table = req.params("table");
            String rowKey = req.params("row");

            if (!tableExists(table)) { res.status(404, "Not Found"); return null; }

            boolean exists;
            if (isPersistent(table)) {
                exists = rowFile(table, rowKey).exists();
            } else {
                ConcurrentHashMap<String, Row> t = tables.get(table);
                exists = t != null && t.containsKey(rowKey);
            }
            if (!exists) { res.status(404, "Not Found"); return null; }
            res.status(200, "OK");
            return null;
        });

        // GET /data/<T> - streaming read
        // EC3: supports keyonly=true param which returns key\thash lines instead of full rows
        // Streams via streamRowKeys so we never materialize the full key list of the table
        // (used to call getRowKeys, which list+sorted every key on every scan).
        Server.get("/data/:table", (req, res) -> {
            String table = req.params("table");

            if (!tableExists(table)) { res.status(404, "Not Found"); return null; }

            String startRow = req.queryParams("startRow");
            String endRow = req.queryParams("endRowExclusive");
            String keyOnly = req.queryParams("keyonly");

            res.type("text/plain");

            streamRowKeys(table, startRow, endRow, key -> {
                try {
                    Row row = worker.getRow(table, key);
                    if (row == null) return;
                    if ("true".equals(keyOnly)) {
                        // EC3: return key\thash\n
                        String line = key + "\t" + hashRow(row) + "\n";
                        res.write(line.getBytes());
                    } else {
                        res.write(row.toByteArray());
                        res.write(new byte[]{10}); // LF after each row
                    }
                } catch (Exception e) {
                    // res.write throws checked Exception; wrap so it surfaces
                    // out of streamRowKeys and the try-with-resources DirectoryStream auto-closes.
                    throw new RuntimeException(e);
                }
            });
            if (!"true".equals(keyOnly)) {
                res.write(new byte[]{10}); // final LF = end of stream
            }
            return null;
        });

        // GET /tables
        Server.get("/tables", (req, res) -> {
            res.type("text/plain");
            StringBuilder sb = new StringBuilder();
            for (String name : getAllTables()) sb.append(name).append("\n");
            return sb.toString();
        });

        // GET /count/<T>
        Server.get("/count/:table", (req, res) -> {
            String table = req.params("table");
            if (!tableExists(table)) { res.status(404, "Not Found"); return null; }
            return String.valueOf(worker.count(table));
        });

        // PUT /rename/<T>
        Server.put("/rename/:table", (req, res) -> {
            String oldName = req.params("table");
            String newName = req.body();

            if (!tableExists(oldName)) { res.status(404, "Not Found"); return null; }
            if (tableExists(newName)) { res.status(409, "Conflict"); return null; }
            if (isPersistent(oldName) != isPersistent(newName)) { res.status(400, "Bad Request"); return null; }

            worker.rename(oldName, newName);
            return "OK";
        });

        // PUT /delete/<T>
        Server.put("/delete/:table", (req, res) -> {
            String table = req.params("table");
            if (!tableExists(table)) { res.status(404, "Not Found"); return null; }
            worker.delete(table);
            return "OK";
        });

        // GET / - table list UI
        Server.get("/", (req, res) -> {
            res.type("text/html");
            StringBuilder sb = new StringBuilder();
            sb.append("<html><body><h1>KVS Worker: ").append(id).append("</h1>");
            sb.append("<table border='1'><tr><th>Table</th><th>Rows</th></tr>");
            for (String name : getAllTables()) {
                sb.append("<tr><td><a href='/view/").append(name).append("'>").append(name).append("</a></td>");
                sb.append("<td>").append(worker.count(name)).append("</td></tr>");
            }
            sb.append("</table></body></html>");
            return sb.toString();
        });

        // GET /view/<T> - paginated table viewer
        Server.get("/view/:table", (req, res) -> {
            String table = req.params("table");
            res.type("text/html");

            if (!tableExists(table)) {
                return "<html><body><h1>Table not found: " + table + "</h1></body></html>";
            }

            String fromRow = req.queryParams("fromRow");
            List<String> allKeys = getRowKeys(table);

            // find where to start
            int startIdx = 0;
            if (fromRow != null) {
                for (int i = 0; i < allKeys.size(); i++) {
                    if (allKeys.get(i).compareTo(fromRow) >= 0) { startIdx = i; break; }
                }
            }

            // grab up to 10 rows
            List<Row> pageRows = new ArrayList<>();
            for (int i = startIdx; i < allKeys.size() && pageRows.size() < 10; i++) {
                Row row = worker.getRow(table, allKeys.get(i));
                if (row != null) pageRows.add(row);
            }

            // collect sorted column names across these rows
            Set<String> colSet = new TreeSet<>();
            for (Row row : pageRows) colSet.addAll(row.columns());
            List<String> cols = new ArrayList<>(colSet);

            StringBuilder sb = new StringBuilder();
            sb.append("<html><body><h1>Table: ").append(table).append("</h1>");
            sb.append("<table border='1'><tr><th>Key</th>");
            for (String col : cols) sb.append("<th>").append(col).append("</th>");
            sb.append("</tr>");

            for (Row row : pageRows) {
                sb.append("<tr><td>").append(row.key()).append("</td>");
                for (String col : cols) {
                    String val = row.get(col);
                    sb.append("<td>").append(val != null ? val : "").append("</td>");
                }
                sb.append("</tr>");
            }

            sb.append("</table>");

            // next page link if more rows exist
            if (startIdx + 10 < allKeys.size()) {
                String nextKey = allKeys.get(startIdx + 10);
                sb.append("<a href='/view/").append(table).append("?fromRow=").append(nextKey).append("'>Next</a>");
            }

            sb.append("</body></html>");
            return sb.toString();
        });
    }
}