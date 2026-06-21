package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.tools.*;
import java.io.*;
import java.net.*;
import java.time.Instant;
import java.util.*;

public class FlameContextImpl implements FlameContext, Serializable {

  private String jarName;
  private transient StringBuilder outputBuilder;
  private int nextSequenceNumber;
  private int concurrencyLevel;
  private String kvsCoordinator;
  private transient KVSClient workerKVS;
  private static final boolean DIAG_ENABLED = isDiagEnabled();

  private static boolean isDiagEnabled() {
    String prop = System.getProperty("cerebro.diag");
    if (prop == null || prop.isEmpty()) {
      prop = System.getenv("CEREBRO_DIAG");
    }
    if (prop == null) {
      return false;
    }
    prop = prop.trim().toLowerCase(Locale.ROOT);
    return prop.equals("1") || prop.equals("true") || prop.equals("yes") || prop.equals("on");
  }

  private static String jsonEscape(String s) {
    if (s == null) return "";
    StringBuilder out = new StringBuilder(s.length() + 16);
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '\\': out.append("\\\\"); break;
        case '"': out.append("\\\""); break;
        case '\n': out.append("\\n"); break;
        case '\r': out.append("\\r"); break;
        case '\t': out.append("\\t"); break;
        default:
          if (c < 0x20) {
            out.append(String.format("\\u%04x", (int) c));
          } else {
            out.append(c);
          }
      }
    }
    return out.toString();
  }

  private static synchronized void diag(String phase, String status, String metricsJson, String message) {
    if (!DIAG_ENABLED) {
      return;
    }
    StringBuilder sb = new StringBuilder();
    sb.append("{\"tsWall\":\"").append(Instant.now().toString()).append("\"");
    sb.append(",\"component\":\"FLAME_COORD\"");
    sb.append(",\"phase\":\"").append(phase).append("\"");
    sb.append(",\"status\":\"").append(status).append("\"");
    if (metricsJson != null && !metricsJson.isEmpty()) {
      sb.append(",\"metrics\":").append(metricsJson);
    }
    if (message != null) {
      sb.append(",\"message\":\"").append(jsonEscape(message)).append("\"");
    }
    sb.append("}");
    System.err.println(sb);
  }

  public FlameContextImpl(String jarNameArg) {
    jarName = jarNameArg;
    outputBuilder = new StringBuilder();
    nextSequenceNumber = 1;
    concurrencyLevel = 1;
    if (Coordinator.kvs != null) {
      kvsCoordinator = Coordinator.kvs.getCoordinator();
    }
  }

  public KVSClient getKVS() {
    // when running on coordinator, use Coordinator.kvs directly
    if (Coordinator.kvs != null) {
      return Coordinator.kvs;
    }
    // when deserialized on a worker, create a new client from saved coordinator addr
    if (workerKVS == null) {
      workerKVS = new KVSClient(kvsCoordinator);
    }
    return workerKVS;
  }

  public void output(String s) {
    if (outputBuilder == null) outputBuilder = new StringBuilder();
    outputBuilder.append(s);
  }

  public String getOutput() {
    if (outputBuilder == null || outputBuilder.length() == 0) {
      return "No output";
    }
    return outputBuilder.toString();
  }

  public void setConcurrencyLevel(int keyRangesPerWorker) {
    this.concurrencyLevel = keyRangesPerWorker;
  }

  public FlameRDD parallelize(List<String> list) throws Exception {
    String tableName = freshTableName();
    KVSClient kvs = getKVS();
    int i = 1;
    for (String s : list) {
      String rowKey = Hasher.hash("" + i);
      kvs.put(tableName, rowKey, "value", s);
      i++;
    }
    FlameRDDImpl rdd = new FlameRDDImpl(tableName, this);
    return rdd;
  }

  public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
    byte[] serializedLambda = Serializer.objectToByteArray(lambda);
    String outputTable = invokeOperation("/context/fromTable", serializedLambda, tableName, null);
    return new FlameRDDImpl(outputTable, this);
  }

  public String freshTableName() {
    return jarName.replace(".jar", "") + "_" + (nextSequenceNumber++);
  }

  public String getJarName() {
    return jarName;
  }

  public String invokeOperation(String operationName, byte[] serializedLambda, String inputTable, String extraArg) throws Exception {
    String outputTable = freshTableName();
    return invokeOperationWithOutput(operationName, serializedLambda, inputTable, outputTable, extraArg);
  }

  public String invokeOperationWithOutput(String operationName, byte[] serializedLambda, String inputTable, String outputTable, String extraArg) throws Exception {
    KVSClient kvs = getKVS();
    long opStartMs = System.currentTimeMillis();

    Partitioner partitioner = new Partitioner();
    partitioner.setKeyRangesPerWorker(concurrencyLevel);

    int numKVSWorkers = kvs.numWorkers();
    for (int i = 0; i < numKVSWorkers; i++) {
      String workerAddr = kvs.getWorkerAddress(i);
      String workerID = kvs.getWorkerID(i);
      if (i < numKVSWorkers - 1) {
        String nextWorkerID = kvs.getWorkerID(i + 1);
        partitioner.addKVSWorker(workerAddr, workerID, nextWorkerID);
      } else {
        partitioner.addKVSWorker(workerAddr, workerID, null);
        partitioner.addKVSWorker(workerAddr, null, kvs.getWorkerID(0));
      }
    }

    Vector<String> flameWorkers = Coordinator.getWorkers();

    File jarFile = new File(jarName);
    if (jarFile.exists()) {
      byte[] jarBytes = java.nio.file.Files.readAllBytes(jarFile.toPath());
      for (String worker : flameWorkers) {
        String[] parts = worker.split(",");
        try { 
          HTTP.doRequest("POST", "http://" + parts[1] + "/useJAR", jarBytes); 
        } catch (Exception e) {}
      }
    }

    for (String fw : flameWorkers) {
      // getWorkers() returns "id,address" - we need just the address part
      String[] parts = fw.split(",");
      partitioner.addFlameWorker(parts[1]);
    }

    Vector<Partitioner.Partition> partitions = partitioner.assignPartitions();
    if (partitions == null) {
      throw new Exception("No partitions assigned");
    }
    diag("FLAME", "ok",
            "{\"op\":\"" + jsonEscape(operationName) + "\"" +
                    ",\"event\":\"operation_start\"" +
                    ",\"inputTable\":\"" + jsonEscape(inputTable) + "\"" +
                    ",\"outputTable\":\"" + jsonEscape(outputTable) + "\"" +
                    ",\"partitionCount\":" + partitions.size() +
                    "}",
            null);

    Thread[] threads = new Thread[partitions.size()];
    String[] results = new String[partitions.size()];
    int[] statusCodes = new int[partitions.size()];
    long[] partitionDurationsMs = new long[partitions.size()];

    for (int i = 0; i < partitions.size(); i++) {
      final int idx = i;
      Partitioner.Partition p = partitions.get(i);

      StringBuilder urlBuilder = new StringBuilder();
      urlBuilder.append("http://").append(p.assignedFlameWorker).append(operationName);
      urlBuilder.append("?inputTable=").append(URLEncoder.encode(inputTable, "UTF-8"));
      urlBuilder.append("&outputTable=").append(URLEncoder.encode(outputTable, "UTF-8"));
      urlBuilder.append("&kvsCoordinator=").append(URLEncoder.encode(kvs.getCoordinator(), "UTF-8"));

      if (p.fromKey != null) {
        urlBuilder.append("&fromKey=").append(URLEncoder.encode(p.fromKey, "UTF-8"));
      }
      if (p.toKeyExclusive != null) {
        urlBuilder.append("&toKeyExclusive=").append(URLEncoder.encode(p.toKeyExclusive, "UTF-8"));
      }
      if (extraArg != null) {
        urlBuilder.append("&extraArg=").append(URLEncoder.encode(extraArg, "UTF-8"));
      }

      final String url = urlBuilder.toString();
      diag("FLAME", "ok",
              "{\"op\":\"" + jsonEscape(operationName) + "\"" +
                      ",\"event\":\"partition_start\"" +
                      ",\"partitionIdx\":" + idx +
                      ",\"assignedWorker\":\"" + jsonEscape(p.assignedFlameWorker) + "\"" +
                      ",\"fromKey\":\"" + jsonEscape(String.valueOf(p.fromKey)) + "\"" +
                      ",\"toKeyExclusive\":\"" + jsonEscape(String.valueOf(p.toKeyExclusive)) + "\"" +
                      "}",
              null);

      threads[i] = new Thread("Operation " + operationName + " #" + i) {
        public void run() {
          long partitionStart = System.currentTimeMillis();
          try {
            HTTP.Response resp = HTTP.doRequest("POST", url, serializedLambda);
            if (resp == null) throw new Exception("No response from Flame worker " + p.assignedFlameWorker);
            statusCodes[idx] = resp.statusCode();
            results[idx] = new String(resp.body());
            partitionDurationsMs[idx] = System.currentTimeMillis() - partitionStart;
            diag("FLAME", statusCodes[idx] == 200 ? "ok" : "error",
                    "{\"op\":\"" + jsonEscape(operationName) + "\"" +
                            ",\"event\":\"partition_end\"" +
                            ",\"partitionIdx\":" + idx +
                            ",\"assignedWorker\":\"" + jsonEscape(p.assignedFlameWorker) + "\"" +
                            ",\"durationMs\":" + partitionDurationsMs[idx] +
                            ",\"statusCode\":" + statusCodes[idx] +
                            "}",
                    null);
          } catch (Exception e) {
            results[idx] = "Exception: " + e.getMessage();
            statusCodes[idx] = -1;
            partitionDurationsMs[idx] = System.currentTimeMillis() - partitionStart;
            diag("FLAME", "error",
                    "{\"op\":\"" + jsonEscape(operationName) + "\"" +
                            ",\"event\":\"partition_end\"" +
                            ",\"partitionIdx\":" + idx +
                            ",\"assignedWorker\":\"" + jsonEscape(p.assignedFlameWorker) + "\"" +
                            ",\"durationMs\":" + partitionDurationsMs[idx] +
                            ",\"statusCode\":-1" +
                            "}",
                    e.getClass().getSimpleName() + ": " + e.getMessage());
            e.printStackTrace();
          }
        }
      };
      threads[i].start();
    }

    for (int i = 0; i < threads.length; i++) {
      try {
        threads[i].join();
      } catch (InterruptedException ie) {
      }
    }

    for (int i = 0; i < threads.length; i++) {
      if (statusCodes[i] != 200) {
        summarizeOperation(operationName, statusCodes, partitionDurationsMs, opStartMs);
        throw new Exception("Worker " + i + " returned status " + statusCodes[i] + ": " + results[i]);
      }
    }
    summarizeOperation(operationName, statusCodes, partitionDurationsMs, opStartMs);

    return outputTable;
  }

  public String[] invokeOperationCollectResults(String operationName, byte[] serializedLambda, String inputTable, String extraArg) throws Exception {
    KVSClient kvs = getKVS();
    long opStartMs = System.currentTimeMillis();

    Partitioner partitioner = new Partitioner();
    partitioner.setKeyRangesPerWorker(concurrencyLevel);

    int numKVSWorkers = kvs.numWorkers();
    for (int i = 0; i < numKVSWorkers; i++) {
      String workerAddr = kvs.getWorkerAddress(i);
      String workerID = kvs.getWorkerID(i);
      if (i < numKVSWorkers - 1) {
        String nextWorkerID = kvs.getWorkerID(i + 1);
        partitioner.addKVSWorker(workerAddr, workerID, nextWorkerID);
      } else {
        partitioner.addKVSWorker(workerAddr, workerID, null);
        partitioner.addKVSWorker(workerAddr, null, kvs.getWorkerID(0));
      }
    }

    Vector<String> flameWorkers = Coordinator.getWorkers();
    for (String fw : flameWorkers) {
      String[] parts = fw.split(",");
      partitioner.addFlameWorker(parts[1]);
    }

    Vector<Partitioner.Partition> partitions = partitioner.assignPartitions();
    if (partitions == null) {
      throw new Exception("No partitions assigned");
    }
    diag("FLAME", "ok",
            "{\"op\":\"" + jsonEscape(operationName) + "\"" +
                    ",\"event\":\"operation_start\"" +
                    ",\"inputTable\":\"" + jsonEscape(inputTable) + "\"" +
                    ",\"outputTable\":\"none\"" +
                    ",\"partitionCount\":" + partitions.size() +
                    "}",
            null);

    Thread[] threads = new Thread[partitions.size()];
    String[] results = new String[partitions.size()];
    int[] statusCodes = new int[partitions.size()];
    long[] partitionDurationsMs = new long[partitions.size()];

    for (int i = 0; i < partitions.size(); i++) {
      final int idx = i;
      Partitioner.Partition p = partitions.get(i);

      StringBuilder urlBuilder = new StringBuilder();
      urlBuilder.append("http://").append(p.assignedFlameWorker).append(operationName);
      urlBuilder.append("?inputTable=").append(URLEncoder.encode(inputTable, "UTF-8"));
      urlBuilder.append("&outputTable=").append(URLEncoder.encode("none", "UTF-8"));
      urlBuilder.append("&kvsCoordinator=").append(URLEncoder.encode(kvs.getCoordinator(), "UTF-8"));

      if (p.fromKey != null) {
        urlBuilder.append("&fromKey=").append(URLEncoder.encode(p.fromKey, "UTF-8"));
      }
      if (p.toKeyExclusive != null) {
        urlBuilder.append("&toKeyExclusive=").append(URLEncoder.encode(p.toKeyExclusive, "UTF-8"));
      }
      if (extraArg != null) {
        urlBuilder.append("&extraArg=").append(URLEncoder.encode(extraArg, "UTF-8"));
      }

      final String url = urlBuilder.toString();
      diag("FLAME", "ok",
              "{\"op\":\"" + jsonEscape(operationName) + "\"" +
                      ",\"event\":\"partition_start\"" +
                      ",\"partitionIdx\":" + idx +
                      ",\"assignedWorker\":\"" + jsonEscape(p.assignedFlameWorker) + "\"" +
                      ",\"fromKey\":\"" + jsonEscape(String.valueOf(p.fromKey)) + "\"" +
                      ",\"toKeyExclusive\":\"" + jsonEscape(String.valueOf(p.toKeyExclusive)) + "\"" +
                      "}",
              null);

      threads[i] = new Thread("Operation " + operationName + " #" + i) {
        public void run() {
          long partitionStart = System.currentTimeMillis();
          try {
            HTTP.Response resp = HTTP.doRequest("POST", url, serializedLambda);
            if (resp == null) throw new Exception("No response from Flame worker " + p.assignedFlameWorker);
            statusCodes[idx] = resp.statusCode();
            results[idx] = new String(resp.body());
            partitionDurationsMs[idx] = System.currentTimeMillis() - partitionStart;
            diag("FLAME", statusCodes[idx] == 200 ? "ok" : "error",
                    "{\"op\":\"" + jsonEscape(operationName) + "\"" +
                            ",\"event\":\"partition_end\"" +
                            ",\"partitionIdx\":" + idx +
                            ",\"assignedWorker\":\"" + jsonEscape(p.assignedFlameWorker) + "\"" +
                            ",\"durationMs\":" + partitionDurationsMs[idx] +
                            ",\"statusCode\":" + statusCodes[idx] +
                            "}",
                    null);
          } catch (Exception e) {
            results[idx] = "Exception: " + e.getMessage();
            statusCodes[idx] = -1;
            partitionDurationsMs[idx] = System.currentTimeMillis() - partitionStart;
            diag("FLAME", "error",
                    "{\"op\":\"" + jsonEscape(operationName) + "\"" +
                            ",\"event\":\"partition_end\"" +
                            ",\"partitionIdx\":" + idx +
                            ",\"assignedWorker\":\"" + jsonEscape(p.assignedFlameWorker) + "\"" +
                            ",\"durationMs\":" + partitionDurationsMs[idx] +
                            ",\"statusCode\":-1" +
                            "}",
                    e.getClass().getSimpleName() + ": " + e.getMessage());
            e.printStackTrace();
          }
        }
      };
      threads[i].start();
    }

    for (int i = 0; i < threads.length; i++) {
      try {
        threads[i].join();
      } catch (InterruptedException ie) {
      }
    }

    for (int i = 0; i < threads.length; i++) {
      if (statusCodes[i] != 200) {
        summarizeOperation(operationName, statusCodes, partitionDurationsMs, opStartMs);
        throw new Exception("Worker " + i + " returned status " + statusCodes[i] + ": " + results[i]);
      }
    }
    summarizeOperation(operationName, statusCodes, partitionDurationsMs, opStartMs);

    return results;
  }

  private static void summarizeOperation(String operationName, int[] statusCodes, long[] partitionDurationsMs, long opStartMs) {
    long minMs = Long.MAX_VALUE;
    long maxMs = 0;
    long sumMs = 0;
    int failures = 0;
    for (int i = 0; i < partitionDurationsMs.length; i++) {
      long d = partitionDurationsMs[i];
      minMs = Math.min(minMs, d);
      maxMs = Math.max(maxMs, d);
      sumMs += d;
      if (statusCodes[i] != 200) {
        failures++;
      }
    }
    long avgMs = partitionDurationsMs.length == 0 ? 0 : (sumMs / partitionDurationsMs.length);
    if (minMs == Long.MAX_VALUE) minMs = 0;
    diag("FLAME", failures == 0 ? "ok" : "error",
            "{\"op\":\"" + jsonEscape(operationName) + "\"" +
                    ",\"event\":\"operation_end\"" +
                    ",\"partitionCount\":" + partitionDurationsMs.length +
                    ",\"failureCount\":" + failures +
                    ",\"durationMs\":" + (System.currentTimeMillis() - opStartMs) +
                    ",\"partitionMinMs\":" + minMs +
                    ",\"partitionAvgMs\":" + avgMs +
                    ",\"partitionMaxMs\":" + maxMs +
                    "}",
            null);
  }
}
