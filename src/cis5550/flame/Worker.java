package cis5550.flame;

import cis5550.kvs.*;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;
import static cis5550.webserver.Server.*;
import java.io.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class Worker extends cis5550.generic.Worker {

	private static final int NUM_THREADS =
			Integer.getInteger("flame.worker.concurrency", 32);
	// Bounded queue + CallerRuns: when the queue is full, the submitting thread runs the task
	// itself, which back-pressures the row-scanning loops in the handlers below. Without this,
	// a handler scans an entire partition up front and queues one task per row — each task holds
	// its Row in the closure — so for a big-valued table (pt-crawl pages) that's gigabytes of
	// in-flight Rows on a 1 GB-heap worker -> OutOfMemoryError. The bound caps in-flight Rows to
	// ~queue+pool, a few MB.
	private static final ExecutorService POOL = new ThreadPoolExecutor(
			NUM_THREADS, NUM_THREADS, 0L, TimeUnit.MILLISECONDS,
			new ArrayBlockingQueue<Runnable>(Math.max(NUM_THREADS * 2, 32)),
			new ThreadPoolExecutor.CallerRunsPolicy());
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
		sb.append(",\"component\":\"FLAME_WORKER\"");
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

	private static void awaitAll(List<Future<?>> futures) throws Exception {
		Exception first = null;
		for (Future<?> f : futures) {
			try {
				f.get();
			} catch (ExecutionException ee) {
				Throwable c = ee.getCause();
				Exception ex = (c instanceof Exception) ? (Exception) c : new RuntimeException(c);
				if (first == null) first = ex;
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
				if (first == null) first = ie;
			}
		}
		if (first != null) throw first;
	}

	public static void main(String args[]) {
    if (args.length != 2) {
    	System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
    	System.exit(1);
    }

    int port = Integer.parseInt(args[0]);
    String server = args[1];
    System.err.println("flame.worker.concurrency=" + NUM_THREADS);
	  startPingThread(server, ""+port, port);
    final File myJAR = new File("__worker"+port+"-current.jar");

  	port(port);

    post("/useJAR", (request,response) -> {
      FileOutputStream fos = new FileOutputStream(myJAR);
      fos.write(request.bodyAsBytes());
      fos.close();
      return "OK";
    });

    // ---- RDD flatMap (from HW6) ----
    post("/rdd/flatMap", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");
      long routeStart = System.currentTimeMillis();
      ThreadPoolExecutor pool = (POOL instanceof ThreadPoolExecutor) ? (ThreadPoolExecutor) POOL : null;
      int queueDepth = pool != null ? pool.getQueue().size() : -1;
      int activeCount = pool != null ? pool.getActiveCount() : -1;
      diag("FLAME", "ok",
              "{\"op\":\"/rdd/flatMap\"" +
                      ",\"event\":\"route_start\"" +
                      ",\"inputTable\":\"" + jsonEscape(inputTable) + "\"" +
                      ",\"outputTable\":\"" + jsonEscape(outputTable) + "\"" +
                      ",\"fromKey\":\"" + jsonEscape(String.valueOf(fromKey)) + "\"" +
                      ",\"toKeyExclusive\":\"" + jsonEscape(String.valueOf(toKeyExclusive)) + "\"" +
                      ",\"poolThreads\":" + NUM_THREADS +
                      ",\"poolQueueDepth\":" + queueDepth +
                      ",\"poolActive\":" + activeCount +
                      "}",
              null);

      KVSClient kvs = new KVSClient(kvsCoordinator);
      FlameRDD.StringToIterable lambda = (FlameRDD.StringToIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

      Iterator<Row> iter = kvs.scan(inputTable, fromKey, toKeyExclusive);
      AtomicInteger counter = new AtomicInteger(0);
      AtomicLong scannedRows = new AtomicLong(0);
      AtomicLong submittedTasks = new AtomicLong(0);
      AtomicLong emittedValues = new AtomicLong(0);
      AtomicLong taskErrors = new AtomicLong(0);
      List<Future<?>> futures = new ArrayList<>();
      while (iter.hasNext()) {
        final Row row = iter.next();
        scannedRows.incrementAndGet();
        submittedTasks.incrementAndGet();
        futures.add(POOL.submit(() -> {
          try {
            String value = row.get("value");
            if (value != null) {
              Iterable<String> results = lambda.op(value);
              if (results != null) {
                for (String s : results) {
                  String rowKey = Hasher.hash(row.key() + counter.getAndIncrement());
                  kvs.put(outputTable, rowKey, "value", s);
                  emittedValues.incrementAndGet();
                }
              }
            }
            return null;
          } catch (Exception e) {
            taskErrors.incrementAndGet();
            throw e;
          }
        }));
      }
      try {
        awaitAll(futures);
      } catch (Exception e) {
        diag("FLAME", "error",
                "{\"op\":\"/rdd/flatMap\"" +
                        ",\"event\":\"route_end\"" +
                        ",\"durationMs\":" + (System.currentTimeMillis() - routeStart) +
                        ",\"scannedRows\":" + scannedRows.get() +
                        ",\"submittedTasks\":" + submittedTasks.get() +
                        ",\"emittedValues\":" + emittedValues.get() +
                        ",\"taskErrors\":" + taskErrors.get() +
                        "}",
                e.getClass().getSimpleName() + ": " + e.getMessage());
        throw e;
      }
      diag("FLAME", "ok",
              "{\"op\":\"/rdd/flatMap\"" +
                      ",\"event\":\"route_end\"" +
                      ",\"durationMs\":" + (System.currentTimeMillis() - routeStart) +
                      ",\"scannedRows\":" + scannedRows.get() +
                      ",\"submittedTasks\":" + submittedTasks.get() +
                      ",\"emittedValues\":" + emittedValues.get() +
                      ",\"taskErrors\":" + taskErrors.get() +
                      "}",
              null);
      return "OK";
    });

    // ---- RDD mapToPair (from HW6) ----
    post("/rdd/mapToPair", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");

      KVSClient kvs = new KVSClient(kvsCoordinator);
      FlameRDD.StringToPair lambda = (FlameRDD.StringToPair) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

      Iterator<Row> iter = kvs.scan(inputTable, fromKey, toKeyExclusive);
      List<Future<?>> futures = new ArrayList<>();
      while (iter.hasNext()) {
        final Row row = iter.next();
        futures.add(POOL.submit(() -> {
          String value = row.get("value");
          if (value != null) {
            FlamePair pair = lambda.op(value);
            if (pair != null) {
              kvs.put(outputTable, pair._1(), row.key(), pair._2());
            }
          }
          return null;
        }));
      }
      awaitAll(futures);
      return "OK";
    });

    // ---- PairRDD foldByKey (from HW6) ----
    post("/pairrdd/foldByKey", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");
      String zeroElement = request.queryParams("extraArg");

      KVSClient kvs = new KVSClient(kvsCoordinator);
      FlamePairRDD.TwoStringsToString lambda = (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
      final String zero = zeroElement;

      // Parallelize per input row: each row is folded independently and produces one output row,
      // so this is safe. The single-threaded `while` loop here used to be a bottleneck when a row
      // had tens of thousands of columns (a high-frequency word in the inverted index).
      Iterator<Row> iter = kvs.scan(inputTable, fromKey, toKeyExclusive);
      List<Future<?>> futures = new ArrayList<>();
      while (iter.hasNext()) {
        final Row row = iter.next();
        futures.add(POOL.submit(() -> {
          String accumulator = zero;
          for (String col : row.columns()) {
            String val = row.get(col);
            if (val != null) {
              accumulator = lambda.op(accumulator, val);
            }
          }
          kvs.put(outputTable, row.key(), "value", accumulator);
          return null;
        }));
      }
      awaitAll(futures);
      return "OK";
    });

    // ---- Context fromTable ----
    post("/context/fromTable", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");

      KVSClient kvs = new KVSClient(kvsCoordinator);
      FlameContext.RowToString lambda = (FlameContext.RowToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

      Iterator<Row> iter = kvs.scan(inputTable, fromKey, toKeyExclusive);
      AtomicInteger counter = new AtomicInteger(0);
      List<Future<?>> futures = new ArrayList<>();
      while (iter.hasNext()) {
        final Row row = iter.next();
        futures.add(POOL.submit(() -> {
          String result = lambda.op(row);
          if (result != null) {
            String rowKey = Hasher.hash(row.key() + counter.getAndIncrement());
            kvs.put(outputTable, rowKey, "value", result);
          }
          return null;
        }));
      }
      awaitAll(futures);
      return "OK";
    });

    // ---- RDD flatMapToPair ----
    post("/rdd/flatMapToPair", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");

      KVSClient kvs = new KVSClient(kvsCoordinator);
      FlameRDD.StringToPairIterable lambda = (FlameRDD.StringToPairIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

      Iterator<Row> iter = kvs.scan(inputTable, fromKey, toKeyExclusive);
      AtomicInteger counter = new AtomicInteger(0);
      List<Future<?>> futures = new ArrayList<>();
      while (iter.hasNext()) {
        final Row row = iter.next();
        futures.add(POOL.submit(() -> {
          String value = row.get("value");
          if (value != null) {
            Iterable<FlamePair> results = lambda.op(value);
            if (results != null) {
              for (FlamePair p : results) {
                String colName = Hasher.hash(row.key() + counter.getAndIncrement());
                kvs.put(outputTable, p._1(), colName, p._2());
              }
            }
          }
          return null;
        }));
      }
      awaitAll(futures);
      return "OK";
    });

    // ---- PairRDD flatMap ----
    post("/pairrdd/flatMap", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");

      KVSClient kvs = new KVSClient(kvsCoordinator);
      FlamePairRDD.PairToStringIterable lambda = (FlamePairRDD.PairToStringIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

      Iterator<Row> iter = kvs.scan(inputTable, fromKey, toKeyExclusive);
      AtomicInteger counter = new AtomicInteger(0);
      List<Future<?>> futures = new ArrayList<>();
      while (iter.hasNext()) {
        final Row row = iter.next();
        futures.add(POOL.submit(() -> {
          String key = row.key();
          Set<String> columns = row.columns();
          for (String col : columns) {
            String val = row.get(col);
            if (val != null) {
              FlamePair pair = new FlamePair(key, val);
              Iterable<String> results = lambda.op(pair);
              if (results != null) {
                for (String s : results) {
                  String rowKey = Hasher.hash(row.key() + counter.getAndIncrement());
                  kvs.put(outputTable, rowKey, "value", s);
                }
              }
            }
          }
          return null;
        }));
      }
      awaitAll(futures);
      return "OK";
    });

    // ---- PairRDD flatMapToPair ----
    post("/pairrdd/flatMapToPair", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");

      KVSClient kvs = new KVSClient(kvsCoordinator);
      FlamePairRDD.PairToPairIterable lambda = (FlamePairRDD.PairToPairIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

      Iterator<Row> iter = kvs.scan(inputTable, fromKey, toKeyExclusive);
      AtomicInteger counter = new AtomicInteger(0);
      List<Future<?>> futures = new ArrayList<>();
      while (iter.hasNext()) {
        final Row row = iter.next();
        futures.add(POOL.submit(() -> {
          String key = row.key();
          Set<String> columns = row.columns();
          for (String col : columns) {
            String val = row.get(col);
            if (val != null) {
              FlamePair pair = new FlamePair(key, val);
              Iterable<FlamePair> results = lambda.op(pair);
              if (results != null) {
                for (FlamePair p : results) {
                  String colName = Hasher.hash(row.key() + counter.getAndIncrement());
                  kvs.put(outputTable, p._1(), colName, p._2());
                }
              }
            }
          }
          return null;
        }));
      }
      awaitAll(futures);
      return "OK";
    });

    // ---- RDD distinct ----
    // Parallelized with POOL.submit because the multiset input from a
    // multiset-emitting flatMap (e.g. crawler frontier with many anchors per
    // page) can reach 500k+ rows. Single-threaded one-PUT-per-row scaled to
    // ~8 minutes wall time on that input; parallel keeps it in the seconds
    // range. Duplicate writes to Hasher.hash(value) are safe (same key, same
    // value, last-write-wins).
    post("/rdd/distinct", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");

      KVSClient kvs = new KVSClient(kvsCoordinator);

      Iterator<Row> iter = kvs.scan(inputTable, fromKey, toKeyExclusive);
      List<Future<?>> futures = new ArrayList<>();
      while (iter.hasNext()) {
        final Row row = iter.next();
        futures.add(POOL.submit(() -> {
          String value = row.get("value");
          if (value != null) {
            kvs.put(outputTable, Hasher.hash(value), "value", value);
          }
          return null;
        }));
      }
      awaitAll(futures);
      return "OK";
    });

    // ---- RDD filter (EC HW7) ----
    post("/rdd/filter", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");

      KVSClient kvs = new KVSClient(kvsCoordinator);
      FlameRDD.StringToBoolean lambda = (FlameRDD.StringToBoolean) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

      Iterator<Row> iter = kvs.scan(inputTable, fromKey, toKeyExclusive);
      List<Future<?>> futures = new ArrayList<>();
      while (iter.hasNext()) {
        final Row row = iter.next();
        futures.add(POOL.submit(() -> {
          String value = row.get("value");
          if (value != null && lambda.op(value)) {
            kvs.put(outputTable, row.key(), "value", value);
          }
          return null;
        }));
      }
      awaitAll(futures);
      return "OK";
    });

    // ---- RDD sample (EC HW7) ----
    post("/rdd/sample", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");
      double prob = Double.parseDouble(request.queryParams("prob"));

      KVSClient kvs = new KVSClient(kvsCoordinator);

      Iterator<Row> iter = kvs.scan(inputTable, fromKey, toKeyExclusive);
      List<Future<?>> futures = new ArrayList<>();
      while (iter.hasNext()) {
        final Row row = iter.next();
        futures.add(POOL.submit(() -> {
          if (Math.random() < prob) {
            String value = row.get("value");
            if (value != null) {
              kvs.put(outputTable, row.key(), "value", value);
            }
          }
          return null;
        }));
      }
      awaitAll(futures);
      return "OK";
    });

    // ---- PairRDD join ----
    post("/pairrdd/join", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");
      String otherTable = request.queryParams("extraArg");

      KVSClient kvs = new KVSClient(kvsCoordinator);

      Iterator<Row> iter = kvs.scan(inputTable, fromKey, toKeyExclusive);
      int seq = 0;
      while (iter.hasNext()) {
        Row rowA = iter.next();
        String key = rowA.key();
        Row rowB = kvs.getRow(otherTable, key);
        if (rowB != null) {
          Set<String> colsA = rowA.columns();
          Set<String> colsB = rowB.columns();
          for (String colA : colsA) {
            String valA = rowA.get(colA);
            if (valA != null) {
              for (String colB : colsB) {
                String valB = rowB.get(colB);
                if (valB != null) {
                  String combined = valA + "," + valB;
                  String colName = Hasher.hash(colA + "_" + colB + seq + System.nanoTime());
                  kvs.put(outputTable, key, colName, combined);
                  seq++;
                }
              }
            }
          }
        }
      }
      return "OK";
    });

    // ---- RDD fold ----
    post("/rdd/fold", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String kvsCoordinator = request.queryParams("kvsCoordinator");
      String fromKey = request.queryParams("fromKey");
      String toKeyExclusive = request.queryParams("toKeyExclusive");
      String zeroElement = request.queryParams("extraArg");

      KVSClient kvs = new KVSClient(kvsCoordinator);
      FlamePairRDD.TwoStringsToString lambda = (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

      String accumulator = zeroElement;
      Iterator<Row> iter = kvs.scan(inputTable, fromKey, toKeyExclusive);
      while (iter.hasNext()) {
        Row row = iter.next();
        String value = row.get("value");
        if (value != null) {
          accumulator = lambda.op(accumulator, value);
        }
      }
      return accumulator;
    });

	}
}
