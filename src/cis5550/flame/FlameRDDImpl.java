package cis5550.flame;

import cis5550.flame.FlameRDD.IteratorToIterator;
import cis5550.flame.FlameRDD.StringToBoolean;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.flame.FlameRDD.StringToPairIterable;
import cis5550.flame.FlameRDD.StringToString;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;
import java.util.*;

public class FlameRDDImpl implements FlameRDD {

  String tableName;
  FlameContextImpl ctx;

  public FlameRDDImpl(String tableNameArg, FlameContextImpl ctxArg) {
    tableName = tableNameArg;
    ctx = ctxArg;
  }

  public String getTableName() {
    return tableName;
  }

  public int count() throws Exception {
    KVSClient kvs = ctx.getKVS();
    return kvs.count(tableName);
  }

  public void saveAsTable(String tableNameArg) throws Exception {
    KVSClient kvs = ctx.getKVS();
    kvs.rename(tableName, tableNameArg);
    tableName = tableNameArg;
  }

  public FlameRDD distinct() throws Exception {
    byte[] serializedLambda = Serializer.objectToByteArray((StringToIterable) s -> Collections.singletonList(s));
    String outputTable = ctx.invokeOperation("/rdd/distinct", serializedLambda, tableName, null);
    return new FlameRDDImpl(outputTable, ctx);
  }

  public void destroy() throws Exception {
    KVSClient kvs = ctx.getKVS();
    try {
      if (!kvs.delete(tableName)) {
        System.err.println("destroy: incomplete cleanup of table '" + tableName + "'");
      }
    } catch (java.io.IOException ioe) {
      System.err.println("destroy: cleanup failed for table '" + tableName + "': " + ioe.getMessage());
    }
    tableName = null;
  }

  public Vector<String> take(int num) throws Exception {
    KVSClient kvs = ctx.getKVS();
    Vector<String> result = new Vector<>();
    Iterator<Row> iter = kvs.scan(tableName);
    while (iter.hasNext() && result.size() < num) {
      Row row = iter.next();
      String val = row.get("value");
      if (val != null) {
        result.add(val);
      }
    }
    return result;
  }

  public String fold(String zeroElement, FlamePairRDD.TwoStringsToString lambda) throws Exception {
    byte[] serializedLambda = Serializer.objectToByteArray(lambda);
    String[] workerResults = ctx.invokeOperationCollectResults("/rdd/fold", serializedLambda, tableName, zeroElement);
    String accumulator = zeroElement;
    for (String result : workerResults) {
      accumulator = lambda.op(accumulator, result);
    }
    return accumulator;
  }

  public List<String> collect() throws Exception {
    KVSClient kvs = ctx.getKVS();
    List<String> result = new ArrayList<>();
    Iterator<Row> iter = kvs.scan(tableName);
    while (iter.hasNext()) {
      Row row = iter.next();
      String val = row.get("value");
      if (val != null) {
        result.add(val);
      }
    }
    return result;
  }

  public FlameRDD flatMap(StringToIterable lambda) throws Exception {
    byte[] serializedLambda = Serializer.objectToByteArray(lambda);
    String outputTable = ctx.invokeOperation("/rdd/flatMap", serializedLambda, tableName, null);
    return new FlameRDDImpl(outputTable, ctx);
  }

  public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
    byte[] serializedLambda = Serializer.objectToByteArray(lambda);
    String outputTable = ctx.invokeOperation("/rdd/flatMapToPair", serializedLambda, tableName, null);
    return new FlamePairRDDImpl(outputTable, ctx);
  }

  public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
    byte[] serializedLambda = Serializer.objectToByteArray(lambda);
    String outputTable = ctx.invokeOperation("/rdd/mapToPair", serializedLambda, tableName, null);
    return new FlamePairRDDImpl(outputTable, ctx);
  }

  // EC from HW7
  public FlameRDD intersection(FlameRDD r) throws Exception {
      FlameRDDImpl other = (FlameRDDImpl) r;
      byte[] lambda1 = Serializer.objectToByteArray((StringToPair) (s -> new FlamePair(s, "1")));
      String table1 = ctx.invokeOperation("/rdd/mapToPair", lambda1, tableName, null);
      String table2 = ctx.invokeOperation("/rdd/mapToPair", lambda1, other.tableName, null);
      String outputTable = ctx.invokeOperation("/rdd/intersection?other=" + java.net.URLEncoder.encode(table2, "UTF-8"), new byte[0], table1, null);
      return new FlameRDDImpl(outputTable, ctx);
  }

  // EC from HW7
  public FlameRDD sample(double f) throws Exception {
      String outputTable = ctx.invokeOperation("/rdd/sample?prob=" + f, new byte[0], tableName, null);
      return new FlameRDDImpl(outputTable, ctx);
  }

  // EC from HW7
  public FlamePairRDD groupBy(StringToString lambda) throws Exception {
      byte[] serialized = Serializer.objectToByteArray(lambda);
      String intermediate = ctx.invokeOperation("/rdd/groupBy", serialized, tableName, null);
      FlamePairRDDImpl temp = new FlamePairRDDImpl(intermediate, ctx);
      return temp.foldByKey("", (a, b) -> a.isEmpty() ? b : a + "," + b);
  }

  // EC HW7
  public FlameRDD filter(StringToBoolean lambda) throws Exception {
      byte[] serialized = Serializer.objectToByteArray(lambda);
      String outputTable = ctx.invokeOperation("/rdd/filter", serialized, tableName, null);
      return new FlameRDDImpl(outputTable, ctx);
  }

  public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
      byte[] serialized = Serializer.objectToByteArray(lambda);
      String outputTable = ctx.invokeOperation("/rdd/mapPartitions", serialized, tableName, null);
      return new FlameRDDImpl(outputTable, ctx);
  }
}