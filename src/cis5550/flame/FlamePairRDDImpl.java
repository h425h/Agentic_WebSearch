package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;
import java.util.*;

public class FlamePairRDDImpl implements FlamePairRDD {

  String tableName;
  FlameContextImpl ctx;

  public FlamePairRDDImpl(String tableNameArg, FlameContextImpl ctxArg) {
    tableName = tableNameArg;
    ctx = ctxArg;
  }

  public String getTableName() {
    return tableName;
  }

  public List<FlamePair> collect() throws Exception {
    KVSClient kvs = ctx.getKVS();
    List<FlamePair> result = new ArrayList<>();
    Iterator<Row> iter = kvs.scan(tableName);
    while (iter.hasNext()) {
      Row row = iter.next();
      String key = row.key();
      Set<String> columns = row.columns();
      for (String col : columns) {
        String val = row.get(col);
        if (val != null) {
          result.add(new FlamePair(key, val));
        }
      }
    }
    return result;
  }

  public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
    byte[] serializedLambda = Serializer.objectToByteArray(lambda);
    String outputTable = ctx.invokeOperation("/pairrdd/foldByKey", serializedLambda, tableName, zeroElement);
    return new FlamePairRDDImpl(outputTable, ctx);
  }

  public void saveAsTable(String tableNameArg) throws Exception {
    KVSClient kvs = ctx.getKVS();
    boolean renamed = kvs.rename(tableName, tableNameArg);
    if (!renamed) {
      Iterator<Row> iter = kvs.scan(tableName, null, null);
      while (iter.hasNext()) {
        kvs.putRow(tableNameArg, iter.next());
      }
    }
    tableName = tableNameArg;
  }

  public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
    byte[] serializedLambda = Serializer.objectToByteArray(lambda);
    String outputTable = ctx.invokeOperation("/pairrdd/flatMap", serializedLambda, tableName, null);
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

  public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
    byte[] serializedLambda = Serializer.objectToByteArray(lambda);
    String outputTable = ctx.invokeOperation("/pairrdd/flatMapToPair", serializedLambda, tableName, null);
    return new FlamePairRDDImpl(outputTable, ctx);
  }

  public FlamePairRDD join(FlamePairRDD other) throws Exception {
    FlamePairRDDImpl otherImpl = (FlamePairRDDImpl) other;
    byte[] noLambda = new byte[0];
    String outputTable = ctx.invokeOperation("/pairrdd/join", noLambda, tableName, otherImpl.getTableName());
    return new FlamePairRDDImpl(outputTable, ctx);
  }

  // EC HW7
  public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
      FlamePairRDDImpl o = (FlamePairRDDImpl) other;
      String outputTable = ctx.invokeOperation("/pairrdd/cogroup?other=" + java.net.URLEncoder.encode(o.tableName, "UTF-8"),
          new byte[0], tableName, null);
      return new FlamePairRDDImpl(outputTable, ctx);
  }
}
