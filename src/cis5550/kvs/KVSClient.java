package cis5550.kvs;

import java.util.*;
import java.net.*;
import java.io.*;
import cis5550.tools.HTTP;

public class KVSClient implements KVS {

  String coordinator;

  static class WorkerEntry implements Comparable<WorkerEntry> {
    String address;
    String id;

    WorkerEntry(String addressArg, String idArg) {
      address = addressArg;
      id = idArg;
    }

    public int compareTo(WorkerEntry e) {
      return id.compareTo(e.id);
    }
  };

  // volatile so readers see the latest published Vector reference. downloadWorkers
  // builds a fresh Vector and assigns once -- no in-place mutation, no torn reads.
  // Callers must capture via snapshot() and reuse the local reference for the entire
  // operation; never re-read this.workers between index lookup and elementAt.
  volatile Vector<WorkerEntry> workers;
  volatile boolean haveWorkers;

  public int numWorkers() throws IOException {
    return snapshot().size();
  }

  public static String getVersion() {
    return "v1.5";
  }

  public String getCoordinator() {
    return coordinator;
  }

  public String getWorkerAddress(int idx) throws IOException {
    return snapshot().elementAt(idx).address;
  }

  public String getWorkerID(int idx) throws IOException {
    return snapshot().elementAt(idx).id;
  }

  class KVSIterator implements Iterator<Row> {
    InputStream in;
    boolean atEnd;
    Row nextRow;
    int currentRangeIndex;
    String endRowExclusive;
    String startRow;
    String tableName;
    Vector<String> ranges;
    final Vector<WorkerEntry> iterSnap;

    KVSIterator(String tableNameArg, String startRowArg, String endRowExclusiveArg) throws IOException {
      in = null;
      currentRangeIndex = 0;
      atEnd = false;
      endRowExclusive = endRowExclusiveArg;
      tableName = tableNameArg;
      startRow = startRowArg;
      ranges = new Vector<String>();
      iterSnap = snapshot();
      int n = iterSnap.size();
      if ((startRowArg == null) || (startRowArg.compareTo(iterSnap.elementAt(0).id) < 0)) {
        String url = getURL(tableNameArg, n-1, startRowArg, ((endRowExclusiveArg != null) && (endRowExclusiveArg.compareTo(iterSnap.elementAt(0).id)<0)) ? endRowExclusiveArg : iterSnap.elementAt(0).id);
        ranges.add(url);
      }
      for (int i=0; i<n; i++) {
        if ((startRowArg == null) || (i == n-1) || (startRowArg.compareTo(iterSnap.elementAt(i+1).id)<0)) {
          if ((endRowExclusiveArg == null) || (endRowExclusiveArg.compareTo(iterSnap.elementAt(i).id) > 0)) {
            boolean useActualStartRow = (startRowArg != null) && (startRowArg.compareTo(iterSnap.elementAt(i).id)>0);
            boolean useActualEndRow = (endRowExclusiveArg != null) && ((i==(n-1)) || (endRowExclusiveArg.compareTo(iterSnap.elementAt(i+1).id)<0));
            String url = getURL(tableNameArg, i, useActualStartRow ? startRowArg : iterSnap.elementAt(i).id, useActualEndRow ? endRowExclusiveArg : ((i<n-1) ? iterSnap.elementAt(i+1).id : null));
            ranges.add(url);
          }
        }
      }

      openConnectionAndFill();
    }

    protected String getURL(String tableNameArg, int workerIndexArg, String startRowArg, String endRowExclusiveArg) throws IOException {
      String params = "";
      if (startRowArg != null)
        params = "startRow="+startRowArg;
      if (endRowExclusiveArg != null)
        params = (params.equals("") ? "" : (params+"&"))+"endRowExclusive="+endRowExclusiveArg;
      return "http://"+iterSnap.elementAt(workerIndexArg).address+"/data/"+tableNameArg+(params.equals("") ? "" : "?"+params);
    }

    void openConnectionAndFill() {
      try {
        if (in != null) {
          in.close();
          in = null;
        }

        if (atEnd)
          return;

        while (true) {
          if (currentRangeIndex >= ranges.size()) {
            atEnd = true;
            return;
          }

          try {
            URL url = new URI(ranges.elementAt(currentRangeIndex)).toURL();
            HttpURLConnection con = (HttpURLConnection)url.openConnection();
            con.setRequestMethod("GET");
            con.connect();
            in = con.getInputStream();
            Row r = fill();
            if (r != null) {
              nextRow = r;
              break;
            }
          } catch (FileNotFoundException fnfe) {
          } catch (URISyntaxException use) {
          }

          currentRangeIndex ++;
        }
      } catch (IOException ioe) {
        if (in != null) {
          try { in.close(); } catch (Exception e) {}
          in = null;
        }
        atEnd = true;
      }
    }

    synchronized Row fill() {
      try {
        Row r = Row.readFrom(in);
        return r;
      } catch (Exception e) {
        return null;
      }
    }

    public synchronized Row next() {
      if (atEnd)
        return null;
      Row r = nextRow;
      nextRow = fill();
      while ((nextRow == null) && !atEnd) {
        currentRangeIndex ++;
        openConnectionAndFill();
      }

      return r;
    }

    public synchronized boolean hasNext() {
      return !atEnd;
    }
  }

  synchronized void downloadWorkers() throws IOException {
    String result = new String(HTTP.doRequest("GET", "http://"+coordinator+"/workers", null).body());
    String[] pieces = result.split("\n");
    int numWorkers = Integer.parseInt(pieces[0]);
    if (numWorkers < 1)
      throw new IOException("No active KVS workers");
    if (pieces.length != (numWorkers+1))
      throw new RuntimeException("Received truncated response when asking KVS coordinator for list of workers");
    Vector<WorkerEntry> next = new Vector<WorkerEntry>(numWorkers);
    for (int i=0; i<numWorkers; i++) {
      String[] pcs = pieces[1+i].split(",");
      next.add(new WorkerEntry(pcs[1], pcs[0]));
    }
    Collections.sort(next);
    workers = next;
    haveWorkers = true;
  }

  // Stable snapshot of the worker list. The returned Vector is never mutated
  // post-publish; callers should reuse this reference for the entire operation
  // (both index lookup and elementAt) -- never re-read this.workers mid-call.
  Vector<WorkerEntry> snapshot() throws IOException {
    if (!haveWorkers)
      downloadWorkers();
    return workers;
  }

  static int workerIndexForKey(String key, Vector<WorkerEntry> snap) {
    int chosenWorker = snap.size()-1;
    if (key != null) {
      for (int i=0; i<snap.size()-1; i++) {
        if ((key.compareTo(snap.elementAt(i).id) >= 0) && (key.compareTo(snap.elementAt(i+1).id) < 0))
          chosenWorker = i;
      }
    }

    return chosenWorker;
  }

  public KVSClient(String coordinatorArg) {
    coordinator = coordinatorArg;
    workers = new Vector<WorkerEntry>();
    haveWorkers = false;
  }

  public boolean rename(String oldTableName, String newTableName) throws IOException {
    Vector<WorkerEntry> snap = snapshot();

    boolean result = true;
    for (WorkerEntry w : snap) {
      try {
        byte[] response = HTTP.doRequest("PUT", "http://"+w.address+"/rename/"+java.net.URLEncoder.encode(oldTableName, "UTF-8"), newTableName.getBytes()).body();
        String res = new String(response);
        result &= res.equals("OK");
      } catch (Exception e) {}
    }

    return result;
  }

  public boolean delete(String oldTableName) throws IOException {
    Vector<WorkerEntry> snap = snapshot();

    boolean allOk = true;
    IOException firstError = null;
    for (WorkerEntry w : snap) {
      try {
        HTTP.Response resp = HTTP.doRequest(
            "PUT",
            "http://" + w.address + "/delete/"
                + java.net.URLEncoder.encode(oldTableName, "UTF-8"),
            null);
        int code = resp.statusCode();
        // 200 = deleted, 404 = table absent on this worker (normal under
        // partitioning + replication); both are acceptable terminal states.
        if (code != 200 && code != 404) allOk = false;
      } catch (IOException ioe) {
        allOk = false;
        if (firstError == null) firstError = ioe;
      }
    }
    if (firstError != null) throw firstError;
    return allOk;
  }

  public void put(String tableName, String row, String column, byte value[]) throws IOException {
    Vector<WorkerEntry> snap = snapshot();

    try {
      String target = "http://"+snap.elementAt(workerIndexForKey(row, snap)).address+"/data/"+tableName+"/"+java.net.URLEncoder.encode(row, "UTF-8")+"/"+java.net.URLEncoder.encode(column, "UTF-8");
      byte[] response = HTTP.doRequest("PUT", target, value).body();
      String result = new String(response);
      if (!result.equals("OK"))
      	throw new RuntimeException("PUT returned something other than OK: "+result+ "("+target+")");
    } catch (UnsupportedEncodingException uee) {
      throw new RuntimeException("UTF-8 encoding not supported?!?");
    }
  }

  public void put(String tableName, String row, String column, String value) throws IOException {
    put(tableName, row, column,value.getBytes());
  }

  public void putRow(String tableName, Row row) throws FileNotFoundException, IOException {
    Vector<WorkerEntry> snap = snapshot();
    if (row.key().equals(""))
      throw new RuntimeException("Row key can't be empty!");

    byte[] response = HTTP.doRequest("PUT", "http://"+snap.elementAt(workerIndexForKey(row.key(), snap)).address+"/data/"+tableName, row.toByteArray()).body();
    String result = new String(response);
    if (!result.equals("OK"))
      throw new RuntimeException("PUT returned something other than OK: "+result);
  }

  public Row getRow(String tableName, String row) throws IOException {
    Vector<WorkerEntry> snap = snapshot();
    if (row.equals(""))
      throw new RuntimeException("Row key can't be empty!");

    HTTP.Response resp = HTTP.doRequest("GET", "http://"+snap.elementAt(workerIndexForKey(row, snap)).address+"/data/"+tableName+"/"+java.net.URLEncoder.encode(row, "UTF-8"), null);
    if (resp.statusCode() == 404)
      return null;

    byte[] result = resp.body();
    try {
      return Row.readFrom(new ByteArrayInputStream(result));
    } catch (Exception e) {
      throw new RuntimeException("Decoding error while reading Row '"+row+"' in table '"+tableName+"' from getRow() URL (encoded as '"+java.net.URLEncoder.encode(row, "UTF-8")+"')");
    }
  }

  public byte[] get(String tableName, String row, String column) throws IOException {
    Vector<WorkerEntry> snap = snapshot();
    if (row.equals(""))
      throw new RuntimeException("Row key can't be empty!");

    HTTP.Response res = HTTP.doRequest("GET", "http://"+snap.elementAt(workerIndexForKey(row, snap)).address+"/data/"+tableName+"/"+java.net.URLEncoder.encode(row, "UTF-8")+"/"+java.net.URLEncoder.encode(column, "UTF-8"), null);
    return ((res != null) && (res.statusCode() == 200)) ? res.body() : null;
  }

  public boolean existsRow(String tableName, String row) throws FileNotFoundException, IOException {
    Vector<WorkerEntry> snap = snapshot();

    // HEAD instead of GET: server returns Content-Length: 0 and never reads the
    // row file. Avoids shipping up to a 5 MB page body for an existence check
    // that the crawler does once per anchor (~50-100 per page).
    HTTP.Response r = HTTP.doRequest("HEAD", "http://"+snap.elementAt(workerIndexForKey(row, snap)).address+"/data/"+tableName+"/"+java.net.URLEncoder.encode(row, "UTF-8"), null);
    return r.statusCode() == 200;
  }

  public int count(String tableName) throws IOException {
    Vector<WorkerEntry> snap = snapshot();

    int total = 0;
    for (WorkerEntry w : snap) {
      HTTP.Response r = HTTP.doRequest("GET", "http://"+w.address+"/count/"+tableName, null);
      if ((r != null) && (r.statusCode() == 200)) {
        String result = new String(r.body());
        total += Integer.valueOf(result).intValue();
      }
    }
    return total;
  }

  public Iterator<Row> scan(String tableName) throws FileNotFoundException, IOException {
    return scan(tableName, null, null);
  }

  public Iterator<Row> scan(String tableName, String startRow, String endRowExclusive) throws FileNotFoundException, IOException {
    return new KVSIterator(tableName, startRow, endRowExclusive);
  }

  public static void main(String args[]) throws Exception {
  	if (args.length < 2) {
      System.err.println("Syntax: client <coordinator> get <tableName> <row> <column>");
  		System.err.println("Syntax: client <coordinator> put <tableName> <row> <column> <value>");
      System.err.println("Syntax: client <coordinator> scan <tableName>");
      System.err.println("Syntax: client <coordinator> count <tableName>");
      System.err.println("Syntax: client <coordinator> rename <oldTableName> <newTableName>");
      System.err.println("Syntax: client <coordinator> delete <tableName>");
  		System.exit(1);
  	}

  	KVSClient client = new KVSClient(args[0]);
    if (args[1].equals("put")) {
    	if (args.length != 6) {
	  		System.err.println("Syntax: client <coordinator> put <tableName> <row> <column> <value>");
	  		System.exit(1);
    	}
      client.put(args[2], args[3], args[4], args[5].getBytes("UTF-8"));
    } else if (args[1].equals("get")) {
      if (args.length != 5) {
        System.err.println("Syntax: client <coordinator> get <tableName> <row> <column>");
        System.exit(1);
      }
      byte[] val = client.get(args[2], args[3], args[4]);
      if (val == null)
        System.err.println("No value found");
      else
        System.out.write(val);
    } else if (args[1].equals("scan")) {
      if (args.length != 3) {
        System.err.println("Syntax: client <coordinator> scan <tableName>");
        System.exit(1);
      }

      Iterator<Row> iter = client.scan(args[2], null, null);
      int count = 0;
      while (iter.hasNext()) {
        System.out.println(iter.next());
        count ++;
      }
      System.err.println(count+" row(s) scanned");
    } else if (args[1].equals("count")) {
      if (args.length != 3) {
        System.err.println("Syntax: client <coordinator> count <tableName>");
        System.exit(1);
      }

      System.out.println(client.count(args[2])+" row(s) in table '"+args[2]+"'");
    } else if (args[1].equals("delete")) {
      if (args.length != 3) {
        System.err.println("Syntax: client <coordinator> delete <tableName>");
        System.exit(1);
      }

      if (client.delete(args[2]))
        System.err.println("Table '"+args[2]+"' deleted");
      else
        System.err.println("Table '"+args[2]+"' delete reported partial failure");
    } else if (args[1].equals("rename")) {
      if (args.length != 4) {
        System.err.println("Syntax: client <coordinator> rename <oldTableName> <newTableName>");
        System.exit(1);
      }
      if (client.rename(args[2], args[3]))
        System.out.println("Success");
      else
        System.out.println("Failure");
    } else {
    	System.err.println("Unknown command: "+args[1]);
    	System.exit(1);
    }
  }
};
