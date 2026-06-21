package cis5550.generic;

import java.util.*;
import java.util.concurrent.*;
import cis5550.webserver.Server;

public class Coordinator {

  static final long EXPIRATION_MS = 15000;

  static class WorkerEntry {
    String ip;
    int port;
    long lastPing;

    WorkerEntry(String ip, int port) {
      this.ip = ip;
      this.port = port;
      this.lastPing = System.currentTimeMillis();
    }
  }

  static Map<String, WorkerEntry> workerMap = new ConcurrentHashMap<>();

  public static Vector<String> getWorkers() {
    long now = System.currentTimeMillis();
    Vector<String> result = new Vector<>();
    for (Map.Entry<String, WorkerEntry> e : workerMap.entrySet()) {
      if (now - e.getValue().lastPing < EXPIRATION_MS)
        result.add(e.getKey() + "," + e.getValue().ip + ":" + e.getValue().port);
    }
    Collections.sort(result);
    return result;
  }

  public static String clientTable() {
    return workerTable();
  }

  public static String workerTable() {
    List<String> workers = getWorkers();
    StringBuilder sb = new StringBuilder();
    sb.append("<table border=\"1\"><tr><th>ID</th><th>Address</th></tr>");
    for (String w : workers) {
      String[] parts = w.split(",");
      String id = parts[0];
      String addr = parts[1];
      sb.append("<tr><td><a href=\"http://").append(addr).append("/\">").append(id).append("</a></td><td>").append(addr).append("</td></tr>");
    }
    sb.append("</table>");
    return sb.toString();
  }

  public static void registerRoutes() {
    Server.get("/ping", (req, res) -> {
      String id = req.queryParams("id");
      String portStr = req.queryParams("port");
      if (id == null || portStr == null) {
        res.status(400, "Bad Request");
        return "Missing id or port";
      }
      int port = Integer.parseInt(portStr);
      workerMap.put(id, new WorkerEntry(req.ip(), port));
      return "OK";
    });

    Server.get("/workers", (req, res) -> {
      List<String> workers = getWorkers();
      StringBuilder sb = new StringBuilder();
      sb.append(workers.size()).append("\n");
      for (String w : workers)
        sb.append(w).append("\n");
      return sb.toString();
    });
  }
}
