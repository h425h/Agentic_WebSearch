package cis5550.kvs;

import cis5550.webserver.Server;

public class Coordinator extends cis5550.generic.Coordinator {

  public static void registerRoutes() {
    cis5550.generic.Coordinator.registerRoutes();
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: Coordinator <port>");
      System.exit(1);
    }

    int port = Integer.parseInt(args[0]);
    Server.port(port);

    registerRoutes();

    Server.get("/", (req, res) -> {
      res.type("text/html");
      return "<html><head><title>KVS Coordinator</title></head><body>"
           + "<h1>KVS Coordinator</h1>"
           + workerTable()
           + "</body></html>";
    });
  }
}
