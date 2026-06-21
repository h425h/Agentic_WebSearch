package cis5550.generic;

import java.net.URI;

public class Worker {

  protected static int port;
  protected static String coordinatorAddr;
  protected static String id;

  public static void startPingThread() {
    Thread t = new Thread(() -> {
      while (true) {
        try {
          Thread.sleep(5000);
          URI.create("http://" + coordinatorAddr + "/ping?id=" + id + "&port=" + port)
             .toURL()
             .openStream()
             .close();
        } catch (Exception e) {
          // ignore transient ping failures
        }
      }
    });
    t.setDaemon(false);
    t.start();
  }

  public static void startPingThread(String coordinatorAddr, String id, int port) {
    Worker.coordinatorAddr = coordinatorAddr;
    Worker.id = id;
    Worker.port = port;
    startPingThread();
  }
}
