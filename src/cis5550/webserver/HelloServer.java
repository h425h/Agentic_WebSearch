package cis5550.webserver;

import static cis5550.webserver.Server.*;

public class HelloServer {
    public static void main(String[] args) {
        securePort(443);
        get("/", (req, res) -> { return "Hello World - this is Hashem Awad"; });
    }
}
