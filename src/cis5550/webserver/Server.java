package cis5550.webserver;

import cis5550.tools.Logger;
import cis5550.tools.SNIInspector;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;


public class Server {

    private static final Logger logger = Logger.getLogger(Server.class);
    private static final int BUFFER_SIZE = 1024;
    // Tunable via -Dwebserver.numWorkers; clamped to [1, 4096] to survive typos.
    private static final int NUM_WORKERS = clampPoolSize(
            Integer.getInteger("webserver.numWorkers", 100));

    private static int clampPoolSize(int n) {
        if (n < 1) return 1;
        if (n > 4096) return 4096;
        return n;
    }

    private static Server serverInstance = null;
    private static boolean running = false;

    private int portNumber = 80;
    private int securePortNumber = -1;
    private String staticFilesDir = null;

    private static String currentHost = null;
    private Map<String, String> hostStaticDirs = new HashMap<>();
    private java.util.Set<String> knownHosts = new java.util.HashSet<>();
    private Map<String, String[]> hostKeystoreInfo = new HashMap<>();

    private static class RouteEntry {
        String method;
        String pathPattern;
        Route handler;
        String host;

        RouteEntry(String method, String pathPattern, Route handler, String host) {
            this.method = method;
            this.pathPattern = pathPattern;
            this.handler = handler;
            this.host = host;
        }
    }

    private List<RouteEntry> routes = new ArrayList<>();
    private List<Route> beforeFilters = new ArrayList<>();
    private List<Route> afterFilters = new ArrayList<>();

    private Map<String, SessionImpl> sessions = new ConcurrentHashMap<>();

    private static final String SESSION_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    private static final int SESSION_ID_LENGTH = 20;

    String generateSessionId() {
        Random rand = new Random();
        StringBuilder sb = new StringBuilder(SESSION_ID_LENGTH);
        for (int i = 0; i < SESSION_ID_LENGTH; i++) {
            sb.append(SESSION_CHARS.charAt(rand.nextInt(SESSION_CHARS.length())));
        }
        return sb.toString();
    }

    SessionImpl getSession(String id) {
        if (id == null) return null;
        SessionImpl session = sessions.get(id);
        if (session == null) return null;
        if (!session.isValid()) {
            sessions.remove(id);
            return null;
        }
        long now = System.currentTimeMillis();
        if (session.getMaxActiveInterval() > 0 &&
            (now - session.lastAccessedTime()) > session.getMaxActiveInterval() * 1000L) {
            sessions.remove(id);
            return null;
        }
        session.setLastAccessedTime(now);
        return session;
    }

    SessionImpl createSession() {
        String id = generateSessionId();
        SessionImpl session = new SessionImpl(id);
        sessions.put(id, session);
        return session;
    }

    public static class staticFiles {
        public static void location(String s) {
            if (serverInstance == null) {
                serverInstance = new Server();
            }
            if (currentHost != null) {
                serverInstance.hostStaticDirs.put(currentHost, s);
            } else {
                serverInstance.staticFilesDir = s;
            }
            if (!running) {
                running = true;
                new Thread(() -> serverInstance.run()).start();
            }
        }
    }

    public static void host(String h) {
        if (serverInstance == null) {
            serverInstance = new Server();
        }
        currentHost = h;
        serverInstance.knownHosts.add(h);
    }

    public static void host(String h, String keystoreFile, String password) {
        if (serverInstance == null) {
            serverInstance = new Server();
        }
        currentHost = h;
        serverInstance.knownHosts.add(h);
        serverInstance.hostKeystoreInfo.put(h, new String[]{keystoreFile, password});
    }

    public static void get(String path, Route route) {
        if (serverInstance == null) {
            serverInstance = new Server();
        }
        serverInstance.routes.add(new RouteEntry("GET", path, route, currentHost));
        if (currentHost != null) serverInstance.knownHosts.add(currentHost);
        if (!running) {
            running = true;
            new Thread(() -> serverInstance.run()).start();
        }
    }

    public static void post(String path, Route route) {
        if (serverInstance == null) {
            serverInstance = new Server();
        }
        serverInstance.routes.add(new RouteEntry("POST", path, route, currentHost));
        if (currentHost != null) serverInstance.knownHosts.add(currentHost);
        if (!running) {
            running = true;
            new Thread(() -> serverInstance.run()).start();
        }
    }

    public static void put(String path, Route route) {
        if (serverInstance == null) {
            serverInstance = new Server();
        }
        serverInstance.routes.add(new RouteEntry("PUT", path, route, currentHost));
        if (currentHost != null) serverInstance.knownHosts.add(currentHost);
        if (!running) {
            running = true;
            new Thread(() -> serverInstance.run()).start();
        }
    }

    public static void head(String path, Route route) {
        if (serverInstance == null) {
            serverInstance = new Server();
        }
        serverInstance.routes.add(new RouteEntry("HEAD", path, route, currentHost));
        if (currentHost != null) serverInstance.knownHosts.add(currentHost);
        if (!running) {
            running = true;
            new Thread(() -> serverInstance.run()).start();
        }
    }

    public static void port(int port) {
        if (serverInstance == null) {
            serverInstance = new Server();
        }
        serverInstance.portNumber = port;
    }

    public static void securePort(int port) {
        if (serverInstance == null) {
            serverInstance = new Server();
        }
        serverInstance.securePortNumber = port;
    }

    public static void before(Route filter) {
        if (serverInstance == null) {
            serverInstance = new Server();
        }
        serverInstance.beforeFilters.add(filter);
    }

    public static void after(Route filter) {
        if (serverInstance == null) {
            serverInstance = new Server();
        }
        serverInstance.afterFilters.add(filter);
    }

    public static void main(String[] args) {
        System.out.println("Written by Hashem Awad");
    }

    public void run() {
        startServer(portNumber, staticFilesDir);
    }

    private void startServer(int port, String dir) {
        BlockingQueue<Socket> connectionQueue = new LinkedBlockingQueue<>();

        for (int i = 0; i < NUM_WORKERS; i++) {
            Thread worker = new Thread(() -> {
                while (true) {
                    try {
                        Socket sock = connectionQueue.take();
                        handleConnection(sock, dir);
                    } catch (InterruptedException e) {
                        logger.error("Worker thread interrupted", e);
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });
            worker.start();
        }
        logger.info("Started " + NUM_WORKERS + " worker threads");

        // Start session expiration thread
        Thread sessionCleaner = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                    long now = System.currentTimeMillis();
                    sessions.entrySet().removeIf(entry -> {
                        SessionImpl s = entry.getValue();
                        if (!s.isValid()) return true;
                        if (s.getMaxActiveInterval() > 0 &&
                            (now - s.lastAccessedTime()) > s.getMaxActiveInterval() * 1000L) {
                            return true;
                        }
                        return false;
                    });
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        sessionCleaner.setDaemon(true);
        sessionCleaner.start();

        // Start HTTPS listener if securePort was configured
        if (securePortNumber > 0) {
            // Build default SSLContext from keystore.jks
            SSLContext defaultSSLContext = null;
            try {
                defaultSSLContext = buildSSLContext("keystore.jks", "secret");
            } catch (Exception e) {
                logger.error("Could not load default keystore", e);
            }

            // Build per-host SSLContexts from host-specific keystores
            Map<String, SSLContext> hostSSLContexts = new HashMap<>();
            for (Map.Entry<String, String[]> entry : hostKeystoreInfo.entrySet()) {
                try {
                    SSLContext ctx = buildSSLContext(entry.getValue()[0], entry.getValue()[1]);
                    hostSSLContexts.put(entry.getKey(), ctx);
                } catch (Exception e) {
                    logger.error("Could not load keystore for host " + entry.getKey(), e);
                }
            }

            final SSLContext finalDefaultSSLContext = defaultSSLContext;
            Thread tlsThread = new Thread(() -> {
                try {
                    ServerSocket plainServerSocket = new ServerSocket(securePortNumber);
                    logger.info("HTTPS server started on port " + securePortNumber);

                    while (true) {
                        Socket rawSocket = plainServerSocket.accept();
                        logger.info("Accepted TLS connection from " + rawSocket.getRemoteSocketAddress());

                        // Process SNI in a separate thread to avoid blocking acceptance
                        Thread sniThread = new Thread(() -> {
                            try {
                                SNIInspector inspector = new SNIInspector();
                                inspector.parseConnection(rawSocket);

                                String sniHostname = null;
                                if (inspector.getHostName() != null) {
                                    sniHostname = inspector.getHostName().getAsciiName();
                                }

                                // Pick the right SSLContext: host-specific if available, otherwise default
                                SSLContext ctx = finalDefaultSSLContext;
                                if (sniHostname != null && hostSSLContexts.containsKey(sniHostname)) {
                                    ctx = hostSSLContexts.get(sniHostname);
                                }

                                if (ctx == null) {
                                    logger.error("No SSL context available for connection");
                                    rawSocket.close();
                                    return;
                                }

                                SSLSocket sslSocket = (SSLSocket) ctx.getSocketFactory().createSocket(
                                    rawSocket, inspector.getInputStream(), true);
                                sslSocket.setUseClientMode(false);
                                sslSocket.startHandshake();

                                connectionQueue.put(sslSocket);
                            } catch (Exception e) {
                                logger.error("Error processing SNI for TLS connection", e);
                                try { rawSocket.close(); } catch (IOException ex) { }
                            }
                        });
                        sniThread.start();
                    }
                } catch (Exception e) {
                    logger.error("Could not start TLS server on port " + securePortNumber, e);
                }
            });
            tlsThread.start();
        }

        // Start HTTP listener
        try (ServerSocket ssock = new ServerSocket(port)) {
            logger.info("Server started on port " + port + " with root directory " + (dir != null ? dir : "(none)"));

            while (true) {
                Socket sock = ssock.accept();
                logger.info("Accepted connection from " + sock.getRemoteSocketAddress());
                connectionQueue.put(sock);
            }
        } catch (IOException e) {
            logger.error("Could not start server on port " + port, e);
        } catch (InterruptedException e) {
            logger.error("Accept thread interrupted", e);
            Thread.currentThread().interrupt();
        }
    }

    private void handleConnection(Socket sock, String dir) {
        try {
            InputStream in = sock.getInputStream();
            OutputStream out = sock.getOutputStream();
            PrintWriter pw = new PrintWriter(out);
            byte[] leftover = new byte[0];

            while (true) {
                byte[] headerBytes = readUntilDoubleCRLF(in, leftover);
                if (headerBytes == null) {
                    logger.info("Connection closed before full header was read");
                    break;
                }

                int headerEnd = findDoubleCRLF(headerBytes);
                int alreadyRead = headerBytes.length - headerEnd;

                BufferedReader br = new BufferedReader(
                    new InputStreamReader(new ByteArrayInputStream(headerBytes)));
                String requestLine = br.readLine();

                if (requestLine == null) {
                    sendErrorResponse(pw, 400, "Bad Request");
                    continue;
                }

                String[] requestParts = requestLine.split(" ");
                if (requestParts.length != 3) {
                    sendErrorResponse(pw, 400, "Bad Request");
                    continue;
                }

                String method = requestParts[0];
                String url = requestParts[1];
                String version = requestParts[2];

                String path = url;
                if (url.contains("?")) {
                    path = url.substring(0, url.indexOf("?"));
                }

                Map<String, String> headers = parseHeaders(br);

                if (!headers.containsKey("host")) {
                    sendErrorResponse(pw, 400, "Bad Request");
                    continue;
                }
                if (!version.equals("HTTP/1.1")) {
                    sendErrorResponse(pw, 505, "HTTP Version Not Supported");
                    continue;
                }

                int contentLength = getContentLength(headers);
                byte[] bodyBytes = readBody(in, headerBytes, headerEnd, alreadyRead, contentLength);
                leftover = getLeftoverBytes(headerBytes, headerEnd, alreadyRead, contentLength);

                String hostHeader = headers.get("host");
                String effectiveHost = resolveHost(hostHeader);

                String effectiveDir;
                if (effectiveHost != null) {
                    effectiveDir = hostStaticDirs.get(effectiveHost);
                } else {
                    effectiveDir = dir;
                }

                RouteEntry matchedRoute = findMatchingRoute(method, path, effectiveHost);

                if (matchedRoute != null) {
                    boolean keepAlive = handleDynamicRequest(matchedRoute, method, path, url, version, headers, bodyBytes, sock, pw, out);
                    if (!keepAlive) {
                        break;
                    }
                } else {
                    if (method.equals("POST") || method.equals("PUT")) {
                        sendErrorResponse(pw, 405, "Method Not Allowed");
                        continue;
                    }
                    if (!method.equals("GET") && !method.equals("HEAD")) {
                        sendErrorResponse(pw, 501, "Not Implemented");
                        continue;
                    }
                    processRequest(method, path, effectiveDir, headers, pw, out);
                }
            }
        } catch (IOException e) {
            logger.error("Error handling connection", e);
        } finally {
            closeSocket(sock);
        }
    }

    private String resolveHost(String hostHeader) {
        if (hostHeader == null) return null;
        String host = hostHeader;
        int colonIdx = host.indexOf(':');
        if (colonIdx >= 0) {
            host = host.substring(0, colonIdx);
        }

        if (knownHosts.contains(host)) {
            return host;
        }
        return null;
    }

    private RouteEntry findMatchingRoute(String method, String path, String effectiveHost) {
        for (RouteEntry route : routes) {
            if (route.method.equals(method) && matchPath(route.pathPattern, path) != null) {
                if ((route.host == null && effectiveHost == null) ||
                    (route.host != null && route.host.equals(effectiveHost))) {
                    return route;
                }
            }
        }
        return null;
    }

    private static Map<String, String> matchPath(String pattern, String path) {
        String[] patternParts = pattern.split("/", -1);
        String[] pathParts = path.split("/", -1);

        if (patternParts.length != pathParts.length) {
            return null;
        }

        Map<String, String> params = new HashMap<>();
        for (int i = 0; i < patternParts.length; i++) {
            if (patternParts[i].startsWith(":")) {
                params.put(patternParts[i].substring(1), pathParts[i]);
            } else if (!patternParts[i].equals(pathParts[i])) {
                return null;
            }
        }
        return params;
    }

    private static Map<String, String> parseQueryParameters(String url, Map<String, String> headers, byte[] bodyBytes) {
        Map<String, String> queryParams = new HashMap<>();

        if (url.contains("?")) {
            String queryString = url.substring(url.indexOf("?") + 1);
            addQueryParams(queryString, queryParams);
        }

        String contentType = headers.get("content-type");
        if (contentType != null && contentType.equals("application/x-www-form-urlencoded") && bodyBytes != null && bodyBytes.length > 0) {
            String bodyString = new String(bodyBytes, StandardCharsets.UTF_8);
            addQueryParams(bodyString, queryParams);
        }

        return queryParams;
    }

    private static void addQueryParams(String queryString, Map<String, String> params) {
        if (queryString == null || queryString.isEmpty()) return;
        String[] pairs = queryString.split("&");
        for (String pair : pairs) {
            int eqIdx = pair.indexOf('=');
            String key, value;
            if (eqIdx >= 0) {
                key = URLDecoder.decode(pair.substring(0, eqIdx), StandardCharsets.UTF_8);
                value = URLDecoder.decode(pair.substring(eqIdx + 1), StandardCharsets.UTF_8);
            } else {
                key = URLDecoder.decode(pair, StandardCharsets.UTF_8);
                value = "";
            }
            if (params.containsKey(key)) {
                params.put(key, params.get(key) + "," + value);
            } else {
                params.put(key, value);
            }
        }
    }

    private boolean handleDynamicRequest(RouteEntry route, String method, String path, String url,
            String version, Map<String, String> headers, byte[] bodyBytes,
            Socket sock, PrintWriter pw, OutputStream out) {
        try {
            InetSocketAddress remoteAddr = (InetSocketAddress) sock.getRemoteSocketAddress();
            Map<String, String> params = matchPath(route.pathPattern, path);

            Map<String, String> queryParams = parseQueryParameters(url, headers, bodyBytes);

            // Parse cookies to find existing session
            String sessionIdFromCookie = parseCookieSessionId(headers);
            SessionImpl existingSession = getSession(sessionIdFromCookie);

            RequestImpl request = new RequestImpl(method, path, version, headers, queryParams, params, remoteAddr, bodyBytes, this);
            if (existingSession != null) {
                request.setSession(existingSession);
            }
            ResponseImpl response = new ResponseImpl();
            response.setOutputStream(out);

            for (Route filter : beforeFilters) {
                try {
                    filter.handle(request, response);
                } catch (Exception e) {
                    logger.error("Exception in before filter", e);
                }
                if (response.isHalted()) {
                    sendErrorResponse(pw, response.getHaltStatusCode(), response.getHaltReasonPhrase());
                    return true;
                }
            }

            Object result = null;
            try {
                result = route.handler.handle(request, response);
            } catch (Exception e) {
                logger.error("Exception in route handler", e);
                if (response.isWriteCommitted()) {
                    return false;
                }
                sendErrorResponse(pw, 500, "Internal Server Error");
                return true;
            }

            for (Route filter : afterFilters) {
                try {
                    filter.handle(request, response);
                } catch (Exception e) {
                    logger.error("Exception in after filter", e);
                }
            }

            if (response.isWriteCommitted()) {
                return false;
            }

            byte[] responseBody = null;
            if (result != null) {
                responseBody = result.toString().getBytes();
            } else if (response.getBody() != null) {
                responseBody = response.getBody();
            }

            pw.print("HTTP/1.1 " + response.getStatusCode() + " " + response.getReasonPhrase() + "\r\n");
            pw.print("Content-Type: " + response.getContentType() + "\r\n");
            if (responseBody != null) {
                pw.print("Content-Length: " + responseBody.length + "\r\n");
            } else {
                pw.print("Content-Length: 0\r\n");
            }
            for (String[] header : response.getHeaders()) {
                pw.print(header[0] + ": " + header[1] + "\r\n");
            }
            // Send Set-Cookie header if a new session was created during this request
            if (request.isNewSession()) {
                StringBuilder cookie = new StringBuilder();
                cookie.append("Set-Cookie: SessionID=").append(request.getSessionImpl().id());
                cookie.append("; HttpOnly");
                cookie.append("; SameSite=Strict");
                if (sock instanceof SSLSocket) {
                    cookie.append("; Secure");
                }
                cookie.append("\r\n");
                pw.print(cookie.toString());
            }
            pw.print("Server: CIS5550\r\n");
            pw.print("\r\n");
            pw.flush();

            if (responseBody != null) {
                out.write(responseBody);
                out.flush();
            }
            return true;
        } catch (IOException e) {
            logger.error("Error handling dynamic request", e);
            return false;
        }
    }

    private static byte[] readBody(InputStream in, byte[] headerBytes,
            int headerEnd, int alreadyRead, int contentLength) throws IOException {
        
        byte[] body = new byte[contentLength];

        if (alreadyRead > 0 && contentLength > 0) {
            int copyLen = Math.min(alreadyRead, contentLength);
            System.arraycopy(headerBytes, headerEnd, body, 0, copyLen);
        }

        int remaining = contentLength - alreadyRead;
        int offset = Math.max(0, Math.min(alreadyRead, contentLength));

        while (remaining > 0) {
            int r = in.read(body, offset, remaining);
            if (r == -1) break;
            offset += r;
            remaining -= r;
        }

        return body;
    }

    private static byte[] getLeftoverBytes(byte[] headerBytes, int headerEnd, int alreadyRead, int contentLength) {
        if (alreadyRead > contentLength) {
            int leftoverLen = alreadyRead - contentLength;
            byte[] leftover = new byte[leftoverLen];
            System.arraycopy(headerBytes, headerEnd + contentLength, leftover, 0, leftoverLen);
            return leftover;
        }
        return new byte[0];
    }


    private static byte[] readUntilDoubleCRLF(InputStream in, byte[] leftover) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[BUFFER_SIZE];

        if (leftover.length > 0) {
            baos.write(leftover);
        }

        byte[] data = baos.toByteArray();
        if (findDoubleCRLF(data) != -1) {
            return data;
        }

        int n;
        while ((n = in.read(buffer)) != -1) {
            baos.write(buffer, 0, n);
            data = baos.toByteArray();
            if (findDoubleCRLF(data) != -1) {
                return data;
            }
        }
        return null;
    }

    private static int findDoubleCRLF(byte[] data) {
        for (int i = 0; i <= data.length - 4; i++) {
            if (data[i] == '\r' && data[i + 1] == '\n' &&
                data[i + 2] == '\r' && data[i + 3] == '\n') {
                return i + 4;
            }
        }
        return -1;
    }

    private static String parseCookieSessionId(Map<String, String> headers) {
        String cookieHeader = headers.get("cookie");
        if (cookieHeader == null) return null;
        String[] cookies = cookieHeader.split(";");
        for (String cookie : cookies) {
            String trimmed = cookie.trim();
            if (trimmed.startsWith("SessionID=")) {
                return trimmed.substring("SessionID=".length());
            }
        }
        return null;
    }

    private static Map<String, String> parseHeaders(BufferedReader br) throws IOException {
        Map<String, String> headers = new HashMap<>();
        String line;
        while ((line = br.readLine()) != null && !line.isEmpty()) {
            String[] parts = line.split(":", 2);
            if (parts.length == 2) {
                headers.put(parts[0].trim().toLowerCase(), parts[1].trim());
            }
        }
        return headers;
    }

    private static int getContentLength(Map<String, String> headers) {
        String value = headers.get("content-length");
        if (value == null) return 0;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static void processRequest(String method, String path, String dir,
            Map<String, String> headers, PrintWriter pw, OutputStream out) throws IOException {
        
        if (dir == null) {
            sendErrorResponse(pw, 404, "Not Found");
            logger.info("Sent 404 Not Found for path " + path + " (no static files directory)");
            return;
        }

        if (path.contains("..")) {
            sendErrorResponse(pw, 403, "Forbidden");
            logger.info("Sent 403 Forbidden for path " + path);
            return;
        }

        File file = new File(dir, path);

        if (!file.exists() || file.isDirectory()) {
            sendErrorResponse(pw, 404, "Not Found");
            logger.info("Sent 404 Not Found for path " + path);
            return;
        }

        if (!file.canRead()) {
            sendErrorResponse(pw, 403, "Forbidden");
            logger.info("Sent 403 Forbidden for unreadable file " + path);
            return;
        }

        String ifModifiedSince = headers.get("if-modified-since");
        if (ifModifiedSince != null) {
            long ifModifiedSinceTime = parseRfc1123Date(ifModifiedSince);
            if (ifModifiedSinceTime != -1) {
                long fileLastModified = (file.lastModified() / 1000) * 1000;
                if (fileLastModified <= ifModifiedSinceTime) {
                    sendErrorResponse(pw, 304, "Not Modified");
                    logger.info("Sent 304 Not Modified for path " + path);
                    return;
                }
            }
        }

        serveFile(method, path, file, headers, pw, out);
    }

    private static long parseRfc1123Date(String dateStr) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(
                "EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
            ZonedDateTime zdt = ZonedDateTime.parse(dateStr, formatter);
            return zdt.toInstant().toEpochMilli();
        } catch (DateTimeParseException e) {
            logger.error("Failed to parse If-Modified-Since date: " + dateStr, e);
            return -1;
        }
    }

    private static void serveFile(String method, String path, File file,
            Map<String, String> headers, PrintWriter pw, OutputStream out) throws IOException {
        
        String contentType = getContentType(path);

        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] fileBytes = fis.readAllBytes();
            int totalLength = fileBytes.length;

            String rangeHeader = headers.get("range");
            if (rangeHeader != null && rangeHeader.startsWith("bytes=")) {
                int[] range = parseRangeHeader(rangeHeader, totalLength);
                if (range != null) {
                    int start = range[0];
                    int end = range[1];
                    int rangeLength = end - start + 1;

                    pw.print("HTTP/1.1 206 Partial Content\r\n");
                    pw.print("Content-Type: " + contentType + "\r\n");
                    pw.print("Content-Length: " + rangeLength + "\r\n");
                    pw.print("Content-Range: bytes " + start + "-" + end + "/" + totalLength + "\r\n");
                    pw.print("Server: CIS5550\r\n");
                    pw.print("\r\n");
                    pw.flush();

                    if (method.equals("GET")) {
                        out.write(fileBytes, start, rangeLength);
                        out.flush();
                        logger.info("Served GET range " + start + "-" + end + " for " + path);
                    } else {
                        logger.info("Served HEAD range " + start + "-" + end + " for " + path);
                    }
                    return;
                }
            }

            pw.print("HTTP/1.1 200 OK\r\n");
            pw.print("Content-Type: " + contentType + "\r\n");
            pw.print("Content-Length: " + fileBytes.length + "\r\n");
            pw.print("Server: CIS5550\r\n");
            pw.print("\r\n");
            pw.flush();

            if (method.equals("GET")) {
                out.write(fileBytes);
                out.flush();
                logger.info("Served GET file " + path + " with length " + fileBytes.length);
            } else {
                logger.info("Served HEAD for file " + path + " with length " + fileBytes.length);
            }
        } catch (IOException e) {
            sendErrorResponse(pw, 500, "Internal Server Error");
            logger.error("Error reading file " + path, e);
        }
    }

    private static int[] parseRangeHeader(String rangeHeader, int totalLength) {
        try {
            String rangeSpec = rangeHeader.substring(6).trim();
            int dashIndex = rangeSpec.indexOf('-');
            
            if (dashIndex == -1) {
                return null;
            }

            int start, end;

            if (dashIndex == 0) {
                int suffixLength = Integer.parseInt(rangeSpec.substring(1));
                start = totalLength - suffixLength;
                end = totalLength - 1;
            } else if (dashIndex == rangeSpec.length() - 1) {
                start = Integer.parseInt(rangeSpec.substring(0, dashIndex));
                end = totalLength - 1;
            } else {
                start = Integer.parseInt(rangeSpec.substring(0, dashIndex));
                end = Integer.parseInt(rangeSpec.substring(dashIndex + 1));
            }

            if (start < 0) start = 0;
            if (end >= totalLength) end = totalLength - 1;
            if (start > end) return null;

            return new int[] { start, end };
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String getContentType(String path) {
        if (path.endsWith(".jpg") || path.endsWith(".jpeg")) {
            return "image/jpeg";
        } else if (path.endsWith(".txt")) {
            return "text/plain";
        } else if (path.endsWith(".html")) {
            return "text/html";
        }
        return "application/octet-stream";
    }

    private static void sendErrorResponse(PrintWriter pw, int statusCode, String statusMessage) {
        pw.print("HTTP/1.1 " + statusCode + " " + statusMessage + "\r\n");
        pw.print("Content-Length: 0\r\n");
        pw.print("\r\n");
        pw.flush();
    }

    private static void closeSocket(Socket sock) {
        try {
            sock.close();
        } catch (IOException e) {
            logger.error("Error closing socket", e);
        }
    }

    private static SSLContext buildSSLContext(String keystoreFile, String password) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(new FileInputStream(keystoreFile), password.toCharArray());
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
        keyManagerFactory.init(keyStore, password.toCharArray());
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
        return sslContext;
    }
}