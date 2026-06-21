package cis5550.webserver;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class ResponseImpl implements Response {

    private int statusCode = 200;
    private String reasonPhrase = "OK";

    private byte[] body = null;

    private List<String[]> headers = new ArrayList<>();

    private String contentType = "text/html";

    private boolean writeCommitted = false;

    private OutputStream out = null;

    public void setOutputStream(OutputStream out) {
        this.out = out;
    }

    @Override
    public void body(String body) {
        if (!writeCommitted) {
            this.body = body != null ? body.getBytes() : null;
        }
    }

    @Override
    public void bodyAsBytes(byte[] bodyArg) {
        if (!writeCommitted) {
            this.body = bodyArg;
        }
    }

    @Override
    public void header(String name, String value) {
        if (!writeCommitted) {
            headers.add(new String[] { name, value });
        }
    }

    @Override
    public void type(String contentType) {
        if (!writeCommitted) {
            this.contentType = contentType;
        }
    }

    @Override
    public void status(int statusCode, String reasonPhrase) {
        if (!writeCommitted) {
            this.statusCode = statusCode;
            this.reasonPhrase = reasonPhrase;
        }
    }

    @Override
    public void write(byte[] b) throws Exception {
        if (!writeCommitted) {
            writeCommitted = true;
            PrintWriter pw = new PrintWriter(out);
            pw.print("HTTP/1.1 " + statusCode + " " + reasonPhrase + "\r\n");
            pw.print("Content-Type: " + contentType + "\r\n");
            pw.print("Connection: close\r\n");
            for (String[] header : headers) {
                pw.print(header[0] + ": " + header[1] + "\r\n");
            }
            pw.print("Server: CIS5550\r\n");
            pw.print("\r\n");
            pw.flush();
        }
        out.write(b);
        out.flush();
    }

    @Override
    public void redirect(String url, int responseCode) {
        String reason;
        switch (responseCode) {
            case 301: reason = "Moved Permanently"; break;
            case 302: reason = "Found"; break;
            case 303: reason = "See Other"; break;
            case 307: reason = "Temporary Redirect"; break;
            case 308: reason = "Permanent Redirect"; break;
            default: reason = "Redirect"; break;
        }
        this.statusCode = responseCode;
        this.reasonPhrase = reason;
        headers.add(new String[] { "Location", url });
    }

    private boolean halted = false;
    private int haltStatusCode;
    private String haltReasonPhrase;

    @Override
    public void halt(int statusCode, String reasonPhrase) {
        this.halted = true;
        this.haltStatusCode = statusCode;
        this.haltReasonPhrase = reasonPhrase;
    }

    public boolean isHalted() {
        return halted;
    }

    public int getHaltStatusCode() {
        return haltStatusCode;
    }

    public String getHaltReasonPhrase() {
        return haltReasonPhrase;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getReasonPhrase() {
        return reasonPhrase;
    }

    public byte[] getBody() {
        return body;
    }

    public List<String[]> getHeaders() {
        return headers;
    }

    public String getContentType() {
        return contentType;
    }

    public boolean isWriteCommitted() {
        return writeCommitted;
    }
}
