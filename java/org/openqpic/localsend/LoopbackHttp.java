package org.openqpic.localsend;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** Loopback-only HTTP for local playback. No LocalSend session, UDP or discovery. */
public final class LoopbackHttp implements AutoCloseable {
    private static final int MAX_CLIENTS = 16;
    private final HttpChannel.Listener listener;
    private final Handler handler;
    private final Set<HttpChannel> clients = new HashSet<>();
    private volatile boolean closed;

    public interface Handler {
        void handle(String path, String range, boolean head, Reply response) throws IOException;
    }

    public LoopbackHttp(Handler handler) throws IOException {
        this(handler,null);
    }
    public LoopbackHttp(Handler handler, SystemTls identity) throws IOException {
        this.handler = handler;
        listener = new HttpChannel.Listener("127.0.0.1",0,identity);
        try {
            thread(this::acceptLoop, "QP video listener").start();
        } catch (RuntimeException | Error e) {
            listener.close();
            throw e;
        }
    }

    public int port() { return listener.port; }
    public boolean isOpen() { return !closed; }

    private static Thread thread(Runnable work, String name) {
        Thread t = new Thread(work, name);
        t.setDaemon(true);
        return t;
    }

    private void acceptLoop() {
        try {
            while (!closed) {
                HttpChannel fd = listener.accept(250);
                if (fd == null) continue;
                synchronized (this) {
                    if (closed || clients.size() >= MAX_CLIENTS) {
                        fd.close();
                        continue;
                    }
                    clients.add(fd);
                }
                try {
                    thread(() -> serve(fd), "QP video connection").start();
                } catch (RuntimeException | Error e) {
                    release(fd);
                    throw e;
                }
            }
        } catch (IOException ignored) {
        } finally {
            close();
            listener.close();
        }
    }

    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        // Workers own close(); shutdown interrupts their socket I/O without reusing an FD.
        for (HttpChannel fd : clients) fd.interrupt();
        // The accept loop owns its listener and returns from poll within 250 ms.
    }

    private void release(HttpChannel fd) {
        synchronized(this) { clients.remove(fd); }
        fd.close();
    }

    private void serve(HttpChannel fd) {
        Reply reply = new Reply(fd);
        try {
            fd.acceptTls();
            fd.timeout(15);
            String[] fields;
            try {
                fields = new String(fd.header(false,true), StandardCharsets.UTF_8).split("\n", 3);
            } catch (IOException e) {
                if (!closed) reply.empty(400);
                return;
            }
            int space = fields[0].indexOf(' ');
            if (fields.length < 2 || space <= 0) { reply.empty(400); return; }
            String method = fields[0].substring(0, space);
            boolean head = "HEAD".equals(method);
            if (!head && !"GET".equals(method)) { reply.empty(405); return; }
            if (!"-1".equals(fields[1]) && !"0".equals(fields[1])) { reply.empty(400); return; }
            handler.handle(fields[0].substring(space + 1),
                    !head && fields.length == 3 ? fields[2] : null, head, reply);
            if (!reply.started) reply.empty(404);
        } catch (IOException | RuntimeException e) {
            if (!closed && fd.ready() && !reply.started) try { reply.empty(404); } catch (IOException ignored) {}
        } finally {
            release(fd);
        }
    }

    public static final class Reply {
        private final HttpChannel fd;
        private boolean started;

        private Reply(HttpChannel fd) { this.fd = fd; }

        private void empty(int status) throws IOException { headers(status, 0, null, null, false); }

        public void headers(int status, long length, String type, String contentRange,
                            boolean seekable) throws IOException {
            if (started || length < 0) throw new IOException("Invalid HTTP response");
            String reason;
            switch (status) {
                case 200: reason = "OK"; break;
                case 206: reason = "Partial Content"; break;
                case 400: reason = "Bad Request"; break;
                case 404: reason = "Not Found"; break;
                case 405: reason = "Method Not Allowed"; break;
                case 416: reason = "Range Not Satisfiable"; break;
                default: throw new IOException("Unsupported HTTP status");
            }
            StringBuilder text = new StringBuilder("HTTP/1.1 ").append(status).append(' ').append(reason)
                    .append("\r\nConnection: close\r\nContent-Length: ").append(length).append("\r\n");
            if (status == 405) text.append("Allow: GET, HEAD\r\n");
            if (seekable) text.append("Accept-Ranges: bytes\r\n");
            if (headerValue(type)) text.append("Content-Type: ").append(type).append("\r\n");
            if (headerValue(contentRange)) text.append("Content-Range: ").append(contentRange).append("\r\n");
            started = true;
            fd.write(text.append("\r\n").toString().getBytes(StandardCharsets.US_ASCII));
        }

        /** The caller owns the source FD and keeps it open until this synchronous call returns. */
        public void file(int source, long offset, long length) throws IOException {
            fd.sendFile(source, offset, length, null);
        }

        /** Sequential fallback for providers that supply a pipe rather than a seekable FD. */
        public void stream(InputStream input, long length) throws IOException {
            byte[] buffer = new byte[32768];
            while (length > 0) {
                int n = input.read(buffer, 0, (int) Math.min(length, buffer.length));
                if (n <= 0) throw new EOFException("Incomplete video source");
                fd.write(n == buffer.length ? buffer : Arrays.copyOf(buffer, n));
                length -= n;
            }
        }

        private static boolean headerValue(String value) {
            if (value == null || value.isEmpty()) return false;
            for (int i = 0; i < value.length(); i++)
                if (value.charAt(i) < 0x20 || value.charAt(i) > 0x7e) return false;
            return true;
        }
    }
}
