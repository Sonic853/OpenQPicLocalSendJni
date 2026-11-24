package org.openqpic.localsend;

import java.io.IOException;

/** All sockets have one owning worker. shutdown interrupts it; only the owner closes.
 * Data FDs are borrowed for the duration of a synchronous call. Text on the wire is UTF-8. */
final class Wire {
    static { System.loadLibrary("qpiclocalsend"); }
    interface Progress { void bytes(long count); }
    static native int socket() throws IOException;
    static native int listen(String local, int port) throws IOException;
    static native int port(int fd);
    static native int accept(int listener) throws IOException; // nonblocking, -1 means empty
    static native String peer(int fd) throws IOException;
    static native int multicast(String local) throws IOException;
    static native void announce(int udp, byte[] json) throws IOException;
    static native byte[] datagram(int udp) throws IOException; // address LF JSON, null if empty
    static native void connect(int fd, String local, String ip, int port, int timeout) throws IOException;
    static native void timeout(int fd, int seconds) throws IOException;
    static native void shutdown(int fd);
    static native boolean disconnected(int fd);
    static native void close(int fd);
    static native byte[] header(int fd, boolean response) throws IOException;
    static native void write(int fd, byte[] data) throws IOException;
    static native byte[] body(int fd, long framing, int limit) throws IOException;
    static native void sendFile(int fd, int source, long offset, long size, Progress progress) throws IOException;
    static native void receiveFile(int fd, long framing, int output, long size, Progress progress) throws IOException;
    private Wire() {}
}
