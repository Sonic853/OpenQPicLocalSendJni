package org.openqpic.localsend;

import android.system.ErrnoException;
import android.system.Os;
import android.os.ParcelFileDescriptor;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLSocket;

/** One HTTP connection. Plain connections retain native FD I/O. TLS only sees bounded plaintext buffers. */
final class HttpChannel implements Closeable {
    static final class Listener implements Closeable {
        final SystemTls identity;
        final int fd, port;
        final ServerSocket socket;
        Listener(String local,int requested,SystemTls identity) throws IOException {
            this.identity=identity;
            if(identity==null) {
                socket=null; fd=Wire.listen(local,requested); port=Wire.port(fd);
                if(port<=0) { Wire.close(fd); throw new IOException("Cannot get listener port"); }
            } else {
                fd=-1; socket=identity.serverSocket();
                try {
                    socket.setReuseAddress(true); socket.bind(new InetSocketAddress(local,requested),24);
                    port=socket.getLocalPort();
                } catch(IOException | RuntimeException e) { socket.close(); throw e; }
            }
        }
        HttpChannel accept(int wait) throws IOException {
            if(socket==null) {
                int client=wait==0?Wire.accept(fd):Wire.awaitAccept(fd,wait);
                return client<0?null:new HttpChannel(client,null,null);
            }
            socket.setSoTimeout(Math.max(1,wait));
            try { return new HttpChannel(-1,socket.accept(),identity); }
            catch(SocketTimeoutException idle) { return null; }
        }
        @Override public void close() {
            if(socket==null) Wire.close(fd); else try { socket.close(); } catch(IOException ignored) {}
        }
    }
    private final int fd;
    private final Socket socket;
    private final SystemTls identity;
    private SSLSocket tls;
    private BufferedInputStream in;
    private OutputStream out;
    private boolean abortRequested;
    String peerFingerprint;
    private HttpChannel(int fd,Socket socket,SystemTls identity) {
        this.fd=fd; this.socket=socket; this.identity=identity;
    }
    static HttpChannel outgoing(SystemTls identity) throws IOException {
        return identity==null ? new HttpChannel(Wire.socket(),null,null) : new HttpChannel(-1,new Socket(),identity);
    }
    void connect(String local,String host,int port,int ms,String pin,boolean discovery) throws IOException {
        if(socket==null) { Wire.connect(fd,local,host,port,ms); return; }
        socket.bind(new InetSocketAddress(local,0));
        socket.connect(new InetSocketAddress(host,port),ms); handshake(false,pin,discovery);
    }
    void acceptTls() throws IOException { if(socket!=null) handshake(true,null,false); }
    private void handshake(boolean server,String pin,boolean discovery) throws IOException {
        socket.setSoTimeout(8000);
        tls=server?(SSLSocket)socket:identity.wrapClient(socket,pin,discovery);
        tls.setSoTimeout(15000);
        tls.startHandshake();
        if(!server || tls.getNeedClientAuth()) try {
            peerFingerprint=SystemTls.digest((X509Certificate)tls.getSession().getPeerCertificates()[0]);
        } catch(java.security.cert.CertificateException e) { throw new IOException(e); }
        in=new BufferedInputStream(tls.getInputStream(),16384); out=tls.getOutputStream();
    }
    boolean ready() { return socket==null || out!=null; }
    String peer() throws IOException { return socket==null?Wire.peer(fd):socket.getInetAddress().getHostAddress(); }
    void timeout(int sec) throws IOException {
        if(socket==null) Wire.timeout(fd,sec); else (tls==null?socket:tls).setSoTimeout(sec*1000);
    }
    synchronized void interrupt() {
        if(abortRequested) return;
        abortRequested=true;
        if(socket==null) Wire.shutdown(fd);
        else {
            // Conscrypt may write close_notify. Never do that on the Activity thread
            // (nor while holding a session lock that the I/O owner needs to finish).
            Thread t=new Thread(()->{ try { socket.close(); } catch(IOException ignored) {} },"QP TLS cancel");
            t.setDaemon(true); t.start();
        }
    }
    @Override public void close() {
        if(socket==null) Wire.close(fd);
        else {
            try { if(tls!=null) tls.close(); } catch(IOException ignored) {}
            try { socket.close(); } catch(IOException ignored) {}
        }
    }
    boolean disconnected() {
        if(socket==null) return Wire.disconnected(fd);
        try {
            int timeout=tls.getSoTimeout();
            try {
                tls.setSoTimeout(1); in.mark(1);
                int b=in.read(); if(b>=0) in.reset(); return b<0;
            } finally { tls.setSoTimeout(timeout); }
        } catch(SocketTimeoutException idle) { return false; } catch(IOException closed) { return true; }
    }
    void write(byte[] b) throws IOException { if(socket==null) Wire.write(fd,b); else out.write(b); }
    byte[] header(boolean response,boolean video) throws IOException {
        if(socket==null) return video?Wire.videoHeader(fd):Wire.header(fd,response);
        byte[] raw=new byte[16384]; int pos=0;
        while(pos<raw.length) {
            int b=in.read(); if(b<0) throw new EOFException("Incomplete HTTP header");
            raw[pos++]=(byte)b;
            if(pos>=4 && raw[pos-4]=='\r' && raw[pos-3]=='\n' && raw[pos-2]=='\r' && b=='\n')
                return Wire.parsedHeader(raw,pos,response,video);
        }
        throw new IOException("HTTP header too large");
    }
    byte[] body(long framing,int limit) throws IOException {
        if(socket==null) return Wire.body(fd,framing,limit);
        ByteArrayOutputStream data=new ByteArrayOutputStream();
        readBody(framing,limit,data,null); return data.toByteArray();
    }
    void receiveFile(long framing,int dest,long size,Wire.Progress progress) throws IOException {
        if(socket==null) { Wire.receiveFile(fd,framing,dest,size,progress); return; }
        if(framing==-1 || (framing>=0 && framing!=size)) throw new IOException("Wrong upload length");
        try(OutputStream f=new ParcelFileDescriptor.AutoCloseOutputStream(ParcelFileDescriptor.fromFd(dest))) {
            if(readBody(framing,size,f,progress)!=size) throw new EOFException("Incomplete upload");
        }
    }
    private String chunkLine(int max) throws IOException {
        ByteArrayOutputStream s=new ByteArrayOutputStream();
        while(s.size()<max) {
            int c=in.read();
            if(c=='\r') {
                if(in.read()!='\n') throw new IOException("Invalid chunk delimiter");
                return s.toString("US-ASCII");
            }
            if(c<0x20 && c!='\t') throw new IOException("Invalid chunk line");
            s.write(c);
        }
        throw new IOException("Chunk line too long");
    }
    private long readBody(long frame,long limit,OutputStream dest,Wire.Progress progress) throws IOException {
        if(frame < -2 || frame>limit || limit<0) throw new IOException("HTTP length exceeds limit");
        byte[] b=new byte[32768]; long total=0,remaining=frame,last=0;
        while(true) {
            if(frame==-2 && remaining<=0) {
                String line=chunkLine(4096); int semi=line.indexOf(';');
                String hex=semi<0?line:line.substring(0,semi);
                if(!hex.matches("[0-9a-fA-F]{1,16}")) throw new IOException("Invalid chunk size");
                try { remaining=Long.parseLong(hex,16); } catch(NumberFormatException e) { throw new IOException("Invalid chunk size",e); }
                if(remaining>limit-total) throw new IOException("HTTP length exceeds limit");
                if(remaining==0) {
                    int budget=16384;
                    while(true) { String t=chunkLine(budget); if(t.isEmpty()) break; budget-=t.length()+2; }
                    break;
                }
            }
            if(remaining==0) break;
            int n=in.read(b,0,remaining<0?b.length:(int)Math.min(b.length,remaining));
            if(n<0 && frame==-1) break;
            if(n<=0 || n>limit-total) throw new IOException("Invalid HTTP body length");
            dest.write(b,0,n); total+=n;
            if(remaining>0) remaining-=n;
            if(frame==-2 && remaining==0 && (in.read()!='\r' || in.read()!='\n')) throw new IOException("Invalid chunk delimiter");
            long now=System.nanoTime();
            if(progress!=null && now-last>=200000000L) { progress.bytes(total); last=now; }
        }
        if(progress!=null) progress.bytes(total); return total;
    }
    void sendFile(int source,long offset,long length,Wire.Progress progress) throws IOException {
        if(socket==null) { Wire.sendFile(fd,source,offset,length,progress); return; }
        if(offset<0 || length<0 || offset>Long.MAX_VALUE-length) throw new IOException("Invalid FD range");
        try(ParcelFileDescriptor p=ParcelFileDescriptor.fromFd(source)) {
            byte[] buf=new byte[32768]; long done=0,last=0;
            while(done<length) {
                int n;
                try { n=Os.pread(p.getFileDescriptor(),buf,0,(int)Math.min(buf.length,length-done),offset+done); }
                catch(ErrnoException e) { throw new IOException(e); }
                if(n<=0) throw new EOFException("Incomplete FD source");
                out.write(buf,0,n); done+=n;
                long now=System.nanoTime();
                if(progress!=null && now-last>=200000000L) { progress.bytes(done); last=now; }
            }
            if(progress!=null) progress.bytes(done);
        }
    }
}
