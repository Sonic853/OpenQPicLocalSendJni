package org.openqpic.localsend;

import org.json.JSONException;
import org.json.JSONObject;
import java.io.Closeable;
import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

/** A transfer owner controls sessions; visibility grants a separate, revocable discovery lease. */
public final class LocalSend implements Closeable {
    public static final int PORT = 53317, MAX_FILES = 256;
    public static final int MAX_TARGETS = 256;
    private static final int PARALLEL_SENDS = 3;
    private static final int MAX_JSON = 512 * 1024, MAX_CLIENTS = 24;
    private static final String API = "/api/localsend/v2/";
    private final String local, fingerprint, model;
    private String alias, pin;
    private final Events events;
    private final Set<Integer> clients = new HashSet<>();
    private final Set<Integer> discoverySockets = new HashSet<>(), admitted = new HashSet<>();
    private final Map<String, Long> probes = new LinkedHashMap<>();
    private final Map<String, long[]> pinFailures = new LinkedHashMap<>();
    private int listener = -1, udp = -1, port, registering;
    private volatile boolean open;
    private boolean visible;
    private int epoch, burst;
    private long nextBeacon;
    private Session incoming;
    private final Map<String,Session> outgoing = new LinkedHashMap<>();
    private int sendWorkers;

    public interface Events {
        void peer(Peer peer);
        /** Canonical acceptable MIME, or null to omit this item before issuing any token. */
        String receiveType(Item item);
        void offer(String key, Peer peer, List<Item> files, int skipped);
        void needPin(String key, Peer peer);
        void update(String key, String label, int percent, boolean finished);
        void problem(String reason);
        Sink destination(Item item,Cancellation cancellation) throws IOException;
    }
    /** The owner keeps the FD valid until close. Commit makes a verified file visible. */
    public interface Sink extends Closeable {
        int fd();
        boolean matches(String sha256) throws IOException;
        boolean isMedia() throws IOException;
        void commit() throws IOException;
    }
    public interface Source extends Closeable {
        Item item();
        int fd();
        long offset();
    }
    public interface Cancellation {
        boolean cancelled();
        /** Hook for the single outgoing provider open; clear it when the open returns. */
        void onCancel(Runnable interrupt);
    }
    public interface Selection { Source open(int index, Cancellation cancellation) throws IOException; }
    public static final class Peer {
        public final String alias, ip, fingerprint, protocol, deviceType, deviceModel;
        public final int port;
        Peer(JSONObject j, String ip, int port, String protocol) throws Exception {
            this.alias = text(j,"alias",256); this.fingerprint = text(j,"fingerprint",256);
            if (!text(j,"version",32).startsWith("2.") || port < 1 || port > 65535 ||
                    !("http".equals(protocol) || "https".equals(protocol))) throw new JSONException("Invalid peer");
            this.ip = ip; this.port = port; this.protocol = protocol;
            this.deviceType=j.optString("deviceType","unknown"); this.deviceModel=j.optString("deviceModel","");
        }
        @Override public String toString() { return alias; }
    }
    public static final class Item {
        public final String id, name, type, hash;
        public final long size;
        public Item(String id, String name, long size, String type, String hash) {
            this.id=id; this.name=name; this.size=size; this.type=type; this.hash=hash;
        }
        JSONObject json() throws JSONException {
            return new JSONObject().put("id",id).put("fileName",name).put("size",size)
                    .put("fileType",type).put("sha256",hash);
        }
    }
    private static final class FileState {
        final Item item; final String token = uuid();
        int tries; boolean busy, done, failed; long bytes;
        FileState(Item i) { item=i; }
    }
    private static class CancelToken implements Cancellation {
        CancelToken() {}
        volatile boolean cancelled;
        private Runnable interrupt;
        public boolean cancelled() { return cancelled; }
        public synchronized void onCancel(Runnable r) {
            if(!cancelled) interrupt=r;
            else if(r!=null) thread(r);
        }
        synchronized void interruptIO() {
            cancelled=true;
            if(interrupt!=null) { thread(interrupt); interrupt=null; }
        }
    }
    /** A group opens/stages its share URIs once. The last job releases them after native reads exit. */
    private static final class Batch extends CancelToken {
        final Map<String,Source> sources=new LinkedHashMap<>();
        final FutureTask<Void> prepare;
        int live; private int refs;
        Batch(int targets,int count,Selection selection) {
            live=refs=targets;
            prepare=new FutureTask<>(()->{
                for(int i=0;i<count;i++) {
                    if(cancelled) throw new IOException("Cancelled");
                    Source source=selection.open(i,this); String id=source.item().id;
                    if(sources.containsKey(id)) { source.close(); throw new IOException("Duplicate file ID"); }
                    sources.put(id,source);
                }
                return null;
            });
        }
        void load() throws Exception {
            prepare.run(); // at most one worker opens sources, including non-seekable providers
            try { prepare.get(); }
            catch(ExecutionException e) { throw new IOException("Cannot open files: "+e.getCause().getMessage(),e.getCause()); }
        }
        void release() {
            synchronized(this) { if(--refs!=0) return; }
            for(Source source:sources.values()) try { source.close(); } catch(IOException ignored) {}
            sources.clear();
        }
    }
    private static final class Session extends CancelToken {
        final String key = uuid(); final Peer peer; final Batch batch;
        final Map<String,FileState> files = new LinkedHashMap<>();
        final Set<Integer> sockets = new HashSet<>();
        final CountDownLatch decision = new CountDownLatch(1);
        CountDownLatch pinPrompt; String enteredPin, remoteId;
        Set<String> selected; boolean offered,started; int percent;
        Session(Peer p,Batch batch) { peer=p; this.batch=batch; }
    }
    private static final class Status extends Exception {
        final int code;
        Status(int code,String s) { super(s); this.code=code; }
    }
    private static final class Reply {
        final int code; final byte[] data;
        Reply(int c,byte[] d) { code=c; data=d; }
        JSONObject json() throws JSONException { return new JSONObject(utf(data)); }
    }
    private static String uuid() { return UUID.randomUUID().toString(); }
    private static long now() { return System.nanoTime()/1000000; }
    private static String utf(byte[] data) { return new String(data,StandardCharsets.UTF_8); }
    private static byte[] buf(String s) { return s.getBytes(StandardCharsets.UTF_8); }
    private static String text(JSONObject j,String field,int max) throws JSONException {
        Object value=j.get(field);
        if (!(value instanceof String) || ((String)value).isEmpty() || ((String)value).length()>max)
            throw new JSONException("Invalid "+field);
        return (String)value;
    }
    private static void thread(Runnable r) { Thread t=new Thread(r,"OpenQPic-LocalSend"); t.setDaemon(true); t.start(); }
    private static String reason(Exception e) { return e.getMessage()==null ? e.getClass().getSimpleName() : e.getMessage(); }

    private LocalSend(String local,String alias,String fingerprint,String model,String pin,Events events) {
        this.local=local; this.alias=alias; this.fingerprint=fingerprint; this.model=model; this.pin=pin; this.events=events;
    }
    public static LocalSend start(String local,String alias,String fingerprint,String model,String pin,Events events)
            throws IOException {
        LocalSend e = new LocalSend(local,alias,fingerprint,model,pin,events);
        try { e.listener=Wire.listen(local,PORT); }
        catch (IOException busy) { e.listener=Wire.listen(local,0); }
        try {
            e.port=Wire.port(e.listener);
            e.open=true;
            e.show(alias,pin);
            thread(e::loop);
            return e;
        } catch (RuntimeException ex) { e.close(); throw ex; }
    }
    public int port() { return port; }
    public boolean isOpen() { return open; }
    public synchronized boolean hasWork() { return incoming!=null || !outgoing.isEmpty(); }
    public synchronized boolean sending() { return !outgoing.isEmpty(); }
    public synchronized boolean sendingTo(Peer p) {
        for(Session s:outgoing.values()) if(s.peer.fingerprint.equals(p.fingerprint) ||
                s.peer.ip.equals(p.ip) && s.peer.port==p.port) return true;
        return false;
    }
    public synchronized boolean hasSession(String key) {
        return incoming!=null && incoming.key.equals(key) || outgoing.containsKey(key);
    }
    private synchronized JSONObject info() throws JSONException {
        return new JSONObject().put("alias",alias).put("version","2.2").put("deviceModel",model)
                .put("deviceType","mobile").put("fingerprint",fingerprint).put("port",port)
                .put("protocol","http").put("download",false);
    }
    public synchronized void show(String name,String receivePin) {
        if(!open) throw new IllegalStateException("Retired endpoint");
        boolean changed=!Objects.equals(alias,name);
        if(!Objects.equals(pin,receivePin)) pinFailures.clear();
        alias=name; pin=receivePin;
        if(!visible) { visible=true; epoch++; probes.clear(); }
        else if(!changed && udp>=0) return;
        if(udp<0) try { udp=Wire.multicast(local); }
        catch(IOException ex) { events.problem("Multicast: "+reason(ex)); }
        burst=0; nextBeacon=now()+100; notifyAll();
    }
    public synchronized void hide() {
        if(visible) { visible=false; epoch++; }
        if(udp>=0) { Wire.close(udp); udp=-1; }
        for(int fd:discoverySockets) Wire.shutdown(fd);
        retireIfIdle(); notifyAll();
    }
    @Override public void close() { hide(); }
    private void retireIfIdle() {
        if(visible || hasWork()) return;
        open=false;
        if(listener>=0) { Wire.close(listener); listener=-1; }
        // The final upload/decline may still be writing its HTTP reply. Its worker owns the FD.
        notifyAll();
    }
    private void stop(Session s) {
        if(s==null) return;
        s.interruptIO(); s.decision.countDown();
        if(s.pinPrompt!=null) s.pinPrompt.countDown();
        for(int fd:s.sockets) Wire.shutdown(fd);
    }
    private void loop() {
        boolean acceptError=false;
        while(true) {
            byte[] packet=null;
            int stamp;
            synchronized(this) {
                if(!open) return;
                stamp=epoch;
                if(udp>=0) {
                    try {
                        if(now()>=nextBeacon) {
                            Wire.announce(udp,buf(info().put("announce",true).toString()));
                            nextBeacon=now()+(burst==0?500:burst==1?2000:30000); ++burst;
                        }
                        packet=Wire.datagram(udp);
                    } catch(IOException | JSONException ex) { Wire.close(udp); udp=-1; events.problem("Multicast: "+reason(ex)); }
                }
                for(int i=0;i<8;i++) {
                    int fd;
                    try { fd=Wire.accept(listener); acceptError=false; }
                    catch(IOException ex) {
                        if(!acceptError) events.problem(reason(ex));
                        acceptError=true; break;
                    }
                    if(fd<0) break;
                    if(clients.size()>=MAX_CLIENTS) { Wire.close(fd); continue; }
                    if(!visible) try {
                        if(!knownPeer(Wire.peer(fd))) { Wire.close(fd); continue; }
                    } catch(IOException disconnected) { Wire.close(fd); continue; }
                    clients.add(fd); thread(()->serve(fd,stamp));
                }
                if(packet==null) try { wait(acceptError?500:60); } catch(InterruptedException ignored) {}
            }
            if(packet!=null) beacon(packet,stamp);
        }
    }
    private boolean knownPeer(String ip) {
        if(incoming!=null && incoming.peer.ip.equals(ip)) return true;
        for(Session s:outgoing.values()) if(s.offered && s.peer.ip.equals(ip)) return true;
        return false;
    }
    private synchronized boolean discovering(int stamp) { return visible && epoch==stamp; }
    private synchronized void beacon(byte[] packet,int stamp) {
        try {
            if(!discovering(stamp)) return;
            String s=utf(packet); int i=s.indexOf('\n'); JSONObject j=new JSONObject(s.substring(i+1));
            Peer p=described(j,s.substring(0,i));
            if(fingerprint.equals(p.fingerprint)) return;
            if("https".equals(p.protocol)) { events.peer(p); return; }
            probe(p.ip,p.port,false);
        } catch(Exception ignored) { /* Not a LocalSend v2 announcement. */ }
    }
    private static Peer described(JSONObject j,String ip) throws Exception {
        return new Peer(j,ip,j.getInt("port"),j.getString("protocol"));
    }
    public synchronized void probe(String ip,int port,boolean manual) {
        if(!visible || registering>=4 || port<1 || port>65535 || !ip.matches("[0-9.]{7,15}")) return;
        final int stamp=epoch;
        String key=ip+":"+port; Long last=probes.get(key);
        if(!manual && last!=null && now()-last<5000) return;
        if(probes.size()>=256) probes.remove(probes.keySet().iterator().next());
        probes.put(key,now()); registering++;
        thread(()->{
            try {
                Reply r=request(ip,port,"register",info(),null,null,8,stamp);
                if(r.code!=200) throw new IOException("Register: HTTP "+r.code);
                Peer p=new Peer(r.json(),ip,port,"http");
                synchronized(this) { if(discovering(stamp) && !fingerprint.equals(p.fingerprint)) events.peer(p); }
            } catch(Exception ex) { synchronized(this) { if(discovering(stamp) && manual) events.problem(reason(ex)); } }
            finally { synchronized(this) { registering--; } }
        });
    }
    private synchronized void check(Session s) throws IOException {
        if(!open || (s!=null && s.cancelled)) throw new IOException("Cancelled");
    }
    private synchronized int socket(Session s,int stamp) throws IOException {
        check(s);
        if(s==null && !discovering(stamp)) throw new IOException("Discovery closed");
        if(s==null && clients.size()>=MAX_CLIENTS+4) throw new IOException("Busy");
        int fd=Wire.socket(); clients.add(fd);
        if(s!=null) s.sockets.add(fd); else discoverySockets.add(fd);
        return fd;
    }
    private synchronized void release(int fd,Session s) {
        if(s!=null) s.sockets.remove(fd);
        discoverySockets.remove(fd); admitted.remove(fd); clients.remove(fd); Wire.close(fd);
    }
    private Reply request(String ip,int port,String route,JSONObject json,Source source,Session s,int timeout,int stamp) throws Exception {
        int fd=socket(s,stamp);
        try {
            Wire.connect(fd,local,ip,port,3500); check(s); Wire.timeout(fd,timeout);
            if(s==null && !discovering(stamp)) throw new IOException("Discovery closed");
            byte[] data=json==null?new byte[0]:buf(json.toString());
            if(data.length>MAX_JSON) throw new IOException("Offer too large");
            long n=source!=null?source.item().size:data.length;
            Wire.write(fd,buf("POST "+API+route+" HTTP/1.1\r\nHost: "+ip+":"+port+"\r\nConnection: close\r\nContent-Type: "+
                    (source!=null?"application/octet-stream":"application/json")+"\r\nContent-Length: "+n+"\r\n\r\n"));
            if(source!=null) Wire.sendFile(fd,source.fd(),source.offset(),n,count->progress(s,source.item().id,count));
            else if(data.length!=0) Wire.write(fd,data);
            String[] h=utf(Wire.header(fd,true)).split("\n",2);
            int status=Integer.parseInt(h[0]); long frame=Long.parseLong(h[1]);
            return new Reply(status,status==204?new byte[0]:Wire.body(fd,frame,MAX_JSON));
        } finally { release(fd,s); }
    }
    private static void respond(int fd,int status,JSONObject body) throws Exception {
        byte[] data=body==null?new byte[0]:buf(body.toString());
        Wire.write(fd,buf("HTTP/1.1 "+status+" "+(status<300?"OK":"Error")+
                "\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: "+data.length+"\r\n\r\n"));
        if(data.length!=0) Wire.write(fd,data);
    }
    private static JSONObject json(int fd,long frame) throws Exception {
        if(frame==-1) throw new Status(400,"Missing body");
        return new JSONObject(utf(Wire.body(fd,frame,MAX_JSON)));
    }
    private static String enc(String s) throws Exception { return URLEncoder.encode(s,"UTF-8"); }
    private void serve(int fd,int stamp) {
        boolean sessionRoute=false;
        try {
            Wire.timeout(fd,20);
            String ip=Wire.peer(fd);
            String[] h=utf(Wire.header(fd,false)).split("\n",2);
            String[] first=h[0].split(" ",2), uri=first[1].split("\\?",2);
            String path=uri[0]; long frame=Long.parseLong(h[1]);
            Map<String,String> q=new HashMap<>();
            if(uri.length>1) for(String pair:uri[1].split("&")) {
                String[] kv=pair.split("=",2);
                if(kv.length==2) q.put(URLDecoder.decode(kv[0],"UTF-8"),URLDecoder.decode(kv[1],"UTF-8"));
            }
            synchronized(this) {
                boolean uploading="POST".equals(first[0]) && path.equals(API+"upload");
                boolean cancelling="POST".equals(first[0]) && path.equals(API+"cancel");
                if(uploading) sessionRoute=ticket(ip,q)!=null;
                if(cancelling) sessionRoute=cancellable(ip,q.get("sessionId"))!=null;
                if(!discovering(stamp) && !sessionRoute) return; // no identifying response, even to /v1/info
                if(!uploading && !cancelling) discoverySockets.add(fd);
            }
            if("GET".equals(first[0]) && (path.equals(API+"info") || path.equals("/api/localsend/v1/info")))
                respond(fd,200,info());
            else if(!"POST".equals(first[0]) || !path.startsWith(API)) throw new Status(404,"Unknown route");
            else switch(path.substring(API.length())) {
                case "register":
                    Peer p=described(json(fd,frame),ip);
                    synchronized(this) { if(!discovering(stamp)) return; if(!p.fingerprint.equals(fingerprint)) events.peer(p); }
                    respond(fd,200,info()); break;
                case "prepare-upload": prepare(fd,ip,q.get("pin"),frame,stamp); break;
                case "upload": upload(fd,ip,q,frame); break;
                case "cancel": remoteCancel(ip,q.get("sessionId")); respond(fd,200,null); break;
                default: throw new Status(404,"Unknown route");
            }
        } catch(Exception ex) {
            boolean reply;
            synchronized(this) { reply=sessionRoute || admitted.contains(fd) || discovering(stamp); }
            try { if(reply) respond(fd,ex instanceof Status?((Status)ex).code:400,
                    new JSONObject().put("message",reason(ex))); } catch(Exception ignored) {}
        } finally { release(fd,null); }
    }
    private synchronized void verifyPin(String ip,String supplied) throws Status {
        if(pin==null || pin.isEmpty()) return;
        long[] f=pinFailures.get(ip);
        if(f!=null && now()-f[0]>60000) { pinFailures.remove(ip); f=null; }
        if(f!=null && f[1]>=5) throw new Status(429,"Too many PIN attempts");
        if(!pin.equals(supplied)) {
            if(supplied!=null) {
                if(f==null) {
                    if(pinFailures.size()>=256) pinFailures.remove(pinFailures.keySet().iterator().next());
                    pinFailures.put(ip,f=new long[]{now(),0});
                }
                f[1]++;
            }
            throw new Status(401,"PIN required");
        }
        pinFailures.remove(ip);
    }
    private void prepare(int fd,String ip,String supplied,long frame,int stamp) throws Exception {
        verifyPin(ip,supplied);
        JSONObject offer=json(fd,frame), files=offer.getJSONObject("files");
        if(files.length()==0 || files.length()>MAX_FILES) throw new Status(400,"Invalid file count");
        Session s=new Session(described(offer.getJSONObject("info"),ip),null);
        List<Item> display=new ArrayList<>(); long total=0;
        for(Iterator<String> it=files.keys();it.hasNext();) {
            String id=it.next(); JSONObject f=files.getJSONObject(id);
            Object length=f.get("size");
            if(!(length instanceof Long || length instanceof Integer) || !id.equals(text(f,"id",256)))
                throw new Status(400,"Invalid file descriptor");
            long size=((Number)length).longValue();
            String hash=f.isNull("sha256")?null:text(f,"sha256",64);
            if(size<0 || size>Long.MAX_VALUE-total || (hash!=null&&!hash.matches("(?i)[0-9a-f]{64}")))
                throw new Status(400,"Invalid file length or checksum");
            total+=size;
            Item item=new Item(id,text(f,"fileName",4096),size,text(f,"fileType",256),hash);
            String mime=events.receiveType(item);
            if(mime==null) continue;
            item=new Item(id,item.name,size,mime,hash);
            display.add(item); s.files.put(id,new FileState(item));
        }
        if(display.isEmpty()) throw new Status(415,"Only media files are accepted");
        synchronized(this) {
            if(!discovering(stamp)) return;
            if(incoming!=null) throw new Status(409,"Busy");
            discoverySockets.remove(fd); admitted.add(fd);
            incoming=s; events.offer(s.key,s.peer,Collections.unmodifiableList(display),files.length()-display.size());
        }
        boolean handedOff=false;
        try {
            while(!s.decision.await(250,TimeUnit.MILLISECONDS)) {
                if(Wire.disconnected(fd)) throw new IOException("Sender disconnected");
            }
            JSONObject tokens=new JSONObject();
            synchronized(this) {
                if(s.cancelled || s.selected==null) throw new Status(403,"Declined");
                s.files.keySet().retainAll(s.selected);
                for(FileState f:s.files.values()) tokens.put(f.item.id,f.token);
                if(tokens.length()>0) {
                    s.remoteId=s.key;
                    events.update(s.key,s.peer.alias+" · …",0,false);
                }
            }
            if(tokens.length()==0) { respond(fd,204,null); return; }
            respond(fd,200,new JSONObject().put("sessionId",s.key).put("files",tokens));
            handedOff=true;
        } finally { if(!handedOff) end(s,"Closed"); }
    }
    public synchronized void answer(String key,Set<String> ids) {
        if(incoming!=null && incoming.key.equals(key) && incoming.decision.getCount()!=0) {
            incoming.selected=ids==null?null:new HashSet<>(ids); incoming.decision.countDown();
        }
    }
    private FileState ticket(String ip,Map<String,String> q) {
        Session s=incoming;
        if(s==null || !s.peer.ip.equals(ip) || s.remoteId==null || !s.remoteId.equals(q.get("sessionId"))) return null;
        FileState f=s.files.get(q.get("fileId"));
        return f!=null && f.token.equals(q.get("token"))?f:null;
    }
    private void upload(int fd,String ip,Map<String,String> q,long frame) throws Exception {
        final Session s; final FileState f;
        synchronized(this) {
            s=incoming;
            f=ticket(ip,q);
            if(f==null || f.done || f.failed) throw new Status(403,"Invalid token");
            if(f.busy) throw new Status(409,"File in progress");
            if(frame==-1 && f.item.size==0) frame=0; // HTTP request without framing has an empty body
            if(frame==-1 || (frame>=0 && frame!=f.item.size)) throw new Status(400,"Wrong length");
            f.busy=true; f.tries++; f.bytes=0; s.sockets.add(fd);
        }
        boolean completed=false, retry=false;
        try(Sink out=events.destination(f.item,s)) {
            Wire.timeout(fd,0); // an admitted file is interrupted explicitly, not by UI/idle timers
            Wire.receiveFile(fd,frame,out.fd(),f.item.size,count->progress(s,f.item.id,count));
            if(f.item.hash!=null && !out.matches(f.item.hash)) {
                retry=f.tries<3; throw new Status(422,"SHA-256 mismatch");
            }
            if(!out.isMedia()) throw new Status(415,"Content is not the offered media");
            check(s); out.commit(); completed=true;
        } catch(IOException e) { throw new Status(500,reason(e)); }
        finally {
            synchronized(this) {
                f.busy=false; f.done=completed; f.failed=!completed && !retry; s.sockets.remove(fd);
                if(allDone(s)) {
                    String result="Received";
                    for(FileState x:s.files.values()) if(x.failed) { result="Failed: "+x.item.name; break; }
                    end(s,result);
                }
            }
        }
        respond(fd,200,null);
    }
    private static boolean allDone(Session s) {
        for(FileState f:s.files.values()) if(!f.done && !f.failed) return false; return true;
    }
    private synchronized void progress(Session s,String id,long count) {
        if(!open || s.cancelled || (s!=incoming && !outgoing.containsKey(s.key))) return;
        FileState f=s.files.get(id); f.bytes=count;
        double size=0,sent=0;
        for(FileState x:s.files.values()) { size+=x.item.size; sent+=x.bytes; }
        s.percent=size==0?0:(int)(sent/size*100);
        events.update(s.key,s.peer.alias+" · "+f.item.name,s.percent,false);
    }
    private synchronized void end(Session s,String label) {
        if(s==null || (s!=incoming && outgoing.remove(s.key)==null)) return;
        stop(s);
        if(s==incoming) incoming=null;
        else {
            if(--s.batch.live==0) s.batch.interruptIO();
            if(!s.started) s.batch.release(); // a cancelled queued job will never get a worker
        }
        retireIfIdle();
        events.update(s.key,s.peer.alias+" · "+label,"Sent".equals(label)||"Received".equals(label)?100:s.percent,true);
    }
    private Session cancellable(String ip,String id) {
        // A cancel without sessionId is valid for a pending incoming offer.
        Session match=incoming!=null && incoming.peer.ip.equals(ip) && Objects.equals(incoming.remoteId,id)?incoming:null;
        if(id!=null) for(Session s:outgoing.values()) if(s.peer.ip.equals(ip) && id.equals(s.remoteId)) {
            if(match!=null) return null; // same IP + remote ID cannot identify one of two peers
            match=s;
        }
        return match;
    }
    private synchronized void remoteCancel(String ip,String id) throws Status {
        Session s=cancellable(ip,id);
        if(s==null) throw new Status(403,"Unknown session");
        end(s,"Cancelled");
    }
    public synchronized void cancel(String key) {
        Session s=incoming!=null && incoming.key.equals(key)?incoming:outgoing.get(key);
        if(s==null) return;
        boolean notify=s!=incoming?s.offered:s.remoteId!=null;
        end(s,"Cancelled"); if(notify) notifyCancel(s);
    }
    private void notifyCancel(Session s) {
        if(!"http".equals(s.peer.protocol)) return;
        final String id=s.remoteId;
        thread(()->{
            int fd=-1;
            try {
                fd=Wire.socket(); Wire.connect(fd,local,s.peer.ip,s.peer.port,1000); Wire.timeout(fd,2);
                String path=API+"cancel"+(id==null?"":"?sessionId="+enc(id));
                Wire.write(fd,buf("POST "+path+" HTTP/1.1\r\nHost: "+s.peer.ip+":"+s.peer.port+
                        "\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"));
                Wire.header(fd,true);
            } catch(Exception ignored) {} finally { if(fd>=0) Wire.close(fd); }
        });
    }
    public synchronized void enterPin(String key,String value) {
        Session s=outgoing.get(key);
        if(s!=null && s.pinPrompt!=null) {
            s.enteredPin=value; s.pinPrompt.countDown();
        }
    }
    public void send(Peer peer,int count,Selection selection) { send(Collections.singletonList(peer),count,selection); }
    public synchronized boolean send(List<Peer> peers,int count,Selection selection) {
        if(!visible || count<1 || count>MAX_FILES || peers.isEmpty() || peers.size()>MAX_TARGETS-outgoing.size()) {
            events.problem("Cannot start transfer"); return false;
        }
        Set<String> ids=new HashSet<>(), addresses=new HashSet<>();
        for(Peer p:peers) if(!"http".equals(p.protocol) || !ids.add(p.fingerprint) ||
                !addresses.add(p.ip+":"+p.port) || sendingTo(p)) {
            events.problem("Unavailable recipient: "+p.alias); return false;
        }
        Batch batch=new Batch(peers.size(),count,selection);
        for(Peer p:peers) {
            Session s=new Session(p,batch); outgoing.put(s.key,s);
            events.update(s.key,p.alias+" · Queued",0,false);
        }
        dispatch(); return true;
    }
    private void dispatch() { // endpoint lock held; queued jobs are already owned by the service
        for(Session s:outgoing.values()) {
            if(sendWorkers>=PARALLEL_SENDS) return;
            if(s.started) continue;
            s.started=true; ++sendWorkers;
            events.update(s.key,s.peer.alias+" · …",0,false);
            thread(()->sendFiles(s));
        }
    }
    private void sendFiles(Session s) {
        Batch batch=s.batch;
        try {
            check(s); batch.load(); check(s);
            JSONObject files=new JSONObject();
            for(Source source:batch.sources.values()) {
                Item f=source.item(); files.put(f.id,f.json()); s.files.put(f.id,new FileState(f));
            }
            JSONObject offer=new JSONObject().put("info",info()).put("files",files);
            Reply r; String supplied=null;
            for(;;) {
                synchronized(this) { check(s); s.offered=true; }
                r=request(s.peer.ip,s.peer.port,"prepare-upload"+(supplied==null?"":"?pin="+enc(supplied)),
                        offer,null,s,0,0);
                if(r.code!=401) break;
                CountDownLatch l;
                synchronized(this) {
                    check(s); s.enteredPin=null; s.pinPrompt=l=new CountDownLatch(1); events.needPin(s.key,s.peer);
                }
                l.await(); if((supplied=s.enteredPin)==null) throw new IOException("Cancelled");
            }
            if(r.code==204) { end(s,"No files requested"); return; }
            if(r.code!=200) throw new IOException("HTTP "+r.code);
            JSONObject result=r.json(),tokens=result.getJSONObject("files");
            synchronized(this) {
                check(s); s.remoteId=text(result,"sessionId",256);
                for(Iterator<String> it=s.files.keySet().iterator();it.hasNext();) if(!tokens.has(it.next())) it.remove();
            }
            for(Iterator<String> it=tokens.keys();it.hasNext();) {
                String id=it.next(); Source source=batch.sources.get(id);
                if(source==null) throw new IOException("Unknown file ID");
                String path="upload?sessionId="+enc(s.remoteId)+"&fileId="+enc(id)+"&token="+enc(text(tokens,id,512));
                for(int attempt=1;;attempt++) {
                    r=request(s.peer.ip,s.peer.port,path,null,source,s,0,0);
                    if(r.code==200) break;
                    if(r.code!=422 || attempt>=3) throw new IOException("HTTP "+r.code);
                }
            }
            end(s,"Sent");
        } catch(Exception e) {
            synchronized(this) {
                if(outgoing.containsKey(s.key)) { boolean notify=s.remoteId!=null; end(s,reason(e)); if(notify) notifyCancel(s); }
            }
        } finally {
            try { batch.release(); }
            finally { synchronized(this) { --sendWorkers; dispatch(); } }
        }
    }
}
