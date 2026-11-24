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
import java.util.concurrent.TimeUnit;

/** One visible screen owns one endpoint. No singleton, service, timer or receiver can restart it. */
public final class LocalSend implements Closeable {
    public static final int PORT = 53317, MAX_FILES = 256;
    private static final int MAX_JSON = 512 * 1024, MAX_CLIENTS = 24;
    private static final String API = "/api/localsend/v2/";
    private final String local, fingerprint, alias, model, pin;
    private final Events events;
    private final Set<Integer> clients = new HashSet<>();
    private final Map<String, Long> probes = new LinkedHashMap<>();
    private final Map<String, long[]> pinFailures = new LinkedHashMap<>();
    private int listener = -1, udp = -1, port, registering;
    private volatile boolean open;
    private Session incoming, outgoing;

    public interface Events {
        void peer(Peer peer);
        void offer(String key, Peer peer, List<Item> files);
        void needPin(String key, Peer peer);
        void update(String key, String label, int percent, boolean finished);
        void problem(String reason);
        Sink destination(Item item) throws IOException;
    }
    /** The owner keeps the FD valid until close. Commit makes a verified file visible. */
    public interface Sink extends Closeable {
        int fd();
        boolean matches(String sha256) throws IOException;
        void commit() throws IOException;
    }
    public interface Source extends Closeable {
        Item item();
        int fd();
        long offset();
    }
    public interface Selection { Source open(int index) throws IOException; }
    public static final class Peer {
        public final String alias, ip, fingerprint, protocol;
        public final int port;
        Peer(JSONObject j, String ip, int port, String protocol) throws Exception {
            this.alias = text(j,"alias",256); this.fingerprint = text(j,"fingerprint",256);
            if (!text(j,"version",32).startsWith("2.") || port < 1 || port > 65535 ||
                    !("http".equals(protocol) || "https".equals(protocol))) throw new JSONException("Invalid peer");
            this.ip = ip; this.port = port; this.protocol = protocol;
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
        int tries; boolean busy, done; long bytes;
        FileState(Item i) { item=i; }
    }
    private static final class Session {
        final String key = uuid(); final Peer peer;
        final Map<String,FileState> files = new LinkedHashMap<>();
        final Set<Integer> sockets = new HashSet<>();
        final CountDownLatch decision = new CountDownLatch(1);
        CountDownLatch pinPrompt; String enteredPin, remoteId;
        Set<String> selected; boolean cancelled, offered;
        long activity = now();
        Session(Peer p) { peer=p; }
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
            try { e.udp=Wire.multicast(local); }
            catch (IOException noGroup) { events.problem("Multicast: "+reason(noGroup)); }
            e.open=true;
            thread(e::loop);
            return e;
        } catch (RuntimeException ex) { e.close(); throw ex; }
    }
    public int port() { return port; }
    public boolean isOpen() { return open; }
    private JSONObject info() throws JSONException {
        return new JSONObject().put("alias",alias).put("version","2.2").put("deviceModel",model)
                .put("deviceType","mobile").put("fingerprint",fingerprint).put("port",port)
                .put("protocol","http").put("download",false);
    }
    @Override public synchronized void close() {
        open=false;
        if(listener>=0) { Wire.close(listener); listener=-1; }
        if(udp>=0) { Wire.close(udp); udp=-1; }
        for(int fd:clients) Wire.shutdown(fd); // each worker removes & closes exactly once
        // A verified peer may already be waiting between uploads. Only send the
        // opaque cancellation, never register or open a listener after closing.
        if(incoming!=null && incoming.remoteId!=null) notifyCancel(incoming);
        if(outgoing!=null && outgoing.offered) notifyCancel(outgoing);
        stop(incoming); stop(outgoing); incoming=outgoing=null; notifyAll();
    }
    private void stop(Session s) {
        if(s==null) return;
        s.cancelled=true; s.decision.countDown();
        if(s.pinPrompt!=null) s.pinPrompt.countDown();
        for(int fd:s.sockets) Wire.shutdown(fd);
    }
    private void loop() {
        try {
            byte[] beacon=buf(info().put("announce",true).toString());
            int burst=0; long next=now()+100;
            while(true) {
                byte[] packet=null;
                synchronized(this) {
                    if(!open) return;
                    if(udp>=0) {
                        try {
                            if(now()>=next) {
                                Wire.announce(udp,beacon);
                                next=now()+(burst==0?500:burst==1?2000:30000); ++burst;
                            }
                            packet=Wire.datagram(udp);
                        } catch(IOException ex) { Wire.close(udp); udp=-1; events.problem("Multicast: "+reason(ex)); }
                    }
                    for(int i=0;i<8;i++) {
                        int fd=Wire.accept(listener); if(fd<0) break;
                        if(clients.size()>=MAX_CLIENTS) Wire.close(fd);
                        else { clients.add(fd); thread(()->serve(fd)); }
                    }
                    if(incoming!=null && incoming.sockets.isEmpty() && now()-incoming.activity>120000) end(incoming,"Timed out");
                    if(packet==null) wait(60);
                }
                if(packet!=null) beacon(packet);
            }
        } catch(Exception e) { synchronized(this) { if(open) { close(); events.problem(reason(e)); } } }
    }
    private void beacon(byte[] packet) {
        try {
            String s=utf(packet); int i=s.indexOf('\n'); JSONObject j=new JSONObject(s.substring(i+1));
            Peer p=described(j,s.substring(0,i));
            if(fingerprint.equals(p.fingerprint)) return;
            if("https".equals(p.protocol)) { synchronized(this) { if(open) events.peer(p); } return; }
            probe(p.ip,p.port,false);
        } catch(Exception ignored) { /* Not a LocalSend v2 announcement. */ }
    }
    private static Peer described(JSONObject j,String ip) throws Exception {
        return new Peer(j,ip,j.getInt("port"),j.getString("protocol"));
    }
    public synchronized void probe(String ip,int port,boolean manual) {
        if(!open || registering>=4 || port<1 || port>65535 || !ip.matches("[0-9.]{7,15}")) return;
        String key=ip+":"+port; Long last=probes.get(key);
        if(!manual && last!=null && now()-last<5000) return;
        if(probes.size()>=256) probes.remove(probes.keySet().iterator().next());
        probes.put(key,now()); registering++;
        thread(()->{
            try {
                Reply r=request(ip,port,"register",info(),null,null,8);
                if(r.code!=200) throw new IOException("Register: HTTP "+r.code);
                Peer p=new Peer(r.json(),ip,port,"http");
                synchronized(this) { if(open && !fingerprint.equals(p.fingerprint)) events.peer(p); }
            } catch(Exception ex) { synchronized(this) { if(open && manual) events.problem(reason(ex)); } }
            finally { synchronized(this) { registering--; } }
        });
    }
    private synchronized void check(Session s) throws IOException {
        if(!open || (s!=null && s.cancelled)) throw new IOException("Cancelled");
    }
    private synchronized int socket(Session s) throws IOException {
        check(s);
        if(clients.size()>=MAX_CLIENTS+4) throw new IOException("Busy");
        int fd=Wire.socket(); clients.add(fd); if(s!=null) s.sockets.add(fd); return fd;
    }
    private synchronized void release(int fd,Session s) {
        if(s!=null) s.sockets.remove(fd);
        clients.remove(fd); Wire.close(fd);
    }
    private Reply request(String ip,int port,String route,JSONObject json,Source source,Session s,int timeout) throws Exception {
        int fd=socket(s);
        try {
            Wire.connect(fd,local,ip,port,3500); check(s); Wire.timeout(fd,timeout);
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
    private void serve(int fd) {
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
            check(null);
            if("GET".equals(first[0]) && (path.equals(API+"info") || path.equals("/api/localsend/v1/info")))
                respond(fd,200,info());
            else if(!"POST".equals(first[0]) || !path.startsWith(API)) throw new Status(404,"Unknown route");
            else switch(path.substring(API.length())) {
                case "register":
                    Peer p=described(json(fd,frame),ip);
                    synchronized(this) { check(null); if(!p.fingerprint.equals(fingerprint)) events.peer(p); }
                    respond(fd,200,info()); break;
                case "prepare-upload": prepare(fd,ip,q.get("pin"),frame); break;
                case "upload": upload(fd,ip,q,frame); break;
                case "cancel": remoteCancel(ip,q.get("sessionId")); respond(fd,200,null); break;
                default: throw new Status(404,"Unknown route");
            }
        } catch(Exception ex) {
            try { if(open) respond(fd,ex instanceof Status?((Status)ex).code:400,
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
    private void prepare(int fd,String ip,String supplied,long frame) throws Exception {
        verifyPin(ip,supplied);
        JSONObject offer=json(fd,frame), files=offer.getJSONObject("files");
        if(files.length()==0 || files.length()>MAX_FILES) throw new Status(400,"Invalid file count");
        Session s=new Session(described(offer.getJSONObject("info"),ip));
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
            display.add(item); s.files.put(id,new FileState(item));
        }
        synchronized(this) {
            check(null); if(incoming!=null) throw new Status(409,"Busy");
            incoming=s; events.offer(s.key,s.peer,Collections.unmodifiableList(display));
        }
        boolean handedOff=false;
        try {
            long deadline=now()+90000;
            boolean answered;
            while(!(answered=s.decision.await(250,TimeUnit.MILLISECONDS)) && now()<deadline) {
                if(Wire.disconnected(fd)) throw new IOException("Sender disconnected");
            }
            JSONObject tokens=new JSONObject();
            synchronized(this) {
                if(!answered || s.cancelled || s.selected==null) throw new Status(403,"Declined");
                s.files.keySet().retainAll(s.selected);
                for(FileState f:s.files.values()) tokens.put(f.item.id,f.token);
                if(tokens.length()>0) {
                    s.remoteId=s.key; s.activity=now();
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
    private void upload(int fd,String ip,Map<String,String> q,long frame) throws Exception {
        final Session s; final FileState f;
        synchronized(this) {
            s=incoming;
            if(s==null || !s.peer.ip.equals(ip) || s.remoteId==null || !s.remoteId.equals(q.get("sessionId")))
                throw new Status(403,"Invalid session");
            f=s.files.get(q.get("fileId"));
            if(f==null || f.done || !f.token.equals(q.get("token"))) throw new Status(403,"Invalid token");
            if(f.busy) throw new Status(409,"File in progress");
            if(frame==-1 && f.item.size==0) frame=0; // HTTP request without framing has an empty body
            if(frame==-1 || (frame>=0 && frame!=f.item.size)) throw new Status(400,"Wrong length");
            f.busy=true; f.tries++; f.bytes=0; s.activity=now(); s.sockets.add(fd);
        }
        boolean completed=false, retry=false;
        try(Sink out=events.destination(f.item)) {
            Wire.receiveFile(fd,frame,out.fd(),f.item.size,count->progress(s,f.item.id,count));
            if(f.item.hash!=null && !out.matches(f.item.hash)) {
                retry=f.tries<3; throw new Status(422,"SHA-256 mismatch");
            }
            // Only fully received and verified files reach this point. Publishing
            // via a document provider must not hold up onPause's socket shutdown.
            check(s); out.commit(); completed=true;
        } catch(IOException e) { throw new Status(500,reason(e)); }
        finally {
            synchronized(this) {
                f.busy=false; f.done=completed; s.sockets.remove(fd);
                if(!completed && !retry) end(s,"Failed: "+f.item.name);
                else if(completed && allDone(s)) end(s,"Received");
            }
        }
        respond(fd,200,null);
    }
    private static boolean allDone(Session s) {
        for(FileState f:s.files.values()) if(!f.done) return false; return true;
    }
    private synchronized void progress(Session s,String id,long count) {
        if(!open || s.cancelled || (s!=incoming && s!=outgoing)) return;
        s.activity=now(); FileState f=s.files.get(id); f.bytes=count;
        double size=0,sent=0;
        for(FileState x:s.files.values()) { size+=x.item.size; sent+=x.bytes; }
        events.update(s.key,s.peer.alias+" · "+f.item.name,size==0?0:(int)(sent/size*100),false);
    }
    private synchronized void end(Session s,String label) {
        if(s==null || (s!=incoming && s!=outgoing)) return;
        stop(s); if(s==incoming) incoming=null; else outgoing=null;
        events.update(s.key,s.peer.alias+" · "+label,100,true);
    }
    private synchronized void remoteCancel(String ip,String id) throws Status {
        // A cancel without sessionId is valid for a pending incoming offer.
        for(Session s:new Session[]{incoming,outgoing}) {
            if(s!=null && s.peer.ip.equals(ip) && Objects.equals(s.remoteId,id)) { end(s,"Cancelled"); return; }
        }
        throw new Status(403,"Unknown session");
    }
    public synchronized void cancel(String key) {
        Session s=incoming!=null && incoming.key.equals(key)?incoming:outgoing;
        if(s==null || !s.key.equals(key)) return;
        boolean notify=s==outgoing?s.offered:s.remoteId!=null;
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
        if(outgoing!=null && outgoing.key.equals(key) && outgoing.pinPrompt!=null) {
            outgoing.enteredPin=value; outgoing.pinPrompt.countDown();
        }
    }
    public synchronized void send(Peer peer,int count,Selection selection) {
        if(!open || outgoing!=null || count<1 || count>MAX_FILES || !"http".equals(peer.protocol)) {
            events.problem("Cannot start transfer"); return;
        }
        Session s=outgoing=new Session(peer);
        events.update(s.key,peer.alias+" · …",0,false);
        thread(()->sendFiles(s,count,selection));
    }
    private void sendFiles(Session s,int count,Selection selection) {
        Map<String,Source> sources=new LinkedHashMap<>();
        try {
            JSONObject files=new JSONObject();
            for(int i=0;i<count;i++) {
                check(s); Source source=selection.open(i); Item f=source.item();
                sources.put(f.id,source); files.put(f.id,f.json());
                synchronized(this) { s.files.put(f.id,new FileState(f)); }
            }
            JSONObject offer=new JSONObject().put("info",info()).put("files",files);
            Reply r; String supplied=null;
            for(;;) {
                synchronized(this) { check(s); s.offered=true; }
                r=request(s.peer.ip,s.peer.port,"prepare-upload"+(supplied==null?"":"?pin="+enc(supplied)),
                        offer,null,s,100);
                if(r.code!=401) break;
                CountDownLatch l;
                synchronized(this) {
                    check(s); s.enteredPin=null; s.pinPrompt=l=new CountDownLatch(1); events.needPin(s.key,s.peer);
                }
                if(!l.await(90,TimeUnit.SECONDS) || (supplied=s.enteredPin)==null) throw new IOException("Cancelled");
            }
            if(r.code==204) { end(s,"No files requested"); return; }
            if(r.code!=200) throw new IOException("HTTP "+r.code);
            JSONObject result=r.json(),tokens=result.getJSONObject("files");
            synchronized(this) {
                check(s); s.remoteId=text(result,"sessionId",256);
                for(Iterator<String> it=s.files.keySet().iterator();it.hasNext();) if(!tokens.has(it.next())) it.remove();
            }
            for(Iterator<String> it=tokens.keys();it.hasNext();) {
                String id=it.next(); Source source=sources.get(id);
                if(source==null) throw new IOException("Unknown file ID");
                String path="upload?sessionId="+enc(s.remoteId)+"&fileId="+enc(id)+"&token="+enc(text(tokens,id,512));
                for(int attempt=1;;attempt++) {
                    r=request(s.peer.ip,s.peer.port,path,null,source,s,30);
                    if(r.code==200) break;
                    if(r.code!=422 || attempt>=3) throw new IOException("HTTP "+r.code);
                }
            }
            end(s,"Sent");
        } catch(Exception e) {
            synchronized(this) {
                if(open && outgoing==s) { boolean notify=s.remoteId!=null; end(s,reason(e)); if(notify) notifyCancel(s); }
            }
        } finally { for(Source source:sources.values()) try { source.close(); } catch(IOException ignored) {} }
    }
}
