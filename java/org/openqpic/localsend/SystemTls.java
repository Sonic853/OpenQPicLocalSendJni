package org.openqpic.localsend;

import android.security.keystore.KeyGenParameterSpec;
import android.security.keystore.KeyProperties;
import java.io.IOException;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import javax.net.ssl.*;
import javax.security.auth.x500.X500Principal;

/** Per-install identities and scoped trust. Never changes JVM-wide trust or hostname verification. */
public final class SystemTls {
    private static SystemTls transfer, video;
    private final String alias;
    private final PrivateKey key;
    private final X509Certificate cert;
    private final SSLContext server, videoClient;
    private final boolean mutual;
    public final String fingerprint;

    public static synchronized SystemTls transfers() throws IOException {
        if (transfer == null) transfer = new SystemTls("OpenQPic.LocalSend.TLS", true);
        return transfer;
    }
    public static synchronized SystemTls video() throws IOException {
        if (video == null) video = new SystemTls("OpenQPic.Video.TLS", false);
        return video;
    }

    private SystemTls(String label, boolean mutual) throws IOException {
        alias = label;
        this.mutual = mutual;
        try {
            KeyStore store = KeyStore.getInstance("AndroidKeyStore");
            store.load(null);
            // An interrupted first-run key generation on API 23 can leave a key with no certificate.
            if (store.getCertificate(label)==null) {
                if(store.containsAlias(label)) store.deleteEntry(label);
                long now = System.currentTimeMillis(), day = 86400000L;
                KeyPairGenerator gen = KeyPairGenerator.getInstance(KeyProperties.KEY_ALGORITHM_EC, "AndroidKeyStore");
                gen.initialize(new KeyGenParameterSpec.Builder(label, KeyProperties.PURPOSE_SIGN | KeyProperties.PURPOSE_VERIFY)
                        .setAlgorithmParameterSpec(new java.security.spec.ECGenParameterSpec("secp256r1"))
                        // API 23 Conscrypt passes the TLS signature digest to NONEwithECDSA.
                        // The digest is negotiated by TLS; this key never leaves AndroidKeyStore.
                        .setDigests(KeyProperties.DIGEST_NONE, KeyProperties.DIGEST_SHA256, KeyProperties.DIGEST_SHA384, KeyProperties.DIGEST_SHA512)
                        .setCertificateSubject(new X500Principal(mutual ? "CN=LocalSend User" : "CN=OpenQPic Loopback"))
                        .setCertificateSerialNumber(new BigInteger(127, new SecureRandom()).setBit(0))
                        .setCertificateNotBefore(new Date(now-day)).setCertificateNotAfter(new Date(now+3650*day)).build());
                gen.generateKeyPair();
            }
            key = (PrivateKey)store.getKey(label, null);
            cert = (X509Certificate)store.getCertificate(label);
            checkIdentity(new X509Certificate[]{cert});
            fingerprint = digest(cert);
            server = context(null, true);
            videoClient = mutual ? null : context(fingerprint,false);
        } catch (GeneralSecurityException e) { throw new IOException("Cannot initialize "+label, e); }
    }

    static String digest(X509Certificate c) throws CertificateException {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(c.getEncoded());
            char[] hex = "0123456789ABCDEF".toCharArray(), out = new char[64];
            for (int i=0;i<hash.length;i++) { out[i*2]=hex[(hash[i]&255)>>>4]; out[i*2+1]=hex[hash[i]&15]; }
            return new String(out);
        } catch (GeneralSecurityException e) { throw new CertificateException(e); }
    }
    private static void checkIdentity(X509Certificate[] chain) throws CertificateException {
        if (chain == null || chain.length == 0 || chain[0] == null) throw new CertificateException("Missing peer certificate");
        X509Certificate leaf=chain[0];
        leaf.checkValidity();
        if (!leaf.getIssuerX500Principal().equals(leaf.getSubjectX500Principal())) throw new CertificateException("Expected a self-signed device identity");
        try { leaf.verify(leaf.getPublicKey()); } catch (GeneralSecurityException e) { throw new CertificateException(e); }
    }
    private SSLContext context(final String pin, final boolean useServer) throws GeneralSecurityException {
        X509KeyManager km = new X509KeyManager() {
            private boolean matches(String type) { return key.getAlgorithm().equalsIgnoreCase(type); }
            public String[] getClientAliases(String type, Principal[] issuers) { return matches(type) ? new String[]{alias} : null; }
            public String[] getServerAliases(String type, Principal[] issuers) { return getClientAliases(type,issuers); }
            public String chooseClientAlias(String[] types, Principal[] issuers, Socket sock) {
                if(mutual && types!=null) for(String t:types) if(matches(t)) return alias;
                return null;
            }
            public String chooseServerAlias(String type, Principal[] issuers, Socket sock) { return matches(type) ? alias : null; }
            public X509Certificate[] getCertificateChain(String a) { return alias.equals(a) ? new X509Certificate[]{cert} : null; }
            public PrivateKey getPrivateKey(String a) { return alias.equals(a) ? key : null; }
        };
        X509TrustManager tm = new X509TrustManager() {
            public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
            public void checkClientTrusted(X509Certificate[] chain,String type) throws CertificateException {
                if (!useServer || !mutual) throw new CertificateException("Unexpected client identity");
                checkIdentity(chain); // LocalSend: the verified leaf is the sender's identity, not a CA claim.
            }
            public void checkServerTrusted(X509Certificate[] chain,String type) throws CertificateException {
                if (useServer) throw new CertificateException("Unexpected server identity");
                checkIdentity(chain);
                if (pin!=null && !pin.equalsIgnoreCase(digest(chain[0]))) throw new CertificateException("Device fingerprint changed");
            }
        };
        SSLContext ctx=SSLContext.getInstance("TLS");
        ctx.init(new KeyManager[]{km},new TrustManager[]{tm},null);
        return ctx;
    }
    SSLServerSocket serverSocket() throws IOException {
        SSLServerSocket s=(SSLServerSocket)server.getServerSocketFactory().createServerSocket();
        s.setUseClientMode(false);
        s.setNeedClientAuth(mutual);
        s.setEnabledProtocols(versions(s.getSupportedProtocols()));
        return s;
    }
    private static String[] versions(String[] supported) {
        ArrayList<String> versions=new ArrayList<>();
        for(String p:supported) if("TLSv1.3".equals(p)||"TLSv1.2".equals(p)) versions.add(p);
        return versions.toArray(new String[0]);
    }
    SSLSocket wrapClient(Socket plain, String pin, boolean discovery) throws IOException {
        if (pin==null && !(discovery && mutual)) throw new IOException("Missing TLS fingerprint");
        if (pin!=null && !pin.matches("(?i)[0-9a-f]{64}")) throw new IOException("Invalid TLS fingerprint");
        try {
            if(!mutual && !fingerprint.equalsIgnoreCase(pin)) throw new IOException("Unexpected video certificate pin");
            SSLContext ctx=mutual?context(pin,false):videoClient;
            SSLSocket tls=(SSLSocket)ctx.getSocketFactory().createSocket(plain,plain.getInetAddress().getHostAddress(),plain.getPort(),true);
            tls.setUseClientMode(true);
            tls.setEnabledProtocols(versions(tls.getSupportedProtocols()));
            return tls;
        } catch(GeneralSecurityException e) { throw new IOException("Cannot configure TLS",e); }
    }
    /** Only the service's own video certificate is accepted on this loopback socket. */
    public SSLSocket openVideo(int port) throws IOException {
        if(mutual) throw new IOException("Not a loopback identity");
        Socket s=new Socket();
        try {
            s.connect(new java.net.InetSocketAddress("127.0.0.1",port),5000);
            s.setSoTimeout(15000);
            SSLSocket tls=wrapClient(s,fingerprint,false); tls.startHandshake(); return tls;
        } catch(IOException | RuntimeException e) { s.close(); throw e; }
    }
}
