#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <jni.h>
#include <limits.h>
#include <netinet/in.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include "picohttpparser.h"

#define FN(n) JNICALL Java_org_openqpic_localsend_Wire_##n
#define ENV JNIEnv *e, jclass cls
#define GROUP "224.0.0.167"
#define DISCOVERY_PORT 53317
#define HEADER_MAX 16384
#define BLOCK 32768
#define CHUNKED (-2)
#define UNTIL_EOF (-1)

static void fail(JNIEnv *e, const char *why) {
    if (!(*e)->ExceptionCheck(e)) (*e)->ThrowNew(e, (*e)->FindClass(e, "java/io/IOException"), why);
}
static int syserr(JNIEnv *e) { fail(e, strerror(errno)); return -1; }
static int bad(JNIEnv *e, const char *reason) { fail(e, reason); return -1; }
static jbyteArray bytes(JNIEnv *e, const void *p, size_t n) {
    jbyteArray a = (*e)->NewByteArray(e, (jsize)n);
    if (a && n) (*e)->SetByteArrayRegion(e, a, 0, (jsize)n, (const jbyte *)p);
    return a;
}
static int addr(JNIEnv *e, jstring ip, int port, struct sockaddr_in *a) {
    memset(a, 0, sizeof *a); a->sin_family = AF_INET;
    if (port < 0 || port > 65535) return bad(e, "Invalid IPv4 port");
    a->sin_port = htons((uint16_t)port);
    const char *s = (*e)->GetStringUTFChars(e, ip, NULL); /* numeric IPv4 only */
    if (!s) return -1;
    int ok = inet_pton(AF_INET, s, &a->sin_addr);
    (*e)->ReleaseStringUTFChars(e, ip, s);
    return ok == 1 ? 0 : bad(e, "Expected a numeric IPv4 address");
}
static int newsock(JNIEnv *e, int kind) {
    int s = socket(AF_INET, kind | SOCK_CLOEXEC, 0);
    return s < 0 ? syserr(e) : s;
}
static int drop(JNIEnv *e, int s) { int n = errno; close(s); errno = n; return syserr(e); }
JNIEXPORT jint FN(socket)(ENV) { return newsock(e, SOCK_STREAM); }
JNIEXPORT void FN(close)(ENV, jint fd) { close(fd); }
JNIEXPORT void FN(shutdown)(ENV, jint fd) { shutdown(fd, SHUT_RDWR); }
JNIEXPORT jboolean FN(disconnected)(ENV, jint fd) {
    char b; ssize_t n=recv(fd,&b,1,MSG_PEEK|MSG_DONTWAIT);
    return n==0 || (n<0 && errno!=EAGAIN && errno!=EWOULDBLOCK && errno!=EINTR);
}
JNIEXPORT jint FN(listen)(ENV, jstring ip, jint port) {
    struct sockaddr_in a; int one = 1;
    if (addr(e, ip, port, &a)) return -1;
    int fd = newsock(e, SOCK_STREAM | SOCK_NONBLOCK);
    if (fd < 0) return -1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    if (bind(fd, (struct sockaddr *)&a, sizeof a) || listen(fd, 24)) return drop(e, fd);
    return fd;
}
JNIEXPORT jint FN(port)(ENV, jint fd) {
    struct sockaddr_in a; socklen_t len = sizeof a;
    return getsockname(fd, (struct sockaddr *)&a, &len) ? syserr(e) : ntohs(a.sin_port);
}
static int accept_peer(JNIEnv *e, int fd) {
    int s = accept4(fd, NULL, NULL, SOCK_CLOEXEC);
    if (s < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) return syserr(e);
    return s;
}
JNIEXPORT jint FN(accept)(ENV, jint fd) { return accept_peer(e, fd); }
JNIEXPORT jint FN(awaitAccept)(ENV, jint fd, jint ms) {
    struct pollfd p = {fd, POLLIN, 0};
    int n;
    do { n = poll(&p, 1, ms); } while (n < 0 && errno == EINTR);
    if (n < 0) return syserr(e);
    if (!n) return -1;
    if (!(p.revents & POLLIN)) return bad(e, "HTTP listener closed");
    return accept_peer(e, fd);
}
JNIEXPORT jstring FN(peer)(ENV, jint fd) {
    struct sockaddr_in a; socklen_t n = sizeof a; char s[INET_ADDRSTRLEN];
    if (getpeername(fd, (struct sockaddr *)&a, &n) || !inet_ntop(AF_INET, &a.sin_addr, s, sizeof s)) {
        syserr(e); return NULL;
    }
    return (*e)->NewStringUTF(e, s);
}
JNIEXPORT jint FN(multicast)(ENV, jstring local) {
    struct sockaddr_in a; struct ip_mreq m; int one = 1; unsigned char ttl = 1;
    if (addr(e, local, DISCOVERY_PORT, &a)) return -1;
    m.imr_interface = a.sin_addr; inet_pton(AF_INET, GROUP, &m.imr_multiaddr);
    a.sin_addr.s_addr = htonl(INADDR_ANY);
    int fd = newsock(e, SOCK_DGRAM | SOCK_NONBLOCK);
    if (fd < 0) return -1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one) ||
        bind(fd, (struct sockaddr *)&a, sizeof a) ||
        setsockopt(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &m, sizeof m) ||
        setsockopt(fd, IPPROTO_IP, IP_MULTICAST_IF, &m.imr_interface, sizeof m.imr_interface) ||
        setsockopt(fd, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof ttl)) return drop(e, fd);
    return fd;
}
JNIEXPORT void FN(announce)(ENV, jint fd, jbyteArray data) {
    struct sockaddr_in a; memset(&a,0,sizeof a); a.sin_family = AF_INET; a.sin_port = htons(DISCOVERY_PORT);
    inet_pton(AF_INET, GROUP, &a.sin_addr);
    jsize n = (*e)->GetArrayLength(e, data);
    jbyte *p = (*e)->GetByteArrayElements(e, data, NULL);
    if (!p) return;
    if (sendto(fd, p, n, MSG_NOSIGNAL, (struct sockaddr *)&a, sizeof a) != n) syserr(e);
    (*e)->ReleaseByteArrayElements(e, data, p, JNI_ABORT);
}
JNIEXPORT jbyteArray FN(datagram)(ENV, jint fd) {
    char buf[INET_ADDRSTRLEN + 8192]; struct sockaddr_in from; socklen_t len = sizeof from;
    ssize_t n = recvfrom(fd, buf + INET_ADDRSTRLEN, 8192, MSG_TRUNC, (struct sockaddr *)&from, &len);
    if (n < 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) syserr(e);
        return NULL;
    }
    if (n > 8192 || !inet_ntop(AF_INET, &from.sin_addr, buf, INET_ADDRSTRLEN)) return NULL;
    size_t k = strlen(buf); buf[k++] = '\n'; memmove(buf + k, buf + INET_ADDRSTRLEN, n);
    return bytes(e, buf, k + n);
}
JNIEXPORT void FN(connect)(ENV, jint fd, jstring local, jstring ip, jint port, jint ms) {
    struct sockaddr_in a, src;
    if (addr(e, ip, port, &a) || addr(e, local, 0, &src)) return;
    int f = fcntl(fd, F_GETFL);
    if (bind(fd, (struct sockaddr *)&src, sizeof src) || f < 0 || fcntl(fd, F_SETFL, f | O_NONBLOCK)) {
        syserr(e); return;
    }
    if (connect(fd, (struct sockaddr *)&a, sizeof a)) {
        if (errno != EINPROGRESS) { syserr(e); return; }
        struct pollfd p = {fd, POLLOUT, 0}; int r = poll(&p, 1, ms), err = 0; socklen_t k = sizeof err;
        if (!r) { fail(e, "Connect timeout"); return; }
        if (r < 0 || getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &k)) { syserr(e); return; }
        if (err) { errno = err; syserr(e); return; }
    }
    if (fcntl(fd, F_SETFL, f)) syserr(e);
}
JNIEXPORT void FN(timeout)(ENV, jint fd, jint sec) {
    struct timeval t = {sec, 0};
    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &t, sizeof t) ||
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &t, sizeof t)) syserr(e);
}
static ssize_t rx(int fd, char *p, size_t n, int flags) {
    ssize_t r; do { r = recv(fd, p, n, flags); } while (r < 0 && errno == EINTR); return r;
}
static int all(JNIEnv *e, int fd, const char *p, size_t n, int network) {
    while (n) {
        ssize_t r = network ? send(fd, p, n, MSG_NOSIGNAL) : write(fd, p, n);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) return syserr(e);
        p += r; n -= (size_t)r;
    }
    return 0;
}
JNIEXPORT void FN(write)(ENV, jint fd, jbyteArray data) {
    jsize n = (*e)->GetArrayLength(e, data);
    jbyte *p = (*e)->GetByteArrayElements(e, data, NULL);
    if (p) { all(e, fd, (char *)p, n, 1); (*e)->ReleaseByteArrayElements(e, data, p, JNI_ABORT); }
}
static int eq(const char *p, size_t n, const char *s) { return strlen(s) == n && !strncasecmp(p, s, n); }
/* Peek and consume just the header. The first body bytes remain in the kernel even
 * when a peer writes header + payload in one packet. One request per connection. */
static jbyteArray read_header(JNIEnv *e, int fd, int response, int read_range, jbyteArray plain, jint plain_len) {
    char buf[HEADER_MAX], result[HEADER_MAX + 64]; size_t used = 0, used_headers;
    const char *method = "", *path = "", *msg; size_t mn = 0, pn = 0, msglen;
    int minor, status = 0; struct phr_header headers[64];
    for (;;) {
        if (used == sizeof buf) { fail(e, "HTTP header too large"); return NULL; }
        ssize_t n;
        if (plain) {
            if (plain_len <= 0 || plain_len > HEADER_MAX || plain_len > (*e)->GetArrayLength(e,plain)) {
                fail(e, "Invalid decrypted header length"); return NULL;
            }
            (*e)->GetByteArrayRegion(e,plain,0,plain_len,(jbyte *)buf);
            if ((*e)->ExceptionCheck(e)) return NULL;
            n=plain_len;
        } else n = rx(fd, buf + used, sizeof buf - used, MSG_PEEK);
        if (n <= 0) { fail(e, "Incomplete HTTP header"); return NULL; }
        used_headers = 64;
        int end = response ? phr_parse_response(buf, used+n, &minor, &status, &msg, &msglen, headers, &used_headers, used)
                           : phr_parse_request(buf, used+n, &method, &mn, &path, &pn, &minor, headers, &used_headers, used);
        if (end == -1) { fail(e, "Malformed HTTP header"); return NULL; }
        if (plain) {
            if (end != plain_len) { fail(e,"Incomplete HTTP header"); return NULL; }
            break;
        }
        size_t target = end < 0 ? used + (size_t)n : (size_t)end;
        while (used < target) {
            n = rx(fd, buf + used, target - used, 0);
            if (n <= 0) { fail(e, "Incomplete HTTP header"); return NULL; }
            used += (size_t)n;
        }
        if (end >= 0) break;
    }
    int64_t length = UNTIL_EOF;
    const char *range = NULL;
    size_t range_len = 0;
    for (size_t i = 0; i < used_headers; ++i) {
        struct phr_header h = headers[i];
        if (!h.name) { fail(e, "Folded HTTP header"); return NULL; }
        if (eq(h.name,h.name_len,"Content-Length")) {
            if (length != UNTIL_EOF || !h.value_len) goto framing_error;
            length = 0;
            for (size_t j = 0; j < h.value_len; ++j) {
                int d = h.value[j] - '0';
                if (d < 0 || d > 9 || length > (INT64_MAX - d)/10) goto framing_error;
                length = length*10 + d;
            }
        } else if (eq(h.name,h.name_len,"Transfer-Encoding")) {
            if (length != UNTIL_EOF || !eq(h.value,h.value_len,"chunked")) goto framing_error;
            length = CHUNKED;
        } else if (read_range && !range && eq(h.name,h.name_len,"Range")) {
            range = h.value;
            range_len = h.value_len;
        }
    }
    int k = response ? snprintf(result,sizeof result,"%d\n%" PRId64,status,length) :
        snprintf(result,sizeof result,"%.*s %.*s\n%" PRId64,(int)mn,method,(int)pn,path,length);
    // Video uses a separate entry point; LocalSend's request line and framing stay unchanged.
    if (k < 0 || (size_t)k >= sizeof result ||
        (range && (size_t)k+1+range_len > sizeof result)) {
        fail(e, "HTTP header too large"); return NULL;
    }
    if (range) {
        result[k++] = '\n';
        memcpy(result + k, range, range_len);
        k += (int)range_len;
    }
    return bytes(e, result, (size_t)k);
framing_error:
    fail(e, "Invalid HTTP body framing"); return NULL;
}
JNIEXPORT jbyteArray FN(header)(ENV, jint fd, jboolean response) {
    return read_header(e, fd, response, 0, NULL, 0);
}
JNIEXPORT jbyteArray FN(videoHeader)(ENV, jint fd) {
    return read_header(e, fd, 0, 1, NULL, 0);
}
JNIEXPORT jbyteArray FN(parsedHeader)(ENV, jbyteArray data, jint len, jboolean response, jboolean video) {
    if (!data) { fail(e,"Missing decrypted header"); return NULL; }
    return read_header(e, -1, response, video, data, len);
}
typedef struct {
    JNIEnv *e; jobject target; jmethodID method; int64_t last;
} meter;
static int progress(meter *m, int64_t count, int force) {
    if (!m->target) return 0;
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC,&ts);
    int64_t now = (int64_t)ts.tv_sec*1000 + ts.tv_nsec/1000000;
    if (!force && now-m->last < 200) return 0;
    if (!m->method) {
        jclass c = (*m->e)->GetObjectClass(m->e,m->target);
        m->method = (*m->e)->GetMethodID(m->e,c,"bytes","(J)V");
        (*m->e)->DeleteLocalRef(m->e,c);
        if (!m->method) return -1;
    }
    m->last = now;
    (*m->e)->CallVoidMethod(m->e,m->target,m->method,(jlong)count);
    return (*m->e)->ExceptionCheck(m->e) ? -1 : 0;
}
static int64_t body(JNIEnv *e, int fd, int64_t frame, int out, char *mem, int64_t limit, meter *m) {
    char buf[BLOCK]; int64_t total = 0;
    struct phr_chunked_decoder decoder; memset(&decoder,0,sizeof decoder); decoder.consume_trailer = 1;
    if (limit < 0 || frame < CHUNKED || frame > limit) return bad(e,"HTTP body exceeds declared size");
    while (frame < 0 || total < frame) {
        size_t want = sizeof buf;
        if (frame >= 0 && frame-total < (int64_t)want) want = (size_t)(frame-total);
        ssize_t n = rx(fd,buf,want,0);
        if (n == 0 && frame == UNTIL_EOF) break;
        if (n <= 0) return bad(e,"Interrupted HTTP body");
        size_t decoded = (size_t)n;
        ssize_t end = frame == CHUNKED ? phr_decode_chunked(&decoder,buf,&decoded) : -2;
        if (end == -1 || decoded > (uint64_t)(limit-total)) return bad(e,"Invalid HTTP body");
        if (mem) memcpy(mem + (size_t)total,buf,decoded);
        else if (all(e,out,buf,decoded,0)) return -1;
        total += decoded;
        if (progress(m,total,0)) return -1;
        if (end >= 0) break;
    }
    return progress(m,total,1) ? -1 : total;
}
JNIEXPORT jbyteArray FN(body)(ENV, jint fd, jlong frame, jint limit) {
    if (limit < 0 || limit > 1024*1024) { fail(e,"Invalid control limit"); return NULL; }
    if (frame >= 0 && frame < limit) limit = (int)frame;
    char *p = malloc((size_t)limit + 1);
    if (!p) { syserr(e); return NULL; }
    meter m = {e,NULL,NULL,0};
    int64_t n = body(e,fd,frame,-1,p,limit,&m);
    jbyteArray a = n < 0 ? NULL : bytes(e,p,(size_t)n); free(p); return a;
}
JNIEXPORT void FN(receiveFile)(ENV, jint fd, jlong frame, jint out, jlong size, jobject callback) {
    if (frame == UNTIL_EOF || (frame >= 0 && frame != size)) { fail(e,"Wrong upload length"); return; }
    meter m = {e,callback,NULL,0};
    int64_t n = body(e,fd,frame,out,NULL,size,&m);
    if (n >= 0 && n != size) fail(e,"Incomplete file");
}
JNIEXPORT void FN(sendFile)(ENV, jint fd, jint src, jlong offset, jlong size, jobject callback) {
    char buf[BLOCK]; int64_t sent = 0; meter m = {e,callback,NULL,0};
    if (offset < 0 || size < 0 || offset > INT64_MAX-size) { fail(e,"Invalid upload range"); return; }
    while (sent < size) {
        size_t want = size-sent < BLOCK ? (size_t)(size-sent) : BLOCK;
        // One immutable source can serve several peers (and retries) at independent offsets.
        ssize_t n = pread64(src,buf,want,offset+sent);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) { fail(e,"Incomplete upload source"); return; }
        if (all(e,fd,buf,(size_t)n,1)) return;
        sent += n; if (progress(&m,sent,0)) return;
    }
    progress(&m,sent,1);
}
