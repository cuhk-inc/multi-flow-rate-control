/* Hop-by-hop file transfer: raw UDP or TCP, no coding.
 * source -> relay -> relay -> dest. Loss is applied before each send.
 * Payload bytes are deterministic, so the receiver can be checked with sha256sum.
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static void mkdir_ok(const char *dir)
{
    if (mkdir(dir, 0755) != 0 && errno != EEXIST)
        perror(dir);
}

static int read_full(int fd, void *buf, size_t n);

#define PAYLOAD 1370
#define MAGIC 0x48584431u

struct pkt {
    uint32_t magic;
    uint16_t type; /* 1 data, 2 end */
    uint16_t flow;
    uint32_t seq;
    uint32_t len;
    uint8_t data[PAYLOAD];
} __attribute__((packed));

static uint64_t loss_state = 0;
static atomic_ullong tcp_rx_bytes;
static atomic_ullong tcp_tx_bytes;
static atomic_ullong tcp_dst_bytes;
static volatile sig_atomic_t tcp_stop;

static void on_tcp_stop(int sig)
{
    (void)sig;
    tcp_stop = 1;
}

static void name_self(const char *name)
{
    pthread_setname_np(pthread_self(), name);
}

static int should_drop(int pct)
{
    if (pct <= 0)
        return 0;
    loss_state = loss_state * 6364136223846793005ULL + 1442695040888963407ULL;
    return (loss_state % 100) < (uint64_t)pct;
}

static void fill_payload(uint8_t *dst, int flow, uint32_t seq, uint32_t len)
{
    uint32_t i;
    for (i = 0; i < len; i++)
        dst[i] = (uint8_t)(seq * 3 + flow * 5 + i * 7 + 11);
}

static double now_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static void pace(double t0, uint64_t wire, double bytes_per_sec)
{
    double ahead;
    struct timespec sl;
    if (bytes_per_sec <= 0)
        return;
    ahead = (double)wire / bytes_per_sec - (now_sec() - t0);
    if (ahead > 0.0002) {
        sl.tv_sec = (time_t)ahead;
        sl.tv_nsec = (long)((ahead - (double)sl.tv_sec) * 1e9);
        nanosleep(&sl, NULL);
    }
}

static int parse_hostport(const char *s, struct sockaddr_in *a)
{
    char buf[128], *colon;
    memset(a, 0, sizeof(*a));
    a->sin_family = AF_INET;
    snprintf(buf, sizeof(buf), "%s", s);
    colon = strrchr(buf, ':');
    if (!colon)
        return -1;
    *colon = 0;
    a->sin_port = htons((uint16_t)atoi(colon + 1));
    return inet_pton(AF_INET, buf, &a->sin_addr) == 1 ? 0 : -1;
}

static int udp_bind(const char *hostport)
{
    int fd, one = 1;
    struct sockaddr_in a;
    if (parse_hostport(hostport, &a) != 0)
        return -1;
    fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0)
        return -1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    int sz = 8 * 1024 * 1024;
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &sz, sizeof(sz));
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sz, sizeof(sz));
    if (bind(fd, (struct sockaddr *)&a, sizeof(a)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static const char *need(int argc, char **argv, const char *key)
{
    int i;
    for (i = 0; i < argc - 1; i++) {
        if (strcmp(argv[i], key) == 0)
            return argv[i + 1];
    }
    fprintf(stderr, "missing %s\n", key);
    exit(2);
}

static int opt_int(int argc, char **argv, const char *key, int def)
{
    int i;
    for (i = 0; i < argc - 1; i++) {
        if (strcmp(argv[i], key) == 0)
            return atoi(argv[i + 1]);
    }
    return def;
}

static void run_udp_source(int argc, char **argv)
{
    const char *bind_ip = need(argc, argv, "--bind");
    const char *to = need(argc, argv, "--to");
    int flows = opt_int(argc, argv, "--flows", 1);
    uint64_t nbytes = strtoull(need(argc, argv, "--bytes"), NULL, 10);
    int rate = opt_int(argc, argv, "--rate-mbps", 600);
    int loss = opt_int(argc, argv, "--loss", 0);
    uint32_t pkts = (uint32_t)(nbytes / PAYLOAD);
    struct sockaddr_in dst, local;
    int fd, f;
    uint32_t seq;
    uint64_t sent_pkts = 0, dropped = 0, wire = 0;
    double t0;
    if (nbytes % PAYLOAD || pkts == 0) {
        fprintf(stderr, "bytes must be a positive multiple of %d\n", PAYLOAD);
        exit(2);
    }
    name_self("hx-src");
    fd = socket(AF_INET, SOCK_DGRAM, 0);
    memset(&local, 0, sizeof(local));
    local.sin_family = AF_INET;
    inet_pton(AF_INET, bind_ip, &local.sin_addr);
    bind(fd, (struct sockaddr *)&local, sizeof(local));
    parse_hostport(to, &dst);
    t0 = now_sec();
    for (seq = 0; seq < pkts; seq++) {
        for (f = 0; f < flows; f++) {
            struct pkt p;
            memset(&p, 0, sizeof(p));
            p.magic = htonl(MAGIC);
            p.type = htons(1);
            p.flow = htons((uint16_t)f);
            p.seq = htonl(seq);
            p.len = htonl(PAYLOAD);
            fill_payload(p.data, f, seq, PAYLOAD);
            wire += sizeof(p);
            if (should_drop(loss))
                dropped++;
            else if (sendto(fd, &p, sizeof(p), 0, (struct sockaddr *)&dst, sizeof(dst)) == (ssize_t)sizeof(p))
                sent_pkts++;
            pace(t0, wire, rate * 1e6 / 8.0);
        }
    }
    for (f = 0; f < flows; f++) {
        int n;
        struct pkt p;
        memset(&p, 0, 16);
        p.magic = htonl(MAGIC);
        p.type = htons(2);
        p.flow = htons((uint16_t)f);
        p.seq = htonl(pkts);
        for (n = 0; n < 8; n++)
            sendto(fd, &p, 16, 0, (struct sockaddr *)&dst, sizeof(dst));
    }
    printf("role=source mode=udp flows=%d bytes_each=%" PRIu64 " sent=%" PRIu64
           " payload_bytes=%" PRIu64 " loss_drops=%" PRIu64 " elapsed_ms=%.0f\n",
           flows, nbytes, sent_pkts, sent_pkts * (uint64_t)PAYLOAD, dropped,
           (now_sec() - t0) * 1000.0);
    close(fd);
}

static void run_udp_relay(int argc, char **argv)
{
    const char *bind_hp = need(argc, argv, "--bind");
    const char *send_ip = need(argc, argv, "--send-bind");
    const char *to = need(argc, argv, "--to");
    int loss = opt_int(argc, argv, "--loss", 0);
    int flows = opt_int(argc, argv, "--flows", 1);
    int fd = udp_bind(bind_hp);
    int outfd;
    struct sockaddr_in dst, local;
    uint64_t in_pkts = 0, out_pkts = 0, dropped = 0;
    int ends = 0;
    uint8_t *got;
    double t0 = 0, drain_until = 0;
    struct timeval tv;
    if (fd < 0) {
        perror("bind");
        exit(1);
    }
    outfd = socket(AF_INET, SOCK_DGRAM, 0);
    memset(&local, 0, sizeof(local));
    local.sin_family = AF_INET;
    inet_pton(AF_INET, send_ip, &local.sin_addr);
    if (bind(outfd, (struct sockaddr *)&local, sizeof(local)) != 0) {
        perror("send-bind");
        exit(1);
    }
    parse_hostport(to, &dst);
    got = calloc((size_t)flows, 1);
    name_self("hx-rly");
    tv.tv_sec = 1;
    tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    printf("role=relay mode=udp listen=%s\n", bind_hp);
    fflush(stdout);
    for (;;) {
        struct pkt p;
        ssize_t n = recv(fd, &p, sizeof(p), 0);
        uint16_t type, flow;
        if (n < 0) {
            if ((errno == EAGAIN || errno == EWOULDBLOCK) && drain_until && now_sec() >= drain_until)
                break;
            continue;
        }
        if (n < 16 || ntohl(p.magic) != MAGIC)
            continue;
        if (t0 == 0)
            t0 = now_sec();
        type = ntohs(p.type);
        flow = ntohs(p.flow);
        if (type == 1)
            in_pkts++;
        if (type == 1 && should_drop(loss)) {
            dropped++;
            continue;
        }
        if (sendto(outfd, &p, (size_t)n, 0, (struct sockaddr *)&dst, sizeof(dst)) == n) {
            if (type == 1)
                out_pkts++;
        }
        if (type == 2 && flow < flows && !got[flow]) {
            got[flow] = 1;
            if (++ends >= flows)
                drain_until = now_sec() + 2.0;
        }
    }
    printf("role=relay mode=udp in=%" PRIu64 " out=%" PRIu64
           " rx_bytes=%" PRIu64 " tx_bytes=%" PRIu64 " loss_drops=%" PRIu64
           " elapsed_ms=%.0f\n",
           in_pkts, out_pkts, in_pkts * (uint64_t)PAYLOAD, out_pkts * (uint64_t)PAYLOAD,
           dropped, t0 ? (now_sec() - t0) * 1000.0 : 0);
}

static void run_udp_dest(int argc, char **argv)
{
    const char *bind_hp = need(argc, argv, "--bind");
    const char *dir = need(argc, argv, "--dir");
    int flows = opt_int(argc, argv, "--flows", 1);
    uint64_t nbytes = strtoull(need(argc, argv, "--bytes"), NULL, 10);
    uint32_t pkts = (uint32_t)(nbytes / PAYLOAD);
    int fd = udp_bind(bind_hp);
    int *out;
    uint8_t *seen;
    uint8_t *got_end;
    uint64_t uniq = 0, content_bad = 0;
    int ends = 0;
    double t0 = 0, t1 = 0;
    struct timeval tv;
    if (fd < 0 || nbytes % PAYLOAD) {
        perror("dest");
        exit(1);
    }
    name_self("hx-dst");
    printf("role=dest mode=udp listen=%s\n", bind_hp);
    fflush(stdout);
    tv.tv_sec = 5;
    tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    out = calloc((size_t)flows, sizeof(int));
    got_end = calloc((size_t)flows, 1);
    seen = calloc(((size_t)flows * pkts + 7) / 8, 1);
    mkdir_ok(dir);
    for (;;) {
        struct pkt p;
        ssize_t n = recv(fd, &p, sizeof(p), 0);
        uint16_t type, flow;
        uint32_t seq;
        size_t bit;
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (ends >= flows && t0)
                    break;
                continue;
            }
            break;
        }
        if (n < 16 || ntohl(p.magic) != MAGIC)
            continue;
        type = ntohs(p.type);
        flow = ntohs(p.flow);
        seq = ntohl(p.seq);
        if (flow >= flows)
            continue;
        if (t0 == 0)
            t0 = now_sec();
        t1 = now_sec();
        if (type == 2) {
            if (!got_end[flow]) {
                got_end[flow] = 1;
                ends++;
            }
            continue;
        }
        if (type != 1 || seq >= pkts || ntohl(p.len) != PAYLOAD)
            continue;
        bit = (size_t)flow * pkts + seq;
        if (seen[bit / 8] & (uint8_t)(1u << (bit % 8)))
            continue;
        seen[bit / 8] |= (uint8_t)(1u << (bit % 8));
        if (out[flow] == 0) {
            char path[512];
            snprintf(path, sizeof(path), flows == 1 ? "%s/out.bin" : "%s/out.bin.%d", dir, flow);
            out[flow] = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
            if (out[flow] < 0) {
                perror(path);
                exit(1);
            }
            if (ftruncate(out[flow], (off_t)nbytes) != 0) {
                perror("truncate");
                exit(1);
            }
        }
        if (pwrite(out[flow], p.data, PAYLOAD, (off_t)seq * PAYLOAD) != PAYLOAD) {
            perror("pwrite");
            exit(1);
        }
        {
            uint8_t expect[PAYLOAD];
            fill_payload(expect, flow, seq, PAYLOAD);
            if (memcmp(p.data, expect, PAYLOAD) != 0)
                content_bad++;
        }
        uniq++;
    }
    printf("role=dest mode=udp flows=%d bytes_each=%" PRIu64 " uniq=%" PRIu64
           " rx_bytes=%" PRIu64 " missing=%" PRIu64 " content_bad=%" PRIu64
           " ends=%d elapsed_ms=%.0f\n",
           flows, nbytes, uniq, uniq * (uint64_t)PAYLOAD,
           (uint64_t)flows * pkts - uniq, content_bad, ends,
           t0 ? (t1 - t0) * 1000.0 : 0);
}

static void run_check(int argc, char **argv)
{
    const char *dir = need(argc, argv, "--dir");
    int flows = opt_int(argc, argv, "--flows", 1);
    uint64_t nbytes = strtoull(need(argc, argv, "--bytes"), NULL, 10);
    uint32_t pkts = (uint32_t)(nbytes / PAYLOAD);
    int bad = 0, f;
    for (f = 0; f < flows; f++) {
        char path[512];
        int fd;
        uint32_t seq;
        snprintf(path, sizeof(path), flows == 1 ? "%s/out.bin" : "%s/out.bin.%d", dir, f);
        fd = open(path, O_RDONLY);
        if (fd < 0) {
            bad++;
            continue;
        }
        for (seq = 0; seq < pkts; seq++) {
            uint8_t got[PAYLOAD], expect[PAYLOAD];
            if (read_full(fd, got, PAYLOAD) != 0) {
                bad++;
                break;
            }
            fill_payload(expect, f, seq, PAYLOAD);
            if (memcmp(got, expect, PAYLOAD) != 0) {
                bad++;
                break;
            }
        }
        close(fd);
    }
    printf("role=check flows=%d bad=%d\n", flows, bad);
}

/* TCP */
static int tcp_listen(const char *hostport)
{
    int fd, one = 1;
    struct sockaddr_in a;
    if (parse_hostport(hostport, &a) != 0)
        return -1;
    fd = socket(AF_INET, SOCK_STREAM, 0);
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    if (bind(fd, (struct sockaddr *)&a, sizeof(a)) != 0 || listen(fd, 2048) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int tcp_connect(const char *bind_ip, const char *to)
{
    int fd, one = 1;
    struct sockaddr_in local, dst;
    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    if (bind_ip && bind_ip[0]) {
        memset(&local, 0, sizeof(local));
        local.sin_family = AF_INET;
        inet_pton(AF_INET, bind_ip, &local.sin_addr);
        bind(fd, (struct sockaddr *)&local, sizeof(local));
    }
    parse_hostport(to, &dst);
    if (connect(fd, (struct sockaddr *)&dst, sizeof(dst)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int write_all(int fd, const void *buf, size_t n)
{
    const uint8_t *p = buf;
    while (n) {
        ssize_t w = write(fd, p, n);
        if (w < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        p += w;
        n -= (size_t)w;
    }
    return 0;
}

static int read_full(int fd, void *buf, size_t n)
{
    uint8_t *p = buf;
    while (n) {
        ssize_t r = read(fd, p, n);
        if (r == 0)
            return -1;
        if (r < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        p += r;
        n -= (size_t)r;
    }
    return 0;
}

static void run_tcp_source(int argc, char **argv)
{
    const char *bind_ip = need(argc, argv, "--bind");
    const char *to = need(argc, argv, "--to");
    int flows = opt_int(argc, argv, "--flows", 1);
    uint64_t nbytes = strtoull(need(argc, argv, "--bytes"), NULL, 10);
    int rate = opt_int(argc, argv, "--rate-mbps", 600);
    int *fds;
    uint64_t left_each;
    uint64_t wire = 0;
    double t0;
    int f;
    uint32_t seq = 0;
    if (nbytes % PAYLOAD) {
        fprintf(stderr, "bytes must be a multiple of %d\n", PAYLOAD);
        exit(2);
    }
    name_self("hx-src");
    fds = calloc((size_t)flows, sizeof(int));
    for (f = 0; f < flows; f++) {
        uint32_t id = htonl((uint32_t)f);
        fds[f] = tcp_connect(bind_ip, to);
        if (fds[f] < 0) {
            perror("connect");
            exit(1);
        }
        if (write_all(fds[f], &id, 4) != 0)
            exit(1);
    }
    left_each = nbytes;
    t0 = now_sec();
    while (left_each) {
        for (f = 0; f < flows; f++) {
            uint8_t buf[PAYLOAD];
            fill_payload(buf, f, seq, PAYLOAD);
            if (write_all(fds[f], buf, PAYLOAD) != 0) {
                perror("write");
                exit(1);
            }
            wire += PAYLOAD;
            pace(t0, wire, rate * 1e6 / 8.0);
        }
        left_each -= PAYLOAD;
        seq++;
    }
    for (f = 0; f < flows; f++) {
        shutdown(fds[f], SHUT_WR);
        close(fds[f]);
    }
    printf("role=source mode=tcp flows=%d bytes_each=%" PRIu64 " wire=%" PRIu64
           " elapsed_ms=%.0f\n",
           flows, nbytes, wire, (now_sec() - t0) * 1000.0);
}

struct relay_arg {
    int in;
    char to[128];
    char bind_ip[64];
};

static void *relay_thread(void *argp)
{
    struct relay_arg *a = argp;
    int out = tcp_connect(a->bind_ip, a->to);
    uint8_t buf[65536];
    name_self("hx-rly");
    if (out < 0)
        exit(1);
    for (;;) {
        ssize_t n = read(a->in, buf, sizeof(buf));
        if (n == 0)
            break;
        if (n < 0) {
            if (errno == EINTR)
                continue;
            break;
        }
        atomic_fetch_add(&tcp_rx_bytes, (unsigned long long)n);
        if (write_all(out, buf, (size_t)n) != 0)
            break;
        atomic_fetch_add(&tcp_tx_bytes, (unsigned long long)n);
    }
    shutdown(out, SHUT_WR);
    close(out);
    close(a->in);
    free(a);
    return NULL;
}

static void run_tcp_relay(int argc, char **argv)
{
    const char *bind_hp = need(argc, argv, "--bind");
    const char *to = need(argc, argv, "--to");
    const char *bind_ip = need(argc, argv, "--bind-ip");
    int lfd = tcp_listen(bind_hp);
    struct sigaction sa;
    if (lfd < 0) {
        perror("listen");
        exit(1);
    }
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_tcp_stop;
    sigaction(SIGTERM, &sa, NULL);
    name_self("hx-rly");
    printf("role=relay mode=tcp listen=%s\n", bind_hp);
    fflush(stdout);
    while (!tcp_stop) {
        struct pollfd pfd;
        int in;
        struct relay_arg *a;
        pthread_t th;
        pfd.fd = lfd;
        pfd.events = POLLIN;
        if (poll(&pfd, 1, 200) <= 0)
            continue;
        in = accept(lfd, NULL, NULL);
        if (in < 0)
            continue;
        a = calloc(1, sizeof(*a));
        a->in = in;
        snprintf(a->to, sizeof(a->to), "%s", to);
        snprintf(a->bind_ip, sizeof(a->bind_ip), "%s", bind_ip);
        pthread_create(&th, NULL, relay_thread, a);
        pthread_detach(th);
    }
    sleep(1);
    printf("role=relay mode=tcp rx_bytes=%llu tx_bytes=%llu\n",
           (unsigned long long)atomic_load(&tcp_rx_bytes),
           (unsigned long long)atomic_load(&tcp_tx_bytes));
    fflush(stdout);
}

struct dest_arg {
    int fd;
    char dir[512];
    uint64_t nbytes;
    int flows;
};

static pthread_mutex_t dest_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t dest_cv = PTHREAD_COND_INITIALIZER;
static int dest_done = 0;

static void *dest_thread(void *argp)
{
    struct dest_arg *a = argp;
    uint32_t id = 0;
    int out;
    char path[512];
    uint64_t left = a->nbytes;
    uint8_t buf[65536];
    name_self("hx-dst");
    if (read_full(a->fd, &id, 4) != 0) {
        close(a->fd);
        free(a);
        return NULL;
    }
    id = ntohl(id);
    snprintf(path, sizeof(path), a->flows == 1 ? "%s/out.bin" : "%s/out.bin.%u", a->dir, id);
    out = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (out < 0) {
        close(a->fd);
        free(a);
        return NULL;
    }
    while (left) {
        size_t chunk = left > sizeof(buf) ? sizeof(buf) : (size_t)left;
        ssize_t n = read(a->fd, buf, chunk);
        if (n <= 0)
            break;
        if (write_all(out, buf, (size_t)n) != 0)
            break;
        atomic_fetch_add(&tcp_dst_bytes, (unsigned long long)n);
        left -= (uint64_t)n;
    }
    close(out);
    close(a->fd);
    pthread_mutex_lock(&dest_mu);
    dest_done++;
    pthread_cond_signal(&dest_cv);
    pthread_mutex_unlock(&dest_mu);
    free(a);
    return NULL;
}

static void run_tcp_dest(int argc, char **argv)
{
    const char *bind_hp = need(argc, argv, "--bind");
    const char *dir = need(argc, argv, "--dir");
    int flows = opt_int(argc, argv, "--flows", 1);
    uint64_t nbytes = strtoull(need(argc, argv, "--bytes"), NULL, 10);
    int lfd = tcp_listen(bind_hp);
    int got = 0;
    double t0 = 0, t1 = 0;
    if (lfd < 0) {
        perror("listen");
        exit(1);
    }
    mkdir_ok(dir);
    name_self("hx-dst");
    printf("role=dest mode=tcp listen=%s\n", bind_hp);
    fflush(stdout);
    while (got < flows) {
        int in = accept(lfd, NULL, NULL);
        struct dest_arg *a;
        pthread_t th;
        if (in < 0)
            continue;
        if (t0 == 0)
            t0 = now_sec();
        a = calloc(1, sizeof(*a));
        a->fd = in;
        a->nbytes = nbytes;
        a->flows = flows;
        snprintf(a->dir, sizeof(a->dir), "%s", dir);
        pthread_create(&th, NULL, dest_thread, a);
        pthread_detach(th);
        got++;
    }
    pthread_mutex_lock(&dest_mu);
    while (dest_done < flows)
        pthread_cond_wait(&dest_cv, &dest_mu);
    pthread_mutex_unlock(&dest_mu);
    t1 = now_sec();
    printf("role=dest mode=tcp flows=%d bytes_each=%" PRIu64 " accepted=%d"
           " rx_bytes=%llu elapsed_ms=%.0f\n",
           flows, nbytes, got, (unsigned long long)atomic_load(&tcp_dst_bytes),
           t0 ? (t1 - t0) * 1000.0 : 0);
}

int main(int argc, char **argv)
{
    const char *role;
    int tcp;
    if (argc < 3) {
        fprintf(stderr, "usage: hop_xfer source|relay|dest --tcp? ...\n");
        return 2;
    }
    role = argv[1];
    tcp = 0;
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--tcp") == 0)
            tcp = 1;
    }
    if (strcmp(role, "source") == 0)
        tcp ? run_tcp_source(argc, argv) : run_udp_source(argc, argv);
    else if (strcmp(role, "relay") == 0)
        tcp ? run_tcp_relay(argc, argv) : run_udp_relay(argc, argv);
    else if (strcmp(role, "dest") == 0)
        tcp ? run_tcp_dest(argc, argv) : run_udp_dest(argc, argv);
    else if (strcmp(role, "check") == 0)
        run_check(argc, argv);
    else {
        fprintf(stderr, "unknown role\n");
        return 2;
    }
    return 0;
}
