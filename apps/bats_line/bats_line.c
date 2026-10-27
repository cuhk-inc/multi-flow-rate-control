/*
 * BATS 四跳线网：源 → 中继1 → 中继2 → 目的。
 *
 * 跑的是 ../bats 的 bats_encode / bats_recode / bats_decode，不是
 * wire_relay --bats-recoder identity（那只是 RS 代的单位阵乘法）。
 *
 * 原理与内存仿真一致：
 *   每个 batch 从 Ψ 抽度数，选 d 个源包，乘 d×M 的 G。G 不进包。
 *   源发出的第 j 个包，系数是 I_M 的第 j 列，载荷是 (BG) 的第 j 列。
 *   中继只混合同一个 (flow_id, generation, batch_id) 里已经收到的包；
 *   没收齐也 recode，一个都没收到就不发。系数和载荷乘同一组随机系数。
 *   同一跳、同一条流上的 recode_state 跨 batch 保持。目的端不 recode，
 *   用同一个 code_seed 和 batch_id 复原 G，再 BP，解不动再 inactivation。
 *
 * 一个 generation 是 K 个长 T 的源包。文件按这个切，最后一块用 0 补齐。
 * batch_id 在每个 generation 内从 0 重新编号；中继和目的用 flow_id 和 generation 分开。
 *
 * UDP 只多了传输需要的头：flow_id、generation、batch_id、8 字节系数、T 字节载荷。
 * 每个 DATA 都带上 file_size、K、batch 数和 generation 数。
 * 中继收齐一个 batch 就 bats_recode 并转发。收包、重编码、发送各是一个线程，
 * 并分别绑在三个核上。缺包的 batch 留到这一跳空闲达到 idle-sec 再发。
 * 目的端收齐一个 generation 就译；缺包的 generation 也留到
 * 空闲达到 idle-sec 再译。一代的缓冲在第一个包到达时分配，译完释放。各代并行译。
 * 丢包只作用在 DATA 上，发生在 sendmmsg 之前。END 不丢。
 *
 * 度数：源和目的启动时若工作目录有 psi_network.txt 就装上它，否则用 psi_line.h。
 * 目的端加 --fit-psi，这一轮仍用测量前的度数；跑完按目的端秩分布写出新的
 * psi_network.txt。把该文件放到源的工作目录后再跑，两端才一起换上新度数。
 */

#define _GNU_SOURCE

#include "bats_internal.h"
#include "psi_line.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <netinet/udp.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#include "circular_buffer.h"

#define BATS_LINE_MAGIC 0x42415453u
#define BATS_LINE_VERSION 1u
#define BATS_LINE_DATA 1u
#define BATS_LINE_END 2u
#define BATS_LINE_HDR 44
#define BATS_LINE_MAX_T 4096
#define BATS_MAX_FLOWS 1024

/* --copy：仍按 generation/batch 走同一条四跳，三步都改成 memcpy。
   再加 --add1 时，encode 每个载荷字节加 1，中继仍拷贝，decode 减 1。 */
static int line_copy;
static int line_add1;
static int line_fit_psi;
static const char *line_fit_path;
static const char *line_psi_path;

#define PSI_NETWORK_FILE "psi_network.txt"

int psi_load(const char *path);
int psi_optimize(const int *counts, int ncounts, int max_degree, double eta_bar, int samples,
                 double q, int scale, int *weights_out, int weights_cap, int *D_out,
                 const char *weights_path);

static void payload_add1(uint8_t *p, size_t n)
{
    size_t i;

    for (i = 0; i < n; i++) {
        p[i] = (uint8_t)(p[i] + 1u);
    }
}

static void payload_sub1(uint8_t *p, size_t n)
{
    size_t i;

    for (i = 0; i < n; i++) {
        p[i] = (uint8_t)(p[i] - 1u);
    }
}

typedef struct {
    int fd;
    struct sockaddr_in next;
    int loss_percent;
    uint64_t loss_state;
    int rate_mbps;
    uint64_t pace_origin_ns;
    uint64_t pace_bytes;
    uint64_t pace_intent_ns;
    uint64_t pace_actual_ns;
    uint64_t pace_sleeps;
    uint64_t pace_skips;
    uint64_t pace_burst;
    uint64_t sent;
    uint64_t dropped;
    uint64_t wire_bytes;
    uint8_t *pkt;
    size_t pkt_cap;
    int gso_off;
    struct RxQueue *outq;
} Tx;

typedef struct RxQueue RxQueue;

static int queue_push(RxQueue *q, const uint8_t *pkt, uint32_t len, int is_data);

#ifndef UDP_SEGMENT
#define UDP_SEGMENT 103
#endif
#ifndef UDP_GRO
#define UDP_GRO 104
#endif

static void die(const char *msg)
{
    fprintf(stderr, "bats-line: %s\n", msg);
    exit(1);
}

static uint64_t mono_ns(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/* 收包线程可能在 now 取样之后才写下时间戳。无符号相减会绕回，把刚到的包看成已经静默很久。 */
static uint64_t since_ns(uint64_t now, uint64_t then)
{
    if (then > now) {
        return 0;
    }
    return now - then;
}

static void put_u16(uint8_t *p, unsigned v)
{
    p[0] = (uint8_t)(v >> 8);
    p[1] = (uint8_t)v;
}

static void put_u32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)v;
}

static unsigned get_u16(const uint8_t *p)
{
    return ((unsigned)p[0] << 8) | p[1];
}

static uint32_t get_u32(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8) | p[3];
}

static int parse_u32(const char *s, uint32_t *out)
{
    char *end = NULL;
    unsigned long v;

    errno = 0;
    v = strtoul(s, &end, 0);
    if (errno != 0 || end == s || *end != '\0' || v > 0xfffffffful) {
        return -1;
    }
    *out = (uint32_t)v;
    return 0;
}

static int parse_u64(const char *s, uint64_t *out)
{
    char *end = NULL;

    errno = 0;
    *out = strtoull(s, &end, 0);
    if (errno != 0 || end == s || *end != '\0') {
        return -1;
    }
    return 0;
}

static int parse_hostport(const char *s, struct sockaddr_in *addr)
{
    char buf[128];
    char *colon;
    unsigned port;

    if (strlen(s) >= sizeof(buf)) {
        return -1;
    }
    memcpy(buf, s, strlen(s) + 1);
    colon = strrchr(buf, ':');
    if (colon == NULL || colon == buf) {
        return -1;
    }
    *colon = '\0';
    port = (unsigned)strtoul(colon + 1, NULL, 10);
    if (port == 0 || port > 65535) {
        return -1;
    }
    memset(addr, 0, sizeof(*addr));
    addr->sin_family = AF_INET;
    addr->sin_port = htons((uint16_t)port);
    if (inet_aton(buf, &addr->sin_addr) == 0) {
        return -1;
    }
    return 0;
}

static void enlarge_buffers(int fd)
{
    int sz = 32 * 1024 * 1024;

    (void)setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &sz, sizeof(sz));
    (void)setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sz, sizeof(sz));
}

static int open_udp(const char *bind_ip, uint16_t port, int do_bind)
{
    int fd;
    int one = 1;
    struct sockaddr_in addr;

    fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        perror("bats-line: socket");
        exit(1);
    }
    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    enlarge_buffers(fd);
    /* 监听套接字打开 GRO。发送端用 UDP_SEGMENT 时，内核会把多个等长数据报合成一次读取。
       拆包按 cmsg 里的段长；没有 cmsg 时，只要总长是单包的整数倍，仍按单包切开。 */
    if (do_bind && port != 0) {
        int gro = 1;

        if (setsockopt(fd, IPPROTO_UDP, UDP_GRO, &gro, sizeof(gro)) != 0) {
            fprintf(stderr, "bats-line: UDP_GRO 未打开 errno=%d\n", errno);
        }
    }
    if (do_bind) {
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        if (bind_ip != NULL && inet_aton(bind_ip, &addr.sin_addr) == 0) {
            die("bind 地址不是 IPv4");
        }
        if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
            perror("bats-line: bind");
            exit(1);
        }
    }
    return fd;
}

static int drop_data(Tx *tx)
{
    if (tx->loss_percent <= 0) {
        return 0;
    }
    tx->loss_state = tx->loss_state * 6364136223846793005ull +
                     1442695040888963407ull;
    return (int)(tx->loss_state % 100ull) < tx->loss_percent;
}

/* 领先达到 2ms 用绝对时间睡眠。不到 2ms 不能 nanosleep，虚拟机会把短睡眠拉长，
   所以空转到点。调用前 due > now。睡完后这一串从当前这次的字节重新计。 */
static void pace_wait(Tx *tx, uint64_t due, uint64_t now, size_t nbytes)
{
    struct timespec ts;
    uint64_t t0 = now;
    uint64_t lead = due - now;

    tx->pace_intent_ns += lead;
    tx->pace_sleeps++;
    if (lead >= 2000000ull) {
        ts.tv_sec = (time_t)(due / 1000000000ull);
        ts.tv_nsec = (long)(due % 1000000000ull);
        while (clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &ts, NULL) == EINTR) {
        }
    } else {
        while (mono_ns() < due) {
        }
    }
    tx->pace_actual_ns += mono_ns() - t0;
    tx->pace_burst = nbytes;
}

static void pace(Tx *tx, size_t nbytes)
{
    uint64_t now;
    uint64_t due;
    uint64_t cap;

    if (tx->rate_mbps <= 0 || nbytes == 0) {
        return;
    }
    now = mono_ns();
    if (tx->pace_origin_ns == 0) {
        tx->pace_origin_ns = now;
    }
    tx->pace_bytes += nbytes;
    tx->pace_burst += nbytes;
    due = tx->pace_origin_ns + tx->pace_bytes * 8000ull / (uint64_t)tx->rate_mbps;
    /* 约 1ms 的量：rate_mbps * 1e6 / 8 / 1000。 */
    cap = (uint64_t)tx->rate_mbps * 125ull;
    if (cap < 1500ull) {
        cap = 1500ull;
    }
    /* 落后不补睡。领先不到 2ms、并且这一串还没到约 1ms 的量，就继续发。 */
    if (due <= now) {
        tx->pace_burst = nbytes;
        tx->pace_skips++;
        return;
    }
    if (tx->pace_burst < cap && due <= now + 2000000ull) {
        tx->pace_skips++;
        return;
    }
    pace_wait(tx, due, now, nbytes);
}

/* 发送和接收是同一个套接字。阻塞的 send 会占着套接字锁，收包线程就读不到，
   内核把还没读走的包丢掉。这里遇 EAGAIN 就让出，锁马上放开。 */
static int send_wait_out(int fd)
{
    struct pollfd pfd;

    pfd.fd = fd;
    pfd.events = POLLOUT;
    for (;;) {
        int pr = poll(&pfd, 1, 5);

        if (pr < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        return 0;
    }
}

static void pin_core(int cpu)
{
    cpu_set_t set;
    long nproc;

    if (cpu < 0) {
        return;
    }
    nproc = sysconf(_SC_NPROCESSORS_ONLN);
    if (nproc <= 0 || cpu >= nproc) {
        return;
    }
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    if (pthread_setaffinity_np(pthread_self(), sizeof(set), &set) != 0) {
        fprintf(stderr, "bats-line: 绑核 %d 失败\n", cpu);
    }
}

static int send_bytes_socket(Tx *tx, const uint8_t *buf, size_t len, int is_data)
{
    pace(tx, len);
    for (;;) {
        ssize_t n = sendto(tx->fd, buf, len, MSG_DONTWAIT, (struct sockaddr *)&tx->next,
                           sizeof(tx->next));

        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
            if (send_wait_out(tx->fd) != 0) {
                perror("bats-line: poll");
                return -1;
            }
            continue;
        }
        if (n < 0 || (size_t)n != len) {
            perror("bats-line: sendto");
            return -1;
        }
        tx->sent++;
        if (is_data) {
            tx->wire_bytes += len;
        }
        return 0;
    }
}

static int send_bytes(Tx *tx, const uint8_t *buf, size_t len, int is_data)
{
    if (tx->outq != NULL) {
        return queue_push(tx->outq, buf, (uint32_t)len, is_data);
    }
    if (is_data && drop_data(tx)) {
        tx->dropped++;
        return 0;
    }
    return send_bytes_socket(tx, buf, len, is_data);
}

static int file_exists(const char *path)
{
    FILE *f = fopen(path, "rb");

    if (f == NULL) {
        return 0;
    }
    fclose(f);
    return 1;
}

static void install_line_psi(const char *explicit_path)
{
    const char *path = explicit_path;

    if (path == NULL && file_exists(PSI_NETWORK_FILE)) {
        path = PSI_NETWORK_FILE;
    }
    if (path != NULL) {
        if (!psi_load(path)) {
            die("无法装入度数分布");
        }
        fprintf(stderr, "bats-line: 装入度数 %s\n", path);
        return;
    }
    if (!psi_set(psi_line_w, psi_line_D)) {
        die("无法装入优化后的度数分布");
    }
    fprintf(stderr, "bats-line: 使用内置 psi_line.h\n");
}

/* η≈0.98：每 K 个源包补 ceil(K*0.02/0.98) 个校验包。校验包也进入后面的 batch。 */
static int precode_parity_count(int K)
{
    int p;

    if (K < 1) {
        return 0;
    }
    p = (K * 2 + 97) / 98;
    if (p < 1) {
        p = 1;
    }
    return p;
}

static int precode_k(int K)
{
    return K + precode_parity_count(K);
}

static uint8_t precode_coeff(uint64_t seed, int parity, int info)
{
    uint64_t x = seed ^ (0x9E3779B97F4A7C15ull * (uint64_t)(parity + 1));

    x ^= (uint64_t)info * 0xBF58476D1CE4E5B9ull;
    x *= 0x94D049BB133111EBull;
    x ^= x >> 31;
    return (uint8_t)(x | 1u);
}

/* block 前 K*T 字节是源包，函数把后面 P 个校验包写上。 */
static void precode_apply(uint8_t *block, int K, int T, uint64_t seed)
{
    int P = precode_parity_count(K);
    int p;

    for (p = 0; p < P; p++) {
        uint8_t *dst = block + (size_t)(K + p) * (size_t)T;
        int i;

        memset(dst, 0, (size_t)T);
        for (i = 0; i < K; i++) {
            gf_axpy(dst, block + (size_t)i * (size_t)T, precode_coeff(seed, p, i), (size_t)T);
        }
    }
}

static int precode_matches(const uint8_t *block, int K, int T, uint64_t seed)
{
    int P = precode_parity_count(K);
    uint8_t *tmp = malloc((size_t)P * (size_t)T);
    int p;
    int ok = 1;

    if (tmp == NULL) {
        return 0;
    }
    for (p = 0; p < P; p++) {
        uint8_t *dst = tmp + (size_t)p * (size_t)T;
        int i;

        memset(dst, 0, (size_t)T);
        for (i = 0; i < K; i++) {
            gf_axpy(dst, block + (size_t)i * (size_t)T, precode_coeff(seed, p, i), (size_t)T);
        }
        if (memcmp(dst, block + (size_t)(K + p) * (size_t)T, (size_t)T) != 0) {
            ok = 0;
            break;
        }
    }
    free(tmp);
    return ok;
}

static uint32_t batches_line(int K)
{
    int n;

    /* 优化后的 Ψ 在 K=128、两跳、0% 和 5% 丢包下 18 个 batch 就能还原。
       预编码后大约按每个中间包 0.15 个 batch，K=131 得到 20。 */
    n = (K * 19 + 127) / 128;
    if (n < 8) {
        n = 8;
    }
    return (uint32_t)n;
}

static uint64_t mbps_of(uint64_t bytes, uint64_t elapsed_ns)
{
    if (elapsed_ns == 0) {
        return 0;
    }
    return bytes * 8000ull / elapsed_ns;
}

/* 一个 batch 的若干包一次 sendmmsg。返回实际发出的包数，失败返回 -1。丢弃的包不计入返回值。 */
static int send_coded(Tx *tx, uint16_t flow_id, uint32_t gen, uint32_t batch, unsigned T,
                      const uint8_t *coeff, const uint8_t *payload, int n_pkts,
                      uint32_t file_size, uint32_t k_pkts, uint32_t n_batches, uint32_t n_gens)
{
    struct mmsghdr msgs[BATS_M];
    struct iovec iov[BATS_M * 2];
    uint8_t hdr[BATS_M][BATS_LINE_HDR];
    int nmsg = 0;
    int j;
    int sent;
    size_t stride;
    size_t bytes = 0;

    if (n_pkts < 1 || n_pkts > BATS_M || T < 1 || T > BATS_LINE_MAX_T) {
        return -1;
    }
    stride = (size_t)BATS_LINE_HDR + T;
    if (tx->outq != NULL) {
        uint8_t pkt[BATS_LINE_HDR + BATS_LINE_MAX_T];
        int queued = 0;

        for (j = 0; j < n_pkts; j++) {
            memset(pkt, 0, BATS_LINE_HDR);
            put_u32(pkt + 0, BATS_LINE_MAGIC);
            pkt[4] = BATS_LINE_VERSION;
            pkt[5] = BATS_LINE_DATA;
            put_u16(pkt + 6, flow_id);
            put_u32(pkt + 8, gen);
            put_u32(pkt + 12, batch);
            put_u16(pkt + 16, (unsigned)j);
            put_u16(pkt + 18, T);
            put_u32(pkt + 20, file_size);
            put_u32(pkt + 24, k_pkts);
            put_u32(pkt + 28, n_batches);
            put_u32(pkt + 32, n_gens);
            memcpy(pkt + 36, coeff + (size_t)j * BATS_M, BATS_M);
            memcpy(pkt + BATS_LINE_HDR, payload + (size_t)j * T, T);
            if (queue_push(tx->outq, pkt, (uint32_t)stride, 1) != 0) {
                return -1;
            }
            queued++;
        }
        return queued;
    }
    memset(msgs, 0, sizeof(msgs));
    for (j = 0; j < n_pkts; j++) {
        if (drop_data(tx)) {
            tx->dropped++;
            continue;
        }
        memset(hdr[nmsg], 0, BATS_LINE_HDR);
        put_u32(hdr[nmsg] + 0, BATS_LINE_MAGIC);
        hdr[nmsg][4] = BATS_LINE_VERSION;
        hdr[nmsg][5] = BATS_LINE_DATA;
        put_u16(hdr[nmsg] + 6, flow_id);
        put_u32(hdr[nmsg] + 8, gen);
        put_u32(hdr[nmsg] + 12, batch);
        put_u16(hdr[nmsg] + 16, (unsigned)j);
        put_u16(hdr[nmsg] + 18, T);
        put_u32(hdr[nmsg] + 20, file_size);
        put_u32(hdr[nmsg] + 24, k_pkts);
        put_u32(hdr[nmsg] + 28, n_batches);
        put_u32(hdr[nmsg] + 32, n_gens);
        memcpy(hdr[nmsg] + 36, coeff + (size_t)j * BATS_M, BATS_M);
        iov[nmsg * 2].iov_base = hdr[nmsg];
        iov[nmsg * 2].iov_len = BATS_LINE_HDR;
        iov[nmsg * 2 + 1].iov_base = (void *)(payload + (size_t)j * T);
        iov[nmsg * 2 + 1].iov_len = T;
        msgs[nmsg].msg_hdr.msg_name = &tx->next;
        msgs[nmsg].msg_hdr.msg_namelen = sizeof(tx->next);
        msgs[nmsg].msg_hdr.msg_iov = &iov[nmsg * 2];
        msgs[nmsg].msg_hdr.msg_iovlen = 2;
        bytes += stride;
        nmsg++;
    }
    if (nmsg == 0) {
        return 0;
    }
    pace(tx, bytes);
    {
        struct mmsghdr *cur = msgs;
        unsigned left = (unsigned)nmsg;

        while (left > 0) {
            sent = sendmmsg(tx->fd, cur, left, MSG_DONTWAIT);
            if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
                if (send_wait_out(tx->fd) != 0) {
                    perror("bats-line: poll");
                    return -1;
                }
                continue;
            }
            if (sent < 0) {
                perror("bats-line: sendmmsg");
                return -1;
            }
            if (sent == 0) {
                if (send_wait_out(tx->fd) != 0) {
                    perror("bats-line: poll");
                    return -1;
                }
                continue;
            }
            cur += sent;
            left -= (unsigned)sent;
        }
    }
    tx->sent += (uint64_t)nmsg;
    tx->wire_bytes += bytes;
    return nmsg;
}

/* 整段已经排好的 UDP 载荷一次送进内核，由 UDP_SEGMENT 切成 stride 字节的数据报。
   成功返回 0。一个字节都没送出返回 -1，调用方可以改走 sendmmsg。送出了一部分返回 -2。 */
static int send_gso(Tx *tx, const uint8_t *data, int n_pkts, size_t stride)
{
    struct msghdr msg;
    struct iovec iov;
    char control[CMSG_SPACE(sizeof(uint16_t))];
    struct cmsghdr *cmsg;
    uint16_t seg;
    int any = 0;

    if (n_pkts < 1 || stride == 0 || stride > 65535u) {
        return -1;
    }
    seg = (uint16_t)stride;
    memset(&msg, 0, sizeof(msg));
    memset(control, 0, sizeof(control));
    iov.iov_base = (void *)data;
    iov.iov_len = (size_t)n_pkts * stride;
    msg.msg_name = &tx->next;
    msg.msg_namelen = sizeof(tx->next);
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control;
    msg.msg_controllen = sizeof(control);
    cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_UDP;
    cmsg->cmsg_type = UDP_SEGMENT;
    cmsg->cmsg_len = CMSG_LEN(sizeof(seg));
    memcpy(CMSG_DATA(cmsg), &seg, sizeof(seg));
    pace(tx, iov.iov_len);
    while (iov.iov_len > 0) {
        ssize_t n = sendmsg(tx->fd, &msg, MSG_DONTWAIT);

        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
            if (send_wait_out(tx->fd) != 0) {
                perror("bats-line: poll");
                return any ? -2 : -1;
            }
            continue;
        }
        if (n < 0) {
            if (!any) {
                fprintf(stderr, "bats-line: udp gso 不可用 errno=%d\n", errno);
            }
            return any ? -2 : -1;
        }
        if (n == 0) {
            if (send_wait_out(tx->fd) != 0) {
                perror("bats-line: poll");
                return any ? -2 : -1;
            }
            continue;
        }
        /* 半个数据报不能接着切，否则后面的包会错位。 */
        if ((size_t)n % stride != 0) {
            fprintf(stderr, "bats-line: udp gso 写出半包\n");
            return -2;
        }
        any = 1;
        iov.iov_base = (uint8_t *)iov.iov_base + n;
        iov.iov_len -= (size_t)n;
    }
    tx->sent += (uint64_t)n_pkts;
    tx->wire_bytes += (uint64_t)n_pkts * stride;
    return 0;
}

/* 编码线程已经写好整段 UDP 包。这里按批 sendmmsg，丢包仍然发生在发送前。 */
static int send_burst(Tx *tx, const uint8_t *pkts, int n_pkts, size_t stride)
{
    enum { SEND_IOV = 128 };
    struct mmsghdr msgs[SEND_IOV];
    struct iovec iov[SEND_IOV];
    int i = 0;

    if (n_pkts < 0 || stride == 0) {
        return -1;
    }
    while (i < n_pkts) {
        int nmsg = 0;
        size_t bytes = 0;
        struct mmsghdr *cur;
        unsigned left;

        if (!tx->gso_off && tx->loss_percent <= 0) {
            int n = n_pkts - i;
            int gr;
            int cap = (int)(60000u / stride);

            /* 虚拟网卡一次 GSO 不能超过约 64KB，再大内核返回 EMSGSIZE。 */
            if (cap < 1) {
                cap = 1;
            }
            if (n > cap) {
                n = cap;
            }
            gr = send_gso(tx, pkts + (size_t)i * stride, n, stride);
            if (gr == 0) {
                i += n;
                continue;
            }
            if (gr == -2) {
                return -1;
            }
            tx->gso_off = 1;
        }
        memset(msgs, 0, sizeof(msgs));
        while (i < n_pkts && nmsg < SEND_IOV) {
            if (drop_data(tx)) {
                tx->dropped++;
                i++;
                continue;
            }
            iov[nmsg].iov_base = (void *)(pkts + (size_t)i * stride);
            iov[nmsg].iov_len = stride;
            msgs[nmsg].msg_hdr.msg_name = &tx->next;
            msgs[nmsg].msg_hdr.msg_namelen = sizeof(tx->next);
            msgs[nmsg].msg_hdr.msg_iov = &iov[nmsg];
            msgs[nmsg].msg_hdr.msg_iovlen = 1;
            bytes += stride;
            nmsg++;
            i++;
        }
        if (nmsg == 0) {
            continue;
        }
        pace(tx, bytes);
        cur = msgs;
        left = (unsigned)nmsg;
        while (left > 0) {
            int sent = sendmmsg(tx->fd, cur, left, MSG_DONTWAIT);

            if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
                if (send_wait_out(tx->fd) != 0) {
                    perror("bats-line: poll");
                    return -1;
                }
                continue;
            }
            if (sent < 0) {
                perror("bats-line: sendmmsg");
                return -1;
            }
            if (sent == 0) {
                if (send_wait_out(tx->fd) != 0) {
                    perror("bats-line: poll");
                    return -1;
                }
                continue;
            }
            cur += sent;
            left -= (unsigned)sent;
        }
        tx->sent += (uint64_t)nmsg;
        tx->wire_bytes += bytes;
    }
    return 0;
}

static int send_end(Tx *tx, uint16_t flow_id, uint32_t file_size, uint32_t k_pkts,
                    uint32_t n_batches, uint32_t n_gens)
{
    uint8_t buf[BATS_LINE_HDR];
    /* 内核收满后丢掉的是最新的包。END 紧贴在数据后面时会一起被丢掉，
       所以数据停了以后再补几轮，等对端把缓冲区读空。 */
    static const int delay_ms[] = {0, 25};
    uint64_t t0;
    int round;

    memset(buf, 0, sizeof(buf));
    put_u32(buf + 0, BATS_LINE_MAGIC);
    buf[4] = BATS_LINE_VERSION;
    buf[5] = BATS_LINE_END;
    put_u16(buf + 6, flow_id);
    put_u32(buf + 20, file_size);
    put_u32(buf + 24, k_pkts);
    put_u32(buf + 28, n_batches);
    put_u32(buf + 32, n_gens);
    t0 = mono_ns();
    for (round = 0; round < (int)(sizeof(delay_ms) / sizeof(delay_ms[0])); round++) {
        uint64_t due = t0 + (uint64_t)delay_ms[round] * 1000000ull;
        uint64_t now = mono_ns();
        int i;

        if (now < due) {
            struct timespec ts;

            ts.tv_sec = (time_t)(due / 1000000000ull);
            ts.tv_nsec = (long)(due % 1000000000ull);
            while (clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &ts, NULL) == EINTR) {
            }
        }
        for (i = 0; i < 3; i++) {
            if (send_bytes(tx, buf, sizeof(buf), 0) != 0) {
                return -1;
            }
        }
    }
    return 0;
}

static int coeff_is_unit_column(const uint8_t *coeff, int j)
{
    int m;

    for (m = 0; m < BATS_M; m++) {
        if (coeff[m] != (uint8_t)(m == j ? 1 : 0)) {
            return 0;
        }
    }
    return 1;
}

static int coeff_nnz(const uint8_t *coeff)
{
    int i;
    int n = 0;

    for (i = 0; i < BATS_M; i++) {
        if (coeff[i] != 0) {
            n++;
        }
    }
    return n;
}

#define SRC_WIN 32

typedef struct {
    int state;
    uint32_t gen;
    uint16_t flow_id;
    int unit_ok;
    uint8_t *pkts;
} SrcSlot;

typedef struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    const uint8_t *file;
    long file_size;
    int K;
    int T;
    int coded_k;
    uint64_t seed;
    uint32_t n_gens;
    uint32_t n_batches;
    uint32_t k_pkts;
    uint16_t flow_base;
    int n_flows;
    size_t stride;
    size_t gen_bytes;
    size_t coded_bytes;
    uint32_t next_claim;
    uint32_t released;
    int failed;
    SrcSlot slot[SRC_WIN];
} SrcPipe;

static int src_encode_gen(SrcPipe *p, uint32_t gen, SrcSlot *slot, uint8_t *block,
                          uint8_t *payload)
{
    size_t off;
    size_t n;
    uint32_t batch;
    int unit_ok = 1;

    off = (size_t)gen * p->gen_bytes;
    n = p->gen_bytes;
    if (off >= (size_t)p->file_size) {
        return -1;
    }
    if (n > (size_t)p->file_size - off) {
        n = (size_t)p->file_size - off;
    }
    memset(block, 0, p->coded_bytes);
    memcpy(block, p->file + off, n);
    if (!line_copy) {
        precode_apply(block, p->K, p->T, p->seed);
    }
    for (batch = 0; batch < p->n_batches; batch++) {
        uint8_t coeff[BATS_M * BATS_M];
        int j;

        if (line_copy) {
            memset(coeff, 0, sizeof(coeff));
            for (j = 0; j < BATS_M; j++) {
                unsigned pno = batch * (unsigned)BATS_M + (unsigned)j;
                unsigned sym = pno < (unsigned)p->K ? pno : pno % (unsigned)p->K;

                coeff[(size_t)j * BATS_M + (size_t)j] = 1;
                memcpy(payload + (size_t)j * (size_t)p->T, block + (size_t)sym * (size_t)p->T,
                       (size_t)p->T);
                if (line_add1) {
                    payload_add1(payload + (size_t)j * (size_t)p->T, (size_t)p->T);
                }
            }
        } else if (bats_encode(block, p->coded_k, p->T, p->seed, batch, coeff, payload) != 0) {
            return -1;
        }
        for (j = 0; j < BATS_M; j++) {
            uint8_t *buf = slot->pkts + ((size_t)batch * BATS_M + (size_t)j) * p->stride;

            if (!coeff_is_unit_column(coeff + (size_t)j * BATS_M, j)) {
                unit_ok = 0;
            }
            memset(buf, 0, BATS_LINE_HDR);
            put_u32(buf + 0, BATS_LINE_MAGIC);
            buf[4] = BATS_LINE_VERSION;
            buf[5] = BATS_LINE_DATA;
            put_u16(buf + 6, slot->flow_id);
            put_u32(buf + 8, gen);
            put_u32(buf + 12, batch);
            put_u16(buf + 16, (unsigned)j);
            put_u16(buf + 18, (unsigned)p->T);
            put_u32(buf + 20, (uint32_t)p->file_size);
            put_u32(buf + 24, p->k_pkts);
            put_u32(buf + 28, p->n_batches);
            put_u32(buf + 32, p->n_gens);
            memcpy(buf + 36, coeff + (size_t)j * BATS_M, BATS_M);
            memcpy(buf + BATS_LINE_HDR, payload + (size_t)j * (size_t)p->T, (size_t)p->T);
        }
    }
    slot->unit_ok = unit_ok;
    return 0;
}

static void *src_worker(void *arg)
{
    SrcPipe *p = arg;

    pthread_setname_np(pthread_self(), "src-enc");
    uint8_t *block;
    uint8_t *payload;

    block = malloc(p->coded_bytes == 0 ? 1 : p->coded_bytes);
    payload = malloc((size_t)BATS_M * (size_t)(p->T > 0 ? p->T : 1));
    if (block == NULL || payload == NULL) {
        free(block);
        free(payload);
        pthread_mutex_lock(&p->mu);
        p->failed = 1;
        pthread_cond_broadcast(&p->cv);
        pthread_mutex_unlock(&p->mu);
        return NULL;
    }
    for (;;) {
        uint32_t gen;
        uint32_t job;
        uint32_t n_jobs;
        int si;
        int rc;

        pthread_mutex_lock(&p->mu);
        n_jobs = p->n_flows > 0 ? (uint32_t)p->n_flows * p->n_gens : p->n_gens;
        while (!p->failed && p->next_claim < n_jobs &&
               p->next_claim >= p->released + (uint32_t)SRC_WIN) {
            pthread_cond_wait(&p->cv, &p->mu);
        }
        if (p->failed || p->next_claim >= n_jobs) {
            pthread_mutex_unlock(&p->mu);
            free(block);
            free(payload);
            return NULL;
        }
        job = p->next_claim++;
        gen = p->n_flows > 0 ? job / (uint32_t)p->n_flows : job;
        si = (int)(job % SRC_WIN);
        while (p->slot[si].state != 0 && !p->failed) {
            pthread_cond_wait(&p->cv, &p->mu);
        }
        if (p->failed) {
            pthread_mutex_unlock(&p->mu);
            free(block);
            free(payload);
            return NULL;
        }
        p->slot[si].state = 1;
        p->slot[si].gen = gen;
        p->slot[si].flow_id =
            (uint16_t)(p->flow_base + (p->n_flows > 0 ? job % (uint32_t)p->n_flows : 0));
        pthread_mutex_unlock(&p->mu);

        rc = src_encode_gen(p, gen, &p->slot[si], block, payload);

        pthread_mutex_lock(&p->mu);
        if (rc != 0) {
            p->failed = 1;
            p->slot[si].state = 0;
        } else {
            p->slot[si].state = 2;
        }
        pthread_cond_broadcast(&p->cv);
        pthread_mutex_unlock(&p->mu);
        if (rc != 0) {
            free(block);
            free(payload);
            return NULL;
        }
    }
}

static int run_source(const char *path, Tx *tx, int K, int T, uint64_t seed,
                      uint32_t n_batches_arg, uint16_t flow_id, int n_flows, int worker_arg)
{
    FILE *fp;
    uint8_t *file;
    long file_size;
    size_t gen_bytes;
    size_t coded_bytes;
    int coded_k;
    uint32_t n_gens;
    uint32_t n_batches;
    int unit_ok = 1;
    uint64_t data_packets = 0;
    uint64_t t0;
    SrcPipe pipe;
    pthread_t workers[SRC_WIN];
    int nworkers;
    int started = 0;
    int i;
    long nproc;

    if (K < 1 || T < 1 || T > BATS_LINE_MAX_T) {
        die("K 或 T 不合法");
    }
    fp = fopen(path, "rb");
    if (fp == NULL) {
        perror("bats-line: fopen");
        return 1;
    }
    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        die("fseek 失败");
    }
    file_size = ftell(fp);
    if (file_size < 0 || file_size > 0x7fffffffL) {
        fclose(fp);
        die("文件大小不合法");
    }
    if (fseek(fp, 0, SEEK_SET) != 0) {
        fclose(fp);
        die("fseek 失败");
    }
    file = calloc((size_t)file_size + 1u, 1u);
    if (file == NULL) {
        fclose(fp);
        die("内存不足");
    }
    if (file_size > 0 &&
        fread(file, 1, (size_t)file_size, fp) != (size_t)file_size) {
        fclose(fp);
        free(file);
        die("读文件失败");
    }
    fclose(fp);

    gen_bytes = (size_t)K * (size_t)T;
    coded_k = precode_k(K);
    coded_bytes = (size_t)coded_k * (size_t)T;
    n_gens = file_size == 0
                 ? 0u
                 : (uint32_t)(((size_t)file_size + gen_bytes - 1u) / gen_bytes);
    n_batches = n_batches_arg == 0 ? batches_line(coded_k) : n_batches_arg;
    if (n_batches < 1u) {
        n_batches = 1u;
    }

    fprintf(stderr,
            "bats-line: role=source api=bats_encode M=%d K=%d precode=%d K_bats=%d T=%d "
            "gens=%u batches_per_gen=%u file_bytes=%ld seed=%" PRIu64 " flow=%u flows=%d "
            "g_on_wire=0 coeff_bytes=%d psi=line copy=%s\n",
            BATS_M, K, coded_k - K, coded_k, T, n_gens, n_batches, file_size, seed,
            (unsigned)flow_id, n_flows, BATS_M, line_add1 ? "add1" : (line_copy ? "copy" : "0"));

    memset(&pipe, 0, sizeof(pipe));
    pipe.file = file;
    pipe.file_size = file_size;
    pipe.K = K;
    pipe.T = T;
    pipe.coded_k = coded_k;
    pipe.seed = seed;
    pipe.n_gens = n_gens;
    pipe.n_batches = n_batches;
    pipe.k_pkts = (uint32_t)coded_k;
    pipe.flow_base = flow_id;
    pipe.n_flows = n_flows < 1 ? 1 : n_flows;
    if ((uint32_t)pipe.flow_base + (uint32_t)pipe.n_flows - 1u > 65535u) {
        die("flow-id 与 flows 超出 65535");
    }
    pipe.stride = (size_t)BATS_LINE_HDR + (size_t)T;
    pipe.gen_bytes = gen_bytes;
    pipe.coded_bytes = coded_bytes;
    for (i = 0; i < SRC_WIN; i++) {
        size_t pkt_n = (size_t)n_batches * BATS_M * pipe.stride;

        pipe.slot[i].pkts = malloc(pkt_n == 0 ? 1 : pkt_n);
        if (pipe.slot[i].pkts == NULL) {
            die("内存不足");
        }
    }
    if (pthread_mutex_init(&pipe.mu, NULL) != 0 || pthread_cond_init(&pipe.cv, NULL) != 0) {
        die("无法创建编码线程");
    }
    nproc = sysconf(_SC_NPROCESSORS_ONLN);
    if (worker_arg > 0) {
        nworkers = worker_arg;
    } else {
        nworkers = nproc > 1 ? (int)nproc - 1 : 1;
    }
    if (nworkers > SRC_WIN) {
        nworkers = SRC_WIN;
    }
    if (n_gens > 0 && (uint32_t)nworkers > (uint32_t)pipe.n_flows * n_gens) {
        nworkers = (int)((uint32_t)pipe.n_flows * n_gens);
    }
    if (n_gens == 0) {
        nworkers = 0;
    }
    t0 = mono_ns();
    pthread_setname_np(pthread_self(), "src-send");
    for (i = 0; i < nworkers; i++) {
        if (pthread_create(&workers[i], NULL, src_worker, &pipe) != 0) {
            pipe.failed = 1;
            break;
        }
        started++;
    }
    {
        uint32_t gen;

        for (gen = 0; gen < n_gens && !pipe.failed; gen++) {
            int base;

            for (base = 0; base < pipe.n_flows && !pipe.failed;) {
                int group = pipe.n_flows - base;
                int k;
                uint32_t batch;

                if (group > SRC_WIN) {
                    group = SRC_WIN;
                }
                for (k = 0; k < group; k++) {
                    uint32_t job = gen * (uint32_t)pipe.n_flows + (uint32_t)base + (uint32_t)k;
                    int si = (int)(job % SRC_WIN);
                    uint16_t fid = (uint16_t)(pipe.flow_base + base + k);

                    pthread_mutex_lock(&pipe.mu);
                    while ((pipe.slot[si].state != 2 || pipe.slot[si].gen != gen ||
                            pipe.slot[si].flow_id != fid) &&
                           !pipe.failed) {
                        pthread_cond_wait(&pipe.cv, &pipe.mu);
                    }
                    if (!pipe.failed && !pipe.slot[si].unit_ok) {
                        unit_ok = 0;
                    }
                    pthread_mutex_unlock(&pipe.mu);
                }
                if (pipe.failed) {
                    break;
                }
                /* 一条流仍一次发完这一代。多条流按 batch 交错，线上同时有这些流的包。 */
                if (group == 1) {
                    uint32_t job = gen * (uint32_t)pipe.n_flows + (uint32_t)base;
                    int si = (int)(job % SRC_WIN);

                    if (send_burst(tx, pipe.slot[si].pkts, (int)n_batches * BATS_M, pipe.stride) !=
                        0) {
                        pthread_mutex_lock(&pipe.mu);
                        pipe.failed = 1;
                        pthread_cond_broadcast(&pipe.cv);
                        pthread_mutex_unlock(&pipe.mu);
                    } else {
                        data_packets += (uint64_t)n_batches * (uint64_t)BATS_M;
                    }
                } else {
                    for (batch = 0; batch < n_batches && !pipe.failed;) {
                        /* 40 个包正好低于虚拟机一次 GSO 的 60KB 上限。 */
                        uint32_t step = n_batches - batch;

                        if (step > 5u) {
                            step = 5u;
                        }
                        for (k = 0; k < group && !pipe.failed; k++) {
                            uint32_t job =
                                gen * (uint32_t)pipe.n_flows + (uint32_t)base + (uint32_t)k;
                            int si = (int)(job % SRC_WIN);
                            uint8_t *pkts = pipe.slot[si].pkts +
                                            (size_t)batch * (size_t)BATS_M * pipe.stride;

                            if (send_burst(tx, pkts, (int)step * BATS_M, pipe.stride) != 0) {
                                pthread_mutex_lock(&pipe.mu);
                                pipe.failed = 1;
                                pthread_cond_broadcast(&pipe.cv);
                                pthread_mutex_unlock(&pipe.mu);
                            } else {
                                data_packets += (uint64_t)step * (uint64_t)BATS_M;
                            }
                        }
                        batch += step;
                    }
                }
                pthread_mutex_lock(&pipe.mu);
                for (k = 0; k < group; k++) {
                    uint32_t job = gen * (uint32_t)pipe.n_flows + (uint32_t)base + (uint32_t)k;
                    int si = (int)(job % SRC_WIN);

                    pipe.slot[si].state = 0;
                }
                pipe.released += (uint32_t)group;
                pthread_cond_broadcast(&pipe.cv);
                pthread_mutex_unlock(&pipe.mu);
                base += group;
            }
        }
    }
    pthread_mutex_lock(&pipe.mu);
    pthread_cond_broadcast(&pipe.cv);
    pthread_mutex_unlock(&pipe.mu);
    for (i = 0; i < started; i++) {
        pthread_join(workers[i], NULL);
    }
    for (i = 0; i < SRC_WIN; i++) {
        free(pipe.slot[i].pkts);
    }
    pthread_cond_destroy(&pipe.cv);
    pthread_mutex_destroy(&pipe.mu);
    free(file);
    if (pipe.failed) {
        return 1;
    }
    if (!unit_ok) {
        die("源系数不是 I_M 的列，编码结果不符合 BATS");
    }
    {
        uint64_t elapsed_ns = mono_ns() - t0;
        int fi;

        for (fi = 0; fi < pipe.n_flows; fi++) {
            if (send_end(tx, (uint16_t)(pipe.flow_base + fi), (uint32_t)file_size,
                         (uint32_t)coded_k, n_batches, n_gens) != 0) {
                return 1;
            }
        }
        fprintf(stderr,
                "bats-line: role=source unit_coeff_ok=1 flows=%d workers=%d data_packets=%" PRIu64
                " loss_drops=%" PRIu64 " wire_bytes=%" PRIu64
                " elapsed_ms=%" PRIu64 " achieved_mbps=%" PRIu64
                " target_mbps=%d pace_intent_us=%" PRIu64
                " pace_actual_us=%" PRIu64 " pace_sleeps=%" PRIu64
                " pace_skips=%" PRIu64 "\n",
                pipe.n_flows, started, data_packets, tx->dropped, tx->wire_bytes,
                (uint64_t)(elapsed_ns / 1000000ull),
                mbps_of(tx->wire_bytes, elapsed_ns), tx->rate_mbps,
                (uint64_t)(tx->pace_intent_ns / 1000u),
                (uint64_t)(tx->pace_actual_ns / 1000u),
                tx->pace_sleeps, tx->pace_skips);
    }
    return 0;
}

#define RELAY_SLOTS 2048
#define GEN_FILLING 1
#define GEN_QUEUED 2
#define GEN_DECODING 3

typedef struct {
    int used;
    uint16_t flow_id;
    uint32_t gen_id;
    uint32_t batch_id;
    uint16_t T;
    int n;
    uint8_t seen[BATS_M];
    uint8_t coeff[BATS_M * BATS_M];
    uint8_t *payload;
    uint64_t last_ns;
} RelaySlot;

typedef struct {
    uint16_t flow_id;
    uint32_t gen_id;
    uint8_t *bits;
    size_t nbytes;
} FlushedGen;

typedef struct {
    int used;
    uint16_t flow_id;
    int end_seen;
    int have_meta;
    uint32_t file_size;
    uint32_t k_pkts;
    uint32_t n_batches;
    uint32_t n_gens;
    uint64_t recode_state;
    uint64_t full_n;
    uint64_t batch_n;
} FlowRx;

typedef struct {
    uint64_t last_ns;
    uint64_t data_pkts;
    uint64_t recode_seed;
    int expect_flows;
    int n_flows;
    int16_t flow_at[BATS_MAX_FLOWS];
    FlowRx flow[BATS_MAX_FLOWS];
} RxMeta;

typedef struct GenBuf {
    int used;
    int phase;
    uint16_t flow_id;
    int flow_i;
    uint32_t gen_id;
    uint64_t last_ns;
    size_t n;
    size_t cap;
    uint32_t *batch_id;
    uint16_t *index;
    uint16_t *place;
    uint8_t *coeff;
    uint8_t *pay;
    struct GenBuf *next;
} GenBuf;

typedef struct {
    int used;
    uint16_t flow_id;
    int fd;
    int have_meta;
    int end_seen;
    uint32_t file_size;
    uint32_t k_pkts;
    uint32_t n_batches;
    uint32_t n_gens;
    uint64_t data_pkts;
    uint64_t last_ns;
    int n_done;
    unsigned char *touched;
    unsigned char *finished;
    unsigned char *ranked;
} DestFlow;

typedef struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    GenBuf *gens;
    DestFlow flows[BATS_MAX_FLOWS];
    int16_t flow_at[BATS_MAX_FLOWS];
    int n_flows;
    int expect_flows;
    const char *out_path;
    int spare_fd;
    uint64_t last_rx_ns;
    int K;
    int T;
    uint64_t seed;
    int stop;
    int failed;
    int n_done;
    uint64_t dense;
    uint64_t symbols;
    int bp_sum;
    int inact_sum;
    int fit_psi;
    int rank_hist[BATS_M + 1];
} DestPool;

static int flows_all_ended(const RxMeta *rx)
{
    int i;
    int n = 0;
    int expect = rx->expect_flows > 0 ? rx->expect_flows : 1;

    for (i = 0; i < rx->n_flows; i++) {
        if (!rx->flow[i].used) {
            continue;
        }
        n++;
        if (!rx->flow[i].end_seen) {
            return 0;
        }
    }
    return n >= expect;
}

static FlowRx *flow_get(RxMeta *rx, uint16_t flow_id, int create)
{
    int i;
    FlowRx *f;

    if (flow_id < BATS_MAX_FLOWS) {
        int at = rx->flow_at[flow_id];

        if (at >= 0 && at < rx->n_flows && rx->flow[at].used && rx->flow[at].flow_id == flow_id) {
            return &rx->flow[at];
        }
    } else {
        for (i = 0; i < rx->n_flows; i++) {
            if (rx->flow[i].used && rx->flow[i].flow_id == flow_id) {
                return &rx->flow[i];
            }
        }
    }
    if (!create) {
        return NULL;
    }
    if (rx->n_flows >= BATS_MAX_FLOWS) {
        return NULL;
    }
    f = &rx->flow[rx->n_flows];
    memset(f, 0, sizeof(*f));
    f->used = 1;
    f->flow_id = flow_id;
    f->recode_state = rx->recode_seed + (uint64_t)flow_id * 0x9E3779B97F4A7C15ull;
    if (f->recode_state == 0) {
        f->recode_state = 1;
    }
    if (flow_id < BATS_MAX_FLOWS) {
        rx->flow_at[flow_id] = (int16_t)rx->n_flows;
    }
    rx->n_flows++;
    return f;
}

static void flow_note_meta(FlowRx *f, const uint8_t *buf)
{
    uint32_t k = get_u32(buf + 24);

    if (k == 0) {
        return;
    }
    f->file_size = get_u32(buf + 20);
    f->k_pkts = k;
    f->n_batches = get_u32(buf + 28);
    f->n_gens = get_u32(buf + 32);
    f->have_meta = 1;
}

static void flow_note_complete(FlowRx *f)
{
    uint64_t expect;

    if (f->end_seen || !f->have_meta) {
        return;
    }
    expect = (uint64_t)f->n_gens * (uint64_t)f->n_batches;
    if (expect > 0 && f->full_n >= expect) {
        f->end_seen = 1;
    }
}

static int batch_was_flushed(FlushedGen *maps, int nmaps, uint16_t flow_id, uint32_t gen,
                             uint32_t batch)
{
    int i;
    size_t byte = (size_t)batch >> 3;

    for (i = 0; i < nmaps; i++) {
        if (maps[i].flow_id != flow_id || maps[i].gen_id != gen) {
            continue;
        }
        if (maps[i].bits == NULL || byte >= maps[i].nbytes) {
            return 0;
        }
        return (maps[i].bits[byte] >> (batch & 7u)) & 1;
    }
    return 0;
}

static int mark_flushed(FlushedGen **maps, int *nmaps, uint16_t flow_id, uint32_t gen,
                        uint32_t batch)
{
    int i;
    FlushedGen *m = NULL;
    size_t byte = (size_t)batch >> 3;
    size_t need = byte + 1;
    uint8_t *nb;

    for (i = 0; i < *nmaps; i++) {
        if ((*maps)[i].flow_id == flow_id && (*maps)[i].gen_id == gen) {
            m = &(*maps)[i];
            break;
        }
    }
    if (m == NULL) {
        FlushedGen *nv = realloc(*maps, (size_t)(*nmaps + 1) * sizeof(FlushedGen));

        if (nv == NULL) {
            return -1;
        }
        *maps = nv;
        m = &nv[*nmaps];
        memset(m, 0, sizeof(*m));
        m->flow_id = flow_id;
        m->gen_id = gen;
        (*nmaps)++;
    }
    if (m->nbytes < need) {
        nb = realloc(m->bits, need);
        if (nb == NULL) {
            return -1;
        }
        memset(nb + m->nbytes, 0, need - m->nbytes);
        m->bits = nb;
        m->nbytes = need;
    }
    m->bits[byte] |= (uint8_t)(1u << (batch & 7u));
    return 0;
}

static void free_flushed(FlushedGen *maps, int nmaps)
{
    int i;

    for (i = 0; i < nmaps; i++) {
        free(maps[i].bits);
    }
    free(maps);
}

static void slot_clear(RelaySlot *s)
{
    free(s->payload);
    memset(s, 0, sizeof(*s));
}

static int slot_ready(const RelaySlot *s, int force)
{
    if (!s->used || s->n <= 0) {
        return 0;
    }
    return force || s->n == BATS_M;
}

static int flush_slot(RelaySlot *s, RxMeta *rx, Tx *tx, FlushedGen **maps, int *nmaps,
                      uint64_t *partial, uint64_t *full, uint64_t *recoded, uint64_t *data_out,
                      int *hist, uint64_t *busy_ns)
{
    uint8_t coeff_in[BATS_M * BATS_M];
    uint8_t coeff_out[BATS_M * BATS_M];
    uint8_t stack_in[BATS_M * 2048];
    uint8_t stack_out[BATS_M * 2048];
    uint8_t *payload_in = NULL;
    uint8_t *payload_out;
    const uint8_t *coeff_src;
    int heap_in = 0;
    int heap_out = 0;
    int direct;
    int n_in = 0;
    int n_out = 0;
    int j;
    int rc;
    unsigned T = s->T;
    uint64_t t0;
    FlowRx *f = flow_get(rx, s->flow_id, 0);

    if (f == NULL) {
        return -1;
    }

    direct = s->n == BATS_M;
    if (direct) {
        for (j = 0; j < BATS_M; j++) {
            if (!s->seen[j]) {
                direct = 0;
                break;
            }
        }
    }
    if (T > 2048u) {
        payload_out = malloc((size_t)BATS_M * T);
        heap_out = 1;
        if (!direct) {
            payload_in = malloc((size_t)BATS_M * T);
            heap_in = 1;
        }
        if (payload_out == NULL || (heap_in && payload_in == NULL)) {
            free(payload_in);
            free(payload_out);
            return -1;
        }
    } else {
        payload_out = stack_out;
        if (!direct) {
            payload_in = stack_in;
        }
    }
    if (direct) {
        coeff_src = s->coeff;
        payload_in = s->payload;
        n_in = BATS_M;
    } else {
        coeff_src = coeff_in;
        for (j = 0; j < BATS_M; j++) {
            if (!s->seen[j]) {
                continue;
            }
            memcpy(coeff_in + (size_t)n_in * BATS_M, s->coeff + (size_t)j * BATS_M, BATS_M);
            memcpy(payload_in + (size_t)n_in * T, s->payload + (size_t)j * T, T);
            n_in++;
        }
    }
    if (n_in <= 0) {
        if (heap_in) {
            free(payload_in);
        }
        if (heap_out) {
            free(payload_out);
        }
        slot_clear(s);
        return 0;
    }
    t0 = mono_ns();
    if (line_copy) {
        memset(coeff_out, 0, sizeof(coeff_out));
        memset(payload_out, 0, (size_t)BATS_M * T);
        n_in = 0;
        for (j = 0; j < BATS_M; j++) {
            if (!s->seen[j]) {
                continue;
            }
            memcpy(coeff_out + (size_t)j * BATS_M, s->coeff + (size_t)j * BATS_M, BATS_M);
            memcpy(payload_out + (size_t)j * T, s->payload + (size_t)j * T, T);
            n_in++;
        }
        n_out = BATS_M;
        rc = 0;
    } else {
        rc = bats_recode(coeff_src, payload_in, n_in, (int)T, &f->recode_state, coeff_out,
                         payload_out, &n_out);
    }
    if (rc != 0 || n_out != BATS_M) {
        if (heap_in) {
            free(payload_in);
        }
        if (heap_out) {
            free(payload_out);
        }
        return -1;
    }
    if (send_coded(tx, f->flow_id, s->gen_id, s->batch_id, T, coeff_out, payload_out, BATS_M,
                   f->file_size, f->k_pkts, f->n_batches, f->n_gens) < 0) {
        if (heap_in) {
            free(payload_in);
        }
        if (heap_out) {
            free(payload_out);
        }
        return -1;
    }
    *busy_ns += mono_ns() - t0;
    if (n_in < BATS_M) {
        (*partial)++;
    } else {
        (*full)++;
        f->full_n++;
    }
    f->batch_n++;
    flow_note_complete(f);
    (*recoded)++;
    *data_out += (uint64_t)BATS_M;
    hist[n_in]++;
    if (heap_in) {
        free(payload_in);
    }
    if (heap_out) {
        free(payload_out);
    }
    if (mark_flushed(maps, nmaps, s->flow_id, s->gen_id, s->batch_id) != 0) {
        slot_clear(s);
        return -1;
    }
    slot_clear(s);
    return 0;
}

static int flush_ready(RelaySlot *slots, RxMeta *rx, Tx *tx, FlushedGen **maps, int *nmaps,
                       uint64_t *partial, uint64_t *full, uint64_t *recoded, uint64_t *data_out,
                       int *hist, uint64_t *busy_ns, int force)
{
    for (;;) {
        int best = -1;
        int i;

        for (i = 0; i < RELAY_SLOTS; i++) {
            if (!slot_ready(&slots[i], force)) {
                continue;
            }
            if (best < 0 || slots[i].gen_id < slots[best].gen_id ||
                (slots[i].gen_id == slots[best].gen_id &&
                 slots[i].batch_id < slots[best].batch_id)) {
                best = i;
            }
        }
        if (best < 0) {
            return 0;
        }
        if (flush_slot(&slots[best], rx, tx, maps, nmaps, partial, full, recoded, data_out, hist,
                       busy_ns) != 0) {
            return -1;
        }
    }
}

static int take_slot(RelaySlot *slots, RxMeta *rx, Tx *tx, FlushedGen **maps, int *nmaps,
                     uint64_t *partial, uint64_t *full, uint64_t *recoded, uint64_t *data_out,
                     int *hist, uint64_t *busy_ns, uint16_t flow_id, uint32_t gen, uint32_t batch)
{
    int i;
    int free_i = -1;

    for (i = 0; i < RELAY_SLOTS; i++) {
        if (slots[i].used && slots[i].flow_id == flow_id && slots[i].gen_id == gen &&
            slots[i].batch_id == batch) {
            return i;
        }
        if (!slots[i].used && free_i < 0) {
            free_i = i;
        }
    }
    if (free_i >= 0) {
        return free_i;
    }
    if (flush_ready(slots, rx, tx, maps, nmaps, partial, full, recoded, data_out, hist, busy_ns,
                    0) != 0) {
        return -2;
    }
    for (i = 0; i < RELAY_SLOTS; i++) {
        if (!slots[i].used && free_i < 0) {
            free_i = i;
        }
    }
    if (free_i >= 0) {
        return free_i;
    }
    return -3;
}

static int relay_on_data(RelaySlot *slots, RxMeta *rx, Tx *tx, FlushedGen **maps, int *nmaps,
                         uint64_t *partial, uint64_t *full, uint64_t *recoded, uint64_t *data_out,
                         int *hist, uint64_t *busy_ns, const uint8_t *buf, size_t len)
{
    uint32_t gen;
    uint32_t batch;
    uint16_t flow_id;
    unsigned index;
    unsigned T;
    int si;
    RelaySlot *s;
    FlowRx *f;

    if (len < BATS_LINE_HDR) {
        return 0;
    }
    T = get_u16(buf + 18);
    index = get_u16(buf + 16);
    if (T < 1 || T > BATS_LINE_MAX_T || index >= BATS_M ||
        len < (size_t)BATS_LINE_HDR + T) {
        return 0;
    }
    flow_id = (uint16_t)get_u16(buf + 6);
    gen = get_u32(buf + 8);
    batch = get_u32(buf + 12);
    f = flow_get(rx, flow_id, 1);
    if (f == NULL) {
        return -1;
    }
    flow_note_meta(f, buf);
    if (batch_was_flushed(*maps, *nmaps, flow_id, gen, batch)) {
        rx->data_pkts++;
        return 0;
    }
    si = take_slot(slots, rx, tx, maps, nmaps, partial, full, recoded, data_out, hist, busy_ns,
                   flow_id, gen, batch);
    if (si == -3) {
        return -3;
    }
    if (si < 0) {
        return -1;
    }
    rx->data_pkts++;
    s = &slots[si];
    if (s->used && (s->T != (uint16_t)T || s->flow_id != flow_id)) {
        return 0;
    }
    if (s->used && s->seen[index]) {
        return 0;
    }
    if (!s->used) {
        memset(s, 0, sizeof(*s));
        s->payload = malloc((size_t)BATS_M * T);
        if (s->payload == NULL) {
            return -1;
        }
        s->used = 1;
        s->flow_id = flow_id;
        s->gen_id = gen;
        s->batch_id = batch;
        s->T = (uint16_t)T;
    }
    memcpy(s->coeff + (size_t)index * BATS_M, buf + 36, BATS_M);
    memcpy(s->payload + (size_t)index * T, buf + BATS_LINE_HDR, T);
    s->seen[index] = 1;
    s->n++;
    s->last_ns = mono_ns();
    return 0;
}

static int relay_has_partial(const RelaySlot *slots);

/* 源后面有两跳中继。看到 END 的一跳会把缺包再留一个 idle-sec。
 * 还没看到 END 时，失败期限要盖住这两跳，所以是 3 个 idle-sec。 */
static uint64_t idle_deadline_ms(int idle_sec, int ended)
{
    uint64_t one;

    if (idle_sec <= 0) {
        return 0;
    }
    one = (uint64_t)idle_sec * 1000ull;
    return ended ? one : one * 3ull;
}

static int relay_wait_ms(const RxMeta *rx, int idle_sec)
{
    uint64_t elapsed_ms = since_ns(mono_ns(), rx->last_ns) / 1000000ull;
    uint64_t idle_ms = idle_deadline_ms(idle_sec, flows_all_ended(rx));
    int wait = 200;

    if (idle_sec > 0) {
        if (elapsed_ms >= idle_ms) {
            return -1;
        }
        if (idle_ms - elapsed_ms < (uint64_t)wait) {
            wait = (int)(idle_ms - elapsed_ms);
        }
    }
    if (wait < 1) {
        wait = 1;
    }
    return wait;
}

static int relay_has_partial(const RelaySlot *slots)
{
    int i;

    for (i = 0; i < RELAY_SLOTS; i++) {
        if (slots[i].used && slots[i].n > 0 && slots[i].n < BATS_M) {
            return 1;
        }
    }
    return 0;
}

#define RELAY_RING_BYTES (512u * 1024u * 1024u)
#define RELAY_SLOT_HDR 16u

typedef struct RxQueue {
    int fd;
    int pkt_T;
    int core;
    CircularBuffer *ring;
    pthread_mutex_t mu;
    pthread_cond_t data_cv;
    pthread_cond_t space_cv;
    size_t slot;
    int stop;
    int ready;
    int failed;
    uint32_t rx_ovfl;
    uint64_t enqueued;
    uint64_t last_rx_ns;
} RxQueue;

static void put_u64_raw(uint8_t *p, uint64_t v)
{
    memcpy(p, &v, sizeof(v));
}

static uint64_t get_u64_raw(const uint8_t *p)
{
    uint64_t v;

    memcpy(&v, p, sizeof(v));
    return v;
}

static void queue_wait_ms(RxQueue *q, int ms)
{
    struct timespec ts;

    if (ms < 1) {
        ms = 1;
    }
    clock_gettime(CLOCK_MONOTONIC, &ts);
    ts.tv_sec += ms / 1000;
    ts.tv_nsec += (long)(ms % 1000) * 1000000L;
    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec++;
        ts.tv_nsec -= 1000000000L;
    }
    pthread_mutex_lock(&q->mu);
    if (q->ring->size < q->slot && !q->stop) {
        (void)pthread_cond_timedwait(&q->data_cv, &q->mu, &ts);
    }
    pthread_mutex_unlock(&q->mu);
}

static int queue_pop(RxQueue *q, uint8_t *slotbuf, uint32_t *len, uint64_t *arrival)
{
    pthread_mutex_lock(&q->mu);
    if (q->ring->size < q->slot) {
        pthread_mutex_unlock(&q->mu);
        return 0;
    }
    if (Buffer_Read(q->ring, slotbuf, q->slot) != CB_OK) {
        q->failed = 1;
        pthread_mutex_unlock(&q->mu);
        return -1;
    }
    pthread_cond_signal(&q->space_cv);
    pthread_mutex_unlock(&q->mu);
    *len = get_u32(slotbuf);
    *arrival = get_u64_raw(slotbuf + 8);
    return 1;
}

static int queue_push(RxQueue *q, const uint8_t *pkt, uint32_t len, int is_data)
{
    uint8_t scratch[RELAY_SLOT_HDR + BATS_LINE_HDR + BATS_LINE_MAX_T];

    if (q == NULL || pkt == NULL || len == 0 || RELAY_SLOT_HDR + (size_t)len > q->slot ||
        q->slot > sizeof(scratch)) {
        return -1;
    }
    put_u32(scratch, len);
    scratch[4] = is_data ? 1 : 0;
    memset(scratch + 5, 0, RELAY_SLOT_HDR - 5);
    memcpy(scratch + RELAY_SLOT_HDR, pkt, len);
    pthread_mutex_lock(&q->mu);
    while (q->ring->size + q->slot > q->ring->capacity && !q->stop && !q->failed) {
        pthread_cond_wait(&q->space_cv, &q->mu);
    }
    if (q->stop || q->failed) {
        pthread_mutex_unlock(&q->mu);
        return -1;
    }
    if (Buffer_Write(q->ring, scratch, q->slot) != CB_OK) {
        q->failed = 1;
        pthread_cond_signal(&q->data_cv);
        pthread_mutex_unlock(&q->mu);
        return -1;
    }
    q->enqueued++;
    pthread_cond_signal(&q->data_cv);
    pthread_mutex_unlock(&q->mu);
    return 0;
}

typedef struct {
    Tx *tx;
    RxQueue *q;
} RelaySend;

static void relay_recv_fail(RxQueue *q);

/* 已经排好的等长数据报，一次 sendmmsg。每个 iovec 仍是一个独立的 UDP 包。 */
static int send_datagrams(Tx *tx, const uint8_t *pkts, int n_pkts, size_t stride, int is_data)
{
    enum { SEND_IOV = 64 };
    struct mmsghdr msgs[SEND_IOV];
    struct iovec iov[SEND_IOV];
    int i = 0;

    if (n_pkts < 1 || stride == 0) {
        return -1;
    }
    while (i < n_pkts) {
        int nmsg = 0;
        size_t bytes = 0;
        struct mmsghdr *cur;
        unsigned left;

        memset(msgs, 0, sizeof(msgs));
        while (i < n_pkts && nmsg < SEND_IOV) {
            iov[nmsg].iov_base = (void *)(pkts + (size_t)i * stride);
            iov[nmsg].iov_len = stride;
            msgs[nmsg].msg_hdr.msg_name = &tx->next;
            msgs[nmsg].msg_hdr.msg_namelen = sizeof(tx->next);
            msgs[nmsg].msg_hdr.msg_iov = &iov[nmsg];
            msgs[nmsg].msg_hdr.msg_iovlen = 1;
            bytes += stride;
            nmsg++;
            i++;
        }
        pace(tx, bytes);
        cur = msgs;
        left = (unsigned)nmsg;
        while (left > 0) {
            int sent = sendmmsg(tx->fd, cur, left, MSG_DONTWAIT);

            if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
                if (send_wait_out(tx->fd) != 0) {
                    perror("bats-line: poll");
                    return -1;
                }
                continue;
            }
            if (sent < 0) {
                perror("bats-line: sendmmsg");
                return -1;
            }
            if (sent == 0) {
                if (send_wait_out(tx->fd) != 0) {
                    perror("bats-line: poll");
                    return -1;
                }
                continue;
            }
            cur += sent;
            left -= (unsigned)sent;
        }
        tx->sent += (uint64_t)nmsg;
        if (is_data) {
            tx->wire_bytes += bytes;
        }
    }
    return 0;
}

/* 等长 DATA 优先走 UDP_SEGMENT，一次系统调用切成多个数据报。切不开就退回 sendmmsg。 */
static int send_train(Tx *tx, uint8_t *pkts, int n, size_t stride, int is_data)
{
    int cap;
    int off = 0;

    if (!is_data || tx->gso_off || tx->loss_percent > 0) {
        return send_datagrams(tx, pkts, n, stride, is_data);
    }
    cap = (int)(60000u / stride);
    if (cap < 1) {
        cap = 1;
    }
    while (off < n) {
        int chunk = n - off;
        int gr;

        if (chunk > cap) {
            chunk = cap;
        }
        gr = send_gso(tx, pkts + (size_t)off * stride, chunk, stride);
        if (gr == 0) {
            off += chunk;
            continue;
        }
        tx->gso_off = 1;
        if (gr == -2) {
            fprintf(stderr, "bats-line: udp gso 只写出一部分，停止转发\n");
            return -1;
        }
        return send_datagrams(tx, pkts + (size_t)off * stride, n - off, stride, is_data);
    }
    return 0;
}

#define RELAY_TX_BATCH 40

static void *relay_send_main(void *arg)
{
    RelaySend *rs = arg;
    Tx *tx = rs->tx;
    RxQueue *q = rs->q;
    uint8_t *slotbuf = NULL;
    uint8_t *batch = NULL;
    uint8_t *stash = NULL;
    uint32_t stash_len = 0;
    int stash_data = 0;
    int have_stash = 0;
    size_t stride_max = (size_t)BATS_LINE_HDR + BATS_LINE_MAX_T;

    pin_core(2);
    pthread_setname_np(pthread_self(), "rly-send");
    slotbuf = malloc(q->slot);
    batch = malloc(RELAY_TX_BATCH * stride_max);
    stash = malloc(stride_max);
    if (slotbuf == NULL || batch == NULL || stash == NULL) {
        free(slotbuf);
        free(batch);
        free(stash);
        relay_recv_fail(q);
        return NULL;
    }
    for (;;) {
        int n = 0;
        uint32_t stride = 0;
        int is_data = 0;
        int stop;

        if (have_stash) {
            memcpy(batch, stash, stash_len);
            stride = stash_len;
            is_data = stash_data;
            n = 1;
            have_stash = 0;
        }
        while (n < RELAY_TX_BATCH) {
            uint32_t len = 0;
            uint64_t arrival = 0;
            int popped;
            int data;

            popped = queue_pop(q, slotbuf, &len, &arrival);
            if (popped < 0) {
                relay_recv_fail(q);
                goto done;
            }
            if (popped == 0) {
                break;
            }
            if (len == 0 || (size_t)len > stride_max) {
                relay_recv_fail(q);
                goto done;
            }
            data = slotbuf[4] ? 1 : 0;
            if (data && drop_data(tx)) {
                tx->dropped++;
                continue;
            }
            if (n > 0 && (len != stride || data != is_data)) {
                memcpy(stash, slotbuf + RELAY_SLOT_HDR, len);
                stash_len = len;
                stash_data = data;
                have_stash = 1;
                break;
            }
            if (n == 0) {
                stride = len;
                is_data = data;
            }
            memcpy(batch + (size_t)n * stride, slotbuf + RELAY_SLOT_HDR, len);
            n++;
        }
        if (n > 0) {
            if (send_train(tx, batch, n, stride, is_data) != 0) {
                relay_recv_fail(q);
                break;
            }
            continue;
        }
        pthread_mutex_lock(&q->mu);
        stop = q->stop && q->ring->size < q->slot;
        pthread_mutex_unlock(&q->mu);
        if (stop && !have_stash) {
            break;
        }
        queue_wait_ms(q, 20);
    }
done:
    free(slotbuf);
    free(batch);
    free(stash);
    return NULL;
}

static uint64_t queue_last_rx(RxQueue *q)
{
    uint64_t v;

    pthread_mutex_lock(&q->mu);
    v = q->last_rx_ns;
    pthread_mutex_unlock(&q->mu);
    return v;
}

#define RELAY_RX_BATCH 64

static void relay_note_ovfl(RxQueue *q, struct msghdr *hdr)
{
    struct cmsghdr *cmsg;

    for (cmsg = CMSG_FIRSTHDR(hdr); cmsg != NULL; cmsg = CMSG_NXTHDR(hdr, cmsg)) {
        uint32_t v;

        if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SO_RXQ_OVFL ||
            cmsg->cmsg_len < CMSG_LEN(sizeof(v))) {
            continue;
        }
        memcpy(&v, CMSG_DATA(cmsg), sizeof(v));
        if (v > q->rx_ovfl) {
            q->rx_ovfl = v;
        }
    }
}

static unsigned udp_gro_seg(struct msghdr *hdr)
{
    struct cmsghdr *cmsg;

    for (cmsg = CMSG_FIRSTHDR(hdr); cmsg != NULL; cmsg = CMSG_NXTHDR(hdr, cmsg)) {
        uint16_t v;

        if (cmsg->cmsg_level != SOL_UDP || cmsg->cmsg_type != UDP_GRO ||
            cmsg->cmsg_len < CMSG_LEN(sizeof(v))) {
            continue;
        }
        memcpy(&v, CMSG_DATA(cmsg), sizeof(v));
        return v;
    }
    return 0;
}

static int relay_pkt_ok(const uint8_t *buf, unsigned n, size_t max_len)
{
    return n >= BATS_LINE_HDR && (size_t)n <= max_len && get_u32(buf) == BATS_LINE_MAGIC &&
           buf[4] == BATS_LINE_VERSION && (buf[5] == BATS_LINE_DATA || buf[5] == BATS_LINE_END);
}

static void relay_recv_fail(RxQueue *q)
{
    pthread_mutex_lock(&q->mu);
    q->failed = 1;
    q->ready = 1;
    pthread_cond_broadcast(&q->data_cv);
    pthread_cond_broadcast(&q->space_cv);
    pthread_mutex_unlock(&q->mu);
}

/* 用户态环放不下时，先把内核套接字里的包拷到这里，再等环空出位置。
   上限大约是一只套接字缓冲；再多说明重编码已经掉队，继续堆只会把内存吃光。 */
#define RELAY_SPILL_MAX (64u * 1024u * 1024u)
#define RELAY_SPILL_HARD (RELAY_SPILL_MAX + 32u * 1024u * 1024u)

typedef struct {
    uint8_t *buf;
    size_t n;
    size_t cap;
} Spill;

static void spill_free(Spill *sp)
{
    free(sp->buf);
    sp->buf = NULL;
    sp->n = 0;
    sp->cap = 0;
}

static void spill_consume(Spill *sp, size_t off)
{
    if (off == 0) {
        return;
    }
    if (off < sp->n) {
        memmove(sp->buf, sp->buf + off, sp->n - off);
    }
    sp->n -= off;
}

static int spill_push(Spill *sp, size_t slot, uint32_t len, uint64_t arrival, const uint8_t *pkt)
{
    uint8_t *d;
    uint8_t *nb;
    size_t need;
    size_t ncap;

    need = sp->n + slot;
    if (need > RELAY_SPILL_HARD) {
        return -1;
    }
    if (need > sp->cap) {
        ncap = sp->cap == 0 ? (1024u * 1024u) : sp->cap * 2u;
        if (ncap < need) {
            ncap = need;
        }
        if (ncap > RELAY_SPILL_HARD) {
            ncap = RELAY_SPILL_HARD;
        }
        nb = realloc(sp->buf, ncap);
        if (nb == NULL) {
            return -1;
        }
        sp->buf = nb;
        sp->cap = ncap;
    }
    d = sp->buf + sp->n;
    put_u32(d, len);
    memset(d + 4, 0, RELAY_SLOT_HDR - 4);
    put_u64_raw(d + 8, arrival);
    memcpy(d + RELAY_SLOT_HDR, pkt, len);
    sp->n += slot;
    return 0;
}

static void relay_prep_msgs(struct mmsghdr *msgs, struct iovec *iov, uint8_t *gro, size_t gro_cap,
                            uint8_t *ctrl, size_t ctrl_stride)
{
    int i;

    for (i = 0; i < RELAY_RX_BATCH; i++) {
        iov[i].iov_base = gro + (size_t)i * gro_cap;
        iov[i].iov_len = gro_cap;
        msgs[i].msg_hdr.msg_name = NULL;
        msgs[i].msg_hdr.msg_namelen = 0;
        msgs[i].msg_hdr.msg_iov = &iov[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
        msgs[i].msg_hdr.msg_control = ctrl + (size_t)i * ctrl_stride;
        msgs[i].msg_hdr.msg_controllen = ctrl_stride;
        msgs[i].msg_hdr.msg_flags = 0;
        msgs[i].msg_len = 0;
    }
}

/* 返回 0 表示这一个合并包不能用。截断、拆不开、段长不对都算。 */
static unsigned relay_resolve_seg(struct msghdr *hdr, unsigned total, size_t max_len)
{
    static int warned_split;
    static int warned_seg;
    unsigned seg;

    if ((hdr->msg_flags & MSG_TRUNC) != 0 || total == 0) {
        return 0;
    }
    seg = udp_gro_seg(hdr);
    if (seg == 0) {
        if (total <= max_len) {
            return total;
        }
        if (max_len > 0 && total % (unsigned)max_len == 0) {
            return (unsigned)max_len;
        }
        if (!warned_split) {
            fprintf(stderr, "bats-line: 无法拆开合并包 total=%u\n", total);
            warned_split = 1;
        }
        return 0;
    }
    if (seg > max_len) {
        if (!warned_seg) {
            fprintf(stderr, "bats-line: GRO 段长 %u 超过单包 %zu\n", seg, max_len);
            warned_seg = 1;
        }
        return 0;
    }
    return seg;
}

/* 不持有 q->mu。把套接字里已经排队的包读进 spill，直到 EAGAIN，或 spill 达到上限。
   返回 0 表示套接字已空，1 表示 spill 满了先停，-1 表示读失败或内存不足。 */
static int relay_drain_socket(RxQueue *q, Spill *sp, struct mmsghdr *msgs, struct iovec *iov,
                              uint8_t *gro, size_t gro_cap, uint8_t *ctrl, size_t ctrl_stride,
                              size_t max_len, uint32_t *ovfl)
{
    for (;;) {
        int nmsg;
        int i;
        uint64_t arrival;

        if (sp->n >= RELAY_SPILL_MAX) {
            return 1;
        }
        relay_prep_msgs(msgs, iov, gro, gro_cap, ctrl, ctrl_stride);
        nmsg = recvmmsg(q->fd, msgs, RELAY_RX_BATCH, MSG_DONTWAIT, NULL);
        if (nmsg < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return 0;
            }
            perror("bats-line: recvmmsg");
            return -1;
        }
        if (nmsg == 0) {
            return 0;
        }
        arrival = mono_ns();
        for (i = 0; i < nmsg; i++) {
            unsigned total = msgs[i].msg_len;
            unsigned seg = relay_resolve_seg(&msgs[i].msg_hdr, total, max_len);
            unsigned off = 0;
            uint8_t *base = gro + (size_t)i * gro_cap;
            uint32_t v;

            v = 0;
            {
                struct cmsghdr *cmsg;

                for (cmsg = CMSG_FIRSTHDR(&msgs[i].msg_hdr); cmsg != NULL;
                     cmsg = CMSG_NXTHDR(&msgs[i].msg_hdr, cmsg)) {
                    uint32_t one;

                    if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SO_RXQ_OVFL ||
                        cmsg->cmsg_len < CMSG_LEN(sizeof(one))) {
                        continue;
                    }
                    memcpy(&one, CMSG_DATA(cmsg), sizeof(one));
                    if (one > v) {
                        v = one;
                    }
                }
            }
            if (v > *ovfl) {
                *ovfl = v;
            }
            if (seg == 0) {
                continue;
            }
            while (off < total) {
                unsigned n = seg;

                if (total - off < n) {
                    n = total - off;
                }
                if (n == 0) {
                    break;
                }
                if (relay_pkt_ok(base + off, n, max_len)) {
                    if (spill_push(sp, q->slot, n, arrival, base + off) != 0) {
                        return -1;
                    }
                }
                off += n;
            }
        }
    }
}

/* 持有 q->mu。把 spill 写入环。环满之前先放开锁，把内核套接字读空，再等空位。
   成功返回 0，失败或停止返回 -1。返回时仍持有锁。 */
static int spill_into_ring(RxQueue *q, Spill *sp, struct mmsghdr *msgs, struct iovec *iov,
                           uint8_t *gro, size_t gro_cap, uint8_t *ctrl, size_t ctrl_stride,
                           size_t max_len)
{
    size_t off = 0;
    int since_drain = 256;
    int wrote = 0;

    for (;;) {
        if (off >= sp->n) {
            if (wrote) {
                pthread_cond_signal(&q->data_cv);
            }
            sp->n = 0;
            return 0;
        }
        if (q->stop) {
            return -1;
        }
        if (q->ring->size + q->slot > q->ring->capacity || since_drain >= 256) {
            int dr;
            uint32_t extra = 0;

            spill_consume(sp, off);
            off = 0;
            since_drain = 0;
            if (wrote) {
                pthread_cond_signal(&q->data_cv);
                wrote = 0;
            }
            pthread_mutex_unlock(&q->mu);
            dr = relay_drain_socket(q, sp, msgs, iov, gro, gro_cap, ctrl, ctrl_stride, max_len,
                                    &extra);
            pthread_mutex_lock(&q->mu);
            if (extra > q->rx_ovfl) {
                q->rx_ovfl = extra;
            }
            if (dr < 0) {
                q->failed = 1;
                q->ready = 1;
                pthread_cond_signal(&q->data_cv);
                return -1;
            }
            if (q->stop) {
                return -1;
            }
            if (q->ring->size + q->slot > q->ring->capacity) {
                pthread_cond_wait(&q->space_cv, &q->mu);
            }
            continue;
        }
        if (Buffer_Write(q->ring, sp->buf + off, q->slot) != CB_OK) {
            q->failed = 1;
            q->ready = 1;
            pthread_cond_signal(&q->data_cv);
            return -1;
        }
        q->last_rx_ns = get_u64_raw(sp->buf + off + 8);
        q->enqueued++;
        off += q->slot;
        since_drain++;
        wrote = 1;
    }
}

static void *relay_recv_main(void *arg)
{
    RxQueue *q = arg;
    struct mmsghdr *msgs = NULL;
    struct iovec *iov = NULL;
    uint8_t *gro = NULL;
    uint8_t *scratch = NULL;
    uint8_t *ctrl = NULL;
    size_t max_len = (size_t)BATS_LINE_HDR + (size_t)q->pkt_T;
    /* 内核一次最多合并约 64KB。按 64 个包留空，避免 MSG_TRUNC 把一整串丢掉。 */
    size_t gro_cap = 64u * max_len;
    size_t ctrl_stride = 256;
    Spill spill;
    int i;

    memset(&spill, 0, sizeof(spill));
    msgs = calloc(RELAY_RX_BATCH, sizeof(*msgs));
    iov = calloc(RELAY_RX_BATCH, sizeof(*iov));
    gro = malloc(RELAY_RX_BATCH * gro_cap);
    scratch = malloc(q->slot);
    ctrl = malloc(RELAY_RX_BATCH * ctrl_stride);
    if (msgs == NULL || iov == NULL || gro == NULL || scratch == NULL || ctrl == NULL) {
        free(msgs);
        free(iov);
        free(gro);
        free(scratch);
        free(ctrl);
        relay_recv_fail(q);
        return NULL;
    }
    if (q->core >= 0) {
        pin_core(q->core);
        pthread_setname_np(pthread_self(), "rly-recv");
    } else {
        pthread_setname_np(pthread_self(), "dst-recv");
    }
    (void)setpriority(PRIO_PROCESS, (id_t)syscall(SYS_gettid), -10);
    pthread_mutex_lock(&q->mu);
    q->ready = 1;
    pthread_cond_signal(&q->data_cv);
    pthread_mutex_unlock(&q->mu);
    for (;;) {
        struct pollfd pfd;
        int pr;
        int stop;

        pthread_mutex_lock(&q->mu);
        stop = q->stop;
        pthread_mutex_unlock(&q->mu);
        if (stop) {
            break;
        }
        pfd.fd = q->fd;
        pfd.events = POLLIN;
        pr = poll(&pfd, 1, 50);
        if (pr < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("bats-line: poll");
            relay_recv_fail(q);
            break;
        }
        if (pr == 0) {
            continue;
        }
        for (;;) {
            int nmsg;
            int nvalid = 0;
            int spilled = 0;
            uint64_t arrival;

            relay_prep_msgs(msgs, iov, gro, gro_cap, ctrl, ctrl_stride);
            nmsg = recvmmsg(q->fd, msgs, RELAY_RX_BATCH, MSG_DONTWAIT, NULL);
            if (nmsg < 0) {
                if (errno == EINTR) {
                    continue;
                }
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break;
                }
                perror("bats-line: recvmmsg");
                relay_recv_fail(q);
                goto done;
            }
            if (nmsg == 0) {
                break;
            }
            arrival = mono_ns();
            pthread_mutex_lock(&q->mu);
            for (i = 0; i < nmsg; i++) {
                unsigned total = msgs[i].msg_len;
                unsigned seg = relay_resolve_seg(&msgs[i].msg_hdr, total, max_len);
                unsigned off = 0;
                uint8_t *base = gro + (size_t)i * gro_cap;

                relay_note_ovfl(q, &msgs[i].msg_hdr);
                if (seg == 0) {
                    continue;
                }
                while (off < total) {
                    unsigned n = seg;

                    if (total - off < n) {
                        n = total - off;
                    }
                    if (n == 0) {
                        break;
                    }
                    if (!relay_pkt_ok(base + off, n, max_len)) {
                        off += n;
                        continue;
                    }
                    /* 环满时不能停在这里等。先把已经读出的包和套接字里剩下的包放进 spill，
                       再等空位，否则内核队列会在等待期间满掉。 */
                    if (spilled || q->ring->size + q->slot > q->ring->capacity) {
                        if (spill_push(&spill, q->slot, n, arrival, base + off) != 0) {
                            q->failed = 1;
                            q->ready = 1;
                            pthread_cond_signal(&q->data_cv);
                            pthread_mutex_unlock(&q->mu);
                            goto done;
                        }
                        spilled = 1;
                    } else {
                        put_u32(scratch, n);
                        put_u64_raw(scratch + 8, arrival);
                        memcpy(scratch + RELAY_SLOT_HDR, base + off, n);
                        if (Buffer_Write(q->ring, scratch, q->slot) != CB_OK) {
                            q->failed = 1;
                            q->ready = 1;
                            pthread_cond_signal(&q->data_cv);
                            pthread_mutex_unlock(&q->mu);
                            goto done;
                        }
                        q->last_rx_ns = arrival;
                        q->enqueued++;
                        nvalid++;
                    }
                    off += n;
                }
            }
            if (nvalid > 0) {
                pthread_cond_signal(&q->data_cv);
            }
            if (spilled &&
                spill_into_ring(q, &spill, msgs, iov, gro, gro_cap, ctrl, ctrl_stride, max_len) !=
                    0) {
                pthread_mutex_unlock(&q->mu);
                goto done;
            }
            pthread_mutex_unlock(&q->mu);
        }
    }
done:
    spill_free(&spill);
    free(msgs);
    free(iov);
    free(gro);
    free(scratch);
    free(ctrl);
    return NULL;
}

typedef struct {
    uint8_t *data;
    uint32_t len;
} HeldPkt;

static int held_push(HeldPkt **arr, int *n, int *cap, const uint8_t *pkt, uint32_t len)
{
    HeldPkt *nv;
    uint8_t *copy;
    int ncap;

    if (len == 0) {
        return -1;
    }
    if (*n == *cap) {
        ncap = *cap == 0 ? 64 : *cap * 2;
        nv = realloc(*arr, (size_t)ncap * sizeof(HeldPkt));
        if (nv == NULL) {
            return -1;
        }
        *arr = nv;
        *cap = ncap;
    }
    copy = malloc(len);
    if (copy == NULL) {
        return -1;
    }
    memcpy(copy, pkt, len);
    (*arr)[*n].data = copy;
    (*arr)[*n].len = len;
    (*n)++;
    return 0;
}

static void held_drop(HeldPkt *arr, int *n, int i)
{
    free(arr[i].data);
    if (i + 1 < *n) {
        memmove(&arr[i], &arr[i + 1], (size_t)(*n - i - 1) * sizeof(HeldPkt));
    }
    (*n)--;
}

static void held_free(HeldPkt *arr, int n)
{
    int i;

    for (i = 0; i < n; i++) {
        free(arr[i].data);
    }
    free(arr);
}

static int run_relay(int fd, Tx *tx, uint64_t recode_seed, int reorder_ms, int idle_sec,
                     int pkt_T, int expect_flows)
{
    RelaySlot *slots = NULL;
    FlushedGen *maps = NULL;
    RxMeta rx;
    RxQueue q;
    RxQueue sq;
    RelaySend send_arg;
    pthread_t recv_th;
    pthread_t send_th;
    int send_started = 0;
    pthread_condattr_t attr;
    uint8_t *slotbuf = NULL;
    uint64_t partial = 0;
    uint64_t full = 0;
    uint64_t empty = 0;
    uint64_t recoded = 0;
    uint64_t data_out = 0;
    uint64_t busy_ns = 0;
    int nmaps = 0;
    int hist[BATS_M + 1];
    int rc = 1;
    int started = 0;
    int failed = 0;
    uint32_t socket_drops = 0;
    size_t block = 256u * 1024u;
    size_t blocks;
    HeldPkt *held = NULL;
    int nheld = 0;
    int capheld = 0;

    if (pkt_T < 1 || pkt_T > BATS_LINE_MAX_T) {
        die("relay 的 T 不合法");
    }
    (void)reorder_ms;
    memset(&rx, 0, sizeof(rx));
    rx.recode_seed = recode_seed ? recode_seed : 1;
    rx.expect_flows = expect_flows > 0 ? expect_flows : 1;
    memset(hist, 0, sizeof(hist));
    memset(&q, 0, sizeof(q));
    memset(&sq, 0, sizeof(sq));
    slots = calloc(RELAY_SLOTS, sizeof(*slots));
    if (slots == NULL) {
        die("relay 槽内存不足");
    }
    q.fd = fd;
    q.pkt_T = pkt_T;
    q.core = 0;
    q.slot = RELAY_SLOT_HDR + (size_t)BATS_LINE_HDR + (size_t)pkt_T;
    q.last_rx_ns = mono_ns();
    blocks = RELAY_RING_BYTES / block;
    if (blocks < 1 || q.slot > block) {
        die("relay 队列参数不合法");
    }
    if (Buffer_Init(&q.ring, blocks, block, CB_OVERFLOW_REJECT) != CB_OK) {
        die("relay 队列内存不足");
    }
    pthread_mutex_init(&q.mu, NULL);
    pthread_condattr_init(&attr);
    pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
    pthread_cond_init(&q.data_cv, &attr);
    pthread_cond_init(&q.space_cv, &attr);
    pthread_condattr_destroy(&attr);
    slotbuf = calloc(1, q.slot);
    if (slotbuf == NULL) {
        die("relay 队列内存不足");
    }
    {
        int on = 1;

        (void)setsockopt(fd, SOL_SOCKET, SO_RXQ_OVFL, &on, sizeof(on));
    }
    if (pthread_create(&recv_th, NULL, relay_recv_main, &q) != 0) {
        die("无法创建收包线程");
    }
    started = 1;
    pthread_mutex_lock(&q.mu);
    while (!q.ready && !q.failed) {
        pthread_cond_wait(&q.data_cv, &q.mu);
    }
    failed = q.failed;
    pthread_mutex_unlock(&q.mu);
    if (failed) {
        fprintf(stderr, "bats-line: 收包线程失败\n");
        goto out;
    }
    {
        int rcvbuf = 0;
        socklen_t sl = sizeof(rcvbuf);

        if (getsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, &sl) != 0) {
            rcvbuf = -1;
        }
        fprintf(stderr, "bats-line: listening\n");
        fprintf(stderr, "bats-line: relay queue_mb=%u slot=%zu rcvbuf=%d cores=recv0,calc1,send2 slots=%d flows_cap=%d\n",
                (unsigned)(RELAY_RING_BYTES / (1024u * 1024u)), q.slot, rcvbuf, RELAY_SLOTS,
                BATS_MAX_FLOWS);
    }
    sq.core = -1;
    sq.slot = q.slot;
    sq.pkt_T = pkt_T;
    blocks = RELAY_RING_BYTES / block;
    if (Buffer_Init(&sq.ring, blocks, block, CB_OVERFLOW_REJECT) != CB_OK) {
        die("relay 发送队列内存不足");
    }
    pthread_mutex_init(&sq.mu, NULL);
    pthread_condattr_init(&attr);
    pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
    pthread_cond_init(&sq.data_cv, &attr);
    pthread_cond_init(&sq.space_cv, &attr);
    pthread_condattr_destroy(&attr);
    send_arg.tx = tx;
    send_arg.q = &sq;
    tx->outq = &sq;
    if (pthread_create(&send_th, NULL, relay_send_main, &send_arg) != 0) {
        die("无法创建发送线程");
    }
    send_started = 1;
    pin_core(1);
    pthread_setname_np(pthread_self(), "rly-calc");
    rx.last_ns = q.last_rx_ns;
    for (;;) {
        uint32_t len = 0;
        uint64_t arrival = 0;
        int popped;
        uint64_t now;
        uint64_t last_rx;
        uint64_t elapsed_ms;
        int wait;
        uint64_t enqueued;
        uint32_t ovfl;

        popped = queue_pop(&q, slotbuf, &len, &arrival);
        if (popped < 0) {
            fprintf(stderr, "bats-line: 队列读取失败\n");
            goto out;
        }
        if (popped > 0) {
            const uint8_t *pkt = slotbuf + RELAY_SLOT_HDR;

            if (len < BATS_LINE_HDR || get_u32(pkt) != BATS_LINE_MAGIC ||
                pkt[4] != BATS_LINE_VERSION) {
                continue;
            }
            if (pkt[5] == BATS_LINE_END) {
                FlowRx *f = flow_get(&rx, (uint16_t)get_u16(pkt + 6), 1);

                if (f == NULL) {
                    fprintf(stderr, "bats-line: 流数量超过 %d\n", BATS_MAX_FLOWS);
                    goto out;
                }
                flow_note_meta(f, pkt);
                f->end_seen = 1;
                continue;
            }
            if (pkt[5] != BATS_LINE_DATA) {
                continue;
            }
            (void)arrival;
            {
                int prc = relay_on_data(slots, &rx, tx, &maps, &nmaps, &partial, &full, &recoded,
                                        &data_out, hist, &busy_ns, pkt, len);

                if (prc == -3) {
                    while (held_push(&held, &nheld, &capheld, pkt, len) != 0) {
                        int hi = 0;
                        int placed = 0;

                        while (hi < nheld) {
                            int hrc = relay_on_data(slots, &rx, tx, &maps, &nmaps, &partial, &full,
                                                    &recoded, &data_out, hist, &busy_ns,
                                                    held[hi].data, held[hi].len);

                            if (hrc == -3) {
                                hi++;
                                continue;
                            }
                            if (hrc != 0 ||
                                flush_ready(slots, &rx, tx, &maps, &nmaps, &partial, &full,
                                            &recoded, &data_out, hist, &busy_ns, 0) != 0) {
                                fprintf(stderr, "bats-line: bats_recode 失败\n");
                                goto out;
                            }
                            held_drop(held, &nheld, hi);
                            placed = 1;
                        }
                        if (!placed) {
                            queue_wait_ms(&q, 1);
                        }
                    }
                    continue;
                }
                if (prc != 0 ||
                    flush_ready(slots, &rx, tx, &maps, &nmaps, &partial, &full, &recoded,
                                &data_out, hist, &busy_ns, 0) != 0) {
                    fprintf(stderr, "bats-line: bats_recode 失败\n");
                    goto out;
                }
            }
            continue;
        }
        pthread_mutex_lock(&q.mu);
        failed = q.failed;
        enqueued = q.enqueued;
        ovfl = q.rx_ovfl;
        pthread_mutex_unlock(&q.mu);
        if (failed) {
            fprintf(stderr, "bats-line: 收包线程失败\n");
            goto out;
        }
        if (nheld > 0) {
            int hi = 0;

            while (hi < nheld) {
                int hrc = relay_on_data(slots, &rx, tx, &maps, &nmaps, &partial, &full, &recoded,
                                        &data_out, hist, &busy_ns, held[hi].data, held[hi].len);

                if (hrc == -3) {
                    hi++;
                    continue;
                }
                if (hrc != 0 ||
                    flush_ready(slots, &rx, tx, &maps, &nmaps, &partial, &full, &recoded,
                                &data_out, hist, &busy_ns, 0) != 0) {
                    fprintf(stderr, "bats-line: bats_recode 失败\n");
                    goto out;
                }
                held_drop(held, &nheld, hi);
            }
            if (nheld == 0) {
                continue;
            }
        }
        now = mono_ns();
        last_rx = queue_last_rx(&q);
        rx.last_ns = last_rx;
        elapsed_ms = since_ns(now, last_rx) / 1000000ull;
        /* 每条流的 batch 都已收齐时不必再等 END。END 在队尾，套接字溢出时会先被丢掉。 */
        {
            int fi;

            for (fi = 0; fi < rx.n_flows; fi++) {
                flow_note_complete(&rx.flow[fi]);
            }
        }
        if (flush_ready(slots, &rx, tx, &maps, &nmaps, &partial, &full, &recoded, &data_out, hist,
                        &busy_ns, 0) != 0) {
            fprintf(stderr, "bats-line: bats_recode 失败\n");
            goto out;
        }
        if (nheld > 0 && !relay_has_partial(slots)) {
            continue;
        }
        if (nheld == 0 && flows_all_ended(&rx) && !relay_has_partial(slots)) {
            int fi;
            int missing = 0;

            for (fi = 0; fi < rx.n_flows; fi++) {
                FlowRx *f = &rx.flow[fi];
                uint64_t expect;

                if (!f->used || !f->have_meta) {
                    continue;
                }
                expect = (uint64_t)f->n_gens * (uint64_t)f->n_batches;
                if (expect > 0 && f->batch_n < expect) {
                    missing = 1;
                    break;
                }
            }
            if (!missing) {
                break;
            }
        }
        if (idle_sec > 0 &&
            elapsed_ms >= idle_deadline_ms(idle_sec, flows_all_ended(&rx))) {
            int hi;

            if (!flows_all_ended(&rx)) {
                fprintf(stderr,
                        "bats-line: idle timeout before END packets=%" PRIu64
                        " enqueued=%" PRIu64 " socket_drops=%u\n",
                        rx.data_pkts, enqueued, ovfl);
                goto out;
            }
            if (flush_ready(slots, &rx, tx, &maps, &nmaps, &partial, &full, &recoded, &data_out,
                            hist, &busy_ns, 1) != 0) {
                fprintf(stderr, "bats-line: bats_recode 失败\n");
                goto out;
            }
            hi = 0;
            while (hi < nheld) {
                int hrc = relay_on_data(slots, &rx, tx, &maps, &nmaps, &partial, &full, &recoded,
                                        &data_out, hist, &busy_ns, held[hi].data, held[hi].len);

                if (hrc == -3) {
                    hi++;
                    continue;
                }
                if (hrc != 0) {
                    fprintf(stderr, "bats-line: bats_recode 失败\n");
                    goto out;
                }
                held_drop(held, &nheld, hi);
            }
            if (flush_ready(slots, &rx, tx, &maps, &nmaps, &partial, &full, &recoded, &data_out,
                            hist, &busy_ns, 1) != 0) {
                fprintf(stderr, "bats-line: bats_recode 失败\n");
                goto out;
            }
            if (nheld != 0) {
                fprintf(stderr, "bats-line: 中继槽已满\n");
                goto out;
            }
            break;
        }
        wait = relay_wait_ms(&rx, idle_sec);
        if (wait < 0) {
            fprintf(stderr,
                    "bats-line: idle timeout before END packets=%" PRIu64
                    " enqueued=%" PRIu64 " socket_drops=%u\n",
                    rx.data_pkts, enqueued, ovfl);
            goto out;
        }
        queue_wait_ms(&q, wait);
    }
    {
        int fi;
        uint64_t expected = 0;
        uint64_t seen = 0;

        for (fi = 0; fi < rx.n_flows; fi++) {
            if (!rx.flow[fi].have_meta) {
                continue;
            }
            expected += (uint64_t)rx.flow[fi].n_gens * (uint64_t)rx.flow[fi].n_batches;
            seen += rx.flow[fi].batch_n;
        }
        if (expected > seen) {
            empty = expected - seen;
            if (empty > 0x7fffffffull) {
                hist[0] += 0x7fffffff;
            } else {
                hist[0] += (int)empty;
            }
        }
        for (fi = 0; fi < rx.n_flows; fi++) {
            FlowRx *f = &rx.flow[fi];

            if (!f->used || !f->have_meta) {
                continue;
            }
            if (send_end(tx, f->flow_id, f->file_size, f->k_pkts, f->n_batches, f->n_gens) != 0) {
                goto out;
            }
        }
    }
    pthread_mutex_lock(&q.mu);
    socket_drops = q.rx_ovfl;
    pthread_mutex_unlock(&q.mu);
    if (send_started) {
        pthread_mutex_lock(&sq.mu);
        sq.stop = 1;
        pthread_cond_broadcast(&sq.data_cv);
        pthread_cond_broadcast(&sq.space_cv);
        pthread_mutex_unlock(&sq.mu);
        pthread_join(send_th, NULL);
        send_started = 0;
        tx->outq = NULL;
    }
    fprintf(stderr,
            "bats-line: role=relay api=bats_recode flows=%d batches_recoded=%" PRIu64
            " partial_batches=%" PRIu64 " empty_batches=%" PRIu64
            " full_batches=%" PRIu64 " packets_in=%" PRIu64
            " packets_out=%" PRIu64 " loss_drops=%" PRIu64
            " wire_bytes=%" PRIu64 " forward_ms=%" PRIu64
            " achieved_mbps=%" PRIu64
            " recv_hist=%d,%d,%d,%d,%d,%d,%d,%d,%d socket_drops=%u"
            " pace_intent_us=%" PRIu64 " pace_actual_us=%" PRIu64
            " pace_sleeps=%" PRIu64 " pace_skips=%" PRIu64 "\n",
            rx.n_flows, recoded, partial, empty, full, rx.data_pkts, data_out, tx->dropped,
            tx->wire_bytes, (uint64_t)(busy_ns / 1000000ull),
            mbps_of(tx->wire_bytes, busy_ns), hist[0], hist[1], hist[2], hist[3], hist[4],
            hist[5], hist[6], hist[7], hist[8], socket_drops,
            (uint64_t)(tx->pace_intent_ns / 1000u),
            (uint64_t)(tx->pace_actual_ns / 1000u),
            tx->pace_sleeps, tx->pace_skips);
    rc = 0;
out:
    tx->outq = NULL;
    if (send_started) {
        pthread_mutex_lock(&sq.mu);
        sq.stop = 1;
        pthread_cond_broadcast(&sq.data_cv);
        pthread_cond_broadcast(&sq.space_cv);
        pthread_mutex_unlock(&sq.mu);
        pthread_join(send_th, NULL);
        send_started = 0;
    }
    if (started) {
        pthread_mutex_lock(&q.mu);
        q.stop = 1;
        pthread_cond_broadcast(&q.data_cv);
        pthread_cond_broadcast(&q.space_cv);
        pthread_mutex_unlock(&q.mu);
        pthread_join(recv_th, NULL);
    }
    {
        int i;

        if (slots != NULL) {
            for (i = 0; i < RELAY_SLOTS; i++) {
                slot_clear(&slots[i]);
            }
        }
    }
    free(slots);
    free(slotbuf);
    if (q.ring != NULL) {
        Buffer_Destroy(&q.ring);
    }
    if (sq.ring != NULL) {
        Buffer_Destroy(&sq.ring);
        pthread_cond_destroy(&sq.data_cv);
        pthread_cond_destroy(&sq.space_cv);
        pthread_mutex_destroy(&sq.mu);
    }
    if (started || q.ring != NULL) {
        pthread_cond_destroy(&q.data_cv);
        pthread_cond_destroy(&q.space_cv);
        pthread_mutex_destroy(&q.mu);
    }
    free_flushed(maps, nmaps);
    held_free(held, nheld);
    return rc;
}

static void gen_clear(GenBuf *g)
{
    free(g->batch_id);
    free(g->index);
    free(g->place);
    free(g->coeff);
    free(g->pay);
    memset(g, 0, sizeof(*g));
}

static void dest_unlink(DestPool *pool, GenBuf *target)
{
    GenBuf **pp;

    for (pp = &pool->gens; *pp != NULL; pp = &(*pp)->next) {
        if (*pp == target) {
            *pp = target->next;
            target->next = NULL;
            return;
        }
    }
}

static GenBuf *gen_find(DestPool *pool, uint16_t flow_id, uint32_t gen, int create)
{
    GenBuf *g;

    for (g = pool->gens; g != NULL; g = g->next) {
        if (g->used && g->flow_id == flow_id && g->gen_id == gen) {
            return g;
        }
    }
    if (!create) {
        return NULL;
    }
    g = calloc(1, sizeof(*g));
    if (g == NULL) {
        return NULL;
    }
    g->used = 1;
    g->phase = GEN_FILLING;
    g->flow_id = flow_id;
    g->gen_id = gen;
    g->flow_i = -1;
    g->next = pool->gens;
    pool->gens = g;
    return g;
}

/* 0 已在 g->n 留下空位，1 是重复包，-1 内存不足。载荷由调用者在放开锁之后写入。
   槽位是 batch * BATS_M + index，第一次按 batch 数一次分好，之后查重只看这一格。 */
static int gen_reserve(GenBuf *g, uint32_t n_batches, uint32_t batch, unsigned index, int T,
                       const uint8_t *coeff, size_t *slot)
{
    size_t nslots;
    size_t pos;
    size_t i;

    if (n_batches == 0 || n_batches > 65535u / (unsigned)BATS_M || index >= BATS_M ||
        batch >= n_batches) {
        return -1;
    }
    nslots = (size_t)n_batches * (size_t)BATS_M;
    pos = (size_t)batch * (size_t)BATS_M + index;
    if (g->cap == 0) {
        g->batch_id = malloc(nslots * sizeof(uint32_t));
        g->index = malloc(nslots * sizeof(uint16_t));
        g->place = malloc(nslots * sizeof(uint16_t));
        g->coeff = malloc(nslots * BATS_M);
        g->pay = malloc(nslots * (size_t)T == 0 ? 1 : nslots * (size_t)T);
        if (g->batch_id == NULL || g->index == NULL || g->place == NULL || g->coeff == NULL ||
            g->pay == NULL) {
            return -1;
        }
        for (i = 0; i < nslots; i++) {
            g->place[i] = 0xffffu;
        }
        g->cap = nslots;
    }
    if (pos >= g->cap || g->place == NULL) {
        return -1;
    }
    if (g->place[pos] != 0xffffu) {
        return 1;
    }
    if (g->n >= g->cap) {
        return -1;
    }
    g->place[pos] = (uint16_t)g->n;
    g->batch_id[g->n] = batch;
    g->index[g->n] = (uint16_t)index;
    memcpy(g->coeff + g->n * BATS_M, coeff, BATS_M);
    *slot = g->n;
    return 0;
}

static int pwrite_all(int fd, const uint8_t *buf, size_t n, off_t off)
{
    while (n > 0) {
        ssize_t w = pwrite(fd, buf, n, off);

        if (w < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (w == 0) {
            return -1;
        }
        buf += w;
        n -= (size_t)w;
        off += w;
    }
    return 0;
}

static void tally_gen_ranks(const GenBuf *g, uint32_t n_batches, int hist[BATS_M + 1])
{
    uint32_t b;

    if (g == NULL || n_batches == 0) {
        return;
    }
    for (b = 0; b < n_batches; b++) {
        size_t i;
        int n = 0;
        int filled = 0;
        int rank;
        uint8_t *A;

        for (i = 0; i < g->n; i++) {
            if (g->batch_id[i] == b) {
                n++;
            }
        }
        if (n == 0) {
            hist[0]++;
            continue;
        }
        A = malloc((size_t)n * BATS_M);
        if (A == NULL) {
            die("内存不足");
        }
        for (i = 0; i < g->n; i++) {
            if (g->batch_id[i] != b) {
                continue;
            }
            memcpy(A + (size_t)filled * BATS_M, g->coeff + i * BATS_M, BATS_M);
            filled++;
        }
        rank = coeff_rank_matrix(A, filled);
        free(A);
        if (rank < 0) {
            rank = 0;
        }
        if (rank > BATS_M) {
            rank = BATS_M;
        }
        hist[rank]++;
    }
}

static int decode_gen(GenBuf *g, int K, int T, uint64_t seed, int out_fd, uint32_t file_size,
                      uint64_t *dense, uint64_t *symbols, int *bp_sum, int *inact_sum,
                      int *rank_hist, uint32_t n_batches)
{
    BatsSymbol *syms;
    uint8_t *dst;
    size_t i;
    size_t gen_bytes;
    size_t coded_bytes;
    size_t off;
    size_t chunk;
    int coded_k;
    int bp = 0;
    int inact = 0;
    int ok;

    if (rank_hist != NULL) {
        tally_gen_ranks(g, n_batches, rank_hist);
    }
    gen_bytes = (size_t)K * (size_t)T;
    coded_k = precode_k(K);
    coded_bytes = (size_t)coded_k * (size_t)T;
    if (line_copy) {
        dst = calloc(1, gen_bytes == 0 ? 1 : gen_bytes);
        if (dst == NULL) {
            fprintf(stderr, "bats-line: 内存不足\n");
            return -1;
        }
        for (i = 0; i < g->n; i++) {
            unsigned pno = g->batch_id[i] * (unsigned)BATS_M + g->index[i];

            if (pno >= (unsigned)K) {
                continue;
            }
            memcpy(dst + (size_t)pno * (size_t)T, g->pay + i * (size_t)T, (size_t)T);
            if (line_add1) {
                payload_sub1(dst + (size_t)pno * (size_t)T, (size_t)T);
            }
        }
        off = (size_t)g->gen_id * gen_bytes;
        chunk = gen_bytes;
        if (off >= file_size) {
            chunk = 0;
        } else if (chunk > (size_t)file_size - off) {
            chunk = (size_t)file_size - off;
        }
        if (chunk > 0 && pwrite_all(out_fd, dst, chunk, (off_t)off) != 0) {
            free(dst);
            fprintf(stderr, "bats-line: 写输出失败\n");
            return -1;
        }
        free(dst);
        return 0;
    }
    syms = calloc(g->n == 0 ? 1 : g->n, sizeof(BatsSymbol));
    dst = calloc(1, coded_bytes == 0 ? 1 : coded_bytes);
    if (syms == NULL || dst == NULL) {
        free(syms);
        free(dst);
        fprintf(stderr, "bats-line: 内存不足\n");
        return -1;
    }
    for (i = 0; i < g->n; i++) {
        syms[i].batch_id = g->batch_id[i];
        memcpy(syms[i].coeff, g->coeff + i * BATS_M, BATS_M);
        syms[i].payload = g->pay + i * (size_t)T;
        if (coeff_nnz(syms[i].coeff) > 1) {
            (*dense)++;
        }
        (*symbols)++;
    }
    ok = bats_decode(syms, (int)g->n, coded_k, T, seed, dst, &bp, &inact);
    free(syms);
    if (ok != 1 || bp + inact != coded_k || !precode_matches(dst, K, T, seed)) {
        fprintf(stderr,
                "bats-line: role=dest api=bats_decode gen=%u ok=%d bp=%d "
                "inact=%d symbols=%zu\n",
                g->gen_id, ok, bp, inact, g->n);
        free(dst);
        return -1;
    }
    off = (size_t)g->gen_id * gen_bytes;
    chunk = gen_bytes;
    if (off >= file_size) {
        chunk = 0;
    } else if (chunk > (size_t)file_size - off) {
        chunk = (size_t)file_size - off;
    }
    if (chunk > 0 && pwrite_all(out_fd, dst, chunk, (off_t)off) != 0) {
        free(dst);
        fprintf(stderr, "bats-line: 写输出失败\n");
        return -1;
    }
    free(dst);
    *bp_sum += bp;
    *inact_sum += inact;
    return 0;
}

static void *dest_worker(void *arg)
{
    DestPool *pool = arg;

    pthread_setname_np(pthread_self(), "dst-dec");

    for (;;) {
        GenBuf *g = NULL;
        int fi;
        DestFlow *flow;
        int K;
        int T;
        int fd;
        uint32_t file_size;
        uint32_t gen_id;
        uint64_t seed;
        uint64_t dense = 0;
        uint64_t symbols = 0;
        uint32_t n_batches = 0;
        int fitting;
        int local_hist[BATS_M + 1];
        int bp = 0;
        int inact = 0;
        int rc;

        pthread_mutex_lock(&pool->mu);
        for (;;) {
            if (pool->stop || pool->failed) {
                pthread_mutex_unlock(&pool->mu);
                return NULL;
            }
            for (g = pool->gens; g != NULL; g = g->next) {
                if (g->used && g->phase == GEN_QUEUED) {
                    g->phase = GEN_DECODING;
                    break;
                }
            }
            if (g != NULL) {
                break;
            }
            pthread_cond_wait(&pool->cv, &pool->mu);
        }
        K = pool->K;
        T = pool->T;
        fi = g->flow_i;
        flow = (fi >= 0 && fi < pool->n_flows) ? &pool->flows[fi] : NULL;
        fd = flow != NULL ? flow->fd : -1;
        file_size = flow != NULL ? flow->file_size : 0;
        seed = pool->seed;
        gen_id = g->gen_id;
        fitting = pool->fit_psi;
        if (flow != NULL) {
            n_batches = flow->n_batches;
        }
        pthread_mutex_unlock(&pool->mu);

        if (fitting) {
            memset(local_hist, 0, sizeof(local_hist));
        }
        rc = decode_gen(g, K, T, seed, fd, file_size, &dense, &symbols, &bp, &inact,
                        fitting ? local_hist : NULL, n_batches);

        pthread_mutex_lock(&pool->mu);
        if (fitting) {
            int r;

            for (r = 0; r <= BATS_M; r++) {
                pool->rank_hist[r] += local_hist[r];
            }
            if (flow != NULL && flow->ranked != NULL && gen_id < flow->n_gens) {
                flow->ranked[gen_id] = 1;
            }
        }
        if (rc != 0) {
            pool->failed = 1;
        } else {
            pool->dense += dense;
            pool->symbols += symbols;
            pool->bp_sum += bp;
            pool->inact_sum += inact;
            pool->n_done++;
            if (flow != NULL) {
                flow->n_done++;
                if (flow->finished != NULL && gen_id < flow->n_gens) {
                    flow->finished[gen_id] = 1;
                }
            }
        }
        dest_unlink(pool, g);
        gen_clear(g);
        free(g);
        pthread_cond_broadcast(&pool->cv);
        pthread_mutex_unlock(&pool->mu);
    }
}

static int dest_inflight(const DestPool *pool)
{
    const GenBuf *g;

    for (g = pool->gens; g != NULL; g = g->next) {
        if (g->used && (g->phase == GEN_QUEUED || g->phase == GEN_DECODING)) {
            return 1;
        }
    }
    return 0;
}

static int dest_open_flow(DestPool *pool, DestFlow *f)
{
    char path[1024];
    int n;

    if (f->flow_id == 0 && pool->spare_fd >= 0) {
        f->fd = pool->spare_fd;
        pool->spare_fd = -1;
        return 0;
    }
    if (f->flow_id == 0) {
        n = snprintf(path, sizeof(path), "%s", pool->out_path);
    } else {
        n = snprintf(path, sizeof(path), "%s.%u", pool->out_path, (unsigned)f->flow_id);
    }
    if (n < 0 || (size_t)n >= sizeof(path)) {
        return -1;
    }
    f->fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    return f->fd < 0 ? -1 : 0;
}

static DestFlow *dest_flow_get(DestPool *pool, uint16_t flow_id)
{
    int i;
    DestFlow *f;

    if (flow_id < BATS_MAX_FLOWS) {
        int at = pool->flow_at[flow_id];

        if (at >= 0 && at < pool->n_flows && pool->flows[at].used &&
            pool->flows[at].flow_id == flow_id) {
            return &pool->flows[at];
        }
    } else {
        for (i = 0; i < pool->n_flows; i++) {
            if (pool->flows[i].used && pool->flows[i].flow_id == flow_id) {
                return &pool->flows[i];
            }
        }
    }
    if (pool->n_flows >= BATS_MAX_FLOWS) {
        return NULL;
    }
    f = &pool->flows[pool->n_flows];
    memset(f, 0, sizeof(*f));
    f->used = 1;
    f->flow_id = flow_id;
    f->fd = -1;
    f->last_ns = mono_ns();
    if (dest_open_flow(pool, f) != 0) {
        return NULL;
    }
    if (flow_id < BATS_MAX_FLOWS) {
        pool->flow_at[flow_id] = (int16_t)pool->n_flows;
    }
    pool->n_flows++;
    return f;
}

static void dest_bind_meta(DestFlow *f, const uint8_t *buf)
{
    uint32_t k = get_u32(buf + 24);

    if (k == 0 || f->have_meta) {
        return;
    }
    f->file_size = get_u32(buf + 20);
    f->k_pkts = k;
    f->n_batches = get_u32(buf + 28);
    f->n_gens = get_u32(buf + 32);
    f->have_meta = 1;
    if (f->n_gens == 0) {
        return;
    }
    f->touched = calloc(f->n_gens, 1);
    f->finished = calloc(f->n_gens, 1);
    f->ranked = calloc(f->n_gens, 1);
    if (f->touched == NULL || f->finished == NULL || f->ranked == NULL) {
        die("内存不足");
    }
}

static int dest_all_done(const DestPool *pool)
{
    int i;
    int n = 0;
    int expect = pool->expect_flows > 0 ? pool->expect_flows : 1;

    for (i = 0; i < pool->n_flows; i++) {
        const DestFlow *f = &pool->flows[i];

        if (!f->used) {
            continue;
        }
        n++;
        if (!f->have_meta || f->n_done != (int)f->n_gens) {
            return 0;
        }
    }
    return n >= expect;
}

static void dest_queue(DestPool *pool, GenBuf *g)
{
    g->phase = GEN_QUEUED;
    pthread_cond_broadcast(&pool->cv);
}

static void dest_submit(DestPool *pool, int give_up)
{
    GenBuf *g;

    for (g = pool->gens; g != NULL; g = g->next) {
        DestFlow *f;
        int complete;

        if (!g->used || g->phase != GEN_FILLING || g->flow_i < 0) {
            continue;
        }
        f = &pool->flows[g->flow_i];
        complete = f->n_batches > 0 && g->n >= (size_t)f->n_batches * (size_t)BATS_M;
        if (complete || give_up) {
            dest_queue(pool, g);
        }
    }
}

static int dest_flows_ended(const DestPool *pool)
{
    int i;
    int n = 0;
    int expect = pool->expect_flows > 0 ? pool->expect_flows : 1;

    for (i = 0; i < pool->n_flows; i++) {
        if (!pool->flows[i].used) {
            continue;
        }
        n++;
        if (!pool->flows[i].end_seen) {
            return 0;
        }
    }
    return n >= expect;
}

static int dest_wait_ms(const DestPool *pool, int idle_sec)
{
    int ended = dest_flows_ended(pool);
    uint64_t elapsed_ms = since_ns(mono_ns(), pool->last_rx_ns) / 1000000ull;
    uint64_t idle_ms = idle_deadline_ms(idle_sec, ended);
    int wait = 200;
    const GenBuf *g;

    if (dest_inflight(pool)) {
        return 1;
    }
    for (g = pool->gens; g != NULL; g = g->next) {
        const DestFlow *f;

        if (!g->used || g->phase != GEN_FILLING || g->flow_i < 0) {
            continue;
        }
        f = &pool->flows[g->flow_i];
        if (f->n_batches > 0 && g->n >= (size_t)f->n_batches * (size_t)BATS_M) {
            return 1;
        }
    }
    if (idle_sec > 0) {
        if (elapsed_ms >= idle_ms) {
            return -1;
        }
        if (idle_ms - elapsed_ms < (uint64_t)wait) {
            wait = (int)(idle_ms - elapsed_ms);
        }
    }
    if (wait < 1) {
        wait = 1;
    }
    return wait;
}

/* 返回 0 已处理，-1 失败且 *locked 可能为 1。 */
static int dest_on_packet(DestPool *pool, const uint8_t *buf, size_t n, int K, int T, int *locked)
{
    unsigned Tpkt;
    unsigned index;
    uint32_t gen;
    uint16_t flow_id;
    GenBuf *g;
    DestFlow *flow;

    *locked = 0;
    if (n < BATS_LINE_HDR || get_u32(buf) != BATS_LINE_MAGIC || buf[4] != BATS_LINE_VERSION) {
        return 0;
    }
    pool->last_rx_ns = mono_ns();
    pthread_mutex_lock(&pool->mu);
    *locked = 1;
    flow_id = (uint16_t)get_u16(buf + 6);
    flow = dest_flow_get(pool, flow_id);
    if (flow == NULL) {
        fprintf(stderr, "bats-line: 流数量超过 %d\n", BATS_MAX_FLOWS);
        pool->failed = 1;
        return -1;
    }
    if (buf[5] == BATS_LINE_END) {
        dest_bind_meta(flow, buf);
        flow->end_seen = 1;
        flow->last_ns = pool->last_rx_ns;
        if (flow->have_meta && (int)flow->k_pkts != precode_k(K)) {
            fprintf(stderr,
                    "bats-line: role=dest api=bats_decode ok=0 header_K=%u local_K=%d\n",
                    flow->k_pkts, precode_k(K));
            pool->failed = 1;
            return -1;
        }
        pthread_mutex_unlock(&pool->mu);
        *locked = 0;
        return 0;
    }
    if (buf[5] != BATS_LINE_DATA) {
        pthread_mutex_unlock(&pool->mu);
        *locked = 0;
        return 0;
    }
    Tpkt = get_u16(buf + 18);
    index = get_u16(buf + 16);
    if (Tpkt != (unsigned)T || index >= BATS_M || n < (size_t)BATS_LINE_HDR + Tpkt) {
        pthread_mutex_unlock(&pool->mu);
        *locked = 0;
        return 0;
    }
    dest_bind_meta(flow, buf);
    if (flow->have_meta && (int)flow->k_pkts != precode_k(K)) {
        fprintf(stderr, "bats-line: role=dest api=bats_decode ok=0 header_K=%u local_K=%d\n",
                flow->k_pkts, precode_k(K));
        pool->failed = 1;
        return -1;
    }
    gen = get_u32(buf + 8);
    if (flow->finished != NULL && gen < flow->n_gens && flow->finished[gen]) {
        flow->data_pkts++;
        pthread_mutex_unlock(&pool->mu);
        *locked = 0;
        return 0;
    }
    if (flow->n_gens > 0 && gen >= flow->n_gens) {
        flow->data_pkts++;
        pthread_mutex_unlock(&pool->mu);
        *locked = 0;
        return 0;
    }
    g = gen_find(pool, flow_id, gen, 1);
    if (g == NULL) {
        fprintf(stderr, "bats-line: 内存不足\n");
        pool->failed = 1;
        return -1;
    }
    if (g->phase != GEN_FILLING) {
        flow->data_pkts++;
        pthread_mutex_unlock(&pool->mu);
        *locked = 0;
        return 0;
    }
    flow->data_pkts++;
    flow->last_ns = pool->last_rx_ns;
    g->flow_i = (int)(flow - pool->flows);
    if (flow->touched != NULL && gen < flow->n_gens) {
        flow->touched[gen] = 1;
    }
    g->last_ns = pool->last_rx_ns;
    {
        size_t slot = 0;
        uint8_t *dst;
        int reserved = gen_reserve(g, flow->n_batches, get_u32(buf + 12), index, T, buf + 36, &slot);

        if (reserved < 0) {
            die("内存不足");
        }
        if (reserved == 0) {
            dst = g->pay + slot * (size_t)T;
            pthread_mutex_unlock(&pool->mu);
            *locked = 0;
            memcpy(dst, buf + BATS_LINE_HDR, (size_t)T);
            pthread_mutex_lock(&pool->mu);
            *locked = 1;
            g->n++;
            g->last_ns = mono_ns();
            if (flow->n_batches > 0 && g->n >= (size_t)flow->n_batches * (size_t)BATS_M) {
                dest_queue(pool, g);
            }
        }
    }
    pthread_mutex_unlock(&pool->mu);
    *locked = 0;
    return 0;
}

static int fit_write(DestPool *pool, int K)
{
    const char *path = line_fit_path != NULL ? line_fit_path : PSI_NETWORK_FILE;
    GenBuf *g;
    int fi;
    int r;
    int cap;
    long long total = 0;

    for (g = pool->gens; g != NULL; g = g->next) {
        DestFlow *flow = NULL;
        uint32_t nb = 0;

        if (!g->used) {
            continue;
        }
        if (g->flow_i >= 0 && g->flow_i < pool->n_flows) {
            flow = &pool->flows[g->flow_i];
            nb = flow->n_batches;
        }
        tally_gen_ranks(g, nb, pool->rank_hist);
        if (flow != NULL && flow->ranked != NULL && g->gen_id < flow->n_gens) {
            flow->ranked[g->gen_id] = 1;
        }
    }
    for (fi = 0; fi < pool->n_flows; fi++) {
        DestFlow *flow = &pool->flows[fi];
        uint32_t gen;

        if (!flow->used || !flow->end_seen || flow->ranked == NULL) {
            continue;
        }
        for (gen = 0; gen < flow->n_gens; gen++) {
            if (!flow->ranked[gen]) {
                pool->rank_hist[0] += (int)flow->n_batches;
            }
        }
    }
    fprintf(stderr, "bats-line: 目的端秩个数（秩 0 到 %d）:", BATS_M);
    for (r = 0; r <= BATS_M; r++) {
        fprintf(stderr, " %d", pool->rank_hist[r]);
        total += pool->rank_hist[r];
    }
    fprintf(stderr, "\n");
    if (total <= 0) {
        fprintf(stderr, "bats-line: 没有测到 batch，未写度数\n");
        return -1;
    }
    cap = precode_k(K);
    if (cap > 128) {
        cap = 128;
    }
    if (cap < 1) {
        cap = 1;
    }
    if (!psi_optimize(pool->rank_hist, BATS_M + 1, cap, 0.98, 100, 256.0, 1000000, NULL, 0, NULL,
                      path)) {
        fprintf(stderr, "bats-line: 度数优化失败\n");
        return -1;
    }
    if (strcmp(path, PSI_NETWORK_FILE) == 0) {
        fprintf(stderr,
                "bats-line: 度数已写入 %s。把这个文件放到源的工作目录后再跑下一轮；"
                "源和目的都会自动装上它。这一轮用的仍是测量前的度数。\n",
                path);
    } else {
        fprintf(stderr,
                "bats-line: 度数已写入 %s。下一轮源和目的都加上 --psi %s。"
                "这一轮用的仍是测量前的度数。\n",
                path, path);
    }
    return 0;
}

static int run_dest(int fd, const char *path, int K, int T, uint64_t seed, int reorder_ms,
                    int idle_sec, int expect_flows, int worker_arg)
{
    DestPool pool;
    pthread_t workers[32];
    long nproc;
    int nworkers;
    int started = 0;
    int locked = 0;
    int rc = 1;
    int i;
    int recv_started = 0;
    int queue_inited = 0;
    RxQueue q;
    pthread_t recv_th;
    pthread_condattr_t attr;
    uint8_t *slotbuf = NULL;

    (void)reorder_ms;
    memset(&pool, 0, sizeof(pool));
    pool.fit_psi = line_fit_psi;
    memset(&q, 0, sizeof(q));
    q.core = -1;
    pool.last_rx_ns = mono_ns();
    pool.K = K;
    pool.T = T;
    pool.seed = seed;
    pool.out_path = path;
    pool.expect_flows = expect_flows > 0 ? expect_flows : 1;
    pool.spare_fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (pool.spare_fd < 0) {
        perror("bats-line: open");
        return 1;
    }
    if (pthread_mutex_init(&pool.mu, NULL) != 0 || pthread_cond_init(&pool.cv, NULL) != 0) {
        die("无法创建译码线程");
    }
    nproc = sysconf(_SC_NPROCESSORS_ONLN);
    if (worker_arg > 0) {
        nworkers = worker_arg;
    } else {
        nworkers = nproc > 0 ? (int)nproc : 1;
    }
    if (nworkers > 32) {
        nworkers = 32;
    }
    for (i = 0; i < nworkers; i++) {
        if (pthread_create(&workers[i], NULL, dest_worker, &pool) != 0) {
            die("无法创建译码线程");
        }
        started++;
    }
    q.fd = fd;
    q.pkt_T = T;
    q.slot = RELAY_SLOT_HDR + (size_t)BATS_LINE_HDR + (size_t)T;
    q.last_rx_ns = mono_ns();
    {
        size_t block = 256u * 1024u;
        size_t blocks = RELAY_RING_BYTES / block;
        int on = 1;

        if (blocks < 1 || q.slot > block) {
            die("dest 队列参数不合法");
        }
        if (Buffer_Init(&q.ring, blocks, block, CB_OVERFLOW_REJECT) != CB_OK) {
            die("dest 队列内存不足");
        }
        pthread_mutex_init(&q.mu, NULL);
        pthread_condattr_init(&attr);
        pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
        pthread_cond_init(&q.data_cv, &attr);
        pthread_cond_init(&q.space_cv, &attr);
        pthread_condattr_destroy(&attr);
        queue_inited = 1;
        slotbuf = calloc(1, q.slot);
        if (slotbuf == NULL) {
            die("dest 队列内存不足");
        }
        (void)setsockopt(fd, SOL_SOCKET, SO_RXQ_OVFL, &on, sizeof(on));
        if (pthread_create(&recv_th, NULL, relay_recv_main, &q) != 0) {
            die("无法创建收包线程");
        }
        recv_started = 1;
        pthread_mutex_lock(&q.mu);
        while (!q.ready && !q.failed) {
            pthread_cond_wait(&q.data_cv, &q.mu);
        }
        if (q.failed) {
            pthread_mutex_unlock(&q.mu);
            fprintf(stderr, "bats-line: 收包线程失败\n");
            goto out;
        }
        pthread_mutex_unlock(&q.mu);
    }
    fprintf(stderr, "bats-line: listening\n");
    pthread_setname_np(pthread_self(), "dst-main");
    for (;;) {
        uint32_t len = 0;
        uint64_t arrival = 0;
        int popped;
        uint64_t now;
        int wait;

        popped = queue_pop(&q, slotbuf, &len, &arrival);
        if (popped < 0) {
            fprintf(stderr, "bats-line: 队列读取失败\n");
            goto out;
        }
        if (popped > 0) {
            const uint8_t *pkt = slotbuf + RELAY_SLOT_HDR;
            int prc = dest_on_packet(&pool, pkt, len, K, T, &locked);

            (void)arrival;
            if (prc == -1) {
                goto out;
            }
            continue;
        }
        pthread_mutex_lock(&q.mu);
        if (q.failed) {
            pthread_mutex_unlock(&q.mu);
            fprintf(stderr, "bats-line: 收包线程失败\n");
            goto out;
        }
        pthread_mutex_unlock(&q.mu);
        {
            int fi;

            for (fi = 0; fi < pool.n_flows; fi++) {
                DestFlow *f = &pool.flows[fi];
                uint64_t expect_pkts;

                if (f->end_seen || !f->have_meta) {
                    continue;
                }
                expect_pkts = (uint64_t)f->n_gens * (uint64_t)f->n_batches * (uint64_t)BATS_M;
                if (expect_pkts > 0 && f->data_pkts >= expect_pkts) {
                    f->end_seen = 1;
                }
            }
        }
        pool.last_rx_ns = queue_last_rx(&q);
        now = mono_ns();
        pthread_mutex_lock(&pool.mu);
        locked = 1;
        {
            int ended = dest_flows_ended(&pool);
            uint64_t elapsed_ms = since_ns(now, pool.last_rx_ns) / 1000000ull;
            int due = idle_sec > 0 && elapsed_ms >= idle_deadline_ms(idle_sec, ended);

            dest_submit(&pool, ended && elapsed_ms >= (uint64_t)idle_sec * 1000ull);
            if (pool.failed) {
                goto out;
            }
            if (dest_all_done(&pool)) {
                rc = 0;
            } else if (due && !ended) {
                fprintf(stderr,
                        "bats-line: idle timeout before END data_pkts=%" PRIu64 " gens_done=%d\n",
                        pool.n_flows > 0 ? pool.flows[0].data_pkts : 0, pool.n_done);
                goto out;
            } else if (due && ended) {
                int fi;
                int seen = 0;
                int ended = 0;
                int expect = pool.expect_flows > 0 ? pool.expect_flows : 1;

                for (fi = 0; fi < pool.n_flows; fi++) {
                    DestFlow *f = &pool.flows[fi];
                    uint32_t g;

                    if (!f->used) {
                        continue;
                    }
                    seen++;
                    if (!f->end_seen) {
                        continue;
                    }
                    ended++;
                    if (f->touched == NULL) {
                        continue;
                    }
                    for (g = 0; g < f->n_gens; g++) {
                        if (!f->touched[g]) {
                            fprintf(stderr,
                                    "bats-line: role=dest api=bats_decode flow=%u gen=%u ok=0 "
                                    "bp=0 inact=0 symbols=0\n",
                                    (unsigned)f->flow_id, g);
                            pool.failed = 1;
                            goto out;
                        }
                    }
                }
                if (seen < expect || ended < expect) {
                    fprintf(stderr, "bats-line: idle timeout before END\n");
                    goto out;
                }
            }
        }
        pthread_mutex_unlock(&pool.mu);
        locked = 0;
        if (rc == 0) {
            break;
        }
        wait = dest_wait_ms(&pool, idle_sec);
        if (wait < 0) {
            continue;
        }
        queue_wait_ms(&q, wait);
    }
out:
    if (locked) {
        pthread_mutex_unlock(&pool.mu);
        locked = 0;
    }
    pthread_mutex_lock(&pool.mu);
    pool.stop = 1;
    pthread_cond_broadcast(&pool.cv);
    pthread_mutex_unlock(&pool.mu);
    for (i = 0; i < started; i++) {
        pthread_join(workers[i], NULL);
    }
    pthread_mutex_lock(&pool.mu);
    if (rc == 0 && !pool.failed) {
        uint32_t file_bytes = 0;
        int fi;

        for (fi = 0; fi < pool.n_flows; fi++) {
            file_bytes += pool.flows[fi].file_size;
        }
        fprintf(stderr,
                "bats-line: role=dest api=bats_decode flows=%d gens_ok=%d file_bytes=%u "
                "symbols=%" PRIu64 " dense_coeff_packets=%" PRIu64
                " bp=%d inact=%d\n",
                pool.n_flows, pool.n_done, file_bytes, pool.symbols, pool.dense, pool.bp_sum,
                pool.inact_sum);
    } else {
        rc = 1;
    }
    pthread_mutex_unlock(&pool.mu);
    if (pool.fit_psi && fit_write(&pool, pool.K) != 0) {
        rc = 1;
    }
    if (pool.spare_fd >= 0) {
        close(pool.spare_fd);
    }
    for (i = 0; i < pool.n_flows; i++) {
        if (pool.flows[i].fd >= 0) {
            close(pool.flows[i].fd);
        }
        free(pool.flows[i].touched);
        free(pool.flows[i].finished);
        free(pool.flows[i].ranked);
    }
    while (pool.gens != NULL) {
        GenBuf *g = pool.gens;

        pool.gens = g->next;
        gen_clear(g);
        free(g);
    }
    pthread_cond_destroy(&pool.cv);
    pthread_mutex_destroy(&pool.mu);
    if (recv_started) {
        pthread_mutex_lock(&q.mu);
        q.stop = 1;
        pthread_cond_broadcast(&q.data_cv);
        pthread_cond_broadcast(&q.space_cv);
        pthread_mutex_unlock(&q.mu);
        pthread_join(recv_th, NULL);
    }
    fprintf(stderr, "bats-line: dest socket_drops=%u enqueued=%" PRIu64 "\n", q.rx_ovfl,
            q.enqueued);
    free(slotbuf);
    if (q.ring != NULL) {
        Buffer_Destroy(&q.ring);
    }
    if (queue_inited) {
        pthread_cond_destroy(&q.data_cv);
        pthread_cond_destroy(&q.space_cv);
        pthread_mutex_destroy(&q.mu);
    }
    return rc;
}

static void usage(void)
{
    fprintf(stderr,
            "用法: bats_line --role source|relay|dest [选项]\n"
            "  --next HOST:PORT     下一跳（source、relay）\n"
            "  --listen PORT        监听端口（relay、dest）\n"
            "  --bind ADDR          绑定 IPv4，默认 0.0.0.0\n"
            "  --input PATH         源文件\n"
            "  --output PATH        目的文件。flow 0 写这个路径，其余写 PATH.<flow_id>\n"
            "  --flow-id N          源的第一条流编号，默认 0\n"
            "  --flows N            源同时发出 N 条流，按 batch 交错。中继和目的要等齐 N 条。1..1024，默认 1\n"
            "  --workers N          源的编码线程或目的的译码线程，0 表示按 CPU 数，最大 32。中继固定为收、算、发三个线程\n"
            "  --k N                每个 generation 的源包数（默认 128）\n"
            "  --t N                每个源包字节数（默认 4096）\n"
            "  --seed N             源和目的共用的 code_seed\n"
            "  --recode-seed N      这一跳的 recode_state 初值（默认 1）\n"
            "  --batches N          每个 generation 的 batch 数，0 表示按优化后的度数分布取值\n"
            "  --loss-percent N     发送 DATA 前丢包，0..100\n"
            "  --loss-seed N\n"
            "  --rate-mbps N        0 表示不限速\n"
            "  --reorder-ms N       保留参数，收发不再用它决定缺包\n"
            "  --idle-sec N         空闲这么久仍缺包就 recode 或译码；一直等不到 END 也在这时失败\n"
            "  --copy               不调用 encode/recode/decode，载荷按符号 memcpy\n"
            "  --add1               和 --copy 一起用：encode 每字节加 1，decode 减 1，中继仍拷贝\n"
            "  --psi PATH           源和目的装入这份度数；不给则有 psi_network.txt 就用它，否则用 psi_line.h\n"
            "  --fit-psi [PATH]     只在目的端：按本轮目的端秩分布写出度数，默认 psi_network.txt\n");
}

int main(int argc, char **argv)
{
    const char *role = NULL;
    const char *bind_ip = NULL;
    const char *input = NULL;
    const char *output = NULL;
    const char *next = NULL;
    uint32_t listen_port = 0;
    uint32_t K = 128;
    uint32_t T = 4096;
    uint32_t batches = 0;
    uint32_t loss_percent = 0;
    uint32_t rate_mbps = 0;
    uint32_t reorder_ms = 20;
    uint32_t idle_sec = 15;
    uint32_t flow_id = 0;
    uint32_t n_flows = 1;
    uint32_t n_workers = 0;
    uint64_t seed = 0xB175ull;
    uint64_t recode_seed = 1;
    uint64_t loss_seed = 1;
    Tx tx;
    int fd;
    int i;

    for (i = 1; i < argc; i++) {
        const char *a = argv[i];
        const char *val = (i + 1 < argc) ? argv[i + 1] : NULL;

        if (strcmp(a, "--role") == 0 && val) {
            role = val;
            i++;
        } else if (strcmp(a, "--bind") == 0 && val) {
            bind_ip = val;
            i++;
        } else if (strcmp(a, "--listen") == 0 && val) {
            if (parse_u32(val, &listen_port) != 0 || listen_port > 65535u) {
                die("--listen 不合法");
            }
            i++;
        } else if (strcmp(a, "--next") == 0 && val) {
            next = val;
            i++;
        } else if (strcmp(a, "--input") == 0 && val) {
            input = val;
            i++;
        } else if (strcmp(a, "--output") == 0 && val) {
            output = val;
            i++;
        } else if (strcmp(a, "--k") == 0 && val) {
            if (parse_u32(val, &K) != 0) {
                die("--k 不合法");
            }
            i++;
        } else if (strcmp(a, "--t") == 0 && val) {
            if (parse_u32(val, &T) != 0) {
                die("--t 不合法");
            }
            i++;
        } else if (strcmp(a, "--seed") == 0 && val) {
            if (parse_u64(val, &seed) != 0) {
                die("--seed 不合法");
            }
            i++;
        } else if (strcmp(a, "--recode-seed") == 0 && val) {
            if (parse_u64(val, &recode_seed) != 0) {
                die("--recode-seed 不合法");
            }
            i++;
        } else if (strcmp(a, "--batches") == 0 && val) {
            if (parse_u32(val, &batches) != 0) {
                die("--batches 不合法");
            }
            i++;
        } else if (strcmp(a, "--loss-percent") == 0 && val) {
            if (parse_u32(val, &loss_percent) != 0 || loss_percent > 100u) {
                die("--loss-percent 不合法");
            }
            i++;
        } else if (strcmp(a, "--loss-seed") == 0 && val) {
            if (parse_u64(val, &loss_seed) != 0) {
                die("--loss-seed 不合法");
            }
            i++;
        } else if (strcmp(a, "--rate-mbps") == 0 && val) {
            if (parse_u32(val, &rate_mbps) != 0) {
                die("--rate-mbps 不合法");
            }
            i++;
        } else if (strcmp(a, "--reorder-ms") == 0 && val) {
            if (parse_u32(val, &reorder_ms) != 0) {
                die("--reorder-ms 不合法");
            }
            i++;
        } else if (strcmp(a, "--idle-sec") == 0 && val) {
            if (parse_u32(val, &idle_sec) != 0) {
                die("--idle-sec 不合法");
            }
            i++;
        } else if (strcmp(a, "--flow-id") == 0 && val) {
            if (parse_u32(val, &flow_id) != 0 || flow_id > 65535u) {
                die("--flow-id 不合法");
            }
            i++;
        } else if (strcmp(a, "--flows") == 0 && val) {
            if (parse_u32(val, &n_flows) != 0 || n_flows < 1u || n_flows > BATS_MAX_FLOWS) {
                die("--flows 不合法");
            }
            i++;
        } else if (strcmp(a, "--workers") == 0 && val) {
            if (parse_u32(val, &n_workers) != 0 || n_workers > 32u) {
                die("--workers 不合法");
            }
            i++;
        } else if (strcmp(a, "--psi") == 0 && val) {
            line_psi_path = val;
            i++;
        } else if (strcmp(a, "--fit-psi") == 0) {
            line_fit_psi = 1;
            if (val != NULL && val[0] != '-') {
                line_fit_path = val;
                i++;
            }
        } else if (strcmp(a, "--copy") == 0) {
            line_copy = 1;
        } else if (strcmp(a, "--add1") == 0) {
            line_copy = 1;
            line_add1 = 1;
        } else if (strcmp(a, "--help") == 0) {
            usage();
            return 0;
        } else {
            usage();
            return 1;
        }
    }
    if (role == NULL) {
        usage();
        return 1;
    }
    if (line_fit_psi && strcmp(role, "dest") != 0) {
        die("--fit-psi 只在目的端使用");
    }
    if (line_fit_psi && line_copy) {
        die("--fit-psi 不能和 --copy 一起用");
    }
    if (line_psi_path != NULL && strcmp(role, "relay") == 0) {
        die("--psi 只给源和目的");
    }

    setvbuf(stderr, NULL, _IOLBF, 0);
    bats_init();
    if (strcmp(role, "source") == 0 || strcmp(role, "dest") == 0) {
        install_line_psi(line_psi_path);
    }
    memset(&tx, 0, sizeof(tx));
    tx.loss_percent = (int)loss_percent;
    tx.loss_state = loss_seed ? loss_seed : 1;
    tx.rate_mbps = (int)rate_mbps;

    if (strcmp(role, "source") == 0) {
        if (input == NULL || next == NULL) {
            die("source 需要 --input 和 --next");
        }
        if (parse_hostport(next, &tx.next) != 0) {
            die("--next 应为 IPv4:PORT");
        }
        fd = open_udp(bind_ip, 0, bind_ip != NULL);
        tx.fd = fd;
        return run_source(input, &tx, (int)K, (int)T, seed, batches, (uint16_t)flow_id,
                          (int)n_flows, (int)n_workers);
    }
    if (strcmp(role, "relay") == 0) {
        if (listen_port == 0 || next == NULL) {
            die("relay 需要 --listen 和 --next");
        }
        if (parse_hostport(next, &tx.next) != 0) {
            die("--next 应为 IPv4:PORT");
        }
        fd = open_udp(bind_ip, (uint16_t)listen_port, 1);
        /* 发送用另一只套接字。同一只上阻塞 send 会占着套接字锁，收包线程读不到。 */
        tx.fd = open_udp(bind_ip, 0, bind_ip != NULL);
        fprintf(stderr,
                "bats-line: role=relay recode_seed=%" PRIu64 " loss=%u%%\n",
                recode_seed, loss_percent);
        return run_relay(fd, &tx, recode_seed, (int)reorder_ms, (int)idle_sec, (int)T,
                         (int)n_flows);
    }
    if (strcmp(role, "dest") == 0) {
        if (listen_port == 0 || output == NULL) {
            die("dest 需要 --listen 和 --output");
        }
        fd = open_udp(bind_ip, (uint16_t)listen_port, 1);
        fprintf(stderr, "bats-line: role=dest api=bats_decode K=%u T=%u flows=%u\n", K, T,
                n_flows);
        return run_dest(fd, output, (int)K, (int)T, seed, (int)reorder_ms, (int)idle_sec,
                        (int)n_flows, (int)n_workers);
    }
    usage();
    return 1;
}
