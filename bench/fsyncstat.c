/* LD_PRELOAD shim for bench/ingest.py: counts fsync/fdatasync calls and the wall time
 * spent inside them, by what was synced. Categories (by the fd's path at sync time):
 *
 *   0 segment file   (path contains /segments/, a regular file: Muninn segment objects)
 *   1 segment dir    (a directory under .../segments/)
 *   2 log file       (path contains /log/: Muninn log entries and checkpoints)
 *   3 log dir
 *   4 other file     (e.g. DuckDB's database file and WAL)
 *   5 other dir
 *
 * The classification (fstat + readlink) happens outside the timed region. Python reads
 * the counters through ctypes: fsyncstat_read(long long out[12]) gives (count, ns) per
 * category; fsyncstat_reset() zeroes them. ClickHouse runs as its own process and is not
 * covered. Build: cc -O2 -shared -fPIC -o fsyncstat.so fsyncstat.c -ldl
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define NCAT 6
static _Atomic long long counts[NCAT], nanos[NCAT];
static int (*real_fsync)(int);
static int (*real_fdatasync)(int);

static int category(int fd) {
    struct stat st;
    int dir = fstat(fd, &st) == 0 && S_ISDIR(st.st_mode);
    char link[64], path[4096];
    snprintf(link, sizeof link, "/proc/self/fd/%d", fd);
    ssize_t n = readlink(link, path, sizeof path - 1);
    if (n < 0) return 4 + dir;
    path[n] = 0;
    if (strstr(path, "/segments/") || (dir && strlen(path) >= 9 && !strcmp(path + n - 9, "/segments")))
        return 0 + dir;
    if (strstr(path, "/log/") || (dir && n >= 4 && !strcmp(path + n - 4, "/log")))
        return 2 + dir;
    return 4 + dir;
}

static long long now_ns(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (long long)t.tv_sec * 1000000000LL + t.tv_nsec;
}

static int timed(int (*f)(int), int fd) {
    int c = category(fd);
    long long t0 = now_ns();
    int r = f(fd);
    nanos[c] += now_ns() - t0;
    counts[c] += 1;
    return r;
}

int fsync(int fd) {
    if (!real_fsync) real_fsync = (int (*)(int))dlsym(RTLD_NEXT, "fsync");
    return timed(real_fsync, fd);
}

int fdatasync(int fd) {
    if (!real_fdatasync) real_fdatasync = (int (*)(int))dlsym(RTLD_NEXT, "fdatasync");
    return timed(real_fdatasync, fd);
}

void fsyncstat_read(long long *out) {
    for (int i = 0; i < NCAT; i++) {
        out[2 * i] = counts[i];
        out[2 * i + 1] = nanos[i];
    }
}

void fsyncstat_reset(void) {
    for (int i = 0; i < NCAT; i++) counts[i] = nanos[i] = 0;
}
