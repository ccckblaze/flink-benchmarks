/* Dependency-free machine baseline probes for rigs without fio/sysbench.
 *
 * Modes (all output is plain key=value lines for the shell collector):
 *   membw copy|triad <MB-per-stream> <seconds> <threads>
 *   randread <file> <seconds> [block-size KiB, default 4]
 *   fsync <file> <bytes> <count>
 *
 * Multi-threaded membw forks one process per stream, each with its own
 * buffers, so the measurement is real parallel traffic rather than shared
 * source reads inflating the number.
 */

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#ifndef O_DIRECT
/* macOS has no O_DIRECT; the probe is built on the Linux rig, but keep the
 * file compilable everywhere so local CI can at least syntax-check it. */
#define O_DIRECT 0
#endif

static double now_sec(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (double)ts.tv_sec + ts.tv_nsec / 1e9;
}

static int cmp_d(const void *a, const void *b)
{
	double x = *(const double *)a, y = *(const double *)b;
	return x < y ? -1 : (x > y ? 1 : 0);
}

static uint64_t xorshift(uint64_t *s)
{
	*s ^= *s << 13; *s ^= *s >> 7; *s ^= *s << 17; return *s;
}

static double membw_child(const char *mode, size_t mb, int seconds)
{
	size_t n = (mb * 1024 * 1024) / sizeof(double);
	double *a = mmap(NULL, n * sizeof(double), PROT_READ | PROT_WRITE,
			MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	double *b = mmap(NULL, n * sizeof(double), PROT_READ | PROT_WRITE,
			MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	double *c = mmap(NULL, n * sizeof(double), PROT_READ | PROT_WRITE,
			MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (a == MAP_FAILED || b == MAP_FAILED || c == MAP_FAILED) {
		fprintf(stderr, "mmap failed: %s\n", strerror(errno));
		exit(2);
	}
	for (size_t i = 0; i < n; i++) { a[i] = 1.0; b[i] = 2.0; c[i] = 0.0; }
	double traffic_per_iter = (strcmp(mode, "copy") == 0) ? 2.0 * n * sizeof(double)
							   : 3.0 * n * sizeof(double);
	double t0 = now_sec(), done = 0.0, deadline = t0 + seconds;
	while (now_sec() < deadline) {
		if (strcmp(mode, "copy") == 0) {
			for (size_t i = 0; i < n; i++) b[i] = a[i];
		} else {
			for (size_t i = 0; i < n; i++) c[i] = a[i] + b[i];
		}
		done += traffic_per_iter;
	}
	return done;
}

static void membw(int argc, char **argv)
{
	if (argc < 5) { fprintf(stderr, "usage: membw copy|triad <MB> <sec> <threads>\n"); exit(1); }
	const char *mode = argv[1];
	size_t mb = (size_t)atol(argv[2]);
	int seconds = atoi(argv[3]);
	int threads = argc > 4 ? atoi(argv[4]) : 1;
	if (threads < 1) threads = 1;
	if (threads == 1) {
		double bytes = membw_child(mode, mb, seconds);
		printf("membw_bytes=%.0f\nmembw_gibps=%.3f\n", bytes,
		       bytes / (1024.0 * 1024 * 1024) / seconds);
		return;
	}
	int pipefds[2];
	if (pipe(pipefds) != 0) { perror("pipe"); exit(2); }
	pid_t *kids = calloc(threads, sizeof(pid_t));
	for (int t = 0; t < threads; t++) {
		pid_t pid = fork();
		if (pid == 0) {
			close(pipefds[0]);
			double bytes = membw_child(mode, mb, seconds);
			char buf[64];
			int len = snprintf(buf, sizeof buf, "%.0f\n", bytes);
			if (write(pipefds[1], buf, len) < 0) { /* ignore */ }
			close(pipefds[1]);
			_exit(0);
		}
		if (pid < 0) { perror("fork"); exit(2); }
		kids[t] = pid;
	}
	close(pipefds[1]);
	double total = 0.0;
	char line[64];
	FILE *rf = fdopen(pipefds[0], "r");
	while (fgets(line, sizeof line, rf)) total += atof(line);
	fclose(rf);
	for (int t = 0; t < threads; t++) waitpid(kids[t], NULL, 0);
	free(kids);
	printf("membw_threads=%d\nmembw_bytes=%.0f\nmembw_gibps=%.3f\n", threads, total,
	       total / (1024.0 * 1024 * 1024) / seconds);
}

static void randread(int argc, char **argv)
{
	if (argc < 3) { fprintf(stderr, "usage: randread <file> <sec> [block-KiB]\n"); exit(1); }
	const char *path = argv[1];
	int seconds = atoi(argv[2]);
	size_t blk = (size_t)(argc > 3 ? atoi(argv[3]) : 4) * 1024;
	int fd = open(path, O_RDONLY | O_DIRECT);
	if (fd < 0) { fprintf(stderr, "open %s: %s\n", path, strerror(errno)); exit(2); }
	struct stat st;
	if (fstat(fd, &st) != 0) { perror("fstat"); exit(2); }
	void *buf = aligned_alloc(4096, blk);
	if (!buf) { perror("aligned_alloc"); exit(2); }
	uint64_t max_blocks = (uint64_t)st.st_size / blk;
	uint64_t seed = 88172645463325252ULL ^ (uint64_t)time(NULL);
	uint64_t ops = 0, bytes = 0;
	double t0 = now_sec(), deadline = t0 + seconds;
	while (now_sec() < deadline) {
		off_t off = (off_t)(xorshift(&seed) % max_blocks) * (off_t)blk;
		ssize_t r = pread(fd, buf, blk, off);
		if (r == (ssize_t)blk) { ops++; bytes += r; }
	}
	double secs = now_sec() - t0;
	printf("randread_block_kib=%zu\nrandread_ops=%llu\nrandread_seconds=%.3f\n"
	       "randread_kiops=%.3f\nrandread_mibps=%.3f\n",
	       blk / 1024, (unsigned long long)ops, secs,
	       ops / secs / 1000.0, bytes / secs / (1024.0 * 1024.0));
	close(fd);
}

static void fsync_loop(int argc, char **argv)
{
	if (argc < 4) { fprintf(stderr, "usage: fsync <file> <bytes> <count>\n"); exit(1); }
	const char *path = argv[1];
	size_t bytes = (size_t)atol(argv[2]);
	int count = atoi(argv[3]);
	int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
	if (fd < 0) { fprintf(stderr, "open %s: %s\n", path, strerror(errno)); exit(2); }
	char *buf = malloc(bytes);
	if (!buf) { perror("malloc"); exit(2); }
	memset(buf, 'x', bytes);
	double *lat = malloc(count * sizeof(double));
	if (!lat) { perror("malloc"); exit(2); }
	for (int i = 0; i < count; i++) {
		double t0 = now_sec();
		if (pwrite(fd, buf, bytes, 0) != (ssize_t)bytes) { perror("pwrite"); exit(2); }
		if (fsync(fd) != 0) { perror("fsync"); exit(2); }
		lat[i] = (now_sec() - t0) * 1000.0;
	}
	qsort(lat, count, sizeof(double), cmp_d);
	printf("fsync_bytes=%zu\nfsync_count=%d\nfsync_p50_ms=%.3f\nfsync_p99_ms=%.3f\n"
	       "fsync_max_ms=%.3f\n",
	       bytes, count, lat[count / 2], lat[(int)(count * 0.99)], lat[count - 1]);
	close(fd);
}

int main(int argc, char **argv)
{
	if (argc < 2) { fprintf(stderr, "usage: machine_probe membw|randread|fsync ...\n"); return 1; }
	const char *mode = argv[1];
	argc--; argv++;
	if (strcmp(mode, "membw") == 0) membw(argc, argv);
	else if (strcmp(mode, "randread") == 0) randread(argc, argv);
	else if (strcmp(mode, "fsync") == 0) fsync_loop(argc, argv);
	else { fprintf(stderr, "unknown mode: %s\n", mode); return 1; }
	return 0;
}
