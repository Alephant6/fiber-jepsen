/*
 * clockshift: moves a process's wall clock by an offset read from a file, for
 * the CKB node in the chain container (jepsen.fiber.faults.outage).
 *
 * Preloaded with LD_PRELOAD, it wraps clock_gettime, gettimeofday and time:
 * CLOCK_REALTIME and CLOCK_REALTIME_COARSE get the offset added, every other
 * clock passes through. The offset is whole seconds, written as +N or -N in
 * the file that CLOCKSHIFT_FILE names; the file is read again at most once a
 * second, so a new offset applies within a second. A missing or unreadable
 * file keeps the last offset (0 at the start).
 *
 * Why not libfaketime, as the fnn nodes use: ckb links jemalloc in place of
 * malloc, jemalloc asks for the time while it initialises, and libfaketime
 * 0.9.10 is still initialising then. It reports "Unexpected recursive calls to
 * clock_gettime()" and the process hangs, so ckb never starts. This wrapper
 * never allocates and takes no locks: until the real clock_gettime is looked
 * up it calls the system call directly, and it reads the file with open, read
 * and close only.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

typedef int (*clock_gettime_fn)(clockid_t, struct timespec *);

static clock_gettime_fn real_clock_gettime;
static const char *offset_file;
static int64_t offset_s;
static int64_t next_read_ns;

static int base_clock_gettime(clockid_t clock, struct timespec *ts)
{
	clock_gettime_fn f = __atomic_load_n(&real_clock_gettime, __ATOMIC_ACQUIRE);
	if (f)
		return f(clock, ts);
	return (int)syscall(SYS_clock_gettime, clock, ts);
}

/* The file's offset in seconds, or `fallback` when it can't be read. */
static int64_t read_offset(const char *path, int64_t fallback)
{
	char buf[32];
	int fd = open(path, O_RDONLY | O_CLOEXEC);
	if (fd < 0)
		return fallback;
	ssize_t n = read(fd, buf, sizeof(buf) - 1);
	close(fd);
	if (n <= 0)
		return fallback;
	buf[n] = '\0';

	int i = 0;
	int64_t sign = 1, value = 0;
	if (buf[i] == '+' || buf[i] == '-') {
		if (buf[i] == '-')
			sign = -1;
		i++;
	}
	if (buf[i] < '0' || buf[i] > '9')
		return fallback;
	for (; buf[i] >= '0' && buf[i] <= '9'; i++)
		value = value * 10 + (buf[i] - '0');
	return sign * value;
}

static int64_t current_offset(void)
{
	if (!offset_file)
		return 0;
	struct timespec now;
	if (base_clock_gettime(CLOCK_MONOTONIC, &now) != 0)
		return __atomic_load_n(&offset_s, __ATOMIC_RELAXED);
	int64_t now_ns = (int64_t)now.tv_sec * 1000000000 + now.tv_nsec;
	if (now_ns >= __atomic_load_n(&next_read_ns, __ATOMIC_RELAXED)) {
		/* Racing threads may both read the file; either result is fine. */
		int saved = errno;
		int64_t off = read_offset(offset_file, __atomic_load_n(&offset_s, __ATOMIC_RELAXED));
		errno = saved;
		__atomic_store_n(&offset_s, off, __ATOMIC_RELAXED);
		__atomic_store_n(&next_read_ns, now_ns + 1000000000, __ATOMIC_RELAXED);
	}
	return __atomic_load_n(&offset_s, __ATOMIC_RELAXED);
}

int clock_gettime(clockid_t clock, struct timespec *ts)
{
	int r = base_clock_gettime(clock, ts);
	if (r == 0 && (clock == CLOCK_REALTIME || clock == CLOCK_REALTIME_COARSE))
		ts->tv_sec += current_offset();
	return r;
}

int gettimeofday(struct timeval *restrict tv, void *restrict tz)
{
	(void)tz;
	struct timespec ts;
	int r = clock_gettime(CLOCK_REALTIME, &ts);
	if (r == 0) {
		tv->tv_sec = ts.tv_sec;
		tv->tv_usec = ts.tv_nsec / 1000;
	}
	return r;
}

time_t time(time_t *t)
{
	struct timespec ts;
	if (clock_gettime(CLOCK_REALTIME, &ts) != 0)
		return (time_t)-1;
	if (t)
		*t = ts.tv_sec;
	return ts.tv_sec;
}

__attribute__((constructor)) static void clockshift_init(void)
{
	offset_file = getenv("CLOCKSHIFT_FILE");
	clock_gettime_fn f = (clock_gettime_fn)dlsym(RTLD_NEXT, "clock_gettime");
	__atomic_store_n(&real_clock_gettime, f, __ATOMIC_RELEASE);
}
