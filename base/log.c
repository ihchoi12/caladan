/*
 * log.c - the logging system
 */

#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <execinfo.h>
#include <sched.h>

#include <base/stddef.h>
#include <base/log.h>
#include <base/time.h>
#include <asm/ops.h>

__attribute__((weak))
int log_get_kthread_id(void)
{
    return -1;  // default: no kthread info available
}

#define MAX_LOG_LEN 4096

/* log levels greater than this value won't be printed */
int max_loglevel = LOG_DEBUG;

static const char *core_colors[] = {
    "\033[31m",              // 0: Red text
    "\033[32m",              // 1: Green text
    "\033[34m",              // 2: Blue text
    "\033[35m",              // 3: Magenta text

    "\033[41m\033[90m",      // 32: Red bg + dark gray text
    "\033[42m\033[90m",      // 33: Green bg + dark gray text
    "\033[44m\033[90m",      // 34: Blue bg + dark gray text
    "\033[45m\033[90m",      // 35: Magenta bg + dark gray text
};
#define COLOR_COUNT (sizeof(core_colors) / sizeof(core_colors[0]))
static const char *color_reset = "\033[0m";
// Returns index in core_colors for a given cpu id (0,1,2,3,32,33,34,35)
static inline int core_color_idx(int cpu) {
    switch (cpu) {
        case 0:  return 0; // red text
        case 32: return 4; // red bg
        case 1:  return 1; // green text
        case 33: return 5; // green bg
        case 2:  return 2; // blue text
        case 34: return 6; // blue bg
        case 3:  return 3; // magenta text
        case 35: return 7; // magenta bg
        default: return -1;
    }
}

void logk(int level, const char *fmt, ...)
{
	char buf[MAX_LOG_LEN];
	va_list ptr;
	off_t off;
	int cpu;

	if (level > max_loglevel)
		return;

	cpu = sched_getcpu();
	const char *color = "";
    int kth_id = -1;


	if (likely(base_init_done)) {
		kth_id = log_get_kthread_id();

        int color_id = (kth_id >= 0) ? kth_id : cpu;
        int idx = core_color_idx(color_id);
        color = (idx < 0) ? "" : core_colors[idx];

		uint64_t us = microtime();
        off = sprintf(buf, "%s[%3d.%06d] CPU %02d| <%d> ",
                      color,
                      (int)(us / ONE_SECOND), (int)(us % ONE_SECOND),
                      cpu, level);

        if (kth_id >= 0)
			off += snprintf(buf + off, MAX_LOG_LEN - off, "KTH %d| ", kth_id);

    } else {
		int idx = core_color_idx(cpu);
        color = (idx < 0) ? "" : core_colors[idx];
        off = sprintf(buf, "%sCPU %02d| <%d> ", color, cpu, level);
    }

	va_start(ptr, fmt);
	vsnprintf(buf + off, MAX_LOG_LEN - off - 8, fmt, ptr);
	va_end(ptr);
	strcat(buf, color_reset);
	puts(buf);

	// if (level <= LOG_ERR)
	fflush(stdout);
}

#define MAX_CALL_DEPTH	256
void logk_backtrace(void)
{
	void *buf[MAX_CALL_DEPTH];
	const int calls = backtrace(buf, ARRAY_SIZE(buf));
	backtrace_symbols_fd(buf, calls, 1);
}

void logk_bug(bool fatal, const char *expr,
	      const char *file, int line, const char *func)
{
	logk(LOG_EMERG, "%s: %s:%d ASSERTION '%s' FAILED IN '%s'",
	     fatal ? "FATAL" : "WARN", file, line, expr, func);
	logk_backtrace();

	if (fatal)
		init_shutdown(EXIT_FAILURE);
}
