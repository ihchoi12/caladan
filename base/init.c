/*
 * init.c - support for initialization
 */

#include <stdlib.h>

#include <base/init.h>
#include <base/log.h>
#include <base/thread.h>

#include "init_internal.h"

bool base_init_done __aligned(CACHE_LINE_SIZE);

void __weak init_shutdown(int status)
{
	log_info("init: shutting down -> %s",
		 status == EXIT_SUCCESS ? "SUCCESS" : "FAILURE");
	exit(status);
}

/* we initialize these early subsystems by hand */
static int init_internal(void)
{
	int ret;
	log_debug("	cpu_init()");
	ret = cpu_init(); // Detect and parse CPU and NUMA topology info
	if (ret)
		return ret;

	log_debug("	time_init()"); // Calibrate TSC (timestamp counter) for time measurement
	ret = time_init();
	if (ret)
		return ret;
	log_debug("	page_init()");
	ret = page_init(); // Initialize physical and virtual memory system (uses hugepages)
	if (ret) {
		log_err("Could not intialize memory. Please ensure that hugepages are "
			    "enabled/available.");
		return ret;
	}
	log_debug("	slab_init()");
	return slab_init();
}


extern int thread_init_perthread(void);

/**
 * base_init - initializes the base library
 *
 * Call this function before using the library.
 * Returns 0 if successful, otherwise fail.
 */
int base_init(void)
{
	log_debug("base_init()");
	int ret;

	ret = thread_init_perthread();
	if (ret)
		return ret;
	
	ret = init_internal();
	if (ret)
		return ret;

	base_init_done = true;
	return 0;
}

static int init_thread_internal(void)
{
	return page_init_thread();
}

/**
 * base_init_thread - prepares a thread for use by the base library
 *
 * Returns 0 if successful, otherwise fail.
 */
int base_init_thread(void)
{
	// log_debug("START base_init_thread()");
	int ret;

	ret = thread_init_perthread();
	if (ret)
		return ret;

	ret = init_thread_internal();
	if (ret)
		return ret;

	perthread_store(thread_init_done, true);
	return 0;
}

