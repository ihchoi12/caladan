/*
 * ksched.c - an interface to the ksched kernel module
 */

#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

#include <base/log.h>

#include "ksched.h"

/* a file descriptor handle to the ksched kernel module */
int ksched_fd;
/* the number of pending interrupts */
int ksched_count;
/* the shared memory region with the kernel module */
struct ksched_shm_cpu *ksched_shm;
/* the set of pending cores to send interrupts to */
cpu_set_t ksched_set;
/* the generation number for each core */
unsigned int ksched_gens[NCPU];


/**
 * ksched_init - initializes the ksched kernel module interface
 *
 * Returns 0 if successful.
 */
int ksched_init(void)
{
	log_debug("########## START ksched_init() ##########");
	char *ksched_addr;
	int i;

	/* first open the file descriptor */
	log_debug("Opening /dev/ksched...");
	ksched_fd = open("/dev/ksched", O_RDWR);
	if (ksched_fd < 0) {
		log_err("Could not find ksched kernel module (%s). Please ensure that "
			    "ksched is compiled and inserted (see README for more details)",
			    strerror(errno));
		return -errno;
	}
	log_debug("Opened /dev/ksched (fd = %d)", ksched_fd);

	/* then map the shared memory region with the kernel */
	log_debug("Mapping shared memory for %d CPUs...", NCPU);
	ksched_addr = mmap(NULL, sizeof(struct ksched_shm_cpu) * NCPU,
		    PROT_READ | PROT_WRITE, MAP_SHARED, ksched_fd, 0);
	if (ksched_addr == MAP_FAILED) {
		log_err("mmap failed: %s", strerror(errno));
		return -errno;
	}
	log_debug("Shared memory mapped at %p", ksched_addr);
	log_debug("Shared memory allows userspace to notify CPU wakeups and monitor state changes");

	/* then initialize the generation numbers */
	log_debug("Initializing generation numbers and clearing idle hints...");
	ksched_shm = (struct ksched_shm_cpu *)ksched_addr;
	for (i = 0; i < NCPU; i++) {
		ksched_gens[i] = load_acquire(&ksched_shm[i].last_gen);
		ksched_idle_hint(i, 0);
	}
	log_debug("Generation numbers track state changes from kernel to userspace");
	log_debug("Idle hints prevent kernel from scheduling on managed cores");

	log_debug("########## FINISH ksched_init() ##########");
	return 0;
}
