#ifndef BENCH_UTIL_H
#define BENCH_UTIL_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <stdbool.h>
#include <stdint.h>
#include <inttypes.h>
#include <unistd.h>
#include <nats.h>

// Global flag for signal handling
extern volatile bool keep_running;

// Statistics structure
typedef struct {
    uint64_t msg_count;
    uint64_t success_count;
    uint64_t error_count;
    uint64_t last_msg_count;
    uint64_t last_success_count;
    uint64_t last_error_count;
    struct timespec start_time;
    struct timespec last_report_time;
} bench_stats_t;

// Initialize benchmark statistics
void bench_stats_init(bench_stats_t *stats);

// Calculate elapsed time in seconds from a timespec
double bench_elapsed_s(const struct timespec *start);

// Print throughput statistics
void bench_print_throughput(bench_stats_t *stats, const char *action);

// Print error statistics
void bench_print_error(bench_stats_t *stats, natsStatus status);

// Print final summary
void bench_print_summary(bench_stats_t *stats, const char *metric_name);

// Signal handler for graceful shutdown
void bench_signal_handler(int sig);

// Setup signal handlers
void bench_setup_signals(void);

// Initialize NATS and connect
natsStatus bench_connect(natsConnection **conn, const char *url);

// Common cleanup
void bench_cleanup(natsConnection *conn);

#endif // BENCH_UTIL_H