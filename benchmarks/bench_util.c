#include "bench_util.h"

volatile bool keep_running = true;

void bench_stats_init(bench_stats_t *stats) {
    memset(stats, 0, sizeof(bench_stats_t));
    clock_gettime(CLOCK_MONOTONIC, &stats->start_time);
    stats->last_report_time = stats->start_time;
}

double bench_elapsed_s(const struct timespec *start) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    
    time_t ds  = now.tv_sec  - start->tv_sec;
    long   dns = now.tv_nsec - start->tv_nsec;
    if (dns < 0) {
        ds  -= 1;
        dns += 1000000000L;
    }
    return (double)ds + (double)dns / 1e9;
}

void bench_print_throughput(bench_stats_t *stats, const char *action) {
    double interval_s = bench_elapsed_s(&stats->last_report_time);
    uint64_t interval_msgs = stats->msg_count - stats->last_msg_count;
    uint64_t interval_errors = stats->error_count - stats->last_error_count;
    double msg_per_s = (double)interval_msgs / interval_s;
    
    if (interval_errors > 0) {
        double interval_error_rate = ((double)interval_errors / (double)(interval_msgs + interval_errors)) * 100.0;
        printf("%s %" PRIu64 " messages in %.1fs, %.2f msg/s, %.2f%% errors\n",
               action, interval_msgs, interval_s, msg_per_s, interval_error_rate);
    } else {
        printf("%s %" PRIu64 " messages in %.1fs, %.2f msg/s\n",
               action, interval_msgs, interval_s, msg_per_s);
    }
    
    stats->last_msg_count = stats->msg_count;
    stats->last_success_count = stats->success_count;
    stats->last_error_count = stats->error_count;
    clock_gettime(CLOCK_MONOTONIC, &stats->last_report_time);
}

void bench_print_error(bench_stats_t *stats, natsStatus status) {
    stats->error_count++;
    if (stats->error_count % 1000 == 0) {
        printf("Error #%" PRIu64 ": %s\n", stats->error_count, natsStatus_GetText(status));
    }
}

void bench_print_summary(bench_stats_t *stats, const char *metric_name) {
    printf("\nShutting down...\n");
    printf("Total %s: %" PRIu64 "\n", metric_name, stats->msg_count);
    if (stats->success_count > 0) {
        printf("Successful: %" PRIu64 "\n", stats->success_count);
    }
    if (stats->error_count > 0) {
        printf("Failed: %" PRIu64 "\n", stats->error_count);
    }
    
    double total_elapsed = bench_elapsed_s(&stats->start_time);
    double avg_rate = (double)stats->msg_count / total_elapsed;
    printf("Average rate: %.2f msg/s\n", avg_rate);
}

void bench_signal_handler(int sig) {
    (void)sig;
    keep_running = false;
}

void bench_setup_signals(void) {
    struct sigaction sa = {0};
    sa.sa_handler = bench_signal_handler;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
}

natsStatus bench_connect(natsConnection **conn, const char *url) {
    natsStatus status;
    
    // Initialize NATS library
    status = nats_Open(-1);
    if (status != NATS_OK) {
        printf("Failed to initialize NATS library: %s\n", natsStatus_GetText(status));
        return status;
    }
    
    // Connect to NATS server
    const char *connect_url = url ? url : NATS_DEFAULT_URL;
    status = natsConnection_ConnectTo(conn, connect_url);
    if (status != NATS_OK) {
        printf("Failed to connect to NATS server: %s\n", natsStatus_GetText(status));
        printf("Make sure NATS server is running at %s\n", connect_url);
        nats_Close();
    }
    
    return status;
}

void bench_cleanup(natsConnection *conn) {
    if (conn) {
        natsConnection_Destroy(conn);
    }
    nats_Close();
}