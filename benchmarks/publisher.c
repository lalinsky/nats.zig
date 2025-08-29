#include "bench_util.h"

#define REPORT_INTERVAL 10000

int main() {
    natsConnection *conn = NULL;
    natsStatus status;
    bench_stats_t stats;
    
    printf("Starting NATS publisher benchmark (C/libnats)\n");
    
    // Setup signal handlers
    bench_setup_signals();
    
    // Initialize statistics
    bench_stats_init(&stats);
    
    // Connect to NATS server
    status = bench_connect(&conn, NULL);
    if (status != NATS_OK) {
        return 1;
    }
    
    const char *subject = "benchmark.data";
    const char *message_data = "Hello, NATS Subscribers! This is benchmark data.";
    
    printf("Publishing messages to subject '%s'...\n", subject);
    printf("Press Ctrl+C to stop\n");
    
    // Continuous loop publishing messages as fast as possible
    while (keep_running) {
        // Publish message
        status = natsConnection_PublishString(conn, subject, message_data);
        if (status != NATS_OK) {
            bench_print_error(&stats, status);
            continue;
        }
        
        stats.msg_count++;
        
        // Print stats every REPORT_INTERVAL messages
        if (stats.msg_count % REPORT_INTERVAL == 0) {
            bench_print_throughput(&stats, "Published");
        }
    }
    
    // Print final statistics
    bench_print_summary(&stats, "messages published");
    
    // Cleanup
    bench_cleanup(conn);
    
    return 0;
}