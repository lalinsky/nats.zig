#include "bench_util.h"

#define REPORT_INTERVAL 10000

int main() {
    natsConnection *conn = NULL;
    natsSubscription *sub = NULL;
    natsStatus status;
    bench_stats_t stats;
    
    printf("Starting NATS subscriber benchmark (C/libnats)\n");
    
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
    
    // Subscribe to subject synchronously
    status = natsConnection_SubscribeSync(&sub, conn, subject);
    if (status != NATS_OK) {
        printf("Failed to subscribe: %s\n", natsStatus_GetText(status));
        bench_cleanup(conn);
        return 2;
    }
    
    printf("Subscriber listening on subject '%s'...\n", subject);
    printf("Press Ctrl+C to stop\n");
    
    // Synchronous message loop
    while (keep_running) {
        natsMsg *msg = NULL;
        
        // Wait for next message with 1 second timeout
        status = natsSubscription_NextMsg(&msg, sub, 1000);
        if (status == NATS_TIMEOUT) {
            continue; // Timeout is expected, just continue
        }
        if (status != NATS_OK) {
            bench_print_error(&stats, status);
            continue;
        }
        
        stats.msg_count++;
        
        // Print stats every REPORT_INTERVAL messages
        if (stats.msg_count % REPORT_INTERVAL == 0) {
            bench_print_throughput(&stats, "Received");
        }
        
        natsMsg_Destroy(msg);
    }
    
    // Print final statistics
    bench_print_summary(&stats, "messages received");
    
    // Cleanup
    natsSubscription_Destroy(sub);
    bench_cleanup(conn);
    
    return 0;
}