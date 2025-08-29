#include "bench_util.h"

#define REPORT_INTERVAL 1000
#define REQUEST_TIMEOUT_MS 5000

int main() {
    natsConnection *conn = NULL;
    natsStatus status;
    bench_stats_t stats;
    
    printf("Starting NATS echo-client benchmark (C/libnats)\n");
    
    // Setup signal handlers
    bench_setup_signals();
    
    // Initialize statistics
    bench_stats_init(&stats);
    
    // Connect to NATS server
    status = bench_connect(&conn, NULL);
    if (status != NATS_OK) {
        return 1;
    }
    
    const char *message_data = "Hello, NATS Echo Server!";
    
    printf("Sending echo requests to subject 'echo'...\n");
    printf("Press Ctrl+C to stop\n");
    
    // Continuous loop sending requests as fast as possible
    while (keep_running) {
        natsMsg *reply = NULL;
        
        stats.msg_count++;
        
        // Send request and wait for echo reply
        status = natsConnection_RequestString(&reply, conn, "echo", message_data, REQUEST_TIMEOUT_MS);
        if (status != NATS_OK) {
            bench_print_error(&stats, status);
            continue;
        }
        
        stats.success_count++;
        
        // Verify echo (optional - could be removed for pure performance test)
        const char *reply_data = natsMsg_GetData(reply);
        int reply_len = natsMsg_GetDataLength(reply);
        
        if (reply_len != (int)strlen(message_data) || 
            memcmp(reply_data, message_data, reply_len) != 0) {
            printf("Warning: Echo mismatch! Expected: '%s', Got: '%.*s'\n", 
                   message_data, reply_len, reply_data);
        }
        
        natsMsg_Destroy(reply);
        
        // Print stats every REPORT_INTERVAL successful messages
        if (stats.success_count % REPORT_INTERVAL == 0) {
            double interval_s = bench_elapsed_s(&stats.last_report_time);
            uint64_t interval_requests = stats.msg_count - stats.last_msg_count;
            uint64_t interval_success = stats.success_count - stats.last_success_count;
            uint64_t interval_errors = stats.error_count - stats.last_error_count;
            double req_per_s = (double)interval_success / interval_s;
            double interval_error_rate = ((double)interval_errors / (double)interval_requests) * 100.0;
            printf("Sent %" PRIu64 " requests, %" PRIu64 " successful in %.1fs, %.2f req/s, %.2f%% errors\n", 
                   interval_requests, interval_success, interval_s, req_per_s, interval_error_rate);
            
            stats.last_msg_count = stats.msg_count;
            stats.last_success_count = stats.success_count;
            stats.last_error_count = stats.error_count;
            clock_gettime(CLOCK_MONOTONIC, &stats.last_report_time);
        }
    }
    
    // Print final statistics
    printf("\nShutting down...\n");
    printf("Total requests sent: %" PRIu64 "\n", stats.msg_count);
    printf("Successful requests: %" PRIu64 "\n", stats.success_count);
    printf("Failed requests: %" PRIu64 "\n", stats.error_count);
    
    double total_elapsed = bench_elapsed_s(&stats.start_time);
    double avg_rate = (double)stats.success_count / total_elapsed;
    printf("Average rate: %.2f req/s\n", avg_rate);
    
    // Cleanup
    bench_cleanup(conn);
    
    return 0;
}