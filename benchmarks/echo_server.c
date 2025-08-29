#include "bench_util.h"

#define REPORT_INTERVAL 10000

int main() {
    natsConnection *conn = NULL;
    natsSubscription *sub = NULL;
    natsStatus status;
    bench_stats_t stats;
    
    printf("Starting NATS echo-server benchmark (C/libnats)\n");
    
    // Setup signal handlers
    bench_setup_signals();
    
    // Initialize statistics
    bench_stats_init(&stats);
    
    // Connect to NATS server
    status = bench_connect(&conn, NULL);
    if (status != NATS_OK) {
        return 1;
    }
    
    // Subscribe to "echo" subject synchronously
    status = natsConnection_SubscribeSync(&sub, conn, "echo");
    if (status != NATS_OK) {
        printf("Failed to subscribe: %s\n", natsStatus_GetText(status));
        bench_cleanup(conn);
        return 2;
    }
    
    printf("Echo server listening on subject 'echo'...\n");
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
        
        // Echo the message back to the reply subject
        const char *reply_subject = natsMsg_GetReply(msg);
        if (reply_subject != NULL) {
            const char *data = natsMsg_GetData(msg);
            int data_len = natsMsg_GetDataLength(msg);
            
            natsStatus reply_status = natsConnection_Publish(conn, reply_subject, data, data_len);
            if (reply_status != NATS_OK) {
                printf("Failed to send echo reply: %s\n", natsStatus_GetText(reply_status));
                stats.error_count++;
            } else {
                stats.success_count++;
            }
        }
        
        // Print stats every REPORT_INTERVAL messages
        if (stats.msg_count % REPORT_INTERVAL == 0) {
            bench_print_throughput(&stats, "Processed");
        }
        
        natsMsg_Destroy(msg);
    }
    
    // Print final statistics
    bench_print_summary(&stats, "messages processed");
    
    // Cleanup
    natsSubscription_Destroy(sub);
    bench_cleanup(conn);
    
    return 0;
}