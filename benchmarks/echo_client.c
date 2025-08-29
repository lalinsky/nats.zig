#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>
#include <nats.h>

static volatile bool keep_running = true;
static uint64_t msg_count = 0;
static uint64_t success_count = 0;
static uint64_t error_count = 0;
static clock_t start_time;

void signal_handler(int sig) {
    (void)sig;
    keep_running = false;
}

int main() {
    natsConnection *conn = NULL;
    natsStatus status;
    
    printf("Starting NATS echo-client benchmark (C/libnats)\n");
    
    // Install signal handler for graceful shutdown
    signal(SIGINT, signal_handler);
    
    // Initialize NATS library
    status = nats_Open(-1);
    if (status != NATS_OK) {
        printf("Failed to initialize NATS library: %s\n", natsStatus_GetText(status));
        return 1;
    }
    
    // Connect to NATS server
    status = natsConnection_ConnectTo(&conn, NATS_DEFAULT_URL);
    if (status != NATS_OK) {
        printf("Failed to connect to NATS server: %s\n", natsStatus_GetText(status));
        printf("Make sure NATS server is running at %s\n", NATS_DEFAULT_URL);
        nats_Close();
        return 2;
    }
    
    const char *message_data = "Hello, NATS Echo Server!";
    const int64_t timeout_ms = 5000; // 5 second timeout
    
    start_time = clock();
    
    printf("Sending echo requests to subject 'echo'...\n");
    printf("Press Ctrl+C to stop\n");
    
    // Continuous loop sending requests as fast as possible
    while (keep_running) {
        msg_count++;
        
        natsMsg *reply = NULL;
        
        // Send request and wait for echo reply
        status = natsConnection_RequestString(&reply, conn, "echo", message_data, timeout_ms);
        if (status != NATS_OK) {
            error_count++;
            if (error_count % 1000 == 0) {
                printf("Error #%lu: %s\n", error_count, natsStatus_GetText(status));
            }
            continue;
        }
        
        success_count++;
        
        // Verify echo (optional - could be removed for pure performance test)
        const char *reply_data = natsMsg_GetData(reply);
        int reply_len = natsMsg_GetDataLength(reply);
        
        if (reply_len != (int)strlen(message_data) || 
            memcmp(reply_data, message_data, reply_len) != 0) {
            printf("Warning: Echo mismatch! Expected: '%s', Got: '%.*s'\n", 
                   message_data, reply_len, reply_data);
        }
        
        natsMsg_Destroy(reply);
        
        // Print stats every 1000 successful messages
        if (success_count % 1000 == 0) {
            clock_t current_time = clock();
            double elapsed_s = ((double)(current_time - start_time)) / CLOCKS_PER_SEC;
            double msg_per_s = (double)success_count / elapsed_s;
            double error_rate = ((double)error_count / (double)msg_count) * 100.0;
            printf("Sent %lu requests, %lu successful, %.2f req/s, %.2f%% errors\n", 
                   msg_count, success_count, msg_per_s, error_rate);
        }
    }
    
    printf("\nShutting down...\n");
    printf("Total requests sent: %lu\n", msg_count);
    printf("Successful requests: %lu\n", success_count);
    printf("Failed requests: %lu\n", error_count);
    
    // Cleanup
    natsConnection_Destroy(conn);
    nats_Close();
    
    return 0;
}