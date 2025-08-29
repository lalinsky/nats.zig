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

static volatile bool keep_running = true;
static uint64_t msg_count = 0;
static uint64_t last_msg_count = 0;
static struct timespec start_time;
static struct timespec last_report_time;

static inline double
monotonic_elapsed_s(const struct timespec *start)
{
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

void
signal_handler(int sig)
{
    (void)sig;
    keep_running = false;
}


int main() {
    natsConnection *conn = NULL;
    natsSubscription *sub = NULL;
    natsStatus status;
    
    printf("Starting NATS echo-server benchmark (C/libnats)\n");
    
    // Install signal handler for graceful shutdown
    struct sigaction sa = {0};
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    
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
    
    clock_gettime(CLOCK_MONOTONIC, &start_time);
    last_report_time = start_time;
    
    // Subscribe to "echo" subject synchronously
    status = natsConnection_SubscribeSync(&sub, conn, "echo");
    if (status != NATS_OK) {
        printf("Failed to subscribe: %s\n", natsStatus_GetText(status));
        natsConnection_Destroy(conn);
        nats_Close();
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
            printf("Error receiving message: %s\n", natsStatus_GetText(status));
            continue;
        }
        
        msg_count++;
        
        // Echo the message back to the reply subject
        const char *reply_subject = natsMsg_GetReply(msg);
        if (reply_subject != NULL) {
            const char *data = natsMsg_GetData(msg);
            int data_len = natsMsg_GetDataLength(msg);
            
            natsStatus reply_status = natsConnection_Publish(conn, reply_subject, data, data_len);
            if (reply_status != NATS_OK) {
                printf("Failed to send echo reply: %s\n", natsStatus_GetText(reply_status));
            }
        }
        
        // Print stats every 10000 messages
        if (msg_count % 10000 == 0) {
            double interval_s = monotonic_elapsed_s(&last_report_time);
            uint64_t interval_msgs = msg_count - last_msg_count;
            double msg_per_s = (double)interval_msgs / interval_s;
            printf("Processed %" PRIu64 " messages, %.2f msg/s\n", msg_count, msg_per_s);
            
            last_msg_count = msg_count;
            clock_gettime(CLOCK_MONOTONIC, &last_report_time);
        }
        
        natsMsg_Destroy(msg);
    }
    
    printf("\nShutting down...\n");
    printf("Total messages processed: %" PRIu64 "\n", msg_count);
    
    // Cleanup
    natsSubscription_Destroy(sub);
    natsConnection_Destroy(conn);
    nats_Close();
    
    return 0;
}