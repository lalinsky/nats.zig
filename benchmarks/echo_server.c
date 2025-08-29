#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>
#include <nats.h>

static volatile bool keep_running = true;
static uint64_t msg_count = 0;
static clock_t start_time;

void signal_handler(int sig) {
    (void)sig;
    keep_running = false;
}

void on_msg(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure) {
    (void)sub;
    (void)closure;
    
    msg_count++;
    
    // Echo the message back to the reply subject
    const char *reply_subject = natsMsg_GetReply(msg);
    if (reply_subject != NULL) {
        const char *data = natsMsg_GetData(msg);
        int data_len = natsMsg_GetDataLength(msg);
        
        natsStatus status = natsConnection_Publish(nc, reply_subject, data, data_len);
        if (status != NATS_OK) {
            printf("Failed to send echo reply: %s\n", natsStatus_GetText(status));
        }
    }
    
    // Print stats every 10000 messages
    if (msg_count % 10000 == 0) {
        clock_t current_time = clock();
        double elapsed_s = ((double)(current_time - start_time)) / CLOCKS_PER_SEC;
        double msg_per_s = (double)msg_count / elapsed_s;
        printf("Processed %lu messages, %.2f msg/s\n", msg_count, msg_per_s);
    }
    
    natsMsg_Destroy(msg);
}

int main() {
    natsConnection *conn = NULL;
    natsSubscription *sub = NULL;
    natsStatus status;
    
    printf("Starting NATS echo-server benchmark (C/libnats)\n");
    
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
    
    start_time = clock();
    
    // Subscribe to "echo" subject with async callback
    status = natsConnection_Subscribe(&sub, conn, "echo", on_msg, NULL);
    if (status != NATS_OK) {
        printf("Failed to subscribe: %s\n", natsStatus_GetText(status));
        natsConnection_Destroy(conn);
        nats_Close();
        return 2;
    }
    
    printf("Echo server listening on subject 'echo'...\n");
    printf("Press Ctrl+C to stop\n");
    
    // Keep the main thread alive while processing messages
    while (keep_running) {
        sleep(1);
    }
    
    printf("\nShutting down...\n");
    printf("Total messages processed: %lu\n", msg_count);
    
    // Cleanup
    natsSubscription_Destroy(sub);
    natsConnection_Destroy(conn);
    nats_Close();
    
    return 0;
}