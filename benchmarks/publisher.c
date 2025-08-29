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
static uint64_t error_count = 0;
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
    natsStatus status;
    
    printf("Starting NATS publisher benchmark (C/libnats)\n");
    
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
    
    const char *subject = "benchmark.data";
    const char *message_data = "Hello, NATS Subscribers! This is benchmark data.";
    
    clock_gettime(CLOCK_MONOTONIC, &start_time);
    last_report_time = start_time;
    
    printf("Publishing messages to subject '%s'...\n", subject);
    printf("Press Ctrl+C to stop\n");
    
    // Continuous loop publishing messages as fast as possible
    while (keep_running) {
        // Publish message
        status = natsConnection_PublishString(conn, subject, message_data);
        if (status != NATS_OK) {
            error_count++;
            if (error_count % 1000 == 0) {
                printf("Error #%" PRIu64 ": %s\n", error_count, natsStatus_GetText(status));
            }
            continue;
        }
        
        msg_count++;
        
        // Print stats every 10000 messages
        if (msg_count % 10000 == 0) {
            double interval_s = monotonic_elapsed_s(&last_report_time);
            uint64_t interval_msgs = msg_count - last_msg_count;
            double msg_per_s = (double)interval_msgs / interval_s;
            double error_rate = ((double)error_count / (double)msg_count) * 100.0;
            printf("Published %" PRIu64 " messages, %.2f msg/s, %.2f%% errors\n",
                   msg_count, msg_per_s, error_rate);
                   
            last_msg_count = msg_count;
            clock_gettime(CLOCK_MONOTONIC, &last_report_time);
        }
    }
    
    printf("\nShutting down...\n");
    printf("Total messages published: %" PRIu64 "\n", msg_count);
    printf("Failed messages: %" PRIu64 "\n", error_count);
    
    // Cleanup
    natsConnection_Destroy(conn);
    nats_Close();
    
    return 0;
}