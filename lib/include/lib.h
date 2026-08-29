
#pragma once

#include <cstdint>
#include "utils.h"
#include <arpa/inet.h>
#include "protocol.h"
/* Maximum segment size, change as you see fit */
#define MAX_DATA_SIZE 1024
#define MAX_SEGMENT_SIZE (MAX_DATA_SIZE + sizeof(poli_tcp_data_hdr))
#define MAX_WINDOW_SEGMENTS 67
#define MAX_CONNECTIONS 32

/* Protocol control block. Used track different parameters about a connection. 
 * Will need to be extenden to solve the homework with other parameters such as
 * last_ack or status depending on how you implement your protocol. */
struct connection {
    /* common window for both the sender and receiver. */
    /* list window: A window representation */
    int sockfd; /* socket used for this connection */
    int conn_id; /* connection identifier */
    struct sockaddr_in servaddr; /* used to identify the destination */
    pthread_mutex_t con_lock; /* Used for syncronization with the handler thread and read/send calls.*/
    int max_window_seq;
    int next_seq;
    uint16_t send_base;
    long long send_time_ms[MAX_WINDOW_SEGMENTS];
    //aici pt fereastra glisanta
    char send_packets[MAX_WINDOW_SEGMENTS][MAX_SEGMENT_SIZE];
    int send_packet_len[MAX_WINDOW_SEGMENTS];
    uint16_t send_seq[MAX_WINDOW_SEGMENTS];
    int send_acked[MAX_WINDOW_SEGMENTS];
    int send_used[MAX_WINDOW_SEGMENTS];
    /* TODO. Parameters used only by the sender */
     /* Used to store the max number of packets that can be inflight, since we can
                           have many more packets in our window */
    int rcv_used;
    int recv_out_of_order_used;
    pthread_cond_t recv_cond;
    int rcv_cap;
    int expected_seq;
    char *recv_buffer;
    char recv_packets[MAX_WINDOW_SEGMENTS][MAX_DATA_SIZE];
    int recv_packet_len[MAX_WINDOW_SEGMENTS];
    uint16_t recv_seq[MAX_WINDOW_SEGMENTS];
    int recv_received[MAX_WINDOW_SEGMENTS];
    int recv_window_seq;
    /* TODO. Parameters used only by the client */
    //parametrii redundanti, ii folosisem la stop and wait
    //de sters
    int recv_buffer_bytes;
    int confirmare_ack;
    int ultimul_len;
    char ultimul[MAX_SEGMENT_SIZE];
    int last_ack;
    int trimis_deja;
    int remote_recv_window;
};

/* ########## API that we expose to the application ########### */

/* Equivalent of listen. Ran by the server to waits for a connection from a
 * client. Returns a connection id. Blocking untill it receives a connection
 * request */
int wait4connect(uint32_t ip, uint16_t port);
/* Equivalent of connect. Used by the client to connect to a server. */
int setup_connection(uint32_t ip, uint16_t port);
/* Equivalent to recv. Blocking if there is no data to be written in buffer */
int recv_data(int connectionid, char *buffer, int len);
/* Equivalent to send. Used by the client to send a stream of bytes as segments */
int send_data(int conn_id, char *buffer, int len);
/* Used to initialize your protocol on the receiver side. */
void init_receiver(int recv_buffer_bytes);
/* Used to initialize your protocol on the sender side */
void init_sender(int speed, int delay);

/* ######### Internal API used by sender and receiver ########### */
int recv_message_or_timeout(char *buff, size_t len, int *conn_id);
