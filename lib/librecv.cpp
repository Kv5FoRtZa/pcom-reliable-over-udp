#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <poll.h>
#include <cassert>
#include <sys/timerfd.h>
#include <bits/stdc++.h>
#include <cstring>
#include <unistd.h>
#include <algorithm>
using namespace std;
int size_citit_jos;
std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

int recv_data(int conn_id, char *buffer, int len)
{
    int size = 0;

    pthread_mutex_lock(&cons[conn_id]->con_lock);
    
    /* We will write code here as to not have sync problems with recv_handler */
    struct connection *conexiune = cons[conn_id];
    while (1)
    {
        if(conexiune->rcv_used != 0)
        {
            break;
        }
        else
        {
            pthread_cond_wait(&conexiune->recv_cond, &conexiune->con_lock);
        }
    }
    //copiez size in buffer, si apoi mut datele cu size mai in fata in rcv buffer
    size  = min(len,conexiune->rcv_used);
    memcpy(buffer,conexiune->recv_buffer,size);
    memmove(conexiune->recv_buffer,conexiune->recv_buffer + size,conexiune->rcv_used - size);
    conexiune->rcv_used -= size;
    while (1) {
        if ( !conexiune->recv_received[conexiune->expected_seq % MAX_WINDOW_SEGMENTS] || conexiune->recv_seq[conexiune->expected_seq % MAX_WINDOW_SEGMENTS] != conexiune->expected_seq) {
            break;
        }
        int len_seg = conexiune->recv_packet_len[conexiune->expected_seq % MAX_WINDOW_SEGMENTS];
        if (conexiune->rcv_used + len_seg > conexiune->rcv_cap) {
            break;
        }
        memcpy(conexiune->recv_buffer + conexiune->rcv_used,conexiune->recv_packets[conexiune->expected_seq % MAX_WINDOW_SEGMENTS],len_seg);
        conexiune->rcv_used += len_seg;
        conexiune->recv_out_of_order_used-= len_seg;
        conexiune->recv_received[conexiune->expected_seq % MAX_WINDOW_SEGMENTS] = 0;
        conexiune->recv_packet_len[conexiune->expected_seq % MAX_WINDOW_SEGMENTS] = 0;
        conexiune->expected_seq ++;
        pthread_cond_signal(&conexiune->recv_cond);
    }
    if (conexiune->expected_seq > 0) {
        poli_tcp_ctrl_hdr ack;
        memset(&ack, 0, sizeof(ack));

        ack.protocol_id = POLI_PROTOCOL_ID;
        ack.conn_id = conn_id;
        ack.type = TYPE_ACK;
        ack.ack_num = conexiune->expected_seq - 1;
        ack.recv_window = max(conexiune->rcv_cap - conexiune->rcv_used - conexiune->recv_out_of_order_used,0);
        sendto(conexiune->sockfd, &ack, sizeof(ack), 0, (struct sockaddr *)&conexiune->servaddr, sizeof(conexiune->servaddr));
    }
    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    return size;
}

void *receiver_handler(void *arg)
{

    char segment[MAX_SEGMENT_SIZE];
    int res;
    DEBUG_PRINT("Starting recviver handler\n");

    while (1) {

        int conn_id = -1;
        do {
            res = recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &conn_id);
        } while(res == -14);
        if (res == -1) 
        {
            continue;
        }
        pthread_mutex_lock(&cons[conn_id]->con_lock);
        //aici a primit un mesaj 100%
        struct connection *conexiune = cons[conn_id];

        poli_tcp_data_hdr *header = (poli_tcp_data_hdr *)segment;
        char *payload = segment + sizeof(poli_tcp_data_hdr);
        int payload_len = header->len;
        int copie = payload_len;
        if (header->protocol_id == POLI_PROTOCOL_ID)
        {
            if(header->conn_id == conn_id)
            {
                if(header->type == TYPE_DATA)
                {
                    //doar daca e expected seq bun si daca s sptiu in buffer
                    //scriu in buffer + ce e deja ocupat 
                    int bun = 0;
                    if(header->seq_num < conexiune->expected_seq){
                        bun = 1;
                        //nu il mai salvez iar, doar dau ack
                    }
                    else if (header->seq_num < conexiune->expected_seq + conexiune->recv_window_seq)
                    { 
                        //header->seq_num % MAX_WINDOW_SEGMENTS
                        if (!conexiune->recv_received[header->seq_num % MAX_WINDOW_SEGMENTS] || conexiune->recv_seq[header->seq_num % MAX_WINDOW_SEGMENTS] != header->seq_num) 
                        {
                            memcpy(conexiune->recv_packets[header->seq_num % MAX_WINDOW_SEGMENTS], payload, payload_len);
                            conexiune->recv_packet_len[header->seq_num % MAX_WINDOW_SEGMENTS] = payload_len;
                            conexiune->recv_seq[header->seq_num % MAX_WINDOW_SEGMENTS] = header->seq_num;
                            conexiune->recv_received[header->seq_num % MAX_WINDOW_SEGMENTS] = 1;
                            conexiune->recv_out_of_order_used += payload_len;
                        }
                        bun = 1;
                    }
                    while (1) {
                        //mut segmentele consecutive in recv buffer
                        //pt a fi prelucrate de cealalta functie gen
                        if ( !conexiune->recv_received[conexiune->expected_seq % MAX_WINDOW_SEGMENTS] || conexiune->recv_seq[conexiune->expected_seq % MAX_WINDOW_SEGMENTS] != conexiune->expected_seq) {
                            break;
                        }
                        int len_seg = conexiune->recv_packet_len[conexiune->expected_seq % MAX_WINDOW_SEGMENTS];
                        if (conexiune->rcv_used + len_seg > conexiune->rcv_cap) {
                            break;
                        }
                        memcpy(conexiune->recv_buffer + conexiune->rcv_used,conexiune->recv_packets[conexiune->expected_seq % MAX_WINDOW_SEGMENTS],len_seg);
                        conexiune->rcv_used += len_seg;
                        conexiune->recv_out_of_order_used-= len_seg;
                        conexiune->recv_received[conexiune->expected_seq % MAX_WINDOW_SEGMENTS] = 0;
                        conexiune->recv_packet_len[conexiune->expected_seq % MAX_WINDOW_SEGMENTS] = 0;
                        conexiune->expected_seq ++;
                        pthread_cond_signal(&conexiune->recv_cond);
                    }
                    //trimit ack cu expected seq
                    if(bun == 1){
                        poli_tcp_ctrl_hdr ack;
                        memset(&ack, 0, sizeof(ack));
                        ack.ack_num = header->seq_num;
                        ack.protocol_id = POLI_PROTOCOL_ID;
                        ack.type = TYPE_ACK;
                        ack.conn_id = conn_id;
                        ack.recv_window = max(conexiune->rcv_cap - conexiune->rcv_used - conexiune->recv_out_of_order_used,0);

                        sendto(conexiune->sockfd, &ack, sizeof(ack), 0,(struct sockaddr *)&conexiune->servaddr,sizeof(conexiune->servaddr));
                    }
                }
            }
        }
        /* Handle segment received from the sender. We use this between locks
        as to not have synchronization issues with the recv_data calls which are
        on the main thread */
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
}
static int next_conn_id = 0,listen_sock = -1;
static std::map<uint64_t, int> seen_clients;
int wait4connect(uint32_t ip, uint16_t port)
{
    /* TODO: Implement the Three Way Handshake on the receiver part. This blocks
     * until a connection is established. */

    struct connection *con = (struct connection *)malloc(sizeof(struct connection));
    /* This can be used to set a timer on a socket, useful once we received a
     * SYN. You may want to disable by setting the time to 0 (tv_sec = 0,
     * tv_usec = 0)
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    } 

     Receive SYN on the connection socket. Create a new socket and bind it to
     * the chosen port. Send the data port number via SYN-ACK to the client */
    ///aici e bind pt server cred
    //con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    con->rcv_cap = size_citit_jos;
    con->rcv_used = 0;
    con->recv_out_of_order_used = 0;
    con->recv_buffer = (char *)malloc(size_citit_jos * sizeof(char));
    if (listen_sock == -1) {
        listen_sock = socket(AF_INET, SOCK_DGRAM, 0);

        struct sockaddr_in listen_addr;
        memset(&listen_addr, 0, sizeof(listen_addr));
        listen_addr.sin_family = AF_INET;
        listen_addr.sin_addr.s_addr = ip;
        listen_addr.sin_port = port;

        bind(listen_sock, (const struct sockaddr *)&listen_addr, sizeof(listen_addr));
    }
    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_addr.s_addr = ip;
    servaddr.sin_port = port;
    servaddr.sin_family = AF_INET;
    //bind(con->sockfd, (const struct sockaddr *)&servaddr, sizeof(servaddr));
    //apoi pt trimitere
    int trimit_sock = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in data_addr;
    memset(&data_addr, 0, sizeof(data_addr));
    data_addr.sin_addr.s_addr = ip;
    data_addr.sin_port = 0;
    data_addr.sin_family = AF_INET;
    ////printf("incepe wait4\n");
    bind(trimit_sock, (const struct sockaddr *)&data_addr, sizeof(data_addr));
    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this 
    This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on a connection */
    poli_tcp_ctrl_hdr p;
    socklen_t clients = sizeof(servaddr);
    while(1)
    {
        recvfrom(listen_sock, &p, sizeof(p), 0,(struct sockaddr *)&servaddr, &clients);
        if(!(p.protocol_id == POLI_PROTOCOL_ID && p.type == TYPE_SYN))
        {
            continue;
        }
        if (seen_clients.count(((uint64_t)servaddr.sin_addr.s_addr << 16) | ntohs(servaddr.sin_port))) {
            continue;
        }
        break;
    }
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 100000; 
    setsockopt(trimit_sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    //timeout
    int conn_id = next_conn_id;
    uint64_t key = ((uint64_t)servaddr.sin_addr.s_addr << 16) |
               ntohs(servaddr.sin_port);
    ////printf("wai4 a primit syn\n");
    next_conn_id ++;
    //setez syn ack sa fie un header + payload;
    char packet[sizeof(poli_tcp_ctrl_hdr) + sizeof(uint16_t)];
    socklen_t data_len = sizeof(data_addr);
    uint16_t *trimit_port = (uint16_t *)(packet + sizeof(poli_tcp_ctrl_hdr));
    getsockname(trimit_sock, (struct sockaddr *)&data_addr, &data_len);
    *trimit_port = data_addr.sin_port;
    poli_tcp_ctrl_hdr *ack = (poli_tcp_ctrl_hdr *)packet;

    ack->ack_num = 0;
    ack->protocol_id = POLI_PROTOCOL_ID;
    ack->type = TYPE_SYN_ACK;
    ack->conn_id = conn_id;
    ack->recv_window = con->rcv_cap;
    while(1){   
        sendto(listen_sock, packet, sizeof(packet), 0, (struct sockaddr *)&servaddr,clients);
        ////printf("wai4 a trimis synack\n");
        poli_tcp_ctrl_hdr ack2;
    
        int rc = recvfrom(trimit_sock, &ack2, sizeof(ack2), 0,(struct sockaddr *)&servaddr, &clients);
        if(rc > 0){
            if(ack2.protocol_id == POLI_PROTOCOL_ID && (ack2.type == TYPE_ACK || ack2.type == TYPE_DATA) && ack2.conn_id == conn_id){
                break;
            }
        }
    }
    seen_clients[key] = conn_id;
    ////printf("wai4 a primit ack\n");
    con->recv_window_seq = 9;
    con->servaddr = servaddr;
    con->conn_id = conn_id;
    con->sockfd = trimit_sock;
    con->expected_seq = 0;
    for (int i = 0; i < MAX_WINDOW_SEGMENTS; i++) {
        con->recv_packet_len[i] = 0;
        con->recv_seq[i] = 0;
        con->recv_received[i] = 0;
    }
    pthread_mutex_init(&con->con_lock, NULL);
    pthread_cond_init(&con->recv_cond, NULL);
    cons.insert({conn_id, con});
    ///chestiile din schelet puse la final
    data_fds[fdmax].fd = trimit_sock;   
    data_fds[fdmax].events = POLLIN;    
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
    timer_fds[fdmax].events = POLLIN;    
    struct itimerspec spec;     
    spec.it_value.tv_sec = 1;    
    spec.it_value.tv_nsec = 0;    
    spec.it_interval.tv_sec = 1;    
    spec.it_interval.tv_nsec = 0;    
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);    
    fdmax++;
    DEBUG_PRINT("Connection established!");
    return conn_id;
}

void init_receiver(int recv_buffer_bytes)
{
    pthread_t thread1;
    int ret;
    size_citit_jos = recv_buffer_bytes;
    /* TODO: Create the connection socket and bind it to 8031 */
    ///bind pe portul asta, nu cred ca il inchid vreodata dar ata ete
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = 8031;
    addr.sin_family = AF_INET;
    bind(sock, (const struct sockaddr *)&addr, sizeof(addr));
    ret = pthread_create( &thread1, NULL, receiver_handler, NULL);
    assert(ret == 0);
}
