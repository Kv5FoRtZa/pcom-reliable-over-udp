#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <cassert>
#include <poll.h>
#include <sys/timerfd.h>
#include <cstring>
#include <unistd.h>
#include <algorithm>
using namespace std;
static long long time_fereastra()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000LL + ts.tv_nsec / 1000000;
}
std::map<int, struct connection *> cons;
int cv_timeout = 67;
struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

int send_data(int conn_id, char *buffer, int len)
{
    
    pthread_mutex_lock(&cons[conn_id]->con_lock);
    struct connection *conexiune = cons[conn_id];

    int segmente_necesare = (len + MAX_DATA_SIZE - 1) / MAX_DATA_SIZE;
    int in_flight = conexiune->next_seq - conexiune->send_base;
    int locuri_libere = conexiune->max_window_seq - in_flight;

    if (locuri_libere < segmente_necesare) {
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
        return -1;
    }
    //if (conexiune->remote_recv_window < MAX_DATA_SIZE) {
       // pthread_mutex_unlock(&cons[conn_id]->con_lock);
       // return -1;
    //}
    int trimis_pana_acum = 0;
    while(trimis_pana_acum < len){
        /* We will write code here as to not have sync problems with sender_handler */
        
        //trimite doar daca fereastra nu e plina
        if (conexiune->next_seq - conexiune->send_base >= conexiune->max_window_seq)
        {
            pthread_mutex_unlock(&cons[conn_id]->con_lock);
            if(trimis_pana_acum == 0)
                return -1;
            return trimis_pana_acum;
        }
        int size = min(len - trimis_pana_acum,MAX_DATA_SIZE);
        //declar pachetul si header
        char packet[MAX_SEGMENT_SIZE];
        memset(packet, 0, sizeof(packet));
        poli_tcp_data_hdr *header = (poli_tcp_data_hdr *)packet;
        header->conn_id = conn_id;
        header->len = size;
        header->type = TYPE_DATA;
        header->seq_num = conexiune->next_seq;
        header->protocol_id = POLI_PROTOCOL_ID;

        memcpy(packet + sizeof(poli_tcp_data_hdr), buffer + trimis_pana_acum, size);

        int packet_len = sizeof(poli_tcp_data_hdr) + size;
        //trimit cu locul liber din fereasta
        memcpy(conexiune->send_packets[conexiune->next_seq % MAX_WINDOW_SEGMENTS], packet, packet_len);
        conexiune->send_packet_len[conexiune->next_seq % MAX_WINDOW_SEGMENTS] = packet_len;
        conexiune->send_seq[conexiune->next_seq % MAX_WINDOW_SEGMENTS] = conexiune->next_seq;
        conexiune->send_acked[conexiune->next_seq % MAX_WINDOW_SEGMENTS] = 0;
        conexiune->send_used[conexiune->next_seq % MAX_WINDOW_SEGMENTS] = 1;
        conexiune->send_time_ms[conexiune->next_seq % MAX_WINDOW_SEGMENTS] = time_fereastra();
        sendto(conexiune->sockfd, packet, packet_len, 0,(struct sockaddr *)&conexiune->servaddr,sizeof(conexiune->servaddr));
        
        //astea nu cred ca mai sunt folosite
        // cred ca pot sa le sterg
        //memcpy(conexiune->ultimul, packet, packet_len);
        conexiune->ultimul_len = packet_len;
        conexiune->confirmare_ack = 1;
        conexiune->trimis_deja = conexiune->next_seq;
        //conexiune->send_base = conexiune->next_seq;
        conexiune->next_seq ++;
        trimis_pana_acum += size;
    }
    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    return trimis_pana_acum;
}

void *sender_handler(void *arg)
{
    int res = 0;
    char buf[MAX_SEGMENT_SIZE];

    while (1) {
        if (cons.size() == 0) {
            continue;
        }
        int conn_id = -1;
        do {
            res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);
        } while(res == -14);

        pthread_mutex_lock(&cons[conn_id]->con_lock);
        struct connection *con = cons[conn_id];
        //daca e timeout -> pierdtu -> trimit iar
        if (res == -1) {
            long long accc = time_fereastra();
            for (uint16_t i = con->send_base; i < con->next_seq; i++) {
                if (con->send_used[i %  MAX_WINDOW_SEGMENTS]) {
                    if(!con->send_acked[i %  MAX_WINDOW_SEGMENTS]){
                        if(accc - con->send_time_ms[i %  MAX_WINDOW_SEGMENTS] >= cv_timeout)
                        {   
                            sendto(con->sockfd, con->send_packets[i %  MAX_WINDOW_SEGMENTS], con->send_packet_len[i %  MAX_WINDOW_SEGMENTS], 0,(struct sockaddr *)&con->servaddr,sizeof(con->servaddr));
                            con->send_time_ms[i %  MAX_WINDOW_SEGMENTS] = accc;
                        }
                    }
                }
            }
            pthread_mutex_unlock(&cons[conn_id]->con_lock);
            continue;
        }
        poli_tcp_ctrl_hdr *ack = (poli_tcp_ctrl_hdr *)buf;
        //daca e ack si e pt ce pachet e bun, trec mai departe la next one
        if (ack->protocol_id == POLI_PROTOCOL_ID)
        {
            if(ack->type == TYPE_ACK) 
            {
                if(ack->conn_id == conn_id) 
                {
                    con->remote_recv_window = ack->recv_window;
                    if (con->send_used[ack->ack_num % MAX_WINDOW_SEGMENTS]) 
                    {
                        if(con->send_seq[ack->ack_num % MAX_WINDOW_SEGMENTS] == ack->ack_num) 
                        {
                            con->send_acked[ack->ack_num % MAX_WINDOW_SEGMENTS] = 1;
                        }
                    }
                }
            }
        }
        //glisez fereastra
        while (con->send_base < con->next_seq) 
        {
            int poz_curenta_de_elim = con->send_base % MAX_WINDOW_SEGMENTS;
            if (!con->send_used[poz_curenta_de_elim] || !con->send_acked[poz_curenta_de_elim]) 
            {
                break;
            }
            con->send_used[poz_curenta_de_elim] = 0;
            con->send_acked[poz_curenta_de_elim] = 0;
            con->send_packet_len[poz_curenta_de_elim] = 0;
            con->send_base ++;
        }
        /* Handle segment received from the receiver. We use this between locks
        as to not have synchronization issues with the send_data calls which are
        on the main thread */

        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
}

int setup_connection(uint32_t ip, uint16_t port)
{
    /* Implement the sender part of the Three Way Handshake. Blocks
    until the connection is established */
    //printf("setup incepe\n");
    struct connection *con = (struct connection *)malloc(sizeof(struct connection));
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_addr.s_addr = ip;
    servaddr.sin_port = port;
    servaddr.sin_family = AF_INET;
    socklen_t lungime_primit = sizeof(servaddr);
    // setez adresa serverului la fel ca in librecv
    //apoi setez si trimit syn
    poli_tcp_ctrl_hdr syn_de_trimis;
    memset(&syn_de_trimis,0,sizeof(syn_de_trimis));
    syn_de_trimis.conn_id = 0;
    syn_de_trimis.type = TYPE_SYN;
    syn_de_trimis.protocol_id =POLI_PROTOCOL_ID;
    syn_de_trimis.recv_window = 0;
    syn_de_trimis.ack_num = 0;
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 100000;
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    char packet_ack[sizeof(poli_tcp_ctrl_hdr) + sizeof(uint16_t)];
    poli_tcp_ctrl_hdr *headerack = (poli_tcp_ctrl_hdr *)packet_ack;
    while(1) {
        sendto(con->sockfd, &syn_de_trimis, sizeof(syn_de_trimis), 0, (struct sockaddr *)&servaddr, sizeof(servaddr));
        //printf("setup a trimis syn\n");
        //acum astept dupa ack sa vina inapoi
        int rc = recvfrom(con->sockfd, packet_ack, sizeof(packet_ack), 0,(struct sockaddr *)&servaddr, &lungime_primit);
        
        if(rc > 0){
            if(headerack->protocol_id == POLI_PROTOCOL_ID && headerack->type == TYPE_SYN_ACK){
                break;
            }
        }
    }
    // am primit ack, acum portul se afla in payload;
    //printf("setup a primit synack\n");
    con->remote_recv_window = headerack->recv_window;
    int conn_id = headerack->conn_id;
    uint16_t *port_primit = (uint16_t *)(packet_ack + sizeof(poli_tcp_ctrl_hdr));
    servaddr.sin_port = *port_primit;
    //trimis ack ca am primit portul & stuff
    //identic ca mai sus
    poli_tcp_ctrl_hdr ack_rasp;
    memset(&ack_rasp,0,sizeof(ack_rasp));
    ack_rasp.conn_id = conn_id;
    ack_rasp.type = TYPE_ACK;
    ack_rasp.protocol_id =POLI_PROTOCOL_ID;
    ack_rasp.recv_window = 0;
    ack_rasp.ack_num = 0;
    for (int i = 0; i < 15; i++) {
        sendto(con->sockfd, &ack_rasp, sizeof(ack_rasp), 0, (struct sockaddr *)&servaddr, sizeof(servaddr));
        //usleep(1000);
    }
    
    //printf("setup a trimis ack\n");
    /* // This can be used to set a timer on a socket 
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    } 

     We will send the SYN on 8031. Then we will receive a SYN-ACK with the connection
     * port. We can use con->sockfd for both cases, but we will need to update server_addr
     * with the port received via SYN-ACK 
 This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on our connection 
     Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    //stuff de la final care deja era in schelet

    con->conn_id = conn_id;
    con->servaddr = servaddr;
    con->next_seq = 0;
    con->ultimul_len = 0;
    con->confirmare_ack = 0;
    con->max_window_seq = 9;
    for (int i = 0; i < MAX_WINDOW_SEGMENTS; i++) {
        con->send_packet_len[i] = 0;
        con->send_acked[i] = 0;
        con->send_used[i] = 0;
    }
    con->send_base = 0;


    pthread_mutex_init(&con->con_lock, NULL);
    cons.insert({conn_id, con});

    data_fds[fdmax].fd = con->sockfd;    
    data_fds[fdmax].events = POLLIN;
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);
    timer_fds[fdmax].events = POLLIN;
    struct itimerspec spec;
    spec.it_value.tv_sec = cv_timeout / 1000;
    spec.it_value.tv_nsec = (cv_timeout % 1000) * 1000000;
    spec.it_interval.tv_sec = cv_timeout / 1000;
    spec.it_interval.tv_nsec = (cv_timeout % 1000) * 1000000;
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);
    fdmax++;

    DEBUG_PRINT("Connection established!");

    return conn_id;
}

void init_sender(int speed, int delay)
{
    pthread_t thread1;
    int ret;
    cv_timeout = TIMEOUT_SEND(delay);
    /* Create a thread that will*/
    ret = pthread_create( &thread1, NULL, sender_handler, NULL);
    assert(ret == 0);
}
//versiunea stop_and_wait care stiu ca merge bine
// o salvez in caz ca distrug totul si trebuie s ama intorc
int send_data_stop_wait(int conn_id, char *buffer, int len)
{
    int size = min(len,MAX_DATA_SIZE);

    pthread_mutex_lock(&cons[conn_id]->con_lock);

    /* We will write code here as to not have sync problems with sender_handler */
    struct connection *conexiune = cons[conn_id];
    //daca nu a primit ack
    if (conexiune->confirmare_ack) 
    {
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
        return -1;
    }

    //declar pachetul si header
    char packet[MAX_SEGMENT_SIZE];
    memset(packet, 0, sizeof(packet));
    poli_tcp_data_hdr *header = (poli_tcp_data_hdr *)packet;
    header->conn_id = conn_id;
    header->len = size;
    header->type = TYPE_DATA;
    header->seq_num = conexiune->next_seq;
    header->protocol_id = POLI_PROTOCOL_ID;

    memcpy(packet + sizeof(poli_tcp_data_hdr), buffer, size);

    int packet_len = sizeof(poli_tcp_data_hdr) + size;

    sendto(conexiune->sockfd, packet, packet_len, 0,(struct sockaddr *)&conexiune->servaddr,sizeof(conexiune->servaddr));

    memcpy(conexiune->ultimul, packet, packet_len);
    conexiune->ultimul_len = packet_len;
    conexiune->confirmare_ack = 1;
    conexiune->trimis_deja = conexiune->next_seq;
    conexiune->send_base = conexiune->next_seq;
    conexiune->next_seq ++;

    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    return size;
}

void *sender_handler_stop_wait(void *arg)
{
    int res = 0;
    char buf[MAX_SEGMENT_SIZE];

    while (1) {
        if (cons.size() == 0) {
            continue;
        }
        int conn_id = -1;
        do {
            res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);
        } while(res == -14);

        pthread_mutex_lock(&cons[conn_id]->con_lock);
        struct connection *con = cons[conn_id];
        //daca e timeout -> pierdtu -> trimit iar
        if (res == -1) {
            if (con->confirmare_ack) {
                sendto(con->sockfd, con->ultimul, con->ultimul_len, 0,(struct sockaddr *)&con->servaddr,sizeof(con->servaddr));
            }
            pthread_mutex_unlock(&cons[conn_id]->con_lock);
            continue;
        }

        poli_tcp_ctrl_hdr *ack = (poli_tcp_ctrl_hdr *)buf;
        //daca e ack si e pt ce pachet e bun, trec mai departe la next one
        if (ack->protocol_id == POLI_PROTOCOL_ID)
        {
            if(ack->type == TYPE_ACK) 
            {
                if(ack->conn_id == conn_id) 
                {
                    if (con->confirmare_ack) {
                        if(ack->ack_num == con->send_base + 1) 
                        {
                            con->confirmare_ack = 0;
                        }
                    }
                }
            }
        }
        /* Handle segment received from the receiver. We use this between locks
        as to not have synchronization issues with the send_data calls which are
        on the main thread */

        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
}
