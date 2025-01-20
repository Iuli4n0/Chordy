#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <errno.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <netdb.h>
#include <string.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <sqlite3.h>
#include <openssl/sha.h>

sqlite3 *db;

#define BUFFER_SIZE 1024
#define M 8

//m => nr biti
//finger[i]=succ( (id+2^(i-1)) mod 2^m )

typedef struct {
    int id;                 // ID nod (hash-ul portului)
    int port;               // Port nod
    int successor;          // Port succ
    int predecessor;        // Port pred
    int finger_table[M];
    int start[M];
    int backup_successor;   // Al doilea succesor (succesorul succesorului)

}Node;

Node node;


void send_join_message(int port_conectare, int port);
void send_update_message(int port, const char *message);
int send_message(int port, const char *message, char* response);
void show_node();
int send_find_successor(int target_port, int id);
int closest_preceding_node(int id);
int find_successor(int id);
void update_finger_table();
void show_finger_table();
void start_table();
void *stabilizer();
int preceding_finger(int id);
void transfer_keys_to_new_node(int new_node_port, int new_node_pred);
int check_node_alive(int port);
void is_succesor_alive();

pthread_mutex_t mutex_chord = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_db = PTHREAD_MUTEX_INITIALIZER;

int hash_sha1(int key) {
    //Convertim key in string
    char string_cheie[64];
    snprintf(string_cheie, sizeof(string_cheie), "%d", key);

    unsigned char digest[SHA_DIGEST_LENGTH];

    SHA1((unsigned char*)string_cheie, strlen(string_cheie), digest);

    uint32_t hash_val = 0;
    for (int i = 0; i < 4; i++) {
        hash_val = (hash_val << 8) | digest[i];
    }

    //Facem modulo 2^m = RING_SIZE
    return (hash_val % 256);
}

int hash(int key) {
    return hash_sha1(key);
}

void init_Node(int port) {
    node.port = port;
    node.successor = port;
    node.predecessor = port;
    node.id = hash(port);
}

/* Gestionare cereri */
void *handle_request(void *arg) {
    int c=*((int*)arg);
    free(arg);
    printf("[server] Using socket %d \n",c);

    char buffer[BUFFER_SIZE];
    int bytes_read=recv(c, buffer, BUFFER_SIZE-1, 0);
    if(bytes_read == -1) {
        perror("error recv");
        close(c);
        pthread_exit(NULL);
    }
    buffer[bytes_read] = '\0';
    printf("[server] Received: %s\n",buffer);


    char raspuns[BUFFER_SIZE];

    //pthread_mutex_lock(&mutex_chord); nu este necesar sa blocam tot handleru
   if (strncmp(buffer, "FIND ", 5) == 0) { ////////////////////   FIND   /////////////////////
    int key = atoi(buffer + 5); // Extragem cheia din cerere
    int key_hash = key; //sau hash(key)

    printf("[FIND] Request for key: %d (hash: %d)\n", key, key_hash);

       if (node.id == hash(node.predecessor) && node.id == hash(node.successor)) { //avem un singur nod in retea
           char comanda[256];
           snprintf(comanda, sizeof(comanda),"SELECT value FROM nod_%d WHERE key = %d;",node.id, key);

           char *mesaj_eroare = NULL;
           char **results = NULL;
           int rows, columns;

           pthread_mutex_lock(&mutex_db);
           //sqlite3_get_table
           if (sqlite3_get_table(db, comanda, &results, &rows, &columns, &mesaj_eroare) != SQLITE_OK) {
               snprintf(raspuns, BUFFER_SIZE - 1,"[FIND] SQLite error: %s",mesaj_eroare);
               sqlite3_free(mesaj_eroare);
           }
           else {
               if (rows > 0) {
                   // results[0] numele coloanei
                   // results[1] valoarea
                   char *valoare_gasita = results[columns];
                   snprintf(raspuns, BUFFER_SIZE - 1, "Key %d found at node %d with value '%s'", key, node.id, valoare_gasita);
               }
               else
                   snprintf(raspuns, BUFFER_SIZE - 1,"Key %d not found at node %d (no entry in DB)",key, node.id);

               sqlite3_free_table(results);
           }
           pthread_mutex_unlock(&mutex_db);
       }

    else if ((node.id > hash(node.predecessor) && key_hash > hash(node.predecessor) && key_hash <= node.id)
        || (node.id < hash(node.predecessor) && (key_hash > hash(node.predecessor) || key_hash <= node.id)))
    {
        char comanda[256];
        snprintf(comanda, sizeof(comanda),"SELECT value FROM nod_%d WHERE key = %d;",node.id, key);

        char *mesaj_eroare = NULL;
        char **results = NULL;
        int rows, columns;

        pthread_mutex_lock(&mutex_db);
        //sqlite3_get_table
        if (sqlite3_get_table(db, comanda, &results, &rows, &columns, &mesaj_eroare) != SQLITE_OK) {
            snprintf(raspuns, BUFFER_SIZE - 1,"[FIND] SQLite error: %s",mesaj_eroare);
            sqlite3_free(mesaj_eroare);
        }
        else {
            if (rows > 0) {
                // results[0] numele coloanei
                // results[1] valoarea
                char *valoare_gasita = results[columns];
                snprintf(raspuns, BUFFER_SIZE - 1, "Key %d found at node %d with value '%s'", key, node.id, valoare_gasita);
            }
            else
                snprintf(raspuns, BUFFER_SIZE - 1,"Key %d not found at node %d (no entry in DB)",key, node.id);

            sqlite3_free_table(results);
        }
        pthread_mutex_unlock(&mutex_db);
    }
    else {
        // rutam cererea
        int next_node = preceding_finger(key);//sau hash(key)
        printf("[FIND] Forwarding request for key %d to node %d\n", key, next_node);

        char send_command[BUFFER_SIZE];
        snprintf(send_command, sizeof(send_command), "FIND %d", key);

        char response[BUFFER_SIZE];
        if (send_message(next_node, send_command, response) == 0) {
            // primim raspusnul de la noul nod
            snprintf(raspuns, BUFFER_SIZE - 1, "%s", response);
            printf("[FIND] Response from node %d: %s\n", next_node, response);
        } else {
            snprintf(raspuns, BUFFER_SIZE - 1, "Error finding key %d", key);
        }
    }
    }

    else if (strncmp(buffer, "ADD ", 4) == 0) { ////////////////////   ADD   /////////////////////
        int key;
        char value[BUFFER_SIZE];

        if (sscanf(buffer + 4, "%d %s", &key, value) == 2) {
            int key_hash = key; //sau hash(key)

            // Avem un singur nod in retea
            if (node.id == hash(node.predecessor) && node.id == hash(node.successor)) {
                char comanda[256];
                snprintf(comanda, sizeof(comanda), "INSERT INTO nod_%d (key, value) VALUES (%d, '%s');",node.id, key, value);

                pthread_mutex_lock(&mutex_db);
                char *mesaj_eroare;
                if (sqlite3_exec(db, comanda, 0, 0, &mesaj_eroare) != SQLITE_OK) {
                    snprintf(raspuns, BUFFER_SIZE - 1, "Failed to ADD key %d: %s", key, mesaj_eroare);
                    sqlite3_free(mesaj_eroare);
                } else {
                    snprintf(raspuns, BUFFER_SIZE - 1,"Key %d with value '%s' added successfully at node %d",key, value, node.id);
                }
                pthread_mutex_unlock(&mutex_db);
            }

            else if ( // nodul curent este responsabil
                (node.id > hash(node.predecessor)&& key_hash > hash(node.predecessor)&& key_hash <= node.id)
                ||
                (node.id < hash(node.predecessor)&& (key_hash > hash(node.predecessor) || key_hash <= node.id))
            ) {
                char comanda[256];
                snprintf(comanda, sizeof(comanda),"INSERT INTO nod_%d (key, value) VALUES (%d, '%s');",node.id, key, value);

                pthread_mutex_lock(&mutex_db);
                char *mesaj_eroare;
                if (sqlite3_exec(db, comanda, 0, 0, &mesaj_eroare) != SQLITE_OK) {
                    snprintf(raspuns, BUFFER_SIZE - 1,"Failed to ADD key %d: %s",key, mesaj_eroare);
                    sqlite3_free(mesaj_eroare);
                } else {
                    snprintf(raspuns, BUFFER_SIZE - 1,"Key %d with value '%s' added successfully at node %d",key, value, node.id);
                }
                pthread_mutex_unlock(&mutex_db);
            }
        else {
            //forward la nodul preceding_finger(key)
            int close_predecesor = preceding_finger(key);//sau hash(key)

            char send_command[BUFFER_SIZE];
            snprintf(send_command, sizeof(send_command),"ADD %d %s", key, value);

            char response[BUFFER_SIZE];
            if (send_message(close_predecesor, send_command, response) == 0) {
                // trimitem clientului care a initiat comanda
                snprintf(raspuns, BUFFER_SIZE - 1, "%s", response);
            } else {
                snprintf(raspuns, BUFFER_SIZE - 1,"Error forwarding ADD key %d to node %d",key, close_predecesor);
            }
        }
    } else {
        snprintf(raspuns, BUFFER_SIZE - 1,"Invalid ADD command. Format: ADD <key> <value>");
    }
}

    else if (strncmp(buffer, "FIND_SUCC ", 10) == 0) {
        int id = atoi(buffer + 10);
        int successor = find_successor(id);
        snprintf(raspuns, BUFFER_SIZE - 1, "%d", successor);
    }

    else if(strncmp(buffer, "REMOVE ", 7) == 0) { //////////////////   REMOVE   ///////////////////////
        int key = atoi(buffer+4);
        if (sscanf(buffer + 7, "%d", &key)) {
            int key_hash = key; //sau hash(key)

            if (node.id == hash(node.predecessor) && node.id == hash(node.successor)) { // avem un singur nod
                char comanda[256];
                snprintf(comanda, sizeof(comanda), "DELETE FROM nod_%d WHERE key = %d;", node.id, key);

                pthread_mutex_lock(&mutex_db);
                char *mesaj_eroare;
                if (sqlite3_exec(db, comanda, 0, 0, &mesaj_eroare) != SQLITE_OK) {
                    snprintf(raspuns, BUFFER_SIZE - 1, "Failed to REMOVE key %d: %s", key, mesaj_eroare);
                    sqlite3_free(mesaj_eroare);
                } else {
                    snprintf(raspuns, BUFFER_SIZE - 1, "Key %d removed successfully", key);
                }
                pthread_mutex_unlock(&mutex_db);
            }

            else if ((node.id > hash(node.predecessor) && key_hash > hash(node.predecessor) && key_hash <= node.id)
                    ||
                     (node.id < hash(node.predecessor) && (key_hash > hash(node.predecessor) || key_hash <= node.id)))
                {

                char comanda[256];
                snprintf(comanda, sizeof(comanda), "DELETE FROM nod_%d WHERE key = %d;", node.id, key);

                pthread_mutex_lock(&mutex_db);
                char *mesaj_eroare;
                if (sqlite3_exec(db, comanda, 0, 0, &mesaj_eroare) != SQLITE_OK) {
                    snprintf(raspuns, BUFFER_SIZE - 1, "Failed to REMOVE key %d: %s", key, mesaj_eroare);
                    sqlite3_free(mesaj_eroare);
                } else {
                    snprintf(raspuns, BUFFER_SIZE - 1, "Key %d removed successfully", key);
                }
                pthread_mutex_unlock(&mutex_db);
            }
            else {
                int close_predecesor=preceding_finger(key);//sau hash(key)

                char send_command[BUFFER_SIZE];
                snprintf(send_command, sizeof(send_command), "REMOVE %d", key);
                send_update_message(close_predecesor, send_command);

                snprintf(raspuns, BUFFER_SIZE - 1, "Key %d remove request forwarded to %d", key, close_predecesor);
            }
        }
        else {
            snprintf(raspuns, BUFFER_SIZE - 1, "Invalid REMOVE command. Format: REMOVE <key>");
        }
    }


    else if (strncmp(buffer, "JOIN ", 5) == 0) { //////////////////////   JOIN    /////////////////////
        int new_port = atoi(buffer + 5);
        int new_id = hash(new_port);
        printf("Node from port %d sent a join message\n", new_port);

        if (node.port == node.successor) { // Avem doar un nod in retea
            pthread_mutex_lock(&mutex_chord);
            node.successor = new_port;
            node.predecessor = new_port;
            pthread_mutex_unlock(&mutex_chord);

           // noul nod isi va actualiza succ si pred
            char send_update[BUFFER_SIZE];

            snprintf(send_update, sizeof(send_update), "UPDATE_P %d", node.port);
            send_update_message(new_port, send_update);
            snprintf(send_update, sizeof(send_update), "UPDATE_S %d", node.port);
            send_update_message(new_port, send_update);

            snprintf(raspuns, BUFFER_SIZE - 1, "JOIN SUCCES");
            pthread_mutex_lock(&mutex_chord);
            show_node();
            update_finger_table();
            show_finger_table();
            pthread_mutex_unlock(&mutex_chord);
        } else {
            // Avem minim 2 noduri in retea
            if ((new_id > node.id && new_id < hash(node.successor)) || (node.id > hash(node.successor) && (new_id > node.id || new_id < hash(node.successor)))) {

                //noul nod isi va act succ si pred
                char send_update[BUFFER_SIZE];

                snprintf(send_update, sizeof(send_update), "UPDATE_P %d", node.port);
                send_update_message(new_port, send_update);

                snprintf(send_update, sizeof(send_update), "UPDATE_S %d", node.successor);
                send_update_message(new_port, send_update);


                // fostul succ isi actualizeaza pred
                snprintf(send_update, sizeof(send_update), "UPDATE_P %d", new_port);
                send_update_message(node.successor, send_update);

                pthread_mutex_lock(&mutex_chord);
                // setam succesorul nodului curent
                node.successor = new_port;
                pthread_mutex_unlock(&mutex_chord);

                snprintf(raspuns, BUFFER_SIZE - 1, "JOIN SUCCES");
                pthread_mutex_lock(&mutex_chord);
                show_node();
                update_finger_table();
                show_finger_table();
                pthread_mutex_unlock(&mutex_chord);
            } else {
                // rutam cererea de join catre succesor
                snprintf(raspuns, BUFFER_SIZE - 1, "JOIN FAILED, FORWARD NEEDED");
                send_join_message(node.successor, new_port);
            }
    }
}
    else if (strncmp(buffer, "UPDATE_P ", 9) == 0) {
        int const new_predecessor = atoi(buffer + 9);
        pthread_mutex_lock(&mutex_chord);
        node.predecessor = new_predecessor;
        pthread_mutex_unlock(&mutex_chord);
        snprintf(raspuns, BUFFER_SIZE - 1, "Node %d Predecessor updated to %d", node.id, node.predecessor);
        show_node();
        /*
        update_finger_table();
        show_finger_table();
        */
}
    else if (strncmp(buffer, "UPDATE_S ", 9) == 0) {
        int const new_successor = atoi(buffer + 9);

        pthread_mutex_lock(&mutex_chord);
        node.successor = new_successor;
        pthread_mutex_unlock(&mutex_chord);

        snprintf(raspuns, BUFFER_SIZE - 1, "Node %d Successor updated to %d", node.id, node.successor);

        pthread_mutex_lock(&mutex_chord);
        show_node();
        // avem un nou succ, vom face update la tot finger table
        update_finger_table();
        show_finger_table();
        pthread_mutex_unlock(&mutex_chord);

        //cerem cheile de la noul succesor
        char request_cmd[BUFFER_SIZE];
        snprintf(request_cmd, sizeof(request_cmd), "REQUEST_KEYS %d %d", node.port, node.predecessor);

        printf("                                    my port is %d\n",node.predecessor);

        send_update_message(node.successor, request_cmd);

}
    else if (strncmp(buffer, "LEAVE", 5) == 0) {
        char send_update[BUFFER_SIZE];

        snprintf(send_update, sizeof(send_update), "UPDATE_P %d", node.predecessor);
        send_update_message(node.successor, send_update);

        snprintf(send_update, sizeof(send_update), "NT_UPDATE_S %d", node.successor);
        send_update_message(node.predecessor, send_update);

        transfer_keys_to_new_node(node.successor, node.predecessor);

        close(c);
        exit(0);

    }
    else if (strncmp(buffer, "NT_UPDATE_S ", 12) == 0) { //functie folosita doar la leave
        int const new_successor = atoi(buffer + 12);

        pthread_mutex_lock(&mutex_chord);
        node.successor = new_successor;
        pthread_mutex_unlock(&mutex_chord);

        snprintf(raspuns, BUFFER_SIZE - 1, "Node %d Successor updated to %d", node.id, node.successor);

        pthread_mutex_lock(&mutex_chord);
        show_node();
        // avem un nou succ, vom face update la tot finger table
        update_finger_table();
        show_finger_table();
        pthread_mutex_unlock(&mutex_chord);

    }

    else if (strncmp(buffer, "REQUEST_KEYS ", 13) == 0) {
        int new_node_port, new_node_pred;
        if (sscanf(buffer + 13, "%d %d", &new_node_port, &new_node_pred) == 2) {
            transfer_keys_to_new_node(new_node_port, new_node_pred); //mutex in function

            snprintf(raspuns, BUFFER_SIZE - 1,"Keys transfer request handled by node %d", node.id);
        }
        else
            snprintf(raspuns, BUFFER_SIZE - 1, "REQUEST_KEYS error. Usage: REQUEST_KEYS <port_nod_nou> <port_pred_nou>");
    }

    else if (strncmp(buffer, "TRANSFER_KEY ", 12) == 0) {

        int cheie;
        char valoare[BUFFER_SIZE];
        int port_nou;

        if (sscanf(buffer + 12, "%d %s %d", &cheie, valoare, &port_nou) == 3) {
            // Inseram in BD a nodului curent (nodul nou)
            char insert_cmd[256];
            snprintf(insert_cmd, sizeof(insert_cmd),"INSERT INTO nod_%d (key, value) VALUES (%d, '%s');",node.id, cheie, valoare);

            pthread_mutex_lock(&mutex_db);
            char *mesaj_eroare = NULL;
            if (sqlite3_exec(db, insert_cmd, 0, 0, &mesaj_eroare) != SQLITE_OK) {
                snprintf(raspuns, BUFFER_SIZE - 1,"Error inserting TRANSFER_KEY %d '%s' at node %d: %s",cheie, valoare, node.id, mesaj_eroare);
                sqlite3_free(mesaj_eroare);
            } else
                snprintf(raspuns, BUFFER_SIZE - 1,"Key %d with value '%s' received at node %d",cheie, valoare, node.id);
            pthread_mutex_unlock(&mutex_db);
        }
        else {
            snprintf(raspuns, BUFFER_SIZE - 1,"TRANSFER_KEY command error: '%s'", buffer);
        }
    }
    else if (strncmp(buffer, "FINGER ", 7) == 0) {
        pthread_mutex_lock(&mutex_chord);
        update_finger_table();
        show_finger_table();
        pthread_mutex_unlock(&mutex_chord);
        snprintf(raspuns, BUFFER_SIZE - 1, "FINGER UPDATE SUCCESS");
    }
    else if (strncmp(buffer, "PING", 4) == 0) {
        strcpy(raspuns, "PONG");
    }
    else if (strncmp(buffer, "GET_SUCC", 8) == 0) {
        snprintf(raspuns, BUFFER_SIZE, "%d", node.successor);
    }
    else if (strncmp(buffer, "GET_PRED", 8) == 0) {
        snprintf(raspuns, BUFFER_SIZE, "%d", node.predecessor);
    }
    else {
        snprintf(raspuns, BUFFER_SIZE-1, "Please use: ( ADD <key> <value> ; FIND <key> ; REMOVE <key> ; exit ): ");
    }


    printf("[server] Sending response to my client: %s\n",raspuns);
    send(c, raspuns, strlen(raspuns), 0);
    close(c);
    //pthread_mutex_unlock(&mutex_chord);

    pthread_exit(NULL);

} //void

void *server_thread(void *arg) {
    if(arg == NULL) {
        printf("[server] Invalid arg");
        exit(1);
    }

    int port=*((int*)arg);
    free(arg); //eliberam memoria
    int s=socket(AF_INET, SOCK_STREAM, 0);
    if(s < 0)
    {
        perror("[server] Socket creation failed");
        exit(1);
    }
    struct sockaddr_in server,client;
    socklen_t len=sizeof(client);

    server.sin_family = AF_INET;
    server.sin_port = htons(port);
    server.sin_addr.s_addr = INADDR_ANY;

    int opt = 1; //permite reutilizarea adreselor si porturilor in starea TIME_WAIT
    if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("[server] setsockopt failed");
        exit(1);
    }


    /* atasam socketul */
    if (bind (s, (struct sockaddr *) &server, sizeof (struct sockaddr)) == -1)
    {
        close(s);
        perror ("[server]Eroare la bind().\n");
        exit(1);
    }

    listen(s, 5);
    printf("[server] Node started on port %d ; ID: %d \n",port,node.id);
    start_table(); //incepem constr la finger table

    pthread_t stabilize_thread;
    if (pthread_create(&stabilize_thread, NULL, stabilizer, NULL)!=0) {
        perror("[server] Thread creation failed for stabilizer");
        exit(1);
    }
    pthread_detach(stabilize_thread);


    while(1) {
        len=sizeof(client);
        int client_socket = accept(s, (struct sockaddr*)&client, &len);
        if(client_socket == -1) {
            printf("[server] Accept failed");
            continue;
        }
        printf("Connection accepted from %s:%d\n", inet_ntoa(client.sin_addr), ntohs(client.sin_port));

        int *new_socket = (int*)malloc(sizeof(int));
        if (*new_socket == -1) {
            printf("[server] Memory allocation failed");
            close(client_socket);
            continue;
        }
        *new_socket = client_socket;

        pthread_t thread;
        if(pthread_create(&thread, NULL, handle_request, (void*)new_socket) != 0) {
            printf("[server] Thread creation failed");
            close(client_socket);
            free(new_socket);
            continue;
        }
        pthread_detach(thread);

    }// while

}

void send_update_message(int port, const char *message) {
    int const s = socket(AF_INET, SOCK_STREAM, 0);
    if (s == -1)
        perror("[client] Eroare la socket");

    struct sockaddr_in server;
    server.sin_family = AF_INET;
    server.sin_port = htons(port);
    server.sin_addr.s_addr = inet_addr("127.0.0.1");
    /* ne conectam la server */
    if (connect (s, (struct sockaddr *) &server,sizeof (struct sockaddr)) == -1)
        perror ("[client] Eroare la connect().\n");

    send(s, message, strlen(message), 0);
}

void send_join_message(int port_conectare, int port) {

    char comanda[BUFFER_SIZE];
    snprintf(comanda, BUFFER_SIZE-1, "JOIN %d", port);
    int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s == -1)
        perror("[client] Eroare la socket");

    struct sockaddr_in server;
    server.sin_family = AF_INET;
    server.sin_port = htons(port_conectare);
    server.sin_addr.s_addr = inet_addr("127.0.0.1");
    /* ne conectam la server */
    if (connect (s, (struct sockaddr *) &server,sizeof (struct sockaddr)) == -1)
        perror ("[client] Eroare la connect().\n");

    send(s, comanda, strlen(comanda), 0);
    printf("S-a trimis comanda %s\n", comanda);

    close(s);

}

int send_message(int target_port, const char *message, char *response) {
    int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s == -1)
        perror("[client] Eroare la socket");
    printf("target port este: %d\n", target_port);

    struct sockaddr_in server;
    server.sin_family = AF_INET;
    server.sin_port = htons(target_port);
    server.sin_addr.s_addr = inet_addr("127.0.0.1");


    /* ne conectam la server */
    if (connect (s, (struct sockaddr *) &server,sizeof (struct sockaddr)) == -1)
        perror ("[client] Eroare la connect().\n");
    else {
        send(s, message, strlen(message), 0);
        printf("S-a trimis comanda %s\n", message);

        int bytes_received = recv(s, response, BUFFER_SIZE, 0);
        if (bytes_received == -1)
            perror("[client] Eroare la recv() de la server.\n");
        else {
            response[bytes_received] = '\0';
        }
    }

    close(s);
    return 0;
}

int find_successor(int id) {
    printf("[find_successor] Finding successor for ID: %d\n", id);

    if (node.successor == node.port) {
        printf("[find_successor] Single-node system.\n");
        return node.port;
    }

    if ((id > node.id && id <= hash(node.successor)) || (node.id > hash(node.successor) && (id > node.id || id <= hash(node.successor)))) {
        printf("[find_successor] ID %d belongs to current node\n", id);
        return node.successor;
        }

    printf("[find_successor] Routing to successor: %d\n", preceding_finger(id));
    int successor = send_find_successor(preceding_finger(id), id);
    if (successor!=-1)
        return successor;
    return node.successor;

}

// Rutam cererea FIND_SUCC
int send_find_successor(int target_port, int id) {

    char send_command[BUFFER_SIZE];
    snprintf(send_command, sizeof(send_command), "FIND_SUCC %d", id);

    char response[BUFFER_SIZE];
    if (send_message(target_port, send_command, response) == 0) {
        int successor = atoi(response);
        if (successor > 0)
            return successor;
    }

    printf("[send_find_succ] Failed to find successor for ID: %d on port: %d\n", id, target_port);
    return -1;
}



void start_table() {
    for (int i = 0; i < M; i++) {
        node.start[i] = (node.id + (1 << i)) % (1 << M);
        printf("[upd_finger_table] Calculating entry %d, start: %d\n", i, node.start[i]);
    }
}

void update_finger_table() {
    for (int i = 0; i < M; i++) {

        int successor = find_successor(node.start[i]);
        if (successor == node.port || successor <= 0) {
            printf("[upd_finger_table] Entry %d remains the same (port %d).\n", i, node.port);
            successor = node.port;
        }

        node.finger_table[i] = successor;
        printf("[upd_finger_table] Finger table[%d]: %d\n", i, successor);
    }
}


void fix_fingers() {
    static int next = 0;
    int start = (node.id + (1 << next)) % (1 << M);

    int successor = find_successor(start);
    if (node.finger_table[next] != successor) {
        printf("[fix_fingers] Updating finger[%d] from %d to %d\n", next, node.finger_table[next], successor);
        node.finger_table[next] = successor;
    }

    next = (next + 1) % M;
}

int preceding_finger(int id) {
    for (int i = M-1; i >= 0; i--) {
        int finger_id = hash(node.finger_table[i]);
        if (
            // caz normal
            ((node.id < id) &&
             (finger_id > node.id && finger_id < id))
            ||
            // caz wrap-around
            ((node.id > id) &&
             (finger_id > node.id || finger_id < id))
        ) {
            return node.finger_table[i];
        }
    }

    return node.successor;
}


void transfer_keys_to_new_node(int new_node_port, int new_node_pred) {
    printf("[transfer_keys] Node %d transferring keys to port %d\n",node.id, new_node_port);

    int new_node_id = hash(new_node_port);
    int pred_new_node = hash(new_node_pred);

    printf("[transfer_keys] new_node_id = %d, pred_new_node = %d\n",new_node_id, pred_new_node);

    char select_cmd[256];
    snprintf(select_cmd, sizeof(select_cmd),"SELECT key, value FROM nod_%d;", node.id);

    char *error = NULL;
    char **rezultat = NULL;
    int rows, cols;

    pthread_mutex_lock(&mutex_db);
    if (sqlite3_get_table(db, select_cmd, &rezultat, &rows, &cols, &error) != SQLITE_OK) {
        fprintf(stderr, "[transfer_keys_to_new_node] SELECT error: %s\n", error);
        sqlite3_free(error);
        return;
    }

    int index = cols;
    for (int i = 0; i < rows; i++) {
        int k = atoi(rezultat[index]);       // key
        char *val = rezultat[index + 1];     // value
        index += cols;

        int key_hash = k; //sau hash(k)

        if (
            // Caz normal(pred_new_node< new_node_id)
            ((pred_new_node < new_node_id) &&
             (key_hash > pred_new_node && key_hash <= new_node_id))
            ||
            // Caz wrap-around(pred_new_node > new_node_id)
            ((pred_new_node > new_node_id) &&
             (key_hash > pred_new_node || key_hash <= new_node_id))
             ||
             pred_new_node == new_node_id // Caz unde ramanem cu un singur nod
        ) {
            char msg[BUFFER_SIZE];
            snprintf(msg, sizeof(msg),"TRANSFER_KEY %d %s %d", k, val, new_node_port);

            send_update_message(new_node_port, msg);
            printf("[transfer_keys] Transferred key %d -> node %d OK.\n",k, new_node_port);

            char delete_cmd[256];
            snprintf(delete_cmd, sizeof(delete_cmd),"DELETE FROM nod_%d WHERE key = %d;", node.id, k);

            char *delete_error = NULL;
            if (sqlite3_exec(db, delete_cmd, 0, 0, &delete_error) != SQLITE_OK) {
                fprintf(stderr, "Error deleting key %d: %s\n", k, delete_error);
                sqlite3_free(delete_error);
            }
        }
    }
    sqlite3_free_table(rezultat);
    pthread_mutex_unlock(&mutex_db);
}

void is_succesor_alive() {
        if (node.successor != node.port) {
            if (check_node_alive(node.successor) == 0) {
                // cerem succesorul succesorului
                char response[BUFFER_SIZE];
                if (send_message(node.successor, "GET_SUCC", response) == 0) {
                    int s2 = atoi(response);
                    pthread_mutex_lock(&mutex_chord);
                    node.backup_successor = s2;
                    pthread_mutex_unlock(&mutex_chord);
                }
            }
            else {
                //succesorul a murit facem fallback la backup
                printf("[stabilizer] Detected that successor %d is dead.\n", node.successor);
                pthread_mutex_lock(&mutex_chord);
                node.successor = node.backup_successor;
                pthread_mutex_unlock(&mutex_chord);


                if (node.successor != node.port) {
                    char response[BUFFER_SIZE];
                    if (send_message(node.successor, "GET_SUCC", response) == 0) {
                        int s2 = atoi(response);
                        pthread_mutex_lock(&mutex_chord);
                        node.backup_successor = s2;
                        pthread_mutex_unlock(&mutex_chord);
                    } else {
                        node.backup_successor = node.port;
                    }
                } else {
                    // E single-node
                    pthread_mutex_lock(&mutex_chord);
                    node.backup_successor = node.port;
                    pthread_mutex_unlock(&mutex_chord);
                }

                // notificam noul succesor sa-si seteze predecesorul la mine
                if (node.successor != node.port) {
                    char notify_cmd[BUFFER_SIZE];
                    snprintf(notify_cmd, sizeof(notify_cmd), "UPDATE_P %d", node.port);
                    send_update_message(node.successor, notify_cmd);
                }
            }
        } else {
            //single-node
            pthread_mutex_lock(&mutex_chord);
            node.backup_successor = node.port;
            pthread_mutex_unlock(&mutex_chord);
        }

        // verificam si predecesorul
        if (node.predecessor != node.port) {
            if (check_node_alive(node.predecessor) != 0) {
                printf("[stabilizer] Predecessor %d seems dead. Setting predecessor to self.\n", node.predecessor);
                pthread_mutex_lock(&mutex_chord);
                node.predecessor = node.port;
                pthread_mutex_unlock(&mutex_chord);
            }
        }
}

void *stabilizer() {
    sleep(1);
    while (1) {

        is_succesor_alive();
        sleep(1);

        printf("[stabilizer] Fixing fingers for node %d\n", node.id);
        pthread_mutex_lock(&mutex_chord);
        fix_fingers();
        pthread_mutex_unlock(&mutex_chord);
        show_finger_table();

        sleep(1);
    }
    pthread_exit(NULL);
}

int check_node_alive(int port)
{
    int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s == -1) {
        printf("[check_node_alive] socket error");
        return -1;
    }

    struct sockaddr_in server;
    server.sin_family = AF_INET;
    server.sin_port = htons(port);
    server.sin_addr.s_addr = inet_addr("127.0.0.1");

    if (connect(s, (struct sockaddr *)&server, sizeof(server)) == -1) {
        close(s);
        return -1;
    }

    const char *ping_msg = "PING";
    if (send(s, ping_msg, strlen(ping_msg), 0) == -1) {
        close(s);
        return -1;
    }

    char buffer[BUFFER_SIZE];
    int bytes = recv(s, buffer, sizeof(buffer)-1, 0);
    if (bytes <= 0) {
        close(s);
        return -1;
    }
    buffer[bytes] = '\0';
    close(s);

    if (strncmp(buffer, "PONG", 4) == 0) {
        return 0;
    }
    return -1; // nu s-au mai intors pingurile
}


void init_database(int id_nod) {
    char sql[256];
    char *err_msg = NULL;

    if (sqlite3_open("bd_centralizata.db", &db)) {
        perror("Eroare la deschiderea bazei de date");
        exit(1);
    }
    printf("Succesfully opened 'bd_centralizata.db'.\n");

    //Setam un busy timeout si bd in modul wall pentru a ajuta concurenta
    sqlite3_busy_timeout(db, 1000);
    if (sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, &err_msg) != SQLITE_OK) {
        fprintf(stderr, "Error setting WAL mode: %s\n", err_msg);
        sqlite3_free(err_msg);
    }

    // Pornim cu un table gol
    snprintf(sql, sizeof(sql), "DELETE FROM nod_%d;", id_nod);
    if (sqlite3_exec(db, sql, 0, 0, &err_msg) != SQLITE_OK) {
        if (strstr(err_msg, "no such table") == NULL) {
            printf("SQL delete error");
            sqlite3_free(err_msg);
            sqlite3_close(db);
            exit(1);
        }
        sqlite3_free(err_msg);
    } else {
        printf("Existing data from 'nod_%d' has been cleared.\n", id_nod);
    }

    //Creeaza tabela
    snprintf(sql, sizeof(sql), "CREATE TABLE IF NOT EXISTS nod_%d (key INTEGER PRIMARY KEY, value TEXT);", id_nod);
    if (sqlite3_exec(db, sql, 0, 0, &err_msg) != SQLITE_OK) {
        printf("SQL table error");
        sqlite3_free(err_msg);
        sqlite3_close(db);
        exit(1);
    }

    printf("Table 'nod_%d' has been created.\n", id_nod);
}

void show_node() {
    printf("[server] Node port: %d\n", node.port);
    printf("[server] Node id: %d\n", node.id);
    printf("[server] Node successor: %d", node.successor);
    printf("[server] Node predecessor: %d\n", node.predecessor);
}

void show_finger_table() {
    for (int i = 0; i < M; i++)
        printf("[server] Finger_table[%d]: %d\n", i, node.finger_table[i]);
}

int main(int argc , char *argv[]) {
    if (argc < 2 || argc>3 ) {
        printf("Usage: %s <port>\n" , argv[0]);
        exit(1);
    }

    int port = atoi(argv[1]);
    init_Node(port);
    init_database(node.id);

    if(argc == 3) {
        int port_nod_conectare=atoi(argv[2]);
        send_join_message(port_nod_conectare,port);
    }

    int *server_port = (int*)malloc(sizeof(int));
    if (*server_port == -1) {
        printf("[server] Memory allocation failed");
        return 1;
    }
    *server_port = port;
    server_thread(server_port);

    return 0;
}





