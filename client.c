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
#include <sys/wait.h>

#define BUFFER_SIZE 1024

void trimite_comanda(const char *ip, int port, const char *comanda) {
    int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s == -1)
        perror("[client] Eroare la socket");

    struct sockaddr_in server;
    server.sin_family = AF_INET;
    server.sin_port = htons(port);
    server.sin_addr.s_addr = inet_addr(ip);

    struct timeval timeout;
    timeout.tv_sec = 5;
    timeout.tv_usec = 0;
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));


    /* Ne conectăm la server */
    if (connect(s, (struct sockaddr *)&server, sizeof(struct sockaddr)) == -1)
        perror("[client] Eroare la connect().\n");

    send(s, comanda, strlen(comanda), 0);
    printf("S-a trimis comanda: %s\n", comanda);

    char buffer[BUFFER_SIZE];
    int bytes_received;

    printf("[client] Astept raspunsuri de la server...\n");

    while ((bytes_received = recv(s, buffer, BUFFER_SIZE - 1, 0)) > 0) {
        buffer[bytes_received] = '\0'; // Terminator pentru string
        printf("Raspuns primit de la server: %s\n", buffer);
    }

    if (bytes_received == 0) {
        printf("[client] Conexiunea cu serverul a fost inchisa.\n");
    } else if (bytes_received == -1) {
        perror("[client] Eroare la recv() de la server.\n");
    }

    close(s);
}


int main(int argc, char *argv[]) {
    printf("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$\nPentru a putea urmari cu usurinta functionalitatea proiectului cheile cu     \nvaloare mai mare de 256(2^8) vor fi adaugate nodului cel mai mic, acest lucru\nse intampla deoarece am scos functia de hash de pe cheie si se va considera  \ncheia insusi ca key_hash\n$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$\n" );
    if (argc != 3) {
        printf("Usage: %s <ip> <port>\n" , argv[0]);
        exit(1);
    }

    const char *ip_server = argv[1];
    int port_serrver=atoi(argv[2]);

    char comanda[BUFFER_SIZE];
    while (1)
    {
        printf("Please use: ( ADD <key> <value> ; FIND <key> ; REMOVE <key> ; exit ): ");
        fgets(comanda, BUFFER_SIZE, stdin);
        comanda[strcspn(comanda, "\n")] = 0; // elimin \n
        if (strcmp(comanda, "exit") == 0)
            break;

        if (strncmp(comanda, "ADD ", 4) == 0 || strncmp(comanda, "FIND ", 5) == 0 || strncmp(comanda, "REMOVE ", 7) == 0
            || strncmp(comanda, "FINGER ", 7) == 0 || strncmp(comanda, "LEAVE", 5) == 0 )
            trimite_comanda(ip_server, port_serrver, comanda);

    }

}