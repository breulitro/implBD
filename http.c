/*
 * As funções get_line e startup foram copiadas "ibsis literis" do tinyhttpd.
 * A função unimplemented foi modificada para dar uma resposta mais coerente.
 * E a função httpok foi criada por mim.
 */


#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <netdb.h>

#include <stdio.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <ctype.h>
#include <strings.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <stdlib.h>

/**********************************************************************/
/* Get a line from a socket, whether the line ends in a newline,
 * carriage return, or a CRLF combination.  Terminates the string read
 * with a null character.  If no newline indicator is found before the
 * end of the buffer, the string is terminated with a null.  If any of
 * the above three line terminators is read, the last character of the
 * string will be a linefeed and the string will be terminated with a
 * null character.
 * Parameters: the socket descriptor
 *             the buffer to save the data in
 *             the size of the buffer
 * Returns: the number of bytes stored (excluding null) */
/**********************************************************************/
int get_line(int sock, char *buf, int size)
{
	int i = 0;
	char c = '\0';
	int n;

	while ((i < size - 1) && (c != '\n'))
	{
		n = recv(sock, &c, 1, 0);
		/* DEBUG DebugLog("%02X\n", c); */
		if (n > 0)
		{
			if (c == '\r')
			{
				n = recv(sock, &c, 1, MSG_PEEK);
				/* DEBUG DebugLog("%02X\n", c); */
				if ((n > 0) && (c == '\n'))
					recv(sock, &c, 1, 0);
				else
					c = '\n';
			}
			buf[i] = c;
			i++;
		}
		else
			c = '\n';
	}
	buf[i] = '\0';

	return(i);
}

/**********************************************************************/
/* This function starts the process of listening for web connections
 * on a specified port.  If the port is 0, then dynamically allocate a
 * port and modify the original port variable to reflect the actual
 * port.
 * Parameters: pointer to variable containing the port to connect on
 * Returns: the socket */
/**********************************************************************/
int startup(int *port)
{
	int httpd = 0;
	struct sockaddr_in name;

	httpd = socket(PF_INET, SOCK_STREAM, 0);
	if (httpd == -1) {
		perror("socket");
		return -1;
	}

	if (setsockopt(httpd, SOL_SOCKET, SO_REUSEADDR, &(int){ 1 }, sizeof(int)) < 0) {
		perror("setsockopt(SO_REUSEADDR) failed");
		return -1;
	}

	memset(&name, 0, sizeof(name));
	name.sin_family = AF_INET;
	name.sin_port = htons(*port);
	name.sin_addr.s_addr = htonl(INADDR_ANY);
	if (bind(httpd, (struct sockaddr *)&name, sizeof(name)) < 0) {
		perror("bind");
		return -1;
	}

	/* if dynamically allocating a port */
	if (*port == 0) {
		int namelen = sizeof(name);
		if (getsockname(httpd, (struct sockaddr *)&name, (socklen_t *)&namelen) == -1) {
			perror("getsockname");
			return -1;
		}

		*port = ntohs(name.sin_port);
	}

	if (listen(httpd, 1) < 0) {
		perror("listen");
		return -1;
	}

	return(httpd);
}

/**********************************************************************/
/* Inform the client that the requested web method has not been
 * implemented.
 * Parameter: the client socket */
/**********************************************************************/
void unimplemented(int client)
{
	char buf[1024];

	sprintf(buf, "HTTP/1.0 501 Method Not Implemented\r\n");
	send(client, buf, strlen(buf), 0);
	sprintf(buf, "Content-Length: %lu\r\n\r\n", strlen("Comando fora do padrão da RESTfull API.\n"));
	send(client, buf, strlen(buf), 0);
	sprintf(buf, "Comando fora do padrão da RESTfull API.\n");
	send(client, buf, strlen(buf), 0);
}

void httpok(int client)
{
	char buf[1024];

	sprintf(buf, "HTTP/1.1 200 OK\r\n\r\n");
	send(client, buf, strlen(buf), 0);
}
