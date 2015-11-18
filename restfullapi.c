/*
 * INSERT = POST /
 * SELECT = GET /$pk
 * UPDATE = PATCH /$pk
 * DELETE = DELETE /$pk
 * SEARCH = GET /search/$tag
 *
 * obs: no UPDATE e no INSERT, o json vem no corpo do comando
 */

#include <stdio.h>
#include <ctype.h>
#include <strings.h>
#include <unistd.h>

#include <netinet/in.h>
#include <sys/socket.h>

#include "http.h"
#include "bufmgmt.h"

FILE *serverlog;

#define LOG(fmt, ...) {\
			fprintf(serverlog, fmt, ##__VA_ARGS__);\
			fflush(serverlog);\
			}

char server_running = 1;

void parse_http(int client) {
	char buf[1024];
	char method[255];
	char *pk, *aux;
	char *p, len[1024];
	char *ret;
	size_t i, j;

	get_line(client, buf, sizeof(buf));
	i = 0; j = 0;

	while (!isspace(buf[j]) && (i < sizeof(method) - 1)) {
		method[i] = buf[j];
		i++; j++;
	}
	method[i] = '\0';

	LOG("%s\n", buf);

	if (!strcasecmp(method, "DELETE")) {
		aux = strchr(buf, '/');
		aux++;
		pk = strtok(aux, " ");
		delete_cmd(pk);
		httpok(client);
	} else if (!strcasecmp(method, "POST")) {
		while (strncmp(buf, "Content-Length:" ,15)) {
			get_line(client, buf, sizeof(buf));
		}

		p = strchr(buf, ' ');

		for (i = 0; *(++p) != '\n'; i++)
			len[i] = *p;
		len[i] = '\0';

		j = (atoi(len)) + 1;
		p = g_new0(char, j + 1);

		while (strlen(buf) > 1) {
			get_line(client, buf, sizeof(buf));
		}

		get_line(client, p, j);
		aux = strchr(p, '=');
		aux++;
		LOG("inserindo: %s\n", aux);
		insert_cmd(aux);
		httpok(client);
	} else if (!strcasecmp(method, "PATCH")) {
		aux = strchr(buf, '/');
		aux++;
		pk = strtok(aux, " ");
		pk = strdup(pk);

		while (strncmp(buf, "Content-Length:" ,15)) {
			get_line(client, buf, sizeof(buf));
		}

		p = strchr(buf, ' ');

		for (i = 0; *(++p) != '\n'; i++)
			len[i] = *p;
		len[i] = '\0';

		j = (atoi(len)) + 1;
		p = g_new0(char, j + 1);

		while (strlen(buf) > 1) {
			get_line(client, buf, sizeof(buf));
		}

		get_line(client, p, j);
		LOG("Update %s: %s\n", pk, p);
		update_cmd_http(pk, p);
		g_free(pk);
		httpok(client);
	} else if (!strcasecmp(method, "GET")) {
		if (strstr(buf, "search")) {
			aux = strstr(buf, "/search/");
			aux += strlen("/search/");
			aux = strtok(aux, " ");
			LOG("Search: %s\n", aux);
			ret = search_cmd_http(aux);

			if (ret)
				aux = g_strdup_printf("HTTP/1.1 200 OK\r\n"
						"Content-Length: %lu\r\n"
						"\r\n"
						"%s", strlen(ret), ret);
			else
				aux = strdup("HTTP/1.1 200 OK\r\n\r\n");

			send(client, aux, strlen(aux), 0);
			g_free(aux);
			g_free(ret);
		} else {
			aux = strchr(buf, '/');
			aux++;
			pk = strtok(aux, " ");
			LOG("Select: %s\n", pk);
			ret = select_cmd_http(pk);
			if (ret)
				aux = g_strdup_printf("HTTP/1.1 200 OK\r\n"
						"Content-Length: %lu\r\n"
						"\r\n"
						"%s", strlen(ret), ret);
			else
				aux = strdup("HTTP/1.1 200 OK\r\n\r\n");

			send(client, aux, strlen(aux), 0);
			g_free(aux);
			g_free(ret);
		}
	} else
		unimplemented(client);
}


/**********************************************************************/
void *run_server(void *arg) {
	int server_sock = -1;
	int client_sock = -1;
	struct sockaddr_in client_name;
	int client_name_len = sizeof(client_name);
	int port = *(int *) arg;

	serverlog = fopen("server.log", "w");

	server_sock = startup(&port);

	if (server_sock < 0) {
		LOG("Startup error\n");
		return NULL;
	}

	LOG("LogicalResource running on port %d\n", port);

	do {
		client_sock = accept(server_sock, (struct sockaddr *)&client_name, (socklen_t *)&client_name_len);

		if (client_sock == -1) {
			LOG("accept");
			return NULL;
		}

		parse_http(client_sock);
		close(client_sock);
	} while (server_running);

	close(server_sock);

	fclose(serverlog);

	return NULL;
}
