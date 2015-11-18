/*
 * O esqueleto deste arquivo é baseado na documentação da biblioteca GNU readline.
 * Disponível em https://cnswww.cns.cwru.edu/php/chet/readline/readline.html
 */

#include <glib.h>
#include <stdio.h>
#include <stdlib.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <string.h>
#include <ctype.h>
#include <pthread.h>
#include <assert.h>

int port;

void help() {
	printf("Comandos disponíveis:\n"
			"\t- insert <json>\n"
			"\t- update <id> <json>\n"
			"\t- search <tag>\n"
			"\t- select <pk>\n"
			"\t- delete <pk>\n"
			"\t- exit | quit\n"
			"\t- help\n");
}
void insert_cli(char *param) {
	char *cmd;

	cmd = g_strdup_printf("curl -X POST 127.0.0.1:%d -d \"json=%s\"", port, param);
	printf("%s\n", cmd);
	system(cmd);
	g_free(cmd);
}

void update_cli(char *param) {
	char *pk, *json, *cmd;
	pk = strtok_r(param, " ", &json);
	cmd = g_strdup_printf("curl -X PATCH 127.0.0.1:%d/%s -d \"%s\"", port, pk, json); 
	printf("%s\n", cmd);
	system(cmd);
	g_free(cmd);
}

void select_cli(char *param) {
	char *cmd;

	cmd = g_strdup_printf("curl -X GET 127.0.0.1:%d/%s", port, param);
	printf("%s\n", cmd);
	system(cmd);
	g_free(cmd);
}

void search_cli(char *param) {
	char *cmd;

	cmd = g_strdup_printf("curl -X GET 127.0.0.1:%d/search/%s", port, param);
	printf("%s\n", cmd);
	system(cmd);
	g_free(cmd);
}

void delete_cli(char *param) {
	char *cmd;

	cmd = g_strdup_printf("curl -X DELETE 127.0.0.1:%d/%s", port, param);
	printf("%s\n", cmd);
	system(cmd);
	g_free(cmd);
}

void parse_cmds(char *full_cmd) {
	char *cmd, *param;

	param = NULL;
	if (!(cmd = strtok_r(full_cmd, " ", &param)))
		return;

	if (!(strcmp(cmd, "insert")))
		insert_cli(param);
	else if (!(strcmp(cmd, "update")))
		update_cli(param);
	else if (!(strcmp(cmd, "select")))
		select_cli(param);
	else if (!(strcmp(cmd, "search")))
		search_cli(param);
	else if (!(strcmp(cmd, "delete")))
		delete_cli(param);
	else if (!(strcmp(cmd, "help")))
		help();
	else
		printf("cmd unknown.\n");
}

char *cmd[] = {"insert", "select", "search", "delete", "update", "help", "exit", "quit", NULL};

char* cmd_generator(const char *text, int state) {
	static int list_index, len;
	char *name;

	if (!state) {
		list_index = 0;
		len = strlen (text);
	}

	while ((name = cmd[list_index])) {
		list_index++;

		if (strncmp (name, text, len) == 0)
			return (strdup(name));
	}

	return ((char *)NULL);

}

static char **cmd_completion(const char *text, int start, int end) {
	char **matches;

	matches = (char **)NULL;

	if (start == 0)
		matches = rl_completion_matches ((char*)text, &cmd_generator);

	return (matches);

}

char *trim(char *str) {
	char *end;

	// Trim leading space
	while(isspace(*str)) str++;

	if(*str == 0)  // All spaces?
		return str;

	// Trim trailing space
	end = str + strlen(str) - 1;
	while(end > str && isspace(*end)) end--;

	// Write new null terminator
	*(end + 1) = 0;

	return str;
}

int main(int argc, char *argv[]) {
	char *cmd, *hist, *aux;
	char prompt[] = "sgbd-client> ";

	if (argc != 2) {
		printf("usage: %s <port>\n", argv[0]);
		exit(1);
	}

	port = atoi(argv[1]);

	printf("Pontifícia Universidade Católica do Rio Grande do Sul\n"
			"4641H-04 - Implementação de Banco de Dados - T(128) - 2015/2\n"
			"Trabalho: Mini Simulador de Sistema de Gestão de Metadados\n"
			"Professor: Eduardo Henrique Pereira de Arruda\n"
			"Aluno: Benito Oswaldo João Romeo Luiz Michelon e Silva\n\n");

	printf("Digite \"help\" ou <tab><tab> para listar os comandos disponíveis.\n\n");
	// Command Line Interface code
	rl_attempted_completion_function = cmd_completion;
	do {
		cmd = readline(prompt);
		rl_bind_key('\t',rl_complete);

		aux = cmd;
		aux = trim(aux);
		if (!strcmp(aux, "exit") || !strcmp(aux, "quit")){
			break;
		}

		hist = strdup(cmd);
		parse_cmds(cmd);
		free(cmd);
		if (hist && *hist)
			add_history(hist);
		free(hist);
	} while (1);

	return 0;
}
