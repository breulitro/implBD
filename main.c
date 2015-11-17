/*
 * O esqueleto deste arquivo é baseado na documentação da biblioteca GNU readline.
 * Disponível em https://cnswww.cns.cwru.edu/php/chet/readline/readline.html
 */

#include <glib.h>
#include <stdio.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <string.h>
#include <ctype.h>

#include "bufmgmt.h"
#include "btree.h"

void help() {
	printf("Comandos disponiveis:\n"
			"\t- insert <json>\n"
			"\t- update <id> <json>\n"
			"\t- search <tag>\n"
			"\t- select <pk>\n"
			"\t- delete <pk>\n"
			"\t- load <file>\n"
			"\t- persist\n"
			"\t- exit\n"
			"\t- quit\n"
			"\t- help\n");
}

void parse_cmds(char *full_cmd) {
	char *cmd, *param;

	param = NULL;
	if (!(cmd = strtok_r(full_cmd, " ", &param)))
		return;

	if (!(strcmp(cmd, "insert")))
		insert_cmd(param);
	else if (!(strcmp(cmd, "update")))
		update_cmd(param);
	else if (!(strcmp(cmd, "select")))
		select_cmd(param);
	else if (!(strcmp(cmd, "search")))
		search_cmd(param);
	else if (!(strcmp(cmd, "delete")))
		delete_cmd(param);
	else if (!(strcmp(cmd, "load")))
		load_cmd(param);
	else if (!(strcmp(cmd, "btreedump")))
		btree_dump();
	else if (!(strcmp(cmd, "persist"))) {
		persist();
		framesLen = 0;
	} else if (!(strcmp(cmd, "help")))
		help();
	else
		printf("cmd unknown.\n");
}

char *cmd[] = {"insert", "select", "search", "delete", "load", "persist", "help", "exit", "quit", "btreedump", "update", NULL};

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

void _atexit() {
	clear_history();

	persist();

	for (int i = 0; i < framesLen; i++) {
		free(frames[i].datablock);
	}

	clear_history();

	g_list_free(free_blocks);

	printf("hit = %d, miss = %d\n", hit, miss);
}

int main() {
	char *cmd, *hist, *aux;
	char prompt[] = "sgbd> ";
	FILE *fd;

	printf("Pontifícia Universidade Católica do Rio Grande do Sul\n"
			"4641H-04 - Implementação de Banco de Dados - T(128) - 2015/2\n"
			"Trabalho: Mini Simulador de Sistema de Gestão de Metadados\n"
			"Professor: Eduardo Henrique Pereira de Arruda\n"
			"Aluno: Benito Oswaldo João Romeo Luiz Michelon e Silva\n\n");

	// Testa para ver se já existe DATAFILE
	if ((fd = fopen(DATAFILE, "r")) == NULL) {
		printf("Criando arquivo \"%s\"...", DATAFILE);
		fflush(stdout);
		create_database();
		printf("\nArquivo \"%s\" criado.\n", DATAFILE);
	} else {
		fclose(fd);
	}

	atexit(_atexit);
	// Inicializa as estruturas de controle do programa.
	init_database();
	DBG("DBG: temos %d datablocks livres\n", g_list_length(free_blocks));

	printf("Digite \"help\" ou <tab><tab> para listar os comandos disponíveis.\n\n");
#if 0
	btree_insert(1, 1, 3);
	btree_insert(2, 2, 3);
	btree_insert(3, 3, 3);
	btree_insert(4, 4, 3);
	btree_insert(5, 5, 3);
	btree_dump();
	btree_insert(6, 6, 3);
	btree_dump();
	btree_insert(7, 6, 3);
	btree_dump();
	btree_insert(8, 6, 3);
	btree_dump();
	btree_insert(9, 6, 3);
	btree_dump();
	btree_insert(10, 6, 3);
	btree_dump();
	btree_insert(11, 6, 3);
	btree_dump();
	btree_insert(12, 6, 3);
	btree_dump();
	DBG("######################\n");
	btree_insert(13, 6, 3);
	DBG("######################\n");
	btree_dump();
	return 0;
#endif

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
