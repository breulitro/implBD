#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <glib.h>
#include <readline/readline.h>
#include <readline/history.h>

#define FILESIZE 268435456 // 256 * 1024 * 1024
#define DATABLOCK 4096
#define CONTROLSIZE DATABLOCK * 3
#define DATAFILE ".datafile"

#define IS_USED(bmap, n) (bmap[n / 8] & (1 << (n % 8)))
#define SET_USED(bmap, n) bmap[(n) / 8] |= (1 << ((n) % 8))

#define DBH(buf) ((DBHeader *) (((char *)&(buf)[DATABLOCK - 1]) - sizeof(DBHeader)));
#define EH(dbh, n) ((EntryHeader *) ((char *)(dbh) - sizeof(EntryHeader) * (n)))

typedef struct Buffer {
	int id;
	char *datablock;
	char dirty;
	char used;
} Buffer;

Buffer frames[256];
int framesLen = 0;
int vitima = 0;
int miss = 0;
int hit = 0;
GList *free_blocks = NULL;

Buffer *get_datablock(int id) {
	int i;
	FILE *fd;

	printf("%s(%d)\n", __func__, id);
	// Caso esteja nos frames (cache hit)
	for (i = 0; i < framesLen; i++) {
		if (frames[i].id == id) {
			frames[i].used = 1;
			hit++;

			printf("Cache hit!\n");
			return &frames[i];
		}
	}
	// Cache miss
	miss++;
	printf("Cache miss\n");

	// Caso NÃO esteja nos frames e o framebuffer ainda não estiver cheio
	if (framesLen < 256) {
		fd = fopen(DATAFILE, "r");
		if (!fd)
			printf("Erro ao abrir .datafile\n"), exit(1);

		//printf("Frames ainda nao ta cheio\n");
		frames[framesLen].datablock = malloc(DATABLOCK);
		frames[framesLen].id = id;
		frames[framesLen].dirty = 0;
		frames[framesLen].used = 0;

		if (fseek(fd, CONTROLSIZE + id * DATABLOCK, SEEK_SET) < 0)
			perror("fseek");
		if (fread(frames[framesLen].datablock, 1, DATABLOCK, fd) < DATABLOCK) {
			perror("fudeu"), printf("Erro lendo datablock %d\n", id), exit(1);
		}
		fclose(fd);
		return &frames[framesLen++];
	}

	// Caso framebuffer estiver cheio
	printf("frames cheio\n");
	while (frames[vitima].used)
		frames[vitima++].used = 0;

	printf("Vitima é o frame %d - id = %d\n", vitima, frames[vitima].id);
	// Se buffer tiver sido alterado, salva ele (write back policy)
	if (frames[vitima].dirty) {
		fd = fopen(DATAFILE, "r+");
		assert(!fseek(fd, CONTROLSIZE + frames[vitima].id * DATABLOCK, SEEK_SET));
		printf("writing on %zd\n", ftell(fd));
		if (fwrite(frames[vitima].datablock, 1, DATABLOCK, fd) < DATABLOCK)
			printf("Erro salvando datablock %d\n", frames[vitima].id), perror("qq deu?"), exit(1);
		fclose(fd);
		fd = NULL;
	}

	fd = fopen(DATAFILE, "r");
	assert(fd);
	// Lê o buffer do arquivo
	assert(!fseek(fd, CONTROLSIZE + id * DATABLOCK, SEEK_SET));
	if(fread(frames[vitima].datablock, 1, DATABLOCK, fd) < DATABLOCK)
		printf("Erro lendo datablock %d\n", id), perror("qq deu?"), exit(1);

	// Configura estrutura de controle do datablock (Buffer)
	frames[vitima].id = id;
	frames[vitima].dirty = 0;
	frames[vitima].used = 1;

	fclose(fd);
	return &frames[vitima++];
};

typedef struct {
	int header_len;
	int free;
	int next_init;
	int next;
} DBHeader;

typedef struct {
	unsigned char bitmap[FILESIZE / DATABLOCK / 8];
	int root;
	int table;
	int nextpk;
} Config;

Config conf;

void persist() {
	int i;
	FILE *fd = fopen(DATAFILE, "r+");
	assert(fd);

	fwrite (&conf, sizeof(Config), 1, fd);
	fclose(fd);

	for (i = 0; i < framesLen; i++) {
		if (frames[i].dirty) {
			fd = fopen(DATAFILE, "r+");
			assert(fd);
			fseek(fd, CONTROLSIZE + i * DATABLOCK, SEEK_SET);
			printf("writing on %zd\n", ftell(fd));

			if (fwrite(frames[i].datablock, DATABLOCK, 1, fd) != 1)
				printf("Erro salvando datablock %d\n", frames[i].id), exit(1);

			fclose(fd);
		}

	}
}


void create_database() {
	FILE *fd;
	DBHeader *dbh;
	char buf[DATABLOCK];

	fd = fopen(DATAFILE, "w");
	if (ftruncate(fileno(fd), FILESIZE) < 0)
		perror("ftruncate");
	fclose(fd);

	fd = fopen(DATAFILE, "r+");
	if (!fd)
		perror("Criando datafile"), exit(1);

	// Escrevendo configuração
	bzero(&conf, sizeof(Config));
	// FIXME: Utilizar desse jeito mesmo, acho que é melhor não usar default...
	conf.root = 0;
	conf.table = 1;
	conf.nextpk = 1;

	// Seta como usado os buffers iniciais da BTREE e da TABELA
	SET_USED(conf.bitmap, 0);
	SET_USED(conf.bitmap, 1);
	// Seta como usado os 3 últimos buffers , ja que o buffer inicia em zero
	//e tem um offset de 3 datablocks (CONTROLSIZE).
	SET_USED(conf.bitmap, 2 * DATABLOCK - 1);
	SET_USED(conf.bitmap, 2 * DATABLOCK - 2);
	SET_USED(conf.bitmap, 2 * DATABLOCK - 3);
	fwrite(&conf, sizeof(Config), 1, fd);
	fclose(fd);

	printf("datafile created\n");
}

void init_database() {
	FILE *fd;
	long i;

	fd = fopen(DATAFILE, "r+");
	fread(&conf, sizeof(Config), 1, fd);
	fclose(fd);

	free_blocks = NULL;
	for (i = 0; i < (FILESIZE / DATABLOCK / 8); i++) {
		if (!IS_USED(conf.bitmap, i))
			free_blocks = g_list_append(free_blocks, (gpointer) i);
		else
			printf("Datablock %ld ocupado\n", i);
	}
}

typedef struct {
	int pk;
	short init;
	short offset;
	//int next;
} EntryHeader;

Buffer *get_insertable_datablock(int len) {
	Buffer *b, *baux;
	DBHeader *dbh;
	GList *id;

	if (len > DATABLOCK - sizeof(DBHeader) - sizeof(EntryHeader)) {
		printf("Arquivos que ocupem mais de um datablock não são suportados ainda.\n");
		return NULL;
	}

	b = get_datablock(conf.table);
	dbh = DBH(b->datablock);
	if (!dbh->free)
		dbh->free = DATABLOCK - sizeof(DBHeader);

	while (dbh->free < len + sizeof(DBHeader)) {
		if (dbh->next)
			b = get_datablock(dbh->next);
		else {
			if ((id = g_list_first(free_blocks)) == NULL) {
				printf("Não tem mais datablocks livres\n");
				return NULL;
			}

			b = get_datablock((int) id->data);
			dbh->next = (int) id->data;
			SET_USED(conf.bitmap, (int) id->data);
			free_blocks = g_list_delete_link(free_blocks, id);
		}

		dbh = DBH(b->datablock);
		if (!dbh->free)
			dbh->free = DATABLOCK - sizeof(DBHeader);
	}

	return b;
}

void insert_cmd(char *params) {
	// "params" deve conter o json a ser inserido
	// NÃO É FEITA VALIDAÇÃO DO DOCUMENTO JSON!
	int i, len;
	Buffer *b;
	DBHeader *dbh;
	EntryHeader *eh;
	int f;

	len = strlen(params);
	b = get_insertable_datablock(len);
	dbh = DBH(b->datablock);
	dbh->header_len++;
	eh = (EntryHeader *) dbh - sizeof(EntryHeader) * (dbh->header_len);
	eh = EH(dbh, dbh->header_len);
	assert((char *)dbh - (char *)eh == 8 * dbh->header_len);
	eh->init = dbh->next_init;
	eh->offset = len;
	eh->pk = conf.nextpk++;
	dbh->next_init += eh->offset;
	printf("free(%ld), offset(%ld), header(%ld) | free - offset - header = %ld\n",
			dbh->free, eh->offset, sizeof(EntryHeader), dbh->free - eh->offset - sizeof(EntryHeader));
	//dbh->free -= eh->offset - sizeof(EntryHeader);
	dbh->free = dbh->free - eh->offset - sizeof(EntryHeader);
	f = dbh->free;
	printf("free(%ld)\n", dbh->free);
	assert(&b->datablock[eh->init] - b->datablock < DATABLOCK);
	//*(((char*)&b->datablock[eh->init])) = memcpy((char*)&b->datablock[eh->init], params, len);
	memcpy(&b->datablock[eh->init], params, len);
	assert(f == dbh->free);
	b->dirty = 1;
	// btree_insert(eh->pk, i - 1, b->id))
}

void select_cmd(char *params) {
	printf("TBD\n");
}

typedef struct {
	int pk;
	char *json;
} TableEntry;

void search_cmd(char *params) {
	Buffer *b;
	DBHeader *dbh;
	EntryHeader *eh;
	TableEntry *te;
	GList *x, *l = NULL;
	short i;
	char buf[DATABLOCK];

	b = get_datablock(conf.table);
	do {
		dbh = DBH(b->datablock);
		for (i = 1; i <= dbh->header_len; i++) {
			eh = EH(dbh, i);
			if (strnstr(&b->datablock[eh->init], params, eh->offset)) {
				te = malloc(sizeof(TableEntry));
				te->json = malloc(eh->offset + 1);
				te->json = memcpy(te->json, &b->datablock[eh->init], eh->offset);
				te->json[eh->offset] = 0;
				te->pk = eh->pk;
				l = g_list_append(l, te);
			}
		}

		if (dbh->next)
			b = get_datablock(dbh->next);
		else
			b = NULL;

	} while(b);

	printf("len = %d\n", g_list_length(l));

	for (x = g_list_first(l); x; x = x->next) {
		te = x->data;
		printf("%d - %s\n", te->pk, te->json);
	}
}

void delete_cmd(char *params) {
	printf("TBD\n");
}

void load_cmd(char *params) {
	printf("TBD\n");
}

void help() {
	printf("Comandos disponiveis:\n"
			"\t- insert <json>\n"
			"\t- search <tag>\n"
			"\t- select <pk>\n"
			"\t- delete <pk>\n"
			"\t- help\n");
}

void parse_cmds(char *full_cmd) {
	char *cmd, *param;

	param = NULL;
	if (!(cmd = strtok_r(full_cmd, " ", &param)))
		return;

	if (!(strcmp(cmd, "insert")))
		insert_cmd(param);
	else if (!(strcmp(cmd, "select")))
		select_cmd(param);
	else if (!(strcmp(cmd, "search")))
		search_cmd(param);
	else if (!(strcmp(cmd, "delete")))
		delete_cmd(param);
	else if (!(strcmp(cmd, "load")))
		load_cmd(param);
	else if (!(strcmp(cmd, "persist"))) {
		persist();
		framesLen = 0;
	} else if (!(strcmp(cmd, "help")))
		help();
	else
		printf("cmd unknown.\n");
}

int main() {
	char *cmd, *hist;
	char prompt[] = "sgbd> ";
	Buffer *b;
	DBHeader *dbh;
	FILE *fd;

	// Testa para ver se já existe DATAFILE
	if ((fd = fopen(DATAFILE, "r")) != 0)
		fclose(fd);
	else {
		printf("First run\n");
		create_database();
	}

	// Inicializa as estruturas de controle do programa.
	init_database();
	printf("DBG: temos %d datablocks livres\n", g_list_length(free_blocks));

	do {
		cmd = readline(prompt);
		if (!strcmp(cmd, "exit") || !strcmp(cmd, "quit")){
			break;
		}
		hist = strdup(cmd);
		parse_cmds(cmd);
		free(cmd);
		if (hist && *hist)
			add_history(hist);
	} while (1);

	clear_history();
	persist();

	printf("hit = %d, miss = %d\n", hit, miss);

	return 0;
}
