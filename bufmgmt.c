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
		fd = fopen(DATAFILE, "r+");
		if (!fd)
			printf("Erro ao abrir .datafile\n"), exit(1);

		//printf("Frames ainda nao ta cheio\n");
		frames[framesLen].datablock = malloc(DATABLOCK);
		frames[framesLen].id = id;
		frames[framesLen].dirty = 0;
		frames[framesLen].used = 0;

		if (fseek(fd, id * DATABLOCK, SEEK_SET) < 0)
			perror("fseek");

		printf("Reading @ %ld\n", ftell(fd));
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
		assert(!fseek(fd, frames[vitima].id * DATABLOCK, SEEK_SET));
		printf("writing on %zd\n", ftell(fd));
		if (fwrite(frames[vitima].datablock, 1, DATABLOCK, fd) < DATABLOCK)
			printf("Erro salvando datablock %d\n", frames[vitima].id), perror("qq deu?"), exit(1);
		fclose(fd);
		fd = NULL;
	}

	fd = fopen(DATAFILE, "r+");
	assert(fd);
	// Lê o buffer do arquivo
	assert(!fseek(fd, id * DATABLOCK, SEEK_SET));
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
			fseek(fd, frames[i].id * DATABLOCK, SEEK_SET);
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
	//conf.root = 0;
	//conf.table = 1;
	conf.nextpk = 1;

	// Seta como usado os buffers iniciais utilizados para a configuração
	SET_USED(conf.bitmap, 0);
	SET_USED(conf.bitmap, 1);
	SET_USED(conf.bitmap, 2);

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

	if (!conf.table) {
		id = g_list_first(free_blocks);
		conf.table = (int) id->data;
		free_blocks = g_list_delete_link(free_blocks, id);
		SET_USED(conf.bitmap, conf.table);
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

			b->dirty = 1;
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

typedef struct {
	short row;
	short id;
} RowId;

typedef enum {
	LEAF,
	BRANCH
} BTType;

typedef struct {
	short len;
	short prev;
	short next;
	BTType type;
} BTHeader;

// Tem que setar o tipo de atributo pra __packed__ senão ele bota padding no
// prev e no next pra estrutura ficar uniforme. Isso deixa a estrutura com 12 bytes.
// E não é isso que a gente quer, queremos ela com seus 8 bytes enxutos!
typedef struct {
	short prev;
	int pk;
	short next;
} __attribute__((__packed__)) BTBNode;

typedef struct {
	int pk;
	RowId rowid;
} BTLNode;

// Desconta o Header da BTree + 1 Nodo para permitir a inserção do 2d+1-ésimo elemento
#define BRANCH_D (((DATABLOCK - sizeof(BTHeader) - sizeof(BTBNode)) / (sizeof(BTBNode) - sizeof(short))) / 2)
#define LEAF_D (((DATABLOCK - sizeof(BTHeader) - sizeof(BTLNode)) / sizeof(BTLNode)) / 2)

#define BR(block, i) (BTBNode *) (i ? (block + sizeof(BTHeader) + i * (sizeof(BTBNode) - sizeof(short)) + sizeof(short)) : block + sizeof(BTHeader))
#define LF(block, i) (BTLNode *) (block + sizeof(BTHeader) + i * sizeof(BTLNode))
void btree_insert(int pk, short row, short id) {
	Buffer *b;
	GList *l;
	BTHeader *bth;
	BTBNode *br;
	BTLNode *lf;
	int i;

	printf("BRANCH_D = %lu, LEAF_D = %lu\n", BRANCH_D, LEAF_D);
	printf("inserindo %d @ %d:%d\n", pk, id, row);
	if (!conf.root) {
		l = g_list_first(free_blocks);
		conf.root = (int) l->data;
		free_blocks = g_list_delete_link(free_blocks, l);
		SET_USED(conf.bitmap, conf.root);
	}

	b = get_datablock(conf.root);
	bth = (BTHeader *) b->datablock;
	if (!bth->len)
		bth->type = LEAF;

	for (i = 0; i < bth->len; i++) {
		if (bth->type == LEAF) {
			lf = LF(bth, i);
			if (lf->pk < pk)
				continue;
			// Se cair aqui é pq estamos inserindo no meio
			if (bth->len) {
				// Se não for o primeiro elemento, temos que abrir espaço
				memmove(lf + sizeof(BTLNode), lf, (bth->len - i) * sizeof(BTLNode));
			}

			lf->pk = pk;
			lf->rowid.row = row;
			lf->rowid.id = id;
			bth->len++;
			printf("meio - inserindo leafnode%d\n", bth->len);
			if (bth->len > LEAF_D * 2)
				// Leaf Split
				printf("TBD: Leaf Split\n");
			break;
		} else {
			br = BR(bth, i);
			printf("TBD: Branch insert\n");
		}
	}

	if (i == bth->len) {
		//Se cair aqui é pq estamos inserindo no final
		if (bth->type == LEAF) {
			lf = LF(bth, i);
			lf->pk = pk;
			lf->rowid.row = row;
			lf->rowid.id = id;
			bth->len++;
			printf("inicio - inserindo leafnode%d\n", bth->len);
			if (bth->len > LEAF_D * 2)
				// Leaf Split
				printf("TBD: Leaf Split\n");
		} else {
			br = BR(bth, i);
			printf("TBD: Branch insert\n");
		}
	}

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
	printf("free(%d), offset(%d), header(%ld) | free - offset - header = %ld\n",
			dbh->free, eh->offset, sizeof(EntryHeader), dbh->free - eh->offset - sizeof(EntryHeader));
	//dbh->free -= eh->offset - sizeof(EntryHeader);
	dbh->free = dbh->free - eh->offset - sizeof(EntryHeader);
	f = dbh->free;
	printf("free(%d)\n", dbh->free);
	assert(&b->datablock[eh->init] - b->datablock < DATABLOCK);
	//*(((char*)&b->datablock[eh->init])) = memcpy((char*)&b->datablock[eh->init], params, len);
	memcpy(&b->datablock[eh->init], params, len);
	assert(f == dbh->free);
	b->dirty = 1;

	btree_insert(eh->pk, dbh->header_len - 1, b->id);
}

// WARN: Não é feita verificação se existe realmente o RowId em questão
void select_cmd(char *params) {
	char *brow, *bid;
	short row, id;
	Buffer *b;
	char *buf;
	DBHeader *dbh;
	EntryHeader *eh;

	bid = strtok_r(params, ":", &brow);
	id = atoi(bid);
	row = atoi(brow);

	b = get_datablock(id);
	dbh = DBH(b->datablock);
	eh = EH(dbh, row + 1);// Row começa em 1
	buf = malloc(eh->offset + 1);
	memcpy(buf, &b->datablock[eh->init], eh->offset);
	buf[eh->offset] = 0;

	printf("%d | %s\n", eh->pk, buf);
	free(buf);
}

typedef struct {
	int pk;
	char *json;
} TableEntry;

void free_table_entry(gpointer data) {
	TableEntry *te = data;

	free(te->json);
	free(te);
}

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
		printf("header_len(%d)\n", dbh->header_len);
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

	g_list_free_full(l, free_table_entry);
}

void delete(char *datablock, short row) {
	printf("TBD\n");
}

void delete_cmd(char *params) {
	char *brow, *bid;
	short row, id;
	Buffer *b;

	brow = strtok_r(params, ":", &bid);
	row = atoi(brow);
	id = atoi(bid);

	printf("Row %d, id %d\n", row, id);
	b = get_datablock(id);
	delete(b->datablock, row);
}

void load_cmd(char *params) {
	FILE *fp;
	char *line = NULL;
	size_t linecap = 0;
	ssize_t linelen;

	printf("Oppening %s\n", params);
	fp = fopen(params, "r+");
	if (!fp) {
		perror("Oppening file");
		return;
	}

	while ((linelen = getline(&line, &linecap, fp)) > 0) {
		if (linelen > 1) {
			line[linelen - 1] = 0;
			printf("inserting json: %s\n", line);
			insert_cmd(line);
		}
	}
}

void help() {
	printf("Comandos disponiveis:\n"
			"\t- insert <json>\n"
			"\t- search <tag>\n"
			"\t- select <pk>\n"
			"\t- delete <pk>\n"
			"\t- load <file>\n"
			"\t- persist\n"
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

	for (int i = 0; i < framesLen; i++) {
		free(frames[i].datablock);
	}

	g_list_free(free_blocks);

	printf("hit = %d, miss = %d\n", hit, miss);

	return 0;
}
