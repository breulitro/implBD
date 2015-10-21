#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <glib.h>

#define FILESIZE 268435456 // 256 * 1024 * 1024
#define DATABLOCK 4096
#define CONTROLSIZE DATABLOCK * 3
#define DATAFILE ".datafile"

#define IS_USED(var, n) (var[n / 8] & (1 << (n % 8)))
#define SET_USED(var, n) var[(n) / 8] |= (1 << ((n) % 8))

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

	//printf("%s(%d)\n", __func__, id);
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
	//printf("Cache miss\n");

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
	int next;
	int prev;
} DBHeader;

typedef struct {
	short row;
	short id;
	int next;
} EntryHeader;

typedef struct {
	unsigned char bitmap[FILESIZE / DATABLOCK / 8];
	int root;
	int table;
} Config;

Config conf;

void persist() {
	int i;
	FILE *fd = fopen(DATAFILE, "r+");

	fwrite (&conf, sizeof(Config), 1, fd);

	for (i = 0; i < framesLen; i++) {
		if (frames[i].dirty) {
			fseek(fd, CONTROLSIZE + i * DATABLOCK, SEEK_SET);
//			printf("writing on %zd\n", ftell(fd));
			if (fwrite(frames[i].datablock, 1, DATABLOCK, fd) < DATABLOCK)
				printf("Erro salvando datablock %d\n", frames[i].id), exit(1);
		}
	}

	fclose(fd);
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

	for (i = 3; i < (FILESIZE / DATABLOCK / 8); i++) {
		if (!IS_USED(conf.bitmap, i))
			free_blocks = g_list_append(free_blocks, i);
		else
			printf("Datablock %ld ocupado\n", i);
	}
}

int main() {
	Buffer *b;
	DBHeader *dbh;
	FILE *fd = fopen(DATAFILE, "r");
	if (!fd) {
		printf("First run\n");
		create_database();
	}
	if (fd)
		fclose(fd);

	init_database();

	printf("temos %d datablocks livres\n", g_list_length(free_blocks));

	for (int i = 0; i < 256; i++) {
		b = get_datablock(i);
		sprintf(b->datablock, "{Object:%d, services: [svc1, svc2]}", i);
		//TODO: inserir no datablock
	//	insert(b->datablock, "foostringjson");
		b->dirty = 1;
	}

	printf("framesLen = %d\n", framesLen);
	b = get_datablock(12);
	b = get_datablock(55);
	printf("[select] %s\n", b->datablock);
	dbh = &b->datablock[DATABLOCK - 1] - sizeof(DBHeader);
	printf("header_len = %d\n", dbh->header_len);
	printf("hit = %d, miss = %d\n", hit, miss);

	SET_USED(conf.bitmap, 666);
	persist();

	b = get_datablock(999);
	dbh = &b->datablock[DATABLOCK - 1] - sizeof(DBHeader);
	printf("header_len = %d\n", dbh->header_len);
	b = get_datablock(0);
	printf("[select] %s\n", b->datablock);
	fd = fopen(DATAFILE, "r");
	fread(&conf, sizeof(Config), 1, fd);
	printf("root = %d\n", conf.root);
	fclose(fd);
	return 0;
}
