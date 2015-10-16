#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#define FILESIZE 268435456 // 256 * 1024 * 1024
#define DATABLOCK 4096
#define CONTROL DATABLOCK * 2

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

Buffer *get_datablock(int id) {
	int i;

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
	FILE *fd = fopen(".datafile", "r");
	if (!fd)
		printf("Erro ao abrir .datafile\n"), exit(1);

	if (framesLen < 256) {
		printf("Frames ainda nao ta cheio\n");
		frames[framesLen].datablock = malloc(DATABLOCK);
		frames[framesLen].id = id;
		frames[framesLen].dirty = 0;
		frames[framesLen].used = 0;
		if(fseek(fd, CONTROL + id * DATABLOCK, SEEK_SET) < 0)
			perror("fseek");
		if(fread(frames[framesLen].datablock, DATABLOCK, 1, fd) < 1)
			perror("fudeu"), printf("Erro lendo datablock %d\n", id), exit(1);

		fclose(fd);
		return &frames[framesLen++];
	}

	// Caso framebuffer estiver cheio
	printf("frames cheio\n");
	while (frames[vitima].used)
		frames[vitima++].used = 0;

	// Se buffer tiver sido alterado, salva ele (write back policy)
	if (frames[vitima].dirty) {
		fseek(fd, CONTROL + frames[vitima].id * DATABLOCK, SEEK_SET);
		if (fwrite(frames[vitima].datablock, DATABLOCK, 1, fd) < DATABLOCK)
			printf("Erro salvando datablock %d\n", frames[vitima].id), exit(1);
	}

	// Lê o buffer do arquivo
	fseek(fd, CONTROL + id * DATABLOCK, SEEK_SET);
	if(fread(frames[vitima].datablock, 1, DATABLOCK, fd) < DATABLOCK)
		printf("Erro lendo datablock %d\n", id), exit(1);

	// Configura estrutura de controle do datablock (Buffer)
	frames[vitima].id = id;
	frames[vitima].dirty = 0;
	frames[vitima].used = 1;

	fclose(fd);
	return &frames[vitima++];
};

void bufmgmt_persist() {
	int i;
	FILE *fd = fopen(".datafile", "w");

	for (i = 0; i < framesLen; i++) {
		if (frames[i].dirty) {
			fseek(fd, CONTROL + i * DATABLOCK, SEEK_SET);
			if (fwrite(frames[i].datablock, 1, DATABLOCK, fd) < DATABLOCK)
				printf("Erro salvando datablock %d\n", frames[i].id), exit(1);
		}
	}

	fclose(fd);
}

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
	char bitmap[268435456 / DATABLOCK / 8];
	int root;
	int table;
} Config;

void init_database() {
	FILE *fd;
	DBHeader *dbh;
	char buf[DATABLOCK];
	Config *conf;

	fd = fopen(".datafile", "w");
	if (ftruncate(fileno(fd), FILESIZE) < 0)
		perror("ftruncate");
	fclose(fd);

	fd = fopen(".datafile", "w");
	if (!fd)
		perror("Criando datafile"), exit(1);
/*
	// Escrevendo configuração
	conf = malloc(sizeof(Config));
	bzero(conf, sizeof(Config));
	conf->root = 666;
	printf("rooot = %d\n", conf->root);
	fwrite(conf, sizeof(Config), 1, fd);
*/
	int val = 666;
	fwrite(&val, 1, sizeof(int), fd);
	puts("asdfasdfasdf");

	fseek(fd, DATABLOCK * 2, SEEK_SET);
	for (int i = 2; i < FILESIZE / DATABLOCK; i++) {
		dbh = &buf[DATABLOCK - 1] - sizeof(DBHeader);
		dbh->header_len = 666;
		dbh->free = DATABLOCK - sizeof(DBHeader) - dbh->header_len * sizeof(EntryHeader);
		fwrite(buf, 1, DATABLOCK, fd);
	}

	printf("datafile created\n");
	fclose(fd);
}

int main() {
	Buffer *b;
	DBHeader *dbh;
	Config *conf;
	FILE *fd = fopen(".datafile", "r");
	if (!fd) {
		printf("First run\n");
		init_database();
	}

	fclose(fd);

	for (int i = 0; i < 256; i++) {
		b = get_datablock(i);
		sprintf(b->datablock, "{Object:%d, services: [svc1, svc2]}", i);
		printf("[insert] %s\n", b->datablock);
		//TODO: inserir no datablock
		b->dirty = 1;
	}

	printf("framesLen = %d\n", framesLen);
	b = get_datablock(12);
	b = get_datablock(55);
	printf("[select] %s\n", b->datablock);
	dbh = &b->datablock[DATABLOCK - 1] - sizeof(DBHeader);
	printf("header_len = %d\n", dbh->header_len);
	printf("hit = %d, miss = %d\n", hit, miss);
	bufmgmt_persist();

	fd = fopen(".datafile", "r");
	fseek(fd, 0, SEEK_SET);
	int foo;
	fread(&foo, 1, sizeof(int), fd);
	printf("root = %d\n", foo);
	fclose(fd);
	return 0;
}
