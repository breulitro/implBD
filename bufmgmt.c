#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#define DATABLOCK 4096
#define CONTROL 0

typedef struct Buffer {
	int id;
	void *datablock;
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

int main() {
	Buffer *b;
	FILE *fd = fopen(".datafile", "w+");
	if(!fd)
		perror("deu merda");

	if (ftruncate(fileno(fd), DATABLOCK * 100000) < 0)
		perror("ftruncate");

	fclose(fd);

	printf("File created\n");
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
	printf("hit = %d, miss = %d\n", hit, miss);
	bufmgmt_persist();
	return 0;
}
