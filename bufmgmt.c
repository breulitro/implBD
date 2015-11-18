#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <glib.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <ctype.h>
//#include <unistd.h>
//#include <sys/stat.h>
//#include <fcntl.h>
#include "bufmgmt.h"
#include "btree.h"

Buffer frames[256];
int framesLen = 0;
int vitima = 0;
int miss = 0;
int hit = 0;
GList *free_blocks = NULL;
Config conf;


Buffer *get_datablock(int id) {
	int i;
	FILE *fd;

	DBG("%s(%d)\n", __func__, id);
	// Caso esteja nos frames (cache hit)
	for (i = 0; i < framesLen; i++) {
		if (frames[i].id == id) {
			frames[i].used = 1;
			hit++;

			DBG("Cache hit!\n");

			return &frames[i];
		}
	}
	// Cache miss
	miss++;
	DBG("Cache miss\n");

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

		DBG("Reading datablock(%d) @ %ld\n", id, ftell(fd));
		if (fread(frames[framesLen].datablock, 1, DATABLOCK, fd) < DATABLOCK) {
			perror("fudeu"), printf("Erro lendo datablock %d\n", id), exit(1);
		}
		fclose(fd);

		return &frames[framesLen++];
	}

	// Caso framebuffer estiver cheio
	DBG("frames cheio\n");

	while (frames[vitima].used)
		frames[vitima++].used = 0;

	DBG("Vitima é o frame %d - id = %d\n", vitima, frames[vitima].id);

	// Se buffer tiver sido alterado, salva ele (write back policy)
	if (frames[vitima].dirty) {
		fd = fopen(DATAFILE, "r+");
		assert(!fseek(fd, frames[vitima].id * DATABLOCK, SEEK_SET));
		//printf("writing on %zd\n", ftell(fd));

		if (fwrite(frames[vitima].datablock, 1, DATABLOCK, fd) < DATABLOCK)
			printf("Erro salvando datablock %d\n", frames[vitima].id), perror("qq deu?"), exit(1);
		fclose(fd);
		//fd = NULL;
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
			//printf("writing on %zd\n", ftell(fd));

			if (fwrite(frames[i].datablock, DATABLOCK, 1, fd) != 1)
				printf("Erro salvando datablock %d\n", frames[i].id), exit(1);

			fclose(fd);
		}
	}
}

void create_database() {
	FILE *fd;

	fd = fopen(DATAFILE, "w");
	if (ftruncate(fileno(fd), FILESIZE) < 0)
		perror("ftruncate"), exit(1);
	fclose(fd);

	fd = fopen(DATAFILE, "r+");
	if (!fd)
		perror("Criando datafile"), exit(1);

	// Escrevendo configuração
	bzero(&conf, sizeof(Config));
	conf.nextpk = 1;

	// Seta como usado os buffers iniciais utilizados para a configuração
	SET_USED(conf.bitmap, 0);
	SET_USED(conf.bitmap, 1);
	SET_USED(conf.bitmap, 2);

	fwrite(&conf, sizeof(Config), 1, fd);
	fclose(fd);
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
#ifdef DEBUG
		else
			DBG("Datablock %ld ocupado\n", i);
#endif
	}
}

uint16_t get_free_datablock_id() {
	GList *l;
	uint16_t i;
	// Pega o id do primeiro datablock livre da lista de datablocks livres
	l = g_list_first(free_blocks);
	if (!l) {
		printf("Não tem mais datablocks livres\n");
		return 0;
	}

	i = (size_t)l->data;
	// Remove o id do dadablock da lista
	free_blocks = g_list_delete_link(free_blocks, l);
	// Seta o datablock como utilizado
	SET_USED(conf.bitmap, i);

	return i;
}

Buffer *get_insertable_datablock() {
	Buffer *b;
	DBHeader *dbh;
	GList *id;

	// Caso ainda não haja um datablock inicial para a tabela, aloca o primeiro
	// datablock livre..
	if (!conf.table) {
		conf.table = get_free_datablock_id();
		if (!conf.table)
			exit(1);

		DBG("new table @ datablock(%d)\n", conf.table);
	}

	b = get_datablock(conf.table);
	dbh = DBH(b->datablock);

	if (!dbh->free)
		if (!dbh->full)
			dbh->free = DATABLOCK - sizeof(DBHeader) - 1;

	DBG("To te pegando um datablock com %d bytes livres\n", dbh->free);

	DBG("db(%d)->free(%d)\n", b->id, dbh->free);
	while (dbh->free <  sizeof(EntryHeader)) {
		printf("entrou no while\n");
		sleep(1);
		DBG("dbh->free(%d) < sizeof(EntryHeader)(%lu)\n", dbh->free, sizeof(EntryHeader));
		DBG("next(%d)\n", dbh->next);
		if (dbh->next) {
			DBG("Ja existe proximo datablock\n");
			b = get_datablock(dbh->next);
		} else {
			DBG("Nao existe proximo datablock\n");

			b->dirty = 1;
			dbh->next = get_free_datablock_id();
			b = get_datablock(dbh->next);

			dbh = DBH(b->datablock);
			if (!dbh->free) 
				if (!dbh->full)
					dbh->free = DATABLOCK - sizeof(DBHeader) - 1;
			DBG("db(%d)->free(%d)\n", b->id, dbh->free);
		}
		dbh = DBH(b->datablock);
		DBG("db(%d)->free(%d)\n", b->id, dbh->free);
	}
	DBG("db(%d)->free(%d)\n", b->id, dbh->free);

	return b;
}


RowId insert(char *json, char chained, int pk) {
	int len;
	RowId rowid;
	Buffer *b;
	DBHeader *dbh;
	EntryHeader *eh;

	b = get_insertable_datablock();
	if (!b) {
		printf("Acabaram-se os datablocks");
		return (RowId){0,0};
	}

	dbh = DBH(b->datablock);
	DBG("insertable datablock with %d bytes, json with %lu\n bytes", dbh->free, strlen(json));

	DBG("Chegando pra inserir: %s\n", json);
	len = strlen(json);
	dbh = DBH(b->datablock);
	dbh->header_len++;
	dbh->free = dbh->free - sizeof(EntryHeader);
	DBG("dbh->free(%d)\n", dbh->free);
	eh = EH(dbh, dbh->header_len);
	eh->init = dbh->next_init;
	DBG("init = %d\n", eh->init);
	// Se for um chained, seta a pk pra zero
	eh->pk = chained ? 0 : pk ? pk : conf.nextpk++;
	DBG("pk = %d\n", eh->pk);

	// A partir deste ponto, len passa a ser uma flag pra sinalizar se
	// o json coube inteiro neste datablock
	if (len > dbh->free) {
		DBG("Vai encadear\n");
		eh->offset = dbh->free; //FIXME: Ta errado botar esse -1...
		len = 1;
		dbh->full = 1;
	} else {
		DBG("NÃO Vai encadear\n");
		eh->offset = len;
		len = 0;
	}
	//dbh->next_init += eh->offset;
	dbh->next_init = dbh->next_init + eh->offset;

	//DBG("free(%d), offset(%d), header(%ld) | free - offset - header = %ld\n",
	//		dbh->free, eh->offset, sizeof(EntryHeader), dbh->free - eh->offset - sizeof(EntryHeader));

	DBG("copiando para a posição %ld do buffer\n", &b->datablock[eh->init] - b->datablock);
	DBG("writing %d bytes @ %d\n", eh->offset, eh->init);
	DBG("antes pk = %d\n", eh->pk);
	DBG("pk @ %ld\n", (char *)&eh->pk - b->datablock);
	DBG("eh @ %ld\n", (char *)eh - b->datablock);
	memcpy(&b->datablock[eh->init], json, eh->offset);
	DBG("depois pk = %d\n", eh->pk);

	//dbh->free -= eh->offset - sizeof(EntryHeader);
	DBG("free(%d) - offset(%d) = %d\n", dbh->free, eh->offset, dbh->free - eh->offset);
	dbh->free = dbh->free - eh->offset;
	if (dbh->free == 0)
		dbh->full = 1;
	DBG("free(%d)\n", dbh->free);

	if (len) {
		DBG("Encadeando\n");
		DBG("antes pk = %d\n", eh->pk);
		rowid = insert(&json[eh->offset], 1, 0);
		DBG("depois pk = %d\n", eh->pk);
		DBG("ChainedRow @ %d:%d\n", rowid.id, rowid.row);
		if ( *(int *) &rowid == 0L) {
			DBG("Não consegui encadear, desfazendo operação\n");
			// Roll-back, desfaz o insert que não coube nos datablocks disponíveis
			dbh->header_len--;
			return rowid;
		}
		eh->next = rowid;
	} else {
		eh->next = (RowId){0,0};
	}

	b->dirty = 1;
	// Se não for um chained row, insere ele na lista
	if (eh->pk)
		btree_insert(eh->pk, dbh->header_len, b->id);

	DBG("Row(%d) @ %d:%d\n", eh->pk, b->id, dbh->header_len);
	return (RowId){dbh->header_len, b->id};
}

void insert_with_id(int pk, char *json) {
	RowId rowid;
	rowid = insert(json, 0, pk);
	printf("inserted @ RowId(%d:%d)\n", rowid.id, rowid.row);
	btree_update(pk, rowid);
}

void insert_cmd(char *params) {
	// "params" deve conter o json a ser inserido
	// NÃO É FEITA VALIDAÇÃO DO DOCUMENTO JSON!
	int len;
	Buffer *b;
	DBHeader *dbh;
	EntryHeader *eh;
	RowId rowid;

	if (!params) {
		printf("insert <json>\n");
		return;
	}

	len = strlen(params);
	if (!len) {
		printf("Documento vazio.\n");
		return;
	}

	rowid = insert(params, 0, 0);
	printf("inserted @ RowId(%d:%d)\n", rowid.id, rowid.row);
}

void _select(RowId rowid, char **buf, int len) {
	Buffer *b;
	DBHeader *dbh;
	EntryHeader *eh;
	char *baux;

	b = get_datablock(rowid.id);
	dbh = DBH(b->datablock);
	eh = EH(dbh, rowid.row);
	baux = malloc(eh->offset + 1);
	memcpy(baux, &b->datablock[eh->init], eh->offset);
	baux[eh->offset] = 0;
	*buf = strcat(*buf, baux);
	DBG("buf = %s\n", *buf);
	if (*(int *)&eh->next) {
		DBG("select denovo\n");
		_select(eh->next, buf, len + eh->offset);
	}
}

char *select_cmd_http(char *params) {
	int pk;
	Buffer *b;
	char *buf, *baux;
	DBHeader *dbh;
	EntryHeader *eh;
	RowId r;
	char *ret;

	if(!params) {
		printf("select <id>\n");
		return NULL;
	}

	pk = atoi(params);
	r = btree_get(pk);
	if (!r.id && !r.row) {
		printf("Arquivo não existe\n");
		return NULL;
	}

	DBG("%d @ %d:%d\n", pk, r.id, r.row);
	b = get_datablock(r.id);
	dbh = DBH(b->datablock);
	eh = EH(dbh, r.row);

	if (!eh->pk) {
		printf("Documento não encontrado\n");
		return NULL;
	}

	buf = malloc(eh->offset + 1);
	memcpy(buf, &b->datablock[eh->init], eh->offset);
	buf[eh->offset] = 0;

	printf("primeiro copy feito\n");
	if (*(int *)&eh->next != 0)
		_select(eh->next, &buf, eh->offset);


	ret = g_strdup_printf("%d | %s\n", eh->pk, buf);
	free(buf);

	return ret;
}

void select_cmd(char *params) {
	int pk;
	Buffer *b;
	char *buf, *baux;
	DBHeader *dbh;
	EntryHeader *eh;
	RowId r;

	if(!params) {
		printf("select <id>\n");
		return;
	}

	pk = atoi(params);
	r = btree_get(pk);
	if (!r.id && !r.row) {
		printf("Arquivo não existe\n");
		return;
	}

	DBG("%d @ %d:%d\n", pk, r.id, r.row);
	b = get_datablock(r.id);
	dbh = DBH(b->datablock);
	eh = EH(dbh, r.row);

	if (!eh->pk) {
		printf("Documento não encontrado\n");
		return;
	}

	buf = malloc(eh->offset + 1);
	memcpy(buf, &b->datablock[eh->init], eh->offset);
	buf[eh->offset] = 0;

	printf("primeiro copy feito\n");
	if (*(int *)&eh->next != 0)
		_select(eh->next, &buf, eh->offset);


	printf("%d | %s\n", eh->pk, buf);
	free(buf);
}

char *get_entry(int pk) {
	Buffer *b;
	char *buf, *baux;
	DBHeader *dbh;
	EntryHeader *eh;
	RowId r;

	r = btree_get(pk);
	if (!r.id && !r.row) {
		printf("Arquivo não existe\n");
		return NULL;
	}

	b = get_datablock(r.id);
	dbh = DBH(b->datablock);
	eh = EH(dbh, r.row);

	if (!eh->pk) {
		printf("Documento não encontrado\n");
		return NULL;
	}

	buf = malloc(eh->offset + 1);
	memcpy(buf, &b->datablock[eh->init], eh->offset);
	buf[eh->offset] = 0;

	printf("primeiro copy feito\n");
	if (*(int *)&eh->next != 0)
		_select(eh->next, &buf, eh->offset);


	return buf;
}

void free_table_entry(gpointer data) {
	TableEntry *te = data;

	g_free(te->json);
	g_free(te);
}

char *search_cmd_http(char *params) {
	Buffer *b;
	DBHeader *dbh;
	EntryHeader *eh;
	TableEntry *te;
	GList *x, *l = NULL;
	uint16_t i;
	char *json;
	char *ret = NULL;
	char *aux, *aux2;

	if (!conf.table)
		return NULL;

	b = get_datablock(conf.table);
	do {
		dbh = DBH(b->datablock);
		for (i = 1; i <= dbh->header_len; i++) {
			eh = EH(dbh, i);

			//Caso o documento tenha sido deletado, ignora a EntryHeader
			if (!eh->pk)
				continue;

			json = get_entry(eh->pk);
			if (!json)
				continue;

			if (strstr(json, params)) {
				te = malloc(sizeof(TableEntry));
				te->json = json;
				te->pk = eh->pk;
				l = g_list_append(l, te);
			} else
				g_free(json);
		}

		if (dbh->next)
			b = get_datablock(dbh->next);
		else
			b = NULL;

	} while(b);

	for (x = g_list_first(l); x; x = x->next) {
		te = x->data;
		aux = g_strdup_printf("%d | %s\n", te->pk, te->json);
		if (!ret)
			ret = aux;
		else {
			aux2 = ret;
			ret = g_strconcat(ret, aux, NULL);
			g_free(aux2);
		}
	}

	g_list_free_full(l, free_table_entry);

	return ret;
}

void search_cmd(char *params) {
	Buffer *b;
	DBHeader *dbh;
	EntryHeader *eh;
	TableEntry *te;
	GList *x, *l = NULL;
	uint16_t i;
	char *json;

	if (!conf.table)
		return;

	b = get_datablock(conf.table);
	do {
		dbh = DBH(b->datablock);
		for (i = 1; i <= dbh->header_len; i++) {
			eh = EH(dbh, i);

			//Caso o documento tenha sido deletado, ignora a EntryHeader
			if (!eh->pk)
				continue;

			json = get_entry(eh->pk);
			if (!json)
				continue;

			if (strstr(json, params)) {
				te = malloc(sizeof(TableEntry));
				te->json = json;
				te->pk = eh->pk;
				l = g_list_append(l, te);
			} else
				g_free(json);
		}

		if (dbh->next)
			b = get_datablock(dbh->next);
		else
			b = NULL;

	} while(b);


	for (x = g_list_first(l); x; x = x->next) {
		te = x->data;
		printf("%d | %s\n", te->pk, te->json);
	}

	printf("%d documentos\n", g_list_length(l));
	g_list_free_full(l, free_table_entry);
}

void delete(char *datablock, uint16_t row) {
	Buffer *b;
	DBHeader *dbh;
	EntryHeader *eh, *ehaux;

	dbh = DBH(datablock);
	eh = EH(dbh, row);

	// Como não da pra mudar o RowId, o datablock fica com um EntryHeader queimado
	eh->pk = 0; // pk começa em 1, 0 indica que o EntryHeader é inválido
	dbh->free = dbh->free + eh->offset;
	if (row < dbh->header_len - 1) {
		// Caso não seja o último row do datablock, desfragmentamos
		ehaux = EH(dbh, row + 1);
		memcpy(&datablock[eh->init], &datablock[ehaux->init], dbh->next_init - ehaux->init);
		ehaux->init = eh->init;
		dbh->next_init = dbh->next_init - (dbh->next_init - ehaux->init);
		// ehaux->offset continua o mesmo
	} else 
		dbh->next_init = dbh->next_init - (dbh->next_init - eh->init);

	if (*(int *)&eh->next != 0) {
		DBG("Deletando chainedrow\n");
		b = get_datablock(eh->next.id);
		delete(b->datablock, eh->next.row);
		b->dirty = 1;
	}
}

void delete_cmd(char *params) {
	Buffer *b;
	int pk;
	RowId r;

	if (!params || strlen(params) == 0) {
		printf("delete <id>\n");
		return;
	}

	pk = atoi(params);
	r = btree_get(pk);
	if (!r.id && !r.row) {
		printf("Arquivo não existe\n");
		return;
	}

	b = get_datablock(r.id);
	b->dirty = 1;
	delete(b->datablock, r.row);

	btree_delete(pk);
}

char delete_without_btree(char *params) {
	Buffer *b;
	int pk;
	RowId r;

	if (!params || strlen(params) == 0) {
		printf("delete <id>\n");
		return 0;
	}

	pk = atoi(params);
	r = btree_get(pk);
	if (!r.id && !r.row) {
		printf("Arquivo não existe\n");
		return 0;
	}

	b = get_datablock(r.id);
	b->dirty = 1;
	delete(b->datablock, r.row);

	return 1;
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
			DBG("inserting json(%lu): %s\n", strlen(line), line);
			insert_cmd(line);
		}
	}

	fclose(fp);
}

void update_cmd_http(char *pk, char *json) {
	printf("id = %s, json = %s\n", pk, json);
	if (delete_without_btree(pk))
		insert_with_id(atoi(pk), json);
}

void update_cmd(char *params) {
	char *json;
	char *cid;

	if (!params || strlen(params) == 0) {
		printf("update <id> <json>\n");
		return;
	}

	cid = strtok_r(params, " ", &json);

	if (!json) {
		printf("update <id> <json>\n");
		return;
	}

	printf("id = %s, json = %s\n", cid, json);
	if (delete_without_btree(cid))
		insert_with_id(atoi(cid), json);
}
