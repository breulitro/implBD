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

#define FILESIZE 268435456 // 256 * 1024 * 1024
#define DATABLOCK 4096
#define DATAFILE ".datafile"

#define IS_USED(bmap, n) (bmap[n / 8] & (1 << (n % 8)))
#define SET_USED(bmap, n) bmap[(n) / 8] |= (1 << ((n) % 8))

#define DBH(buf) ((DBHeader *) (((char *)&(buf)[DATABLOCK - 1]) - sizeof(DBHeader)));
#define EH(dbh, n) ((EntryHeader *) ((char *)(dbh) - sizeof(EntryHeader) * (n)))

#ifdef DEBUG
#define DBG(fmt, ...) printf("%s() @ %s +%d: "fmt, __func__, __FILE__, __LINE__, ##__VA_ARGS__)
#else
#define DBG(fmt, ...)
#endif

typedef struct Buffer {
	int id;
	char *datablock;
	char dirty;
	char used;
} Buffer;

typedef struct {
	int header_len;
	int free;
	int full;
	int next_init;
	int next;
} DBHeader;

typedef struct {
	unsigned char bitmap[FILESIZE / DATABLOCK / 8];
	int root;
	int table;
	int nextpk;
} Config;


typedef struct {
	short row;
	short id;
} RowId;

Buffer frames[256];
int framesLen = 0;
int vitima = 0;
int miss = 0;
int hit = 0;
GList *free_blocks = NULL;

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

typedef struct {
	int pk;
	short init;
	short offset;
	RowId next;
} EntryHeader;

Buffer *get_insertable_datablock() {
	Buffer *b;
	DBHeader *dbh;
	GList *id;

	// Caso ainda não haja um datablock inicial para a tabela, inicia com o primeiro
	// datablock livre..
	if (!conf.table) {
		id = g_list_first(free_blocks);
		conf.table = (int) id->data;
		DBG("new table @ datablock(%d)\n", conf.table);
		free_blocks = g_list_delete_link(free_blocks, id);
		SET_USED(conf.bitmap, conf.table);
	}

	b = get_datablock(conf.table);
	dbh = DBH(b->datablock);

	if (!dbh->free)
		if (!dbh->full)
			dbh->free = DATABLOCK - sizeof(DBHeader);

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
			if ((id = g_list_first(free_blocks)) == NULL) {
				printf("Não tem mais datablocks livres\n");
				return NULL;
			}

			b->dirty = 1;
			b = get_datablock((int) id->data);
			dbh->next = (int) id->data;

			dbh = DBH(b->datablock);
			if (!dbh->free) 
				if (!dbh->full)
					dbh->free = DATABLOCK - sizeof(DBHeader);
			DBG("db(%d)->free(%d)\n", b->id, dbh->free);

			SET_USED(conf.bitmap, (int) id->data);
			free_blocks = g_list_delete_link(free_blocks, id);
		}
		dbh = DBH(b->datablock);
		DBG("db(%d)->free(%d)\n", b->id, dbh->free);
	}
	DBG("db(%d)->free(%d)\n", b->id, dbh->free);

	return b;
}

typedef enum {
	LEAF,
	BRANCH
} BTType;

typedef struct {
	short len;
	short prev;
	short next;
	short parent;
	BTType type;
} BTHeader;

// Tem que setar o tipo de atributo pra __packed__ senão ele bota padding no
// prev e no next pra estrutura ficar uniforme. Isso deixa a estrutura com 12 bytes.
// E não é isso que a gente quer, queremos ela com seus 8 bytes enxutos!
typedef struct {
	short menor;
	int pk;
	short maior;
} __attribute__((__packed__)) BTBNode;

typedef struct {
	int pk;
	RowId rowid;
} BTLNode;

#if 0
// Desconta o Header da BTree + 1 Nodo para permitir a inserção do 2d+1-ésimo elemento
#define BRANCH_D (((DATABLOCK - sizeof(BTHeader) - sizeof(BTBNode)) / (sizeof(BTBNode) - sizeof(short))) / 2)
#define LEAF_D (((DATABLOCK - sizeof(BTHeader) - sizeof(BTLNode)) / sizeof(BTLNode)) / 2)
#else
// Para propósito de testes, até estabilizar a BTree+
#define BRANCH_D 2L
#define LEAF_D 2L
#endif

#define BR(block, i) ((BTBNode *) ((i) ? ((char *)(block) + sizeof(BTHeader) + (i) * (sizeof(BTBNode) - sizeof(short)) + sizeof(short)) : (char *)(block) + sizeof(BTHeader)))
#define LF(block, i) ((BTLNode *) ((char *)(block) + (sizeof(BTHeader) + (i) * sizeof(BTLNode))))
void _btree_delete(int pk, int id) {
	Buffer *b;
	BTHeader *bth;
	BTBNode *br;
	BTLNode *lf, *laux = NULL;

	b = get_datablock(id);
	bth = (BTHeader *)b->datablock;
	printf("btree->len = %d\n", bth->len);

	if (bth->type == LEAF) {
		for (int i = 0; i < bth->len; i++) {
			lf = LF(bth, i);
			if (lf->pk == 0)
				continue;

			if (lf->pk == pk) {
				laux = LF(bth, i + 1);
				bth->len = bth->len - 1;
				lf = memcpy(lf, laux, bth->len * sizeof(BTLNode));
				return;
			}
		}
	} else {
		printf("TBD\n");
		return;
		for (int i = 0; i < bth->len; i++) {
			;
		}
	}
	// Se é igual, é pq é o último
	printf("btree->len = %d\n", bth->len);
}

void btree_delete(int pk) {
	_btree_delete(pk, conf.root);
}

RowId btree_leaf_get(short id, int pk) {
	Buffer *b;
	BTHeader *bth;
	BTLNode *lf;
	int i;

	b = get_datablock(id);
	bth = (BTHeader *) b->datablock;

	for (i = 0; i < bth->len; i++) {
		lf = LF(bth, i);
		if (lf->pk == pk)
			return lf->rowid;
	}

	// ERRO: pk não encontrada
	return (RowId){0,0};
}

RowId btree_branch_get(short id, int pk) {
	Buffer *b;
	BTHeader *bth;
	BTBNode *br;
	int i;

	printf("Search na branch\n");
	b = get_datablock(conf.root);
	bth = (BTHeader *) b->datablock;

	for (i = 0; i < bth->len; i++) {
		br = BR(bth, i);
		if (br->pk < pk) {
			continue;
		} else {
			b = get_datablock(br->menor);
			bth = (BTHeader *) b->datablock;
			if (bth->type == LEAF)
				return btree_leaf_get(b->id, pk);
			else
				return btree_branch_get(b->id, pk);
		}
	}

	b = get_datablock(br->maior);
	bth = (BTHeader *) b->datablock;
	if (bth->type == LEAF)
		return btree_leaf_get(b->id, pk);
	else
		return btree_branch_get(b->id, pk);

	// ERRO: não era pra cair aqui...
	return (RowId){0,0};
}

RowId btree_get(int pk) {
	Buffer *b;
	BTHeader *bth;

	b = get_datablock(conf.root);
	bth = (BTHeader *) b->datablock;

	if (bth->type == LEAF)
		return btree_leaf_get(b->id, pk);
	else
		return btree_branch_get(b->id, pk);
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
void btree_dump_leaf(short id, int padding) {
	Buffer *b, *newroot, *newb;
	GList *l;
	BTHeader *bth;
	BTBNode *br;
	BTLNode *lf;
	int i, p;

	b = get_datablock(id);
	bth = (BTHeader *) b->datablock;

	for (p = 0; p < padding; p++)
		printf("\t");

	for (i = 0; i < bth->len; i++) {
		lf = LF(bth, i);
		printf("%d ", lf->pk);
	}

	printf("\n");
}

void _btree_dump(short id, int padding) {
	Buffer *b, *newroot, *newb;
	GList *l;
	BTHeader *bth;
	BTBNode *br;
	BTLNode *lf;
	int i, p;

	b = get_datablock(id);
	bth = (BTHeader *) b->datablock;

	if (bth->type == LEAF)
		btree_dump_leaf(b->id, padding + 1);
	else {
		for (i = 0; i < bth->len; i++) {
			br = BR(bth, i);
			if (!i) {
				_btree_dump(br->menor, padding + 1);
			}
			for (p = 0; p < padding; p++)
				printf("\t");
			printf("%d\n", br->pk);
			_btree_dump(br->maior, padding + 1);
		}
	}
}

void btree_dump() {
	Buffer *b, *newroot, *newb;
	GList *l;
	BTHeader *bth;
	BTBNode *br;
	BTLNode *lf;
	int i;

	if (!conf.root) {
		printf("Btree+ vazia\n");
		return;
	}

	_btree_dump(conf.root, -1);
}

void btree_insert_branch(short id, int pk, short menor, short maior) {
	Buffer *b;
	BTHeader *bth;
	BTBNode *br;

	b = get_datablock(id);
	bth = (BTHeader *) b->datablock;
	br = BR(bth, bth->len);
	bth->len = bth->len + 1;
	br->pk = pk;
	br->menor = menor;
	br->maior = maior;
	b->dirty = 1;
}

void btree_leaf_split(short id) {
	Buffer *b, *newroot, *newb;
	GList *l;
	BTHeader *bth, *nbth, *rbth;
	BTBNode *br;
	BTLNode *lf, *nlf;
	int i;

	printf("Leaf Split\n");
	b = get_datablock(id);
	bth = (BTHeader *) b->datablock;
	b->dirty = 1;

	// Aloca novo nodo folha
	l = g_list_first(free_blocks);
	i = (int)l->data;
	free_blocks = g_list_delete_link(free_blocks, l);
	SET_USED(conf.bitmap, i);
	newb = get_datablock(i);

	// Setup do novo nodo
	newb->id = i;
	newb->dirty = 1;
	nbth = (BTHeader *) newb->datablock;
	nbth->type = LEAF;

	// Posiciona lf na metade
	lf = LF(bth, LEAF_D);
	// Posiciona nlf no começo
	nlf = LF(nbth, 0);
	// Copia da metade em diate do nodo $id para nlf
	memcpy(nlf, lf, sizeof(BTLNode) * (LEAF_D + 1));
	nbth->len = LEAF_D + 1;

	// Apontamento dos simblings
	bth->next = newb->id;
	nbth->prev = b->id;

	if (!bth->parent) {
		//Aloca novo nodo raiz
		l = g_list_first(free_blocks);
		i = (int)l->data;
		free_blocks = g_list_delete_link(free_blocks, l);
		SET_USED(conf.bitmap, i);
		newroot = get_datablock(i);
		newroot->id = i;
		newroot->dirty = 1;
		rbth = (BTHeader *) newroot->datablock;
		rbth->type = BRANCH;
		br = BR(rbth, 0);
		br->pk = nlf->pk;
		br->menor = b->id;
		br->maior = newb->id;
		rbth->len = 1;

		// Apontamento dos parents
		bth->parent = nbth->parent = i;
		conf.root = i;
	} else {
		btree_insert_branch(bth->parent, nlf->pk, b->id, newb->id);
	}


	bth->len = LEAF_D;
	//conf.root = newroot->id;
}

void btree_insert_node(short id, int pk, RowId rowid) {
	Buffer *b;
	GList *l;
	BTHeader *bth;
	BTBNode *br;
	BTLNode *lf;
	int i;

	printf("Inserindo btree no datablock(%d)\n", id);
	b = get_datablock(id);
	bth = (BTHeader *) b->datablock;

	if (bth->type == LEAF) {
		lf = LF(bth, bth->len);
		/*
		printf("bth @ %d\n", (int)((char *)bth - b->datablock) % DATABLOCK);
		assert((int)((char *)bth - b->datablock) % DATABLOCK == 0);
		printf("lf @ %d\n", (int)((char *)lf - (char *)bth) % DATABLOCK);
		assert((int)((char *)lf - (char *)bth) % DATABLOCK == sizeof(BTHeader));
		*/
		lf->pk = pk;
		lf->rowid.row = rowid.row;
		lf->rowid.id = rowid.id;
		bth->len++;
		b->dirty = 1;
		if (bth->len > 2 * LEAF_D)
			btree_leaf_split(b->id);
	} else {
		br = BR(bth, bth->len - 1);
		if (pk < br->pk)
			btree_insert_node(br->menor, pk, rowid);
		else
			btree_insert_node(br->maior, pk, rowid);
	}
}

void btree_insert(int pk, short row, short id) {
	Buffer *b;
	GList *l;
	BTHeader *bth;
	BTBNode *br;
	BTLNode *lf;
	RowId rowid;
	int i;
#pragma GCC diagnostic pop

	DBG("BRANCH_D = %lu, LEAF_D = %lu\n", BRANCH_D, LEAF_D);
	printf("inserindo %d @ %d:%d\n", pk, id, row);

	// Caso não haja um datablock inicial para a BTree+
	if (!conf.root) {
		l = g_list_first(free_blocks);
		conf.root = (int) l->data;
		DBG("new BTree+ root @ datablock(%d)\n", conf.root);
		free_blocks = g_list_delete_link(free_blocks, l);
		SET_USED(conf.bitmap, conf.root);
	}


	b = get_datablock(conf.root);
	bth = (BTHeader *) b->datablock;

	// Caso não tenha nenhuma entrada na BTree é pq éla está vazia, logo é LEAF
	if (!bth->len) {
		bth->type = LEAF;
	}

	rowid.row = row;
	rowid.id = id;
	// Como sabemos que pk é o maior valor existente na BTree+,
	// pois ele é serial, inserimos ele sempre no final do nodo.
	if (bth->type == LEAF) {
		btree_insert_node(b->id, pk, rowid);
	} else {
		DBG("Não é LEAF!\n");
		br = BR(bth, bth->len - 1);
		if (pk < br->pk)
			btree_insert_node(br->menor, pk, rowid);
		else
			btree_insert_node(br->maior, pk, rowid);
	}

#if 0
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

			DBG("meio - inserindo leafnode %d\n", bth->len);

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

			DBG("inicio - inserindo leafnode %d\n", bth->len);

			if (bth->len > LEAF_D * 2)
				// Leaf Split
				printf("TBD: Leaf Split\n");
		} else {
			br = BR(bth, i);
			printf("TBD: Branch insert\n");
		}
	}
#endif
}

RowId insert(char *json, char chained) {
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
	DBG("len = %d\n", len);
	dbh = DBH(b->datablock);
	dbh->header_len++;
	DBG("dbh->free(%d) - sizeof(EntryHeader)(%lu) = %lu\n", dbh->free, sizeof(EntryHeader), dbh->free - sizeof(EntryHeader));
	dbh->free = dbh->free - sizeof(EntryHeader);
	DBG("dbh->free(%d)\n", dbh->free);
	eh = EH(dbh, dbh->header_len);
	eh->init = dbh->next_init;
	DBG("init = %d\n", eh->init);
	DBG("free(%d), offset(%d), header(%ld) | free - offset - header = %ld\n",
			dbh->free, eh->offset, sizeof(EntryHeader), dbh->free - eh->offset - sizeof(EntryHeader));
	// Se for um chained, seta a pk pra zero
	eh->pk = chained ? 0 : conf.nextpk++;
	DBG("pk = %d\n", eh->pk);

	// A partir deste ponto, len passa a ser uma flag pra sinalizar se
	// o json coube inteiro neste datablock
	DBG("free(%d), offset(%d), header(%ld) | free - offset - header = %ld\n",
			dbh->free, eh->offset, sizeof(EntryHeader), dbh->free - eh->offset - sizeof(EntryHeader));
	if (len > dbh->free) {
		DBG("Vai encadear\n");
		eh->offset = dbh->free - 1; //FIXME: Ta errado botar esse -1...
		dbh->free = dbh->free - 1;
		len = 1;
		dbh->full = 1;
	} else {
		DBG("NÃO Vai encadear\n");
		eh->offset = len;
		len = 0;
	}
	//dbh->next_init += eh->offset;
	dbh->next_init = dbh->next_init + eh->offset;

	DBG("free(%d), offset(%d), header(%ld) | free - offset - header = %ld\n",
			dbh->free, eh->offset, sizeof(EntryHeader), dbh->free - eh->offset - sizeof(EntryHeader));

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
		rowid = insert(&json[eh->offset], 1);
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

	rowid = insert(params, 0);
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

// WARN: Não é feita verificação se existe realmente o RowId em questão
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

	b = get_datablock(conf.table);
	do {
		dbh = DBH(b->datablock);
		for (i = 1; i <= dbh->header_len; i++) {
			eh = EH(dbh, i);

			//Caso o documento tenha sido deletado, ignora a EntryHeader
			if (!eh->pk)
				continue;

			if (g_strstr_len(&b->datablock[eh->init], eh->offset, params)) {
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


	for (x = g_list_first(l); x; x = x->next) {
		te = x->data;
		printf("%d | %s\n", te->pk, te->json);
	}

	printf("%d documentos\n", g_list_length(l));
	g_list_free_full(l, free_table_entry);
}

void delete(char *datablock, short row) {
	Buffer *b;
	DBHeader *dbh;
	EntryHeader *eh, *ehaux;

	dbh = DBH(datablock);
	eh = EH(dbh, row);

	// Como não da pra mudar o RowId, o datablock fica com um EntryHeader queimado
	eh->pk = 0; // pk começa em 1, 0 indica que o EntryHeader é inválido
	if (row < dbh->header_len) {
		// Caso não seja o último row do datablock, desfragmentamos
		ehaux = EH(dbh, row + 1);
		memcpy(&datablock[eh->init], &datablock[ehaux->init], dbh->next_init - ehaux->init);
		ehaux->init = eh->init;
		// ehaux->offset continua o mesmo
	}

	if (*(int *)&eh->next != 0) {
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

void help() {
	printf("Comandos disponiveis:\n"
			"\t- insert <json>\n"
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

char *cmd[] = {"insert", "select", "search", "delete", "load", "persist", "help", "exit", "quit", "btreedump", NULL};

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

	// Inicializa as estruturas de controle do programa.
	init_database();
	DBG("DBG: temos %d datablocks livres\n", g_list_length(free_blocks));

	printf("Digite \"help\" ou <tab><tab> para listar os comandos disponíveis.\n\n");

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

	return 0;

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

	clear_history();

	persist();

	for (int i = 0; i < framesLen; i++) {
		free(frames[i].datablock);
	}

	clear_history();

	g_list_free(free_blocks);

	printf("hit = %d, miss = %d\n", hit, miss);

	return 0;
}
