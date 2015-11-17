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

#define FILESIZE 256 * 1024 * 1024 // 256MB
#define DATABLOCK 4096
#define DATAFILE ".datafile"

#define IS_USED(bmap, n) (bmap[n / 8] & (1 << (n % 8)))
#define SET_USED(bmap, n) bmap[(n) / 8] |= (1 << ((n) % 8))

#define TBH(buf) ((TBHeader *) (((char *)&(buf)[DATABLOCK - 1]) - sizeof(TBHeader)));
#define RH(tbh, n) ((EntryHeader *) ((char *)(tbh) - sizeof(EntryHeader) * (n)))

#ifdef DEBUG
#define DBG(fmt, ...) printf("%s() @ %s +%d: "fmt, __func__, __FILE__, __LINE__, ##__VA_ARGS__)
#else
#define DBG(fmt, ...)
#endif

typedef struct Buffer {
	uint16_t id;
	char *datablock;
	char dirty;
	char used;
} Buffer;

typedef struct {
	uint16_t header_len;
	uint16_t free;
	uint16_t full;
	uint16_t next_init;
	uint16_t next;
} TBHeader;

typedef struct {
	unsigned char bitmap[FILESIZE / DATABLOCK / 8];
	uint16_t root;
	uint16_t table;
	uint16_t nextpk;
} Config;


typedef struct {
	uint16_t row;
	uint16_t id;
} RowId;

static Buffer frames[256];
static uint16_t framesLen;
static uint32_t vitima;
static uint32_t miss;
static uint32_t hit = 0;
static GList *free_blocks;

Buffer *get_datablock(uint16_t id) {
	uint16_t i;
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
	uint16_t i;
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
	uint32_t pk;
	uint16_t init;
	uint16_t offset;
	RowId next;
} EntryHeader;

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
	TBHeader *tbh;
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
	tbh = TBH(b->datablock);

	if (!tbh->free)
		if (!tbh->full)
			tbh->free = DATABLOCK - sizeof(TBHeader);

	DBG("To te pegando um datablock com %d bytes livres\n", tbh->free);

	DBG("db(%d)->free(%d)\n", b->id, tbh->free);
	while (tbh->free <  sizeof(EntryHeader)) {
		printf("entrou no while\n");
		sleep(1);
		DBG("tbh->free(%d) < sizeof(EntryHeader)(%lu)\n", tbh->free, sizeof(EntryHeader));
		DBG("next(%d)\n", tbh->next);
		if (tbh->next) {
			DBG("Ja existe proximo datablock\n");
			b = get_datablock(tbh->next);
		} else {
			DBG("Nao existe proximo datablock\n");

			b->dirty = 1;
			tbh->next = get_free_datablock_id();
			b = get_datablock(tbh->next);

			tbh = TBH(b->datablock);
			if (!tbh->free) 
				if (!tbh->full)
					tbh->free = DATABLOCK - sizeof(TBHeader);
			DBG("db(%d)->free(%d)\n", b->id, tbh->free);

			SET_USED(conf.bitmap, (int) id->data);
			free_blocks = g_list_delete_link(free_blocks, id);
		}
		tbh = TBH(b->datablock);
		DBG("db(%d)->free(%d)\n", b->id, tbh->free);
	}
	DBG("db(%d)->free(%d)\n", b->id, tbh->free);

	return b;
}

typedef enum {
	LEAF,
	BRANCH
} BTType;

typedef struct {
	uint16_t len;
	uint16_t prev;
	uint16_t next;
	uint16_t parent;
	BTType type;
} BTHeader;

// Tem que setar o tipo de atributo pra __packed__ senão ele bota padding no
// prev e no next pra estrutura ficar uniforme. Isso deixa a estrutura com 12 bytes.
// E não é isso que a gente quer, queremos ela com seus 8 bytes enxutos!
typedef struct {
	uint16_t menor;
	uint32_t pk;
	uint16_t maior;
} __attribute__((__packed__)) BTBNode;

typedef struct {
	uint32_t pk;
	RowId rowid;
} BTLNode;

#if 0
// Desconta o Header da BTree + 1 Nodo para permitir a inserção do 2d+1-ésimo elemento
#define BRANCH_D (((DATABLOCK - sizeof(BTHeader) - sizeof(BTBNode)) / (sizeof(BTBNode) - sizeof(uint16_t))) / 2)
#define LEAF_D (((DATABLOCK - sizeof(BTHeader) - sizeof(BTLNode)) / sizeof(BTLNode)) / 2)
#else
// Para propósito de testes, até estabilizar a BTree+
#define BRANCH_D 2L
#define LEAF_D 2L
#endif

#define BR(block, i) ((BTBNode *) ((i) ? ((char *)(block) + sizeof(BTHeader) + (i) * (sizeof(BTBNode) - sizeof(uint16_t)) + sizeof(uint16_t)) : (char *)(block) + sizeof(BTHeader)))
#define LF(block, i) ((BTLNode *) ((char *)(block) + (sizeof(BTHeader) + (i) * sizeof(BTLNode))))
void _btree_delete(uint32_t pk, uint16_t id) {
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

RowId btree_leaf_get(uint16_t id, uint32_t pk) {
	Buffer *b;
	BTHeader *bth;
	BTLNode *lf;
	uint16_t i;

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

RowId btree_branch_get(uint16_t id, uint32_t pk) {
	Buffer *b;
	BTHeader *bth;
	BTBNode *br;
	uint16_t i;

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

RowId btree_get(uint32_t pk) {
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
void btree_dump_leaf(uint16_t id, uint8_t padding) {
	Buffer *b, *newroot, *newb;
	GList *l;
	BTHeader *bth;
	BTBNode *br;
	BTLNode *lf;
	uint16_t i;
	uint8_t p;

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

void _btree_dump(uint16_t id, uint8_t padding) {
	Buffer *b, *newroot, *newb;
	GList *l;
	BTHeader *bth;
	BTBNode *br;
	BTLNode *lf;
	uint16_t i;
	uint8_t p;

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
	if (!conf.root) {
		printf("Btree+ vazia\n");
		return;
	}

	_btree_dump(conf.root, -1);
}

void btree_insert_branch(uint16_t id, uint32_t pk, uint16_t menor, uint16_t maior);

void btree_branch_split(uint16_t id) {
	Buffer *b, *newb, *rootb;
	BTHeader *bth, *nbth, *rbth;
	BTBNode *br, *nbr, *rbr;
	GList *l;

	newb = get_datablock(get_free_datablock_id());
	newb->dirty = 1;
	nbth = (BTHeader *) newb->datablock;
	nbth->type = BRANCH;

	// Carrega datablock aonde será feito o split
	b = get_datablock(id);
	bth = (BTHeader *) b->datablock;

	// Aponta os nodos para fazer a cópia dos dados
	br = BR(bth, BRANCH_D + 1);
	nbr = BR(nbth, 0);

	memcpy(nbr, br, sizeof(BTBNode) * BRANCH_D);
	bth->len = BRANCH_D;
	nbth->len = BRANCH_D;

	// Faz os apontamentos dos siblings e do parent
	nbth->prev = b->id;
	bth->next = newb->id;
	nbth->parent = bth->parent;

	// Aponta para o nodo que vai subir pra raiz (ou branch)
	br = BR(bth, BRANCH_D);
	if (!bth->parent) {
		// Cria novo nodo raiz
		rootb = get_datablock(get_free_datablock_id());
		// TODO: Acabar de alocar a nova raiz
		rbth = (BTHeader *) rootb->datablock;
		rbr = BR(rbth, 0);
		rbth->len = rbth->len + 1;
	} else {
		// Adiciona no parent
		btree_insert_branch(bth->parent, br->pk, b->id, newb->id);
	}
}

void btree_insert_branch(uint16_t id, uint32_t pk, uint16_t menor, uint16_t maior) {
	Buffer *b;
	BTHeader *bth;
	BTBNode *br;

	b = get_datablock(id);
	bth = (BTHeader *) b->datablock;
	DBG("Branch com %d nodos\n", bth->len);
	br = BR(bth, bth->len);
	bth->len = bth->len + 1;
	br->pk = pk;
	br->menor = menor;
	br->maior = maior;
	b->dirty = 1;

	if (bth->len > BRANCH_D * 2) {
		DBG("TBD: Branch split\n");
		btree_branch_split(id);
	}
}

void btree_leaf_split(uint16_t id) {
	Buffer *b, *newroot, *newb;
	GList *l;
	BTHeader *bth, *nbth, *rbth;
	BTBNode *br;
	BTLNode *lf, *nlf;
	uint16_t i;

	printf("Leaf Split\n");
	b = get_datablock(id);
	bth = (BTHeader *) b->datablock;
	b->dirty = 1;

	// Aloca novo nodo folha
	l = g_list_first(free_blocks);
	i = (uint16_t)l->data;
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

	// Apontamento dos simblings e parent
	bth->next = newb->id;
	nbth->prev = b->id;
	nbth->parent = bth->parent;

	DBG("Novo nodo folha(%d) criado\n", i);

	// Se não tiver um nó pai, aloca, senão insere nele
	if (!bth->parent) {
		newroot->id = get_free_datablock_id();
		newroot = get_datablock(newroot->id);
		newroot->dirty = 1;
		rbth = (BTHeader *) newroot->datablock;
		rbth->type = BRANCH;
		br = BR(rbth, 0);
		br->pk = nlf->pk;
		br->menor = b->id;
		br->maior = newb->id;
		rbth->len = 1;

		newroot->dirty = 1;
		// Apontamento dos parents
		bth->parent = nbth->parent = i;
		conf.root = i;
		DBG("Novo nodo raiz(datablock = %d) criado\n", i);
	} else {
		btree_insert_branch(bth->parent, nlf->pk, b->id, newb->id);
	}


	bth->len = LEAF_D;
	//conf.root = newroot->id;
}

void btree_insert_node(uint16_t id, uint32_t pk, RowId rowid) {
	Buffer *b;
	GList *l;
	BTHeader *bth;
	BTBNode *br;
	BTLNode *lf;

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

void btree_insert(uint32_t pk, uint16_t row, uint16_t id) {
	Buffer *b;
	GList *l;
	BTHeader *bth;
	BTBNode *br;
	BTLNode *lf;
	RowId rowid;
#pragma GCC diagnostic pop

	DBG("BRANCH_D = %lu, LEAF_D = %lu\n", BRANCH_D, LEAF_D);
	printf("inserindo %d @ %d:%d\n", pk, id, row);

	// Caso não haja um datablock inicial para a BTree+
	if (!conf.root) {
		conf.root = get_free_datablock_id();
		DBG("new BTree+ root @ datablock(%d)\n", conf.root);
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
		else {
			DBG("Inserindo no maior\n");
			btree_insert_node(br->maior, pk, rowid);
		}
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
	uint32_t len;
	RowId rowid;
	Buffer *b;
	TBHeader *tbh;
	EntryHeader *rh;

	b = get_insertable_datablock();
	if (!b) {
		printf("Acabaram-se os datablocks");
		return (RowId){0,0};
	}

	tbh = TBH(b->datablock);
	DBG("insertable datablock with %d bytes, json with %lu\n bytes", tbh->free, strlen(json));

	DBG("Chegando pra inserir: %s\n", json);
	len = strlen(json);
	DBG("len = %d\n", len);
	tbh = TBH(b->datablock);
	tbh->header_len++;
	DBG("tbh->free(%d) - sizeof(EntryHeader)(%lu) = %lu\n", tbh->free, sizeof(EntryHeader), tbh->free - sizeof(EntryHeader));
	tbh->free = tbh->free - sizeof(EntryHeader);
	DBG("tbh->free(%d)\n", tbh->free);
	rh = RH(tbh, tbh->header_len);
	rh->init = tbh->next_init;
	DBG("init = %d\n", rh->init);
	DBG("free(%d), offset(%d), header(%ld) | free - offset - header = %ld\n",
			tbh->free, rh->offset, sizeof(EntryHeader), tbh->free - rh->offset - sizeof(EntryHeader));
	// Se for um chained, seta a pk pra zero
	rh->pk = chained ? 0 : conf.nextpk++;
	DBG("pk = %d\n", rh->pk);

	// A partir deste ponto, len passa a ser uma flag pra sinalizar se
	// o json coube inteiro neste datablock
	DBG("free(%d), offset(%d), header(%ld) | free - offset - header = %ld\n",
			tbh->free, rh->offset, sizeof(EntryHeader), tbh->free - rh->offset - sizeof(EntryHeader));
	if (len > tbh->free) {
		DBG("Vai encadear\n");
		rh->offset = tbh->free - 1; //FIXME: Ta errado botar esse -1...
		tbh->free = tbh->free - 1;
		len = 1;
		tbh->full = 1;
	} else {
		DBG("NÃO Vai encadear\n");
		rh->offset = len;
		len = 0;
	}
	//tbh->next_init += rh->offset;
	tbh->next_init = tbh->next_init + rh->offset;

	DBG("free(%d), offset(%d), header(%ld) | free - offset - header = %ld\n",
			tbh->free, rh->offset, sizeof(EntryHeader), tbh->free - rh->offset - sizeof(EntryHeader));

	DBG("copiando para a posição %ld do buffer\n", &b->datablock[rh->init] - b->datablock);
	DBG("writing %d bytes @ %d\n", rh->offset, rh->init);
	DBG("antes pk = %d\n", rh->pk);
	DBG("pk @ %ld\n", (char *)&rh->pk - b->datablock);
	DBG("rh @ %ld\n", (char *)rh - b->datablock);
	memcpy(&b->datablock[rh->init], json, rh->offset);
	DBG("depois pk = %d\n", rh->pk);

	//tbh->free -= rh->offset - sizeof(EntryHeader);
	DBG("free(%d) - offset(%d) = %d\n", tbh->free, rh->offset, tbh->free - rh->offset);
	tbh->free = tbh->free - rh->offset;
	if (tbh->free == 0)
		tbh->full = 1;
	DBG("free(%d)\n", tbh->free);

	if (len) {
		DBG("Encadeando\n");
		DBG("antes pk = %d\n", rh->pk);
		rowid = insert(&json[rh->offset], 1);
		DBG("depois pk = %d\n", rh->pk);
		DBG("ChainedRow @ %d:%d\n", rowid.id, rowid.row);
		if ( *(int *) &rowid == 0L) {
			DBG("Não consegui encadear, desfazendo operação\n");
			// Roll-back, desfaz o insert que não coube nos datablocks disponíveis
			tbh->header_len--;
			return rowid;
		}
		rh->next = rowid;
	} else {
		rh->next = (RowId){0,0};
	}

	b->dirty = 1;
	// Se não for um chained row, insere ele na lista
	if (rh->pk)
		btree_insert(rh->pk, tbh->header_len, b->id);

	DBG("Row(%d) @ %d:%d\n", rh->pk, b->id, tbh->header_len);
	return (RowId){tbh->header_len, b->id};
}

void insert_cmd(char *params) {
	// "params" deve conter o json a ser inserido
	// NÃO É FEITA VALIDAÇÃO DO DOCUMENTO JSON!
	uint32_t len;
	Buffer *b;
	TBHeader *tbh;
	EntryHeader *rh;
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

void _select(RowId rowid, char **buf, uint16_t len) {
	Buffer *b;
	TBHeader *tbh;
	EntryHeader *rh;
	char *baux;

	b = get_datablock(rowid.id);
	tbh = TBH(b->datablock);
	rh = RH(tbh, rowid.row);
	baux = malloc(rh->offset + 1);
	memcpy(baux, &b->datablock[rh->init], rh->offset);
	baux[rh->offset] = 0;
	*buf = strcat(*buf, baux);
	DBG("buf = %s\n", *buf);
	if (*(int *)&rh->next) {
		DBG("select denovo\n");
		_select(rh->next, buf, len + rh->offset);
	}
}

// WARN: Não é feita verificação se existe realmente o RowId em questão
void select_cmd(char *params) {
	uint32_t pk;
	Buffer *b;
	char *buf, *baux;
	TBHeader *tbh;
	EntryHeader *rh;
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
	tbh = TBH(b->datablock);
	rh = RH(tbh, r.row);

	if (!rh->pk) {
		printf("Documento não encontrado\n");
		return;
	}

	buf = malloc(rh->offset + 1);
	memcpy(buf, &b->datablock[rh->init], rh->offset);
	buf[rh->offset] = 0;

	printf("primeiro copy feito\n");
	if (*(int *)&rh->next != 0)
		_select(rh->next, &buf, rh->offset);


	printf("%d | %s\n", rh->pk, buf);
	free(buf);
}

typedef struct {
	uint32_t pk;
	char *json;
} TableEntry;

void free_table_entry(gpointer data) {
	TableEntry *te = data;

	free(te->json);
	free(te);
}

void search_cmd(char *params) {
	Buffer *b;
	TBHeader *tbh;
	EntryHeader *rh;
	TableEntry *te;
	GList *x, *l = NULL;
	uint16_t i;

	b = get_datablock(conf.table);
	do {
		tbh = TBH(b->datablock);
		for (i = 1; i <= tbh->header_len; i++) {
			rh = RH(tbh, i);

			//Caso o documento tenha sido deletado, ignora a EntryHeader
			if (!rh->pk)
				continue;

			if (g_strstr_len(&b->datablock[rh->init], rh->offset, params)) {
				te = malloc(sizeof(TableEntry));
				te->json = malloc(rh->offset + 1);
				te->json = memcpy(te->json, &b->datablock[rh->init], rh->offset);
				te->json[rh->offset] = 0;
				te->pk = rh->pk;
				l = g_list_append(l, te);
			}
		}

		if (tbh->next)
			b = get_datablock(tbh->next);
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
	TBHeader *tbh;
	EntryHeader *rh, *rhaux;

	tbh = TBH(datablock);
	rh = RH(tbh, row);

	// Como não da pra mudar o RowId, o datablock fica com um EntryHeader queimado
	rh->pk = 0; // pk começa em 1, 0 indica que o EntryHeader é inválido
	if (row < tbh->header_len) {
		// Caso não seja o último row do datablock, desfragmentamos
		rhaux = RH(tbh, row + 1);
		memcpy(&datablock[rh->init], &datablock[rhaux->init], tbh->next_init - rhaux->init);
		rhaux->init = rh->init;
		// rhaux->offset continua o mesmo
	}

	if (*(uint16_t *)&rh->next != 0) {
		b = get_datablock(rh->next.id);
		delete(b->datablock, rh->next.row);
		b->dirty = 1;
	}
}

void delete_cmd(char *params) {
	Buffer *b;
	uint32_t pk;
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
	btree_insert(13, 6, 3);
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
