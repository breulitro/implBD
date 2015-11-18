#ifndef __BTREE_H__
#define __BTREE_H__
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
	int pk;
	uint16_t maior;
} __attribute__((__packed__)) BTBNode;

typedef struct {
	int pk;
	RowId rowid;
} BTLNode;

#if BTEST
#	define BRANCH_D 2L
#	define LEAF_D 2L
#else
// Desconta o Header da BTree + 1 Nodo para permitir a inserção do 2d+1-ésimo elemento
#	define BRANCH_D (((DATABLOCK - sizeof(BTHeader) - sizeof(BTBNode)) / (sizeof(BTBNode) - sizeof(uint16_t))) / 2)
#	define LEAF_D (((DATABLOCK - sizeof(BTHeader) - sizeof(BTLNode)) / sizeof(BTLNode)) / 2)
#endif

#define BR(block, i) ((BTBNode *) ((i) ? ((char *)(block) + sizeof(BTHeader) + (i) * (sizeof(BTBNode) - sizeof(uint16_t))) : (char *)(block) + sizeof(BTHeader)))
#define LF(block, i) ((BTLNode *) ((char *)(block) + (sizeof(BTHeader) + (i) * sizeof(BTLNode))))

void btree_delete(int pk);
RowId btree_get(int pk);
void btree_dump();
void btree_insert(int pk, uint16_t row, uint16_t id);
void btree_update(int pk, RowId rowid);
#endif
