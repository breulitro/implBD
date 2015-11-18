#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <glib.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <ctype.h>
#include "bufmgmt.h"
#include "btree.h"
void _btree_delete(int pk, int id) {
	Buffer *b;
	BTHeader *bth;
	BTBNode *br;
	BTLNode *lf, *laux = NULL;

	b = get_datablock(id);
	bth = (BTHeader *)b->datablock;
	DBG("btree->len = %d\n", bth->len);

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

		DBG("pk(%d) não encontrada na btree\n", pk);
	} else {
		// TODO: Buscar nodo e deletar o pk dele
		for (int i = 0; i < bth->len; i++) {
			br = BR(bth, i);
			if (pk < br->pk)
				_btree_delete(pk, br->menor);
			else
				_btree_delete(pk, br->maior);
		}
	}

	// Se é igual, é pq é o último
	DBG("btree->len = %d\n", bth->len);
}

void btree_delete(int pk) {
	_btree_delete(pk, conf.root);
}

RowId btree_leaf_get(uint16_t id, int pk) {
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

RowId btree_branch_get(uint16_t id, int pk) {
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
void btree_dump_leaf(uint16_t id, int padding) {
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

void _btree_dump(uint16_t id, int padding) {
	Buffer *b, *newroot, *newb;
	GList *l;
	BTHeader *bth;
	BTBNode *br;
	BTLNode *lf;
	int i, p;

	b = get_datablock(id);
	bth = (BTHeader *) b->datablock;

	if (bth->type == LEAF)
		btree_dump_leaf(b->id, padding);
	else {
		for (i = 0; i < bth->len; i++) {
			br = BR(bth, i);
			if (!i)
				_btree_dump(br->menor, padding + 1);
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

	_btree_dump(conf.root, 0);
}

void btree_insert_branch(uint16_t id, int pk, uint16_t menor, uint16_t maior);

void _btree_update_parent(uint16_t id, uint16_t parent) {
	Buffer *b;
	BTHeader *bth;
	BTBNode *br;
	BTLNode *lf;

	b = get_datablock(id);
	bth = (BTHeader *) b->datablock;
	bth->parent = parent;
	if (bth->type == BRANCH)
		for (int i = 0; i < bth->len; i++) {
			br = BR(bth, i);
			_btree_update_parent(br->menor, id);
			_btree_update_parent(br->maior, id);
		}
}

void btree_update_parent(uint16_t id) {
	Buffer *b;
	BTHeader *bth;
	BTBNode *br;
	BTLNode *lf;

	b = get_datablock(id);
	bth = (BTHeader *) b->datablock;
	if (bth->type == LEAF) {
		printf("Não era pra estar sendo chamada esta função com um nodo LEAF\n");
		return;
	}

	for (int i = 0; i < bth->len; i++) {
		br = BR(bth, i);
		_btree_update_parent(br->menor, id);
		_btree_update_parent(br->maior, id);
	}
}

void btree_branch_split(uint16_t id) {
	Buffer *b, *newroot, *newb;
	GList *l;
	BTHeader *bth, *nbth, *rbth;
	BTBNode *br, *nbr, *rbr;
	int i;

	DBG("Branch Split\n");
	b = get_datablock(id);
	bth = (BTHeader *) b->datablock;
	b->dirty = 1;

	// Aloca novo nodo branch
	l = g_list_first(free_blocks);
	i = (int)l->data;
	free_blocks = g_list_delete_link(free_blocks, l);
	SET_USED(conf.bitmap, i);
	newb = get_datablock(i);

	// Setup do novo nodo
	newb->id = i;
	newb->dirty = 1;
	nbth = (BTHeader *) newb->datablock;
	nbth->type = BRANCH;

	// Posiciona br na metade
	br = BR(bth, BRANCH_D + 1);
	// Posiciona nbr no começo
	nbr = BR(nbth, 0);
	// Copia da metade em diate do nodo $id para nbr
	DBG("Copiando %lu bytes a partir do pk(%d)\n",
			sizeof(uint16_t) + (sizeof(BTBNode) - sizeof(uint16_t)) * (BRANCH_D),
			br->pk);
	memcpy(nbr, br, sizeof(uint16_t) + (sizeof(BTBNode) - sizeof(uint16_t)) * (BRANCH_D));
	nbth->len = BRANCH_D;
	bth->len = BRANCH_D;

#ifdef DEBUG
	DBG("Menores\n");
	_btree_dump(b->id, -1);
	DBG("Maiores\n");
	if (DEBUG)
	_btree_dump(newb->id, -1);
#endif

	// Apontamento dos simblings e parent
	bth->next = newb->id;
	nbth->prev = b->id;

	DBG("Novo nodo folha(%d) criado\n", i);

	// Se não tiver um nó pai, aloca, senão insere nele
	if (!bth->parent) {
		i = get_free_datablock_id();
		DBG("Alocando novo nodo raiz no datablock(%d)\n", i);
		newroot = get_datablock(i);
		newroot->dirty = 1;
		rbth = (BTHeader *) newroot->datablock;
		rbth->type = BRANCH;
		rbr = BR(rbth, 0);
		br = BR(bth, BRANCH_D);
		DBG("Subindo pk(%d)\n", br->pk);
		rbr->pk = br->pk;
		rbr->menor = b->id;
		rbr->maior = newb->id;
		rbth->len = 1;

		newroot->dirty = 1;
		// Apontamento dos parents
		bth->parent = nbth->parent = i;
		btree_update_parent(rbr->menor);
		btree_update_parent(rbr->maior);
		conf.root = i;
		DBG("Novo nodo raiz(datablock = %d) criado\n", i);
	} else {
		nbth->parent = bth->parent;
		br = BR(bth, BRANCH_D);
		btree_insert_branch(bth->parent, br->pk, b->id, newb->id);
		btree_update_parent(bth->parent);
	}
}

void btree_insert_branch(uint16_t id, int pk, uint16_t menor, uint16_t maior) {
	assert(menor < maior);
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
	int i;

	printf("Leaf Split\n");
	b = get_datablock(id);
	b->id = id;
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
	bth->len = LEAF_D;

	// Apontamento dos simblings e parent
	bth->next = newb->id;
	nbth->prev = b->id;

	DBG("Novo nodo folha(%d) criado\n", i);

	// Se não tiver um nó pai, aloca, senão insere nele
	if (!bth->parent) {
		i = get_free_datablock_id();
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

		newroot->dirty = 1;
		// Apontamento dos parents
		bth->parent = nbth->parent = i;
		rbth->parent = 0;
		btree_update_parent(br->menor);
		btree_update_parent(br->maior);
		conf.root = i;
		DBG("Novo nodo raiz(datablock = %d) criado\n", i);
	} else {
		printf("ja tem parent\n");
		printf("menor = %d, maior = %d\n", b->id, newb->id);
		printf("parent = %d\n", bth->parent);
		nbth->parent = bth->parent;
		btree_insert_branch(bth->parent, nlf->pk, b->id, newb->id);
		btree_update_parent(bth->parent);
	}
	//conf.root = newroot->id;
}

void btree_insert_node(uint16_t id, int pk, RowId rowid) {
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

void btree_insert(int pk, uint16_t row, uint16_t id) {
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

void _btree_update(int id, int pk, RowId rowid) {
	Buffer *b;
	BTHeader *bth;
	BTBNode *br;
	BTLNode *lf;

	b = get_datablock(id);
	bth = (BTHeader *) b->datablock;

	if (bth->type == LEAF)
		for(int i = 0; i < bth->len; i++) {
			lf = LF(bth, i);
			if (lf->pk == pk) {
				DBG("Updating %d @ %d:%d -> %d:%d\n", pk, lf->rowid.id, lf->rowid.row, rowid.id, rowid.row);
				lf->rowid = rowid;
				break;
			}
		}
	else
		for(int i = 0; i < bth->len; i++) {
			br = BR(bth, i);
			if (pk < br->pk)
				return _btree_update(br->menor, pk, rowid);
			else
				return _btree_update(br->maior, pk, rowid);
		}
}

void btree_update(int pk, RowId rowid) {
	_btree_update(conf.root, pk, rowid);
}
