#ifndef __BUFMGMT_H__
#define __BUFMGMT_H__

#include <stdlib.h>
#include <unistd.h>

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
	uint32_t id;
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
	uint16_t row;
	uint16_t id;
} RowId;

typedef struct {
	int pk;
	uint16_t init;
	uint16_t offset;
	RowId next;
} EntryHeader;

extern Buffer frames[256];
extern int framesLen;
extern int vitima;
extern int miss;
extern int hit;
extern GList *free_blocks;
extern Config conf;

void load_cmd(char *params);
void delete_cmd(char *params);
void search_cmd(char *params);
void select_cmd(char *params);
void insert_cmd(char *params);
Buffer *get_datablock(int id);
uint16_t get_free_datablock_id();
void persist();
void create_database();
void init_database();

#endif
