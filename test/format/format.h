/*-
 * Copyright (c) 2008-2012 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <sys/types.h>

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#ifdef BDB
#include "build_unix/db.h"
#else
#include <wiredtiger.h>
#endif

#define	M(v)		((v) * 1000000)		/* Million */
#define	UNUSED(var)	(void)(var)		/* Quiet unused var warnings */

#define	FIX		1			/* File types */
#define	ROW		2
#define	VAR		3

/* Get a random value between a min/max pair. */
#define	MMRAND(min, max)	(wts_rand() % (((max) + 1) - (min)) + (min))

#define	WT_TABLENAME	"file:__wt"

#define	SINGLETHREADED	(g.threads == 1)

typedef struct {
	char *progname;				/* Program name */

	void *bdb;				/* BDB comparison handle */
	void *dbc;				/* BDB cursor handle */

	void *wts_conn;				/* WT_CONNECTION handle */

	FILE *rand_log;				/* Random number log */

	uint32_t run_cnt;			/* Run counter */

	enum {
	    LOG_FILE=1,				/* Use a log file */
	    LOG_OPS=2				/* Log all operations */
	} logging;
	FILE *logfp;				/* Log file */

	int replay;				/* Replaying a run. */
	int track;				/* Track progress */
	int threads;				/* Threads doing operations */

	char *config_open;			/* Command-line configuration */

	uint32_t c_bitcnt;			/* Config values */
	uint32_t c_bzip;
	uint32_t c_cache;
	uint32_t c_delete_pct;
	uint32_t c_file_type;
	uint32_t c_huffman_key;
	uint32_t c_huffman_value;
	uint32_t c_insert_pct;
	uint32_t c_intl_page_max;
	uint32_t c_key_max;
	uint32_t c_key_min;
	uint32_t c_leaf_page_max;
	uint32_t c_ops;
	uint32_t c_repeat_data_pct;
	uint32_t c_reverse;
	uint32_t c_rows;
	uint32_t c_runs;
	uint32_t c_value_max;
	uint32_t c_value_min;
	uint32_t c_write_pct;

	uint32_t key_cnt;			/* Keys loaded so far */
	uint32_t rows;				/* Total rows */
	uint16_t key_rand_len[1031];		/* Key lengths */
} GLOBAL;
extern GLOBAL g;

int	 bdb_del(uint64_t, int *);
void	 bdb_insert(const void *, uint32_t, const void *, uint32_t);
int	 bdb_np(int, void *, uint32_t *, void *, uint32_t *, int *);
int	 bdb_put(const void *, uint32_t, const void *, uint32_t, int *);
int	 bdb_read(uint64_t, void *, uint32_t *, int *);
void	 bdb_startup(void);
void	 bdb_teardown(void);
const char *
	 config_dtype(void);
void	 config_error(void);
void	 config_file(const char *);
void	 config_print(int);
void	 config_setup(void);
void	 config_single(const char *, int);
void	 die(const char *,  int);
void	 key_gen(uint8_t *, uint32_t *, uint64_t, int);
void	 key_gen_setup(uint8_t **);
void	 track(const char *, uint64_t);
void	 value_gen(uint8_t *, uint32_t *, uint64_t);
int	 wts_dump(const char *, int);
void	 wts_load(void);
int	 wts_ops(void);
uint32_t wts_rand(void);
int	 wts_read_scan(void);
int	 wts_salvage(void);
int	 wts_startup(void);
void	 wts_stats(void);
int	 wts_teardown(void);
int	 wts_verify(const char *);
