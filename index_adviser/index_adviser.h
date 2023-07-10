/*-------------------------------------------------------------------------
 *
 * index_advisor.h
 *     Prototypes for indexadvisor.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef INDEX_ADVISER_H
#define INDEX_ADVISER_H 1


#include "postgres.h"

#include "nodes/print.h"
#include "parser/parsetree.h"
#include "catalog/namespace.h"
#include "executor/executor.h"

typedef struct {

	Index		varno;					/* index into the rangetable */
	Index		varlevelsup;			/* points to the correct rangetable */
	int16		ncols;					/* number of indexed columns */
	Oid			vartype[INDEX_MAX_KEYS];/* type of the column(s) */
	AttrNumber	varattno[INDEX_MAX_KEYS];/* attribute number of the column(s) */
	Oid			reloid;					/* the table oid */
//TODO1 remove this member
	Oid			idxoid;					/* the virtual index oid */
	BlockNumber	pages;					/* the estimated size of index */
	bool		idxused;				/* was this used by the planner? */
	float4		benefit;				/* benefit made by using this cand */

} IndexCandidate;

extern void _PG_init(void);
extern void _PG_fini(void);

#define compile_assert(x)	extern int	_compile_assert_array[(x)?1:-1]

#if PG_VERSION_NUM < 120000
#define table_open(r, l)	heap_open(r, l)
#define table_close(r, l)	heap_close(r, l)
#endif

#ifndef BTREE_AM_OID
#define BTREE_AM_OID 403
#endif

#endif   /* INDEX_ADVISER_H */
