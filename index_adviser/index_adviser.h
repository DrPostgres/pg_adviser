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
	int2		ncols;					/* number of indexed columns */
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

#endif   /* INDEX_ADVISER_H */
