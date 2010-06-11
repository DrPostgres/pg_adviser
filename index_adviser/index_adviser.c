/*-------------------------------------------------------------------------
 *
 * index_adviser.c
 *		Plugin to recommend potentially useful indexes in a query.
 *
 * too much time? you do not know what to do next? then search for
 *		"TODO:" and "FIXME:" comments!
 *
 * created by Mario Stiffel
 * modified and partly reimplemented by Martin Lhring
 * Almost completely rewritten by gurjeet.singh@enterprisedb.com
 *
 *-------------------------------------------------------------------------
 */

/* ------------------------------------------------------------------------
 * includes (ordered alphabetically)
 * ------------------------------------------------------------------------
 */
#include <sys/time.h>

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/itup.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "index_adviser.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "executor/execdesc.h"
#include "executor/spi.h"
#include "fmgr.h"									   /* for PG_MODULE_MAGIC */
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "nodes/print.h"
#include "optimizer/planner.h"
#include "optimizer/plancat.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/syscache.h"

/* mark this dynamic library to be compatible with PG */
PG_MODULE_MAGIC;

#define CREATE_V_INDEXES 1

/* *****************************************************************************
 * DEBUG Level	: Information dumped
 * ------------   ------------------
 *	DEBUG1		: code level logging. What came in and what went out of a
 *					function. candidates generated, cost estimates, etc.
 *	DEBUG2		: DEBUG1 plus : Profiling info. Time consumed in each of the
 *					major functions.
 *	DEBUG3		: Above plus : function enter/leave info.
 * ****************************************************************************/

#define DEBUG_LEVEL_COST	(log_min_messages >= DEBUG2)
#define DEBUG_LEVEL_PROFILE	(log_min_messages >= DEBUG2)
#define DEBUG_LEVEL_CANDS	(log_min_messages >= DEBUG2)

/* Index Adviser output table */
#define IND_ADV_TABL "index_advisory"

/* IND_ADV_TABL does Not Exist */
#define IND_ADV_ERROR_NE	"relation \""IND_ADV_TABL"\" does not exist."

/* IND_ADV_TABL is Not a Table or a View */
#define IND_ADV_ERROR_NTV	"\""IND_ADV_TABL"\" is not a table or view."

#define IND_ADV_ERROR_DETAIL												   \
	"Index Adviser uses \""IND_ADV_TABL"\" table to store it's advisory. You"  \
	" should have INSERT permissions on a table or an (INSERT-able) view named"\
	" \""IND_ADV_TABL"\". Also, make sure that you are NOT running the Index"  \
	" Adviser under a read-only transaction."

#define IND_ADV_ERROR_HINT													   \
	"Please create the \""IND_ADV_TABL"\" table using the script provided in"  \
	" pg_advise_index contrib module."

/* *****************************************************************************
 * forward declarations of local-only functions
 * ****************************************************************************/

/* scan_* functions go looking for relevant attributes in the query */
static List* scan_query(	const Query* const query,
							List* const opnos,
							List* rangeTableStack );

static List* scan_generic_node(	const Node* const root,
								List* const opnos,
								List* const rangeTableStack );

static List* scan_group_clause(	List* const groupList,
								List* const targtList,
								List* const opnos,
								List* const rangeTblStack );

static List* build_composite_candidates( List* l1, List* l2 );

static List* remove_irrelevant_candidates( List* candidates );

static void mark_used_candidates(	const Node* const plan,
									List* const candidates );

static int compare_candidates(	const IndexCandidate* c1,
								const IndexCandidate* c2 );

static List* merge_candidates( List* l1, List* l2 );

#if CREATE_V_INDEXES

static List* create_virtual_indexes( List* candidates );

static void drop_virtual_indexes( List* candidates );

#endif

static void save_advice( List* candidates );

static void log_candidates( const char* text, List* candidates );

/* function used for estimating the size of virtual indexes */
static int4 estimate_index_pages(Oid rel_oid, Oid ind_oid );

static PlannedStmt* planner_callback(	Query*			query,
										int				cursorOptions,
										ParamListInfo	boundParams);

static void ExplainOneQuery_callback(	Query*			query,
										ExplainStmt*	stmt,
										const char*		queryString,
										ParamListInfo	params,
										TupOutputState*	tstate);

static void get_relation_info_callback(	PlannerInfo*	root,
										Oid				relationObjectId,
										bool			inhparent,
										RelOptInfo*		rel);

static const char* explain_get_index_name_callback( Oid indexId );

static PlannedStmt* index_adviser(	Query*			query,
									int				cursorOptions,
									ParamListInfo	boundParams,
									PlannedStmt*	actual_plan,
									bool			doingExplain);

static void resetSecondaryHooks();
static bool is_virtual_index( Oid oid, IndexCandidate** cand_out );

/* ------------------------------------------------------------------------
 * implementations: types and functions for profiling
 * ------------------------------------------------------------------------
 */

typedef struct {
	bool			running;
	struct timeval	start;
	struct timeval	stop;
	unsigned long	usec;
} Timer;

/* Need this to remember the virtual indexes generated. */
static List* index_candidates;

/* Timer for logCandiates; global, since it is called from different places */
static Timer tLogCandidates;

/* Global variable to hold a value across calls to mark_used_candidates() */
static PlannedStmt* plannedStmtGlobal;

static void
startTimer( Timer* const timer )
{
	gettimeofday( &(timer->start), NULL );

	timer->usec = 0;
	timer->running = true;
}

static void
continueTimer( Timer* const timer )
{
	if( timer->running == false )
	{
		gettimeofday( &(timer->start), NULL );
		timer->running = true;
	}
}

static void
stopTimer( Timer* const timer )
{
	if( timer->running == true )
	{
		gettimeofday( &(timer->stop), NULL );

		timer->usec += ( (unsigned long)timer->stop.tv_sec
							- (unsigned long)timer->start.tv_sec )
						* (unsigned long)1000000
							+ (unsigned long)timer->stop.tv_usec
							- (unsigned long)timer->start.tv_usec;

		timer->running = false;
	}
}

static void
t_reset( Timer *const timer )
{
	timer->usec = 0;
	timer->running = false;
}

/*
 * Since gettimeofday() is expensive, we don't collect profiling data unless
 * elog is going to log this.
 */
#define t_start(x)		do{								\
							if( DEBUG_LEVEL_PROFILE )	\
								startTimer( &(x) );		\
						}while(0)

#define t_continue(x)	do{								\
							if( DEBUG_LEVEL_PROFILE )	\
								continueTimer( &(x) );	\
						}while(0)

#define t_stop(x)		do{								\
							if( DEBUG_LEVEL_PROFILE )	\
								stopTimer( &(x) );		\
						}while(0)

/* ------------------------------------------------------------------------
 * implementations: index adviser
 * ------------------------------------------------------------------------
 */

/* PG calls this func when loading the plugin */
void
_PG_init(void)
{
	planner_hook = planner_callback;
	ExplainOneQuery_hook = ExplainOneQuery_callback;

	/* We dont need to reset the state here since the contrib module has just been
	 * loaded; FIXME: consider removing this call.
	 */
	resetSecondaryHooks();

	elog( NOTICE, "IND ADV: plugin loaded" );
}

/* PG calls this func when un-loading the plugin (if ever) */
void
_PG_fini(void)
{
	planner_hook				= NULL;
	ExplainOneQuery_hook		= NULL;

	resetSecondaryHooks();

	elog( NOTICE, "IND ADV: plugin unloaded." );
}

/* Make sure that Cost datatype can represent negative values */
compile_assert( ((Cost)-1) < 0 );

/**
 * index_adviser
 *		Takes a query and the actual plan generated by the standard planner for
 * that query. It then creates virtual indexes, using the columns used in the
 * query, and asks the standard planner to generate a new plan for the query.
 *
 *     If the new plan appears to be cheaper than the actual plan, it
 * saves the information about the virtual indexes that were used by the
 * planner in an advisory table.
 *
 *     If it is called by the Explain-hook, then it returns the newly generated
 * plan (allocated in caller's memory context), so that ExplainOnePlan() can
 * generate and send a string representation of the plan to the log or the client.
 */

/* TODO: Make recursion suppression more bullet-proof. ERRORs can leave this indicator on. */
static int8	SuppressRecursion = 0;		  /* suppress recursive calls */

static PlannedStmt*
index_adviser(	Query*			queryCopy,
				int				cursorOptions,
				ParamListInfo	boundParams,
				PlannedStmt		*actual_plan,
				bool			doingExplain)
{
	bool		saveCandidates = false;
	int			i;
	ListCell	*prev,							/* temps for list manipulation*/
				*cell,
				*next;
	List*       opnos = NIL;			  /* contains all vailid operator-ids */
	List*		candidates = NIL;				  /* the resulting candidates */

	Timer		tAdviser;
	Timer		tRePlan;
	Timer		tBTreeOperators;
	Timer		tGenCands;
	Timer		tMarkUsedCands;
	Timer		tCreateVInds;
	Timer		tDropVInds;
	Timer		tSaveAdvise;

	Cost		actualStartupCost;
	Cost		actualTotalCost;
	Cost		newStartupCost;
	Cost		newTotalCost;
	Cost		startupCostSaved;
	Cost		totalCostSaved;
	float4		startupGainPerc;							/* in percentages */
	float4		totalGainPerc;

	ResourceOwner	oldResourceOwner;
	PlannedStmt		*new_plan;
	MemoryContext	outerContext;

	char *BTreeOps[] = { "=", "<", ">", "<=", ">=", };

	elog( DEBUG3, "IND ADV: Entering" );

	/* We work only in Normal Mode, and non-recursively; that is, we do not work
	 * on our own DML.
	 */
	if( IsBootstrapProcessingMode() || SuppressRecursion++ > 0 )
	{
		new_plan = NULL;
		goto DoneCleanly;
	}

	/* Remember the memory context; we use it to pass interesting data back. */
	outerContext = CurrentMemoryContext;

	/* reset these globals; since an ERROR might have left them unclean */
	t_reset( &tLogCandidates );
	index_candidates = NIL;

	/* save the start-time */
	t_start( tAdviser );

	/* get the costs without any virtual index */
	actualStartupCost	= actual_plan->planTree->startup_cost;
	actualTotalCost		= actual_plan->planTree->total_cost;

	/* create list containing all operators supported by B-tree */
	t_start( tBTreeOperators );
	for( i=0; i < lengthof(BTreeOps); ++i )
	{
		FuncCandidateList   opnosResult;

		List* btreeop = list_make1( makeString( BTreeOps[i] ) );

		/* TODO: Fix this comment.
		 * get the operator-id's to the operator, and collect the operator-id's
		 * into an array.
		 */
		/* TODO: find out if the memory of opnosResult is ever freed. */
		for(	opnosResult = OpernameGetCandidates( btreeop, '\0' );
				opnosResult != NULL;
				opnosResult = lnext(opnosResult) )
		{
			opnos = lappend_oid( opnos, opnosResult->oid );
		}

		/* free the Value* (T_String) and the list */
		pfree( linitial( btreeop ) );
		list_free( btreeop );
	}
	t_stop( tBTreeOperators );

	/* Generate index candidates */
	t_start( tGenCands );
	candidates = scan_query( queryCopy, opnos, NULL );
	t_stop( tGenCands );

	/* the list of operator oids isn't needed anymore */
	list_free( opnos );

	if (list_length(candidates) == 0)
		goto DoneCleanly;

	log_candidates( "Generated candidates", candidates );

	/* remove all irrelevant candidates */
	candidates = remove_irrelevant_candidates( candidates );

	if (list_length(candidates) == 0)
		goto DoneCleanly;

	log_candidates( "Relevant candidates", candidates );
#if CREATE_V_INDEXES
	/*
	 * We need to restore the resource-owner after RARCST(), only if we are
	 * called from the executor; but we do it all the time because,
	 * (1) Its difficult to determine if we are being called by the executor.
	 * (2) It is harmless.
	 * (3) It is not much of an overhead!
	 */
	oldResourceOwner = CurrentResourceOwner;

	/*
	 * Setup an SPI frame around the BeginInternalSubTransaction() and
	 * RollbackAndReleaseCurrentSubTransaction(), since xact.c assumes that
	 * BIST()/RARCST() infrastructure is used only by PL/ interpreters (like
	 * pl/pgsql), and hence it calls AtEOSubXact_SPI(), and that in turn frees
	 * all the execution context memory of the SPI (which _may_ have invoked the
	 * adviser). By setting up our own SPI frame here, we make sure that
	 * AtEOSubXact_SPI() frees this frame's memory.
	 */
	if( SPI_connect() != SPI_OK_CONNECT )
	{
		elog( WARNING, "IND ADV: SPI_connect() call failed" );
		goto DoneCleanly;
	}

	/*
	 * DO NOT access any data-structure allocated between BEGIN/ROLLBACK
	 * transaction, after the ROLLBACK! All the memory allocated after BEGIN is
	 * freed in ROLLBACK.
	 */
	BeginInternalSubTransaction( "index_adviser" );

	/* now create the virtual indexes */
	t_start( tCreateVInds );
	candidates = create_virtual_indexes( candidates );
	t_stop( tCreateVInds );
#endif
	/* update the global var */
	index_candidates = candidates;

	/*
	 * Setup the hook in the planner that injects information into base-tables
	 * as they are prepared
	 */
	get_relation_info_hook = get_relation_info_callback;

	/* do re-planning using virtual indexes */
	/* TODO: is the plan ever freed? */
	t_start( tRePlan );
	new_plan = standard_planner(queryCopy, cursorOptions, boundParams);
	t_stop( tRePlan );

	/* reset the hook */
	get_relation_info_hook = NULL;
#if CREATE_V_INDEXES
	/* remove the virtual-indexes */
	t_start( tDropVInds );
	drop_virtual_indexes( candidates );
	t_stop( tDropVInds );
#endif
	newStartupCost	= new_plan->planTree->startup_cost;
	newTotalCost	= new_plan->planTree->total_cost;

	/* calculate the cost benefits */
	startupGainPerc =
		actualStartupCost == 0 ? 0 :
			(1 - newStartupCost/actualStartupCost) * 100;

	totalGainPerc =
		actualTotalCost == 0 ? 0 :
			(1 - newTotalCost/actualTotalCost) * 100;

	startupCostSaved = actualStartupCost - newStartupCost;

	totalCostSaved = actualTotalCost - newTotalCost;

	if( startupCostSaved >0 || totalCostSaved > 0 )
	{
		/* scan the plan for virtual indexes used */
		t_start( tMarkUsedCands );
		plannedStmtGlobal = new_plan;

		mark_used_candidates( (Node*)new_plan->planTree, candidates );

		plannedStmtGlobal = NULL;
		t_stop( tMarkUsedCands );
	}

	/* Remove unused candidates from the list. */
	for( prev = NULL, cell = list_head(candidates);
			cell != NULL;
			cell = next )
	{
		IndexCandidate *cand = (IndexCandidate*)lfirst( cell );

		next = lnext( cell );

		if( !cand->idxused )
		{
			pfree( cand );
			candidates = list_delete_cell( candidates, cell, prev );
		}
		else
			prev = cell;
	}

	/* update the global var */
	index_candidates = candidates;

	/* log the candidates used by the planner */
	log_candidates( "Used candidates", candidates );

	if( list_length( candidates ) > 0 )
		saveCandidates = true;

	/* calculate the share of cost saved by each index */
	if( saveCandidates )
	{
		int4 totalSize = 0;
		IndexCandidate *cand;

		foreach( cell, candidates )
			totalSize += ((IndexCandidate*)lfirst( cell ))->pages;

		foreach( cell, candidates )
		{
			cand = (IndexCandidate*)lfirst( cell );

			cand->benefit = (float4)totalCostSaved
							* ((float4)cand->pages/totalSize);
		}
	}

	/* Print the new plan if debugging. */
	if( saveCandidates && Debug_print_plan )
		elog_node_display( DEBUG1, "plan (using Index Adviser)",
							new_plan, Debug_pretty_print );

	/* If called from the EXPLAIN hook, make a copy of the plan to be passed back */
	if( saveCandidates && doingExplain )
	{
		MemoryContext oldContext = MemoryContextSwitchTo( outerContext );

		new_plan = copyObject( new_plan );

		MemoryContextSwitchTo( oldContext );
	}
	else
	{
		/* TODO: try to free the new plan node */
		new_plan = NULL;
	}
#if CREATE_V_INDEXES
	/*
	 * Undo the metadata changes; for eg. pg_depends entries will be removed
	 * (from our MVCC view).
	 *
	 * Again: DO NOT access any data-structure allocated between BEGIN/ROLLBACK
	 * transaction, after the ROLLBACK! All the memory allocated after BEGIN is
	 * freed in ROLLBACK.
	 */
	RollbackAndReleaseCurrentSubTransaction();

	/* restore the resource-owner */
	CurrentResourceOwner = oldResourceOwner;

	if( SPI_finish() != SPI_OK_FINISH )
		elog( WARNING, "IND ADV: SPI_finish failed." );
#endif
	/* save the advise into the table */
	if( saveCandidates )
	{
		t_start( tSaveAdvise );

		/* catch any ERROR */
		PG_TRY();
		{
			save_advice(candidates);
		}
		PG_CATCH();
		{
			/* reset our 'running' state... */
			--SuppressRecursion;

			/*
			 * Add a detailed explanation to the ERROR. Note that these function
			 * calls will overwrite the DETAIL and HINT that are already
			 * associated (if any) with this ERROR. XXX consider errcontext().
			 */
			errdetail( IND_ADV_ERROR_DETAIL );
			errhint( IND_ADV_ERROR_HINT );

			/* ... and re-throw the ERROR */
			PG_RE_THROW();
		}
		PG_END_TRY();

		t_stop( tSaveAdvise );
	}

	/* free the candidate-list */
	elog( DEBUG1, "IND ADV: Deleting candidate list." );
	if( !saveCandidates || !doingExplain )
	{
		foreach( cell, index_candidates )
			pfree( (IndexCandidate*)lfirst( cell ) );

		list_free( index_candidates );

		index_candidates = NIL;
	}

	t_stop( tAdviser );

	/* emit debug info */
	elog( DEBUG1, "IND ADV: old cost %.2f..%.2f", actualStartupCost,
													actualTotalCost );
	elog( DEBUG1, "IND ADV: new cost %.2f..%.2f", newStartupCost, newTotalCost);
	elog( DEBUG1, "IND ADV: cost saved %.2f..%.2f, these are %lu..%lu percent",
					startupCostSaved,
					totalCostSaved,
					(unsigned long)startupGainPerc,
					(unsigned long)totalGainPerc );

	/* print profiler information */
	elog( DEBUG2, "IND ADV: [Prof] * Query String           : %s",
					debug_query_string );
	elog( DEBUG2, "IND ADV: [Prof] * indexAdviser           : %10lu usec",
					tAdviser.usec );
	elog( DEBUG2, "IND ADV: [Prof] |-- replanning           : %10lu usec",
					tRePlan.usec );
	elog( DEBUG2, "IND ADV: [Prof] |-- getBTreeOperators    : %10lu usec",
					tBTreeOperators.usec );
	elog( DEBUG2, "IND ADV: [Prof] |-- scanQuery            : %10lu usec",
					tGenCands.usec );
	elog( DEBUG2, "IND ADV: [Prof] |-- scanPlan             : %10lu usec",
					tMarkUsedCands.usec );
	elog( DEBUG2, "IND ADV: [Prof] |-- createVirtualIndexes : %10lu usec",
					tCreateVInds.usec );
	elog( DEBUG2, "IND ADV: [Prof] |-- dropVirtualIndexes : %10lu usec",
					tDropVInds.usec );
	elog( DEBUG2, "IND ADV: [Prof] |-- saveAdviseToCatalog  : %10lu usec",
					(  saveCandidates == true ) ? tSaveAdvise.usec : 0 );
	elog( DEBUG2, "IND ADV: [Prof] |-- log_candidates       : %10lu usec",
					tLogCandidates.usec );

DoneCleanly:
	/* allow new calls to the index-adviser */
	--SuppressRecursion;

	elog( DEBUG3, "IND ADV: EXIT" );

	return doingExplain && saveCandidates ? new_plan : NULL;
}

/*
 * This callback is registered immediately upon loading this plugin. It is
 * responsible for taking over control from the planner.
 *
 *     It calls the standard planner and sends the resulting plan to
 * index_adviser() for comparison with a plan generated after creating
 * hypothetical indexes.
 */
static PlannedStmt*
planner_callback(	Query*			query,
					int				cursorOptions,
					ParamListInfo	boundParams)
{
	Query	*queryCopy;
	PlannedStmt *actual_plan;
	PlannedStmt *new_plan;

	resetSecondaryHooks();

	/* TODO : try to avoid making a copy if the index_adviser() is not going
	 * to use it; Index Adviser may not use the query copy at all if we are
	 * running in BootProcessing mode, or if the Index Adviser is being called
	 * recursively.
	 */

	/* planner() scribbles on it's input, so make a copy of the query-tree */
	queryCopy = copyObject( query );

	/* Generate a plan using the standard planner */
	actual_plan = standard_planner( query, cursorOptions, boundParams );

	/* send the actual plan for comparison with a hypothetical plan */
	new_plan = index_adviser( queryCopy, cursorOptions, boundParams,
								actual_plan, false );

	/* TODO: try to free the redundant new_plan */

	return actual_plan;
}

/*
 * This callback is registered immediately upon loading this plugin. It is
 * responsible for taking over control from the ExplainOneQuery() function.
 *
 *     It calls the standard planner and sends the resultant plan to
 * index_adviser() for comparison with a plan generated after creating
 * hypothetical indexes.
 *
 *     If the index_adviser() finds the hypothetical plan to be beneficial
 * than the real plan, it returns the hypothetical plan's copy so that this
 * hook can send it to the log.
 */
static void
ExplainOneQuery_callback(	Query			*query,
							ExplainStmt		*stmt,
							const char		*queryString,
							ParamListInfo	params,
							TupOutputState	*tstate)
{
	Query		*queryCopy;
	PlannedStmt	*actual_plan;
	PlannedStmt	*new_plan;
	ListCell	*cell;

	resetSecondaryHooks();

	/* planner() scribbles on it's input, so make a copy of the query-tree */
	queryCopy = copyObject( query );

	/* plan the query */
	actual_plan = standard_planner( query, 0, params );

	/* run it (if needed) and produce output */
	ExplainOnePlan( actual_plan, params, stmt, tstate );

	/* re-plan the query */
	new_plan = index_adviser( queryCopy, 0, params, actual_plan, true );

	if ( new_plan )
	{
		bool analyze = stmt->analyze;

		stmt->analyze = false;

	    explain_get_index_name_hook = explain_get_index_name_callback;

		do_text_output_oneline(tstate, ""); /* separator line */
		do_text_output_oneline(tstate, "** Plan with hypothetical indexes **");
		ExplainOnePlan( new_plan, params, stmt, tstate );

	    explain_get_index_name_hook = NULL;

	    stmt->analyze = analyze;
	}

	/* The candidates might not have been destroyed by the Index Adviser, do it
	 * now. FIXME: this block belongs inside the 'if ( new_plan )' block above. */
	foreach( cell, index_candidates )
		pfree( (IndexCandidate*)lfirst( cell ) );

	list_free( index_candidates );

	index_candidates = NIL;

	/* TODO: try to free the now-redundant new_plan */
}

/*
 * get_relation_info() calls this callback after it has prepared a RelOptInfo
 * for a relation.
 *
 *     The Job of this callback is to fill in the information about the virtual
 * index, that get_rel_info() could not load from the catalogs. As of now, the
 * number of disk-pages that might be occupied by the virtual index (if created
 * on-disk), is the only information that needs to be updated.
 */
static void
get_relation_info_callback(	PlannerInfo	*root,
							Oid			relationObjectId,
							bool		inhparent,
							RelOptInfo	*rel)
{
#if CREATE_V_INDEXES
	ListCell *cell1;
	IndexCandidate *cand;

	foreach( cell1, rel->indexlist )
	{
		IndexOptInfo *info = (IndexOptInfo*)lfirst( cell1 );

		/* We call estimate_index_pages() here, instead of immediately after
		 * index_create() API call, since rel has been run through
		 * estimate_rel_size() by the caller!
		 */

		if( is_virtual_index( info->indexoid, &cand ) )
		{
			/* estimate the size */
			cand->pages = estimate_index_pages(cand->reloid, cand->idxoid);

			info->pages = cand->pages;
		}
	}
#else
	/* This needs implimentation */
	compile_assert( false );
	ListCell *cell1;

	foreach( cell1, index_candidates )
	{
		IndexCandidate *cand = (IndexCandidate*)lfirst( cell1 );

		Form_pg_index index;
		IndexOptInfo *info;
		int			ncolumns;
		int			i;

		/*
		 * Extract info from the relation descriptor for the index.
		 */
		index = indexRelation->rd_index;

		info = makeNode(IndexOptInfo);

		info->indexoid = InvalidOid;
		info->rel = rel;
		info->ncolumns = ncolumns = cand->ncols;

		/*
		 * Allocate per-column info arrays.  To save a few palloc cycles
		 * we allocate all the Oid-type arrays in one request.  Note that
		 * the opfamily array needs an extra, terminating zero at the end.
		 * We pre-zero the ordering info in case the index is unordered.
		 */
		info->indexkeys = (int *) palloc(sizeof(int) * ncolumns);
		info->opfamily = (Oid *) palloc0(sizeof(Oid) * (4 * ncolumns + 1));
		info->opcintype = info->opfamily + (ncolumns + 1);
		info->fwdsortop = info->opcintype + ncolumns;
		info->revsortop = info->fwdsortop + ncolumns;
		info->nulls_first = (bool *) palloc0(sizeof(bool) * ncolumns);

		for (i = 0; i < ncolumns; i++)
		{
			info->indexkeys[i] = cand->varattno[i];
			info->opfamily[i] = InvalidOid;
			info->opcintype[i] = InvalidOid;
		}

		info->relam = InvalidOid;
		info->amcostestimate = InvalidOid;
		info->amoptionalkey = false;
		info->amsearchnulls = false;

		for (i = 0; i < ncolumns; i++)
		{
			if (opt & INDOPTION_DESC)
			{
				fwdstrat = BTGreaterStrategyNumber;
				revstrat = BTLessStrategyNumber;
			}
			else
			{
				fwdstrat = BTLessStrategyNumber;
				revstrat = BTGreaterStrategyNumber;
			}
			/*
			 * Index AM must have a fixed set of strategies for it
			 * to make sense to specify amcanorder, so we
			 * need not allow the case amstrategies == 0.
			 */
			if (fwdstrat > 0)
			{
				Assert(fwdstrat <= nstrat);
				info->fwdsortop[i] = indexRelation->rd_operator[i * nstrat + fwdstrat - 1];
			}
			if (revstrat > 0)
			{
				Assert(revstrat <= nstrat);
				info->revsortop[i] = indexRelation->rd_operator[i * nstrat + revstrat - 1];
			}
			info->nulls_first[i] = (opt & INDOPTION_NULLS_FIRST) != 0;
		}

		/*
		 * Fetch the index expressions and predicate, if any.  We must
		 * modify the copies we obtain from the relcache to have the
		 * correct varno for the parent relation, so that they match up
		 * correctly against qual clauses.
		 */
		info->indexprs = RelationGetIndexExpressions(indexRelation);
		info->indpred = RelationGetIndexPredicate(indexRelation);
		if (info->indexprs && varno != 1)
			ChangeVarNodes((Node *) info->indexprs, 1, varno, 0);
		if (info->indpred && varno != 1)
			ChangeVarNodes((Node *) info->indpred, 1, varno, 0);
		info->predOK = false;		/* set later in indxpath.c */
		info->unique = false;

		/*
		 * Estimate the index size.  If it's not a partial index, we lock
		 * the number-of-tuples estimate to equal the parent table; if it
		 * is partial then we have to use the same methods as we would for
		 * a table, except we can be sure that the index is not larger
		 * than the table.
		 */
		if (info->indpred == NIL)
		{
			info->pages = RelationGetNumberOfBlocks(indexRelation);
			info->tuples = rel->tuples;
		}
		else
		{
			estimate_rel_size(indexRelation, NULL,
							  &info->pages, &info->tuples);
			if (info->tuples > rel->tuples)
				info->tuples = rel->tuples;
		}

		index_close(indexRelation, NoLock);

		indexinfos = lcons(info, indexinfos);
	}
#endif
}

/* Use this function to reset the hooks that are required to be registered only
 * for a short while; these may have been left registered by the previous call, in
 * case of an ERROR.
 */
static void
resetSecondaryHooks()
{
	get_relation_info_hook		= NULL;
	explain_get_index_name_hook	= NULL;
}

static bool
is_virtual_index( Oid oid, IndexCandidate **cand_out )
{
	ListCell *cell1;

	foreach( cell1, index_candidates )
	{
		IndexCandidate *cand = (IndexCandidate*)lfirst( cell1 );

		if( cand->idxoid == oid )
		{
			if( cand_out )
				*cand_out = cand;
			return true;
		}
	}

	return false;
}

static const char *
explain_get_index_name_callback(Oid indexId)
{
	StringInfoData buf;
	IndexCandidate *cand;

	if( is_virtual_index( indexId, &cand ) )
	{
		initStringInfo(&buf);

		appendStringInfo( &buf, "<V-Index>:%d", cand->idxoid );

		return buf.data;
	}

	return NULL;                            /* allow default behavior */
}

/**
 * save_advice
 *		for every candidate insert an entry into IND_ADV_TABL
 */
static void
save_advice( List* candidates )
{
	StringInfoData	query;	/* string for Query */
	StringInfoData	cols;	/* string for Columns */
	Oid				advise_oid;
	ListCell		*cell;

	elog( DEBUG3, "IND ADV: save_advice: ENTER" );

	Assert( list_length(candidates) != 0 );

	/*
	 * Minimal check: check that IND_ADV_TABL is at least visible to us. There
	 * are a lot more checks we should do in order to not let the INSERT fail,
	 * like permissions, datatype mis-match, etc., but we leave those checks
	 * upto the executor.
	 */

	/* find a relation named IND_ADV_TABL on the search path */
	advise_oid = RelnameGetRelid( IND_ADV_TABL );

	if (advise_oid != InvalidOid)
	{
#if 1
		Relation advise_rel = relation_open(advise_oid, AccessShareLock);

		if (advise_rel->rd_rel->relkind != RELKIND_RELATION
			&& advise_rel->rd_rel->relkind != RELKIND_VIEW)
		{
			relation_close(advise_rel, AccessShareLock);

			/* FIXME: add errdetail() and/or errcontext() calls here. */
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg( IND_ADV_ERROR_NTV )));
		}

		relation_close(advise_rel, AccessShareLock);
#else
		/*
		 * heap_open() makes sure that the oid does not represent an INDEX or a
		 * COMPOSITE type, else it will raise an ERROR, which is exactly what we
		 * want. The comments above heap_open() ask the caller not to assume any
		 * storage since the returned relation may be a VIEW; but we don't mind,
		 * since the user may have defined some rules on it to make the INSERTs
		 * work smoothly! If not, we leave it upto the executor to raise ERROR.
		 */
		PG_TRY();
		{
			heap_close(heap_open(advise_oid, AccessShareLock), AccessShareLock);
		}
		PG_CATCH();
		{
			errmsg( IND_ADV_ERROR_NTV );
			PG_RE_THROW();
		}
		PG_END_TRY();
#endif
	}
	else
	{
		/* FIXME: add errdetail() and/or errcontext() calls here. */
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg( IND_ADV_ERROR_NE )));
	}

	initStringInfo( &query );
	initStringInfo( &cols );

	foreach( cell, candidates )
	{
		int i;
		IndexCandidate* idxcd = (IndexCandidate*)lfirst( cell );

		if( !idxcd->idxused )
			continue;

		/* pfree() the memory allocated for the previous candidate. FIXME: Avoid
		 * meddling with the internals of a StringInfo, and try to use an API.
		 */
		if( cols.len > 0 )
		{
			pfree( cols.data );
			cols.data = NULL;
		}

		initStringInfo( &cols );

		for (i = 0; i < idxcd->ncols; ++i)
			appendStringInfo( &cols, "%s%d", (i>0?",":""), idxcd->varattno[i]);

		/* FIXME: Mention the column names explicitly after the table name. */
		appendStringInfo( &query, "insert into \""IND_ADV_TABL"\" values"
									"( %d, array[%s], %f, %d, %d, now());",
									idxcd->reloid,
									cols.data,
									idxcd->benefit,
									idxcd->pages * BLCKSZ/1024, /* in KBs */
									MyProcPid );
	} /* foreach cell in candidates */

	if( query.len > 0 )	/* if we generated any SQL */
	{
		if( SPI_connect() == SPI_OK_CONNECT )
		{
			if( SPI_execute( query.data, false, 0 ) != SPI_OK_INSERT )
					elog( WARNING, "IND ADV: SPI_execute failed while saving advice." );

			if( SPI_finish() != SPI_OK_FINISH )
				elog( WARNING, "IND ADV: SPI_finish failed while saving advice." );
		}
		else
			elog( WARNING, "IND ADV: SPI_connect failed while saving advice." );
	}

	/* TODO: Propose to -hackers to introduce API to free a StringInfoData . */
	if ( query.len > 0 )
		pfree( query.data );

	if ( cols.len > 0 )
		pfree( cols.data );

	elog( DEBUG3, "IND ADV: save_advice: EXIT" );
}

/**
 * remove_irrelevant_candidates
 *
 * A candidate is irrelevant if it has one of the followingg properties:
 *
 * (a) it indexes an unsupported relation (system-relations or temp-relations)
 * (b) it matches an already present index.
 *
 * TODO Log the candidates as they are pruned, and remove the call to
 * log_candidates() in index_adviser() after this function is called.
 *
 * REALLY BIG TODO/FIXME: simplify this function.
 *
 */
static List*
remove_irrelevant_candidates( List* candidates )
{
	ListCell *cell = list_head(candidates);
	ListCell *prev = NULL;

	while(cell != NULL)
	{
		ListCell *old_cell = cell;

		Oid base_rel_oid = ((IndexCandidate*)lfirst( cell ))->reloid;
		Relation base_rel = heap_open( base_rel_oid, AccessShareLock );

		/* decide if the relation is unsupported. This check is now done before
		 * creating a candidate in scan_generic_node(); but still keeping the
		 * code here.
		 */
		if((base_rel->rd_istemp == true)
			|| IsSystemRelation(base_rel))
		{
			ListCell *cell2;
			ListCell *prev2;
			ListCell *next;

			/* remove all candidates that are on currently unsupported relations */
			elog( DEBUG1,
					"Index candidate(s) on an unsupported relation (%d) found!",
					base_rel_oid );

			/* Remove all candidates with same unsupported relation */
			for(cell2 = cell, prev2 = prev; cell2 != NULL; cell2 = next)
			{
				next = lnext(cell2);

				if(((IndexCandidate*)lfirst(cell2))->reloid == base_rel_oid)
				{
					pfree((IndexCandidate*)lfirst(cell2));
					candidates = list_delete_cell( candidates, cell2, prev2 );

					if(cell2 == cell)
						cell = next;
				}
				else
				{
					prev2 = cell2;
				}
			}
		}
		else
		{
			/* Remove candidates that match any of already existing indexes.
			 * The prefix old_ in these variables means 'existing' index
			*/

			/* get all index Oids */
			ListCell	*index_cell;
			List		*old_index_oids = RelationGetIndexList( base_rel );

			foreach( index_cell, old_index_oids )
			{
				/* open index relation and get the index info */
				Oid			old_index_oid	= lfirst_oid( index_cell );
				Relation	old_index_rel	= index_open( old_index_oid,
															AccessShareLock );
				IndexInfo	*old_index_info	= BuildIndexInfo( old_index_rel );

				/* We ignore expressional indexes and partial indexes */
				if( old_index_rel->rd_index->indisvalid
					&& old_index_info->ii_Expressions == NIL
					&& old_index_info->ii_Predicate == NIL )
				{
					ListCell *cell2;
					ListCell *prev2;
					ListCell *next;

					Assert( old_index_info->ii_Expressions == NIL );
					Assert( old_index_info->ii_Predicate == NIL );

					/* search for a matching candidate */
					for(cell2 = cell, prev2 = prev;
						cell2 != NULL;
						cell2 = next)
					{next = lnext(cell2);{ /* FIXME: move this line to the block below; it doesn't need to be here. */

						IndexCandidate* cand = (IndexCandidate*)lfirst(cell2);

						signed int cmp = (signed int)cand->ncols
											- old_index_info->ii_NumIndexAttrs;

						if(cmp == 0)
						{
							int i = 0;
							do
							{
								cmp =
									cand->varattno[i]
									- old_index_info->ii_KeyAttrNumbers[i];
								++i;
							/* FIXME: should this while condition be: cmp==0&&(i<min(ncols,ii_NumIndexAttrs))
 							 * maybe this is to eliminate candidates that are a prefix match of an existing index. */
							} while((cmp == 0) && (i < cand->ncols));
						}

						if(cmp != 0)
						{
							/* current candidate does not match the current
							 * index, so go to next candidate.
							 */
							prev2 = cell2;
						}
						else
						{
							elog( DEBUG1,
									"A candidate matches the index oid of : %d;"
										"hence ignoring it.",
							 		old_index_oid );

							/* remove the candidate from the list */
							candidates = list_delete_cell(candidates,
															cell2, prev2);
							pfree( cand );

							/* If we just deleted the current node of the outer-most loop, fix that. */
							if (cell2 == cell)
								cell = next;

							break;	/* while */
						}
					}} /* for */
				}

				/* close index relation and free index info */
				index_close( old_index_rel, AccessShareLock );
				pfree( old_index_info );
			}

			/* free the list of existing index Oids */
			list_free( old_index_oids );

			/* clear the index-list, else the planner can not see the
			 * virtual-indexes
			 * TODO: Really?? Verify this.
			 */
			base_rel->rd_indexlist  = NIL;
			base_rel->rd_indexvalid = 0;
		}

		/* close the relation */
		heap_close( base_rel, AccessShareLock );

		/*
		 * Move the pointer forward, only if the crazy logic above did not do it
		 * else, cell is already pointing to a new list-element that needs
		 * processing
		 */
		if(cell == old_cell)
		{
			prev = cell;
			cell = lnext(cell);
		}
	}

	return candidates;
}

/**
 * mark_used_candidates
 *    runs thru the plan to find virtual indexes used by the planner
 */
static void
mark_used_candidates(const Node* const node, List* const candidates)
{
	const ListCell	*cell;
	bool			planNode = true;	/* assume it to be a plan node */

	elog( DEBUG3, "IND ADV: mark_used_candidates: ENTER" );

	switch( nodeTag( node ) )
	{
		/* if the node is an indexscan */
		case T_IndexScan:
		{
			/* are there any used virtual-indexes? */
			const IndexScan* const idxScan = (const IndexScan*)node;

			foreach( cell, candidates )
			{

				/* is virtual-index-oid in the IndexScan-list? */
				IndexCandidate* const idxcd = (IndexCandidate*)lfirst( cell );
				const bool used = (idxcd->idxoid == idxScan->indexid);

				/* connect the existing value per OR */
				idxcd->idxused = (idxcd->idxused || used);
			}
		}
		break;

		/* if the node is a bitmap-index-scan */
		case T_BitmapIndexScan:
		{
			/* are there any used virtual-indexes? */
			const BitmapIndexScan* const bmiScan = (const BitmapIndexScan*)node;

			foreach( cell, candidates )
			{
				/* is virtual-index-oid in the BMIndexScan-list? */
				IndexCandidate* const idxcd = (IndexCandidate*)lfirst( cell );

				const bool used = idxcd->idxoid == bmiScan->indexid;

				/* conntect the existing value per OR */
				idxcd->idxused = idxcd->idxused || used;
			}
		}
		break;

		/* if the node is a bitmap-and */
		case T_BitmapAnd:
		{
			/* are there any used virtual-indexes? */
			const BitmapAnd* const bmiAndScan = (const BitmapAnd*)node;

			foreach( cell, bmiAndScan->bitmapplans )
				mark_used_candidates( (Node*)lfirst( cell ), candidates );
		}
		break;

		/* if the node is a bitmap-or */
		case T_BitmapOr:
		{
			/* are there any used virtual-indexes? */
			const BitmapOr* const bmiOrScan = (const BitmapOr*)node;

			foreach( cell, bmiOrScan->bitmapplans )
				mark_used_candidates( (Node*)lfirst( cell ), candidates );
		}
		break;

		case T_SubqueryScan:
		{
			/* scan subqueryplan */
			const SubqueryScan* const subScan = (const SubqueryScan*)node;

			mark_used_candidates( (const Node*)subScan->subplan, candidates );
		}
		break;

		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
		case T_Join:
		{
			/* scan join-quals */
			const Join* const join = (const Join*)node;

			foreach( cell, join->joinqual )
			{
				const Node* const qualPlan = (const Node*)lfirst( cell );
				mark_used_candidates( qualPlan, candidates );
			}
		}
		break;

		case T_OpExpr:
		{
			const OpExpr* const expr = (const OpExpr*)node;

			planNode = false;

			foreach( cell, expr->args )
				mark_used_candidates( (const Node*)lfirst( cell ), candidates );
		}
		break;

		case T_SubPlan:
		{
			/* scan the subplan */
			const SubPlan* const subPlan = (const SubPlan*)node;

			planNode = false;

			mark_used_candidates( (const Node*)&plannedStmtGlobal->subplans[subPlan->plan_id], candidates );
		}
		break;

		case T_BoolExpr:
		{
			const BoolExpr* const expr = (const BoolExpr*)node;

			planNode = false;

			foreach( cell, expr->args )
			{
				const Node* const nodeBool = (const Node*)lfirst( cell );
				mark_used_candidates( nodeBool, candidates );
			}
		}
		break;

		case T_FunctionScan:
		case T_Result:
		case T_Append:
		case T_TidScan:
		case T_Material:
		case T_Sort:
		case T_Group:
		case T_Agg:
		case T_Unique:
		case T_Hash:
		case T_SetOp:
		case T_Limit:
		case T_Scan:
		case T_SeqScan:
		case T_BitmapHeapScan:
		break;

		case T_FuncExpr:
		case T_Const:
		case T_Var:
			planNode = false;
		break;

		/* report parse-node types that we missed */
		default:
		{
			elog( NOTICE, "IND ADV: unhandled plan-node type: %d; Query: %s\n",
					(int)nodeTag( node ), debug_query_string );
			planNode = false;	/* stop scanning the tree here */
		}
		break;
	}

	if( planNode )
	{
		const Plan* const plan = (Plan *) node;

		if( plan->initPlan )
		{
			ListCell *cell;

			foreach( cell, ((Plan*)node)->initPlan )
			{
				SubPlan *subPlan = (SubPlan*)lfirst( cell );

				mark_used_candidates( (Node*)exec_subplan_get_plan(
															plannedStmtGlobal,
															subPlan),
										candidates );
			}
		}

		if( IsA(((Node*)plan), Append) )
		{
			Append	*appendplan = (Append *)node;
			ListCell *cell;

			foreach( cell, appendplan->appendplans )
			{
				Plan *child = (Plan*)lfirst( cell );

				mark_used_candidates( (Node*)child, candidates );
			}
		}


		/* scan left- and right-tree */
		if( outerPlan(plan) )
			mark_used_candidates( (const Node*)outerPlan(plan), candidates );

		if( innerPlan(plan) )
			mark_used_candidates( (const Node*)innerPlan(plan), candidates );

		/* walk through the qual-list */
		foreach( cell, plan->qual )
		{
			const Node* const nodeQual = (const Node*)lfirst( cell );
			mark_used_candidates( nodeQual, candidates );
		}
	}

	elog( DEBUG3, "IND ADV: mark_used_candidates: EXIT" );
}

/**
 * scan_query
 *    Runs thru the whole query to find columns to create index candidates.
 */
static List*
scan_query(	const Query* const query,
					List* const opnos,
					List* rangeTableStack )
{
	const ListCell*	cell;
	List*		candidates		= NIL;
	List*		newCandidates	= NIL;

	elog( DEBUG3, "IND ADV: scan_query: ENTER" );

	/* add the current rangetable to the stack */
	rangeTableStack = lcons( query->rtable, rangeTableStack );

	/* scan sub-queries */
	foreach( cell, query->rtable )
	{
		const RangeTblEntry* const rte = (const RangeTblEntry*)lfirst( cell );

		if( rte->subquery )
		{
			candidates = merge_candidates( candidates, scan_query(
															rte->subquery,
															opnos,
															rangeTableStack));
		}
	}

	/* scan "where" from the current query */
	if( query->jointree->quals != NULL )
	{
		newCandidates = scan_generic_node(	query->jointree->quals, opnos,
											rangeTableStack );
	}

	/* FIXME: Why don't we consider the GROUP BY and ORDER BY clause
	 * irrespective of whether we found candidates in WHERE clause?
	 */

	/* if no indexcadidate found in "where", scan "group" */
	if( ( newCandidates == NIL ) && ( query->groupClause != NULL ) )
	{
		newCandidates = scan_group_clause(	query->groupClause,
											query->targetList,
											opnos,
											rangeTableStack );
	}

	/* if no indexcadidate found in "group", scan "order by" */
	if( ( newCandidates == NIL ) && ( query->sortClause != NULL ) )
	{

		/* As of now the order-by and group-by clauses use the same C-struct.
		 * A rudimentary check to confirm this:
		 */
		compile_assert( sizeof(*query->groupClause) == sizeof(*query->sortClause) );

		newCandidates = scan_group_clause(	query->sortClause,
											query->targetList,
											opnos,
											rangeTableStack );
	}

	/* remove the current rangetable from the stack */
	rangeTableStack = list_delete_ptr( rangeTableStack, query->rtable );

	/* merge indexcandiates */
	candidates = merge_candidates( candidates, newCandidates );

	elog( DEBUG3, "IND ADV: scan_query: EXIT" );

	return candidates;
}

/**
 * scan_group_clause
 *    Runs thru the GROUP BY clause looking for columns to create index candidates.
 */
static List*
scan_group_clause(	List* const groupList,
								List* const targetList,
								List* const opnos,
								List* const rangeTableStack )
{
	const ListCell*	cell;
	List*		candidates = NIL;

	elog( DEBUG3, "IND ADV: scan_group_clause: ENTER" );

	/* scan every entry in the group-list */
	foreach( cell, groupList )
	{
		/* convert to group-element */
		const GroupClause* const groupElm = (const GroupClause*)lfirst( cell );

		/* get the column the group-clause is for */
		const TargetEntry* const targetElm = list_nth( targetList,
												groupElm->tleSortGroupRef - 1);

		/* scan the node and get candidates */
		const Node* const node = (const Node*)targetElm->expr;

		candidates = merge_candidates( candidates, scan_generic_node( node,
															opnos,
															rangeTableStack));
	}

	elog( DEBUG3, "IND ADV: scan_group_clause: EXIT" );

	return candidates;
}

/**
 * scan_generic_node
 *    Runs thru the given Node looking for columns to create index candidates.
 */
static List*
scan_generic_node(	const Node* const root,
							List* const opnos,
							List* const rangeTableStack )
{
	ListCell*		cell;
	List*			candidates = NIL;

	elog( DEBUG3, "IND ADV: scan_generic_node: ENTER" );

	Assert( root != NULL );

	switch( nodeTag( root ) )
	{
		/* if the node is an aggregate */
		case T_Aggref:
		{
			const Aggref* const aggref = (const Aggref*)root;
			const Node* const   list   = (const Node*)aggref->args;

			/* The arg list may be NIL in case of count(*) */
			if( list != NULL )
				candidates = scan_generic_node( list, opnos, rangeTableStack );
		}
		break;

		/* if the node is a boolean-expression */
		case T_BoolExpr:
		{
			const BoolExpr* const expr = (const BoolExpr*)root;

			if( expr->boolop != AND_EXPR )
			{
				/* non-AND expression */
				Assert( expr->boolop == OR_EXPR || expr->boolop == NOT_EXPR );

				foreach( cell, expr->args )
				{
					const Node* const node = (const Node*)lfirst( cell );
					candidates = merge_candidates( candidates,
											scan_generic_node( node, opnos,
															rangeTableStack));
				}
			}
			else
			{
				/* AND expression */
				List* compositeCandidates = NIL;

				foreach( cell, expr->args )
				{
					const Node* const node = (const Node*)lfirst( cell );
					List	*icList; /* Index candidate list */
					List	*cicList; /* Composite index candidate list */

					icList	= scan_generic_node( node, opnos, rangeTableStack );

					cicList = build_composite_candidates(candidates, icList);

					candidates = merge_candidates(candidates, icList);

					compositeCandidates = merge_candidates(compositeCandidates,
															cicList);
				}

				/* now append the composite (multi-col) indexes to the list */
				candidates = merge_candidates(candidates, compositeCandidates);
			}
		}
		break;

		/* if the node is list of other nodes (e.g. group-by expressions) */
		case T_List:
		{
			List* const list = (List*)root;
			foreach( cell, list )
			{
				const Node* const node = (const Node*)lfirst( cell );

				candidates = merge_candidates( candidates,
											scan_generic_node( node, opnos,
															rangeTableStack));
			}
		}
		break;
		/* if the node is an operator */
		case T_OpExpr:
		{
			/* get candidates if operator is supported */
			const OpExpr* const expr = (const OpExpr*)root;

			if( list_member_oid( opnos, expr->opno ) )
			{
				foreach( cell, expr->args )
				{
					const Node* const node = (const Node*)lfirst( cell );

					candidates = merge_candidates( candidates,
											scan_generic_node( node, opnos,
															rangeTableStack));
				}
			}
		}
		break;

		/* if this case is reached, the variable is an index-candidate */
		case T_Var:
		{
			const Var* const expr = (const Var*)root;
			List* rt = list_nth( rangeTableStack, expr->varlevelsup );
			const RangeTblEntry* rte = list_nth( rt, expr->varno - 1 );

			/* only relations have indexes */
			if( rte->rtekind == RTE_RELATION )
			{
				Relation base_rel = heap_open( rte->relid, AccessShareLock );

				/* We do not support catalog tables and temporary tables */
				if( base_rel->rd_istemp != true
					&& !IsSystemRelation(base_rel)
					/* and don't recommend indexes on hidden/system columns */
					&& expr->varattno > 0
					/* and it should have at least two tuples */
					//TODO: Do we really need these checks?
					&& base_rel->rd_rel->relpages > 1
					&& base_rel->rd_rel->reltuples > 1 )
				{
					/* create index-candidate and build a new list */
					int				i;
					IndexCandidate	*cand = (IndexCandidate*)palloc0(
														sizeof(IndexCandidate));

					cand->varno         = expr->varno;
					cand->varlevelsup   = expr->varlevelsup;
					cand->ncols         = 1;
					cand->reloid        = rte->relid;
					cand->idxused       = false;

					cand->vartype[ 0 ]  = expr->vartype;
					cand->varattno[ 0 ] = expr->varattno;

					/*FIXME: Do we really need this loop? palloc0 and ncols,
					 * above, should have taken care of this!
					 */
					for( i = 1; i < INDEX_MAX_KEYS; ++i )
						cand->varattno[i] = 0;

					candidates = list_make1( cand );
				}

				heap_close( base_rel, AccessShareLock );
			}
		}
		break;

		/* subquery in where-clause */
		case T_SubLink:
		{
			/* convert it to sublink-expression */
			const SubLink* const expr = (const SubLink*)root;

			candidates = scan_generic_node( expr->subselect, opnos,
												rangeTableStack );

			/* scan lefthand expression (if any); [NOT] EXISTS operators do not have it */
			if( expr->testexpr )
				candidates = merge_candidates(candidates,
										scan_generic_node(	expr->testexpr,
															opnos,
															rangeTableStack));
		}
		break;

		case T_RelabelType:
		{
			const RelabelType*	const	relabeltype = (const RelabelType*)root;
			const Node* const	node	= (const Node*)relabeltype->arg;

			candidates = scan_generic_node( node, opnos, rangeTableStack );
		}
		break;

		/* Query found */
		case T_Query:
		{
			const Query* const query = (const Query*)root;

			candidates = scan_query( query, opnos, rangeTableStack );
		}
		break;

		/* ignore some types */
		case T_FuncExpr:
		case T_Param:
		case T_Const:
			break;

		/* report non-considered parse-node types */
		default:
			elog( NOTICE, "IND ADV: unhandled parse-node type: %d; Query: %s\n",
						(int)nodeTag( root ), debug_query_string );
			break;
	}

	elog( DEBUG3, "IND ADV: scan_generic_node: EXIT" );

	return candidates;
}



/**
 * compare_candidates
 */
static int
compare_candidates( const IndexCandidate* ic1,
				   const IndexCandidate* ic2 )
{
	int result = (signed int)ic1->reloid - (signed int)ic2->reloid;

	if( result == 0 )
	{
		result = ic1->ncols - ic2->ncols;

		if( result == 0 )
		{
			int i = 0;

			do
			{
				result = ic1->varattno[ i ] - ic2->varattno[ i ];
				++i;
			} while( ( result == 0 ) && ( i < ic1->ncols ) );
		}
	}

	return result;
}

/**
 * log_candidates
 */
static void
log_candidates( const char* prefix, List* list )
{
	ListCell		*cell;
	StringInfoData	str;/* output string */

	/* don't do anything unless we are going to log it */
	if( log_min_messages < DEBUG1 )
		return;

	t_continue( tLogCandidates );

	initStringInfo( &str );

	foreach( cell, list )
	{
		int i;
		const IndexCandidate* const cand = (IndexCandidate*)lfirst( cell );

		appendStringInfo( &str, " %d_(", cand->reloid );

		for( i = 0; i < cand->ncols; ++i )
			appendStringInfo( &str, "%s%d", (i>0?",":""), cand->varattno[ i ] );

		appendStringInfo( &str, ")%c", ((lnext( cell ) != NULL)?',':' ') );
	}

	elog( DEBUG1, "IND ADV: %s: |%d| {%s}", prefix, list_length(list),
			str.len ? str.data : "" );

	if( str.len > 0 ) pfree( str.data );

	t_stop( tLogCandidates );
}

/**
 * merge_candidates
 * 		It builds new list out of passed in lists, and then frees the two lists.
 *
 * This function maintains order of the candidates as determined by
 * compare_candidates() function.
 */
static List*
merge_candidates( List* list1, List* list2 )
{
	List *ret;
	ListCell *cell1;
	ListCell *cell2;
	ListCell *prev2;

	if( list_length( list1 ) == 0 && list_length( list2 ) == 0 )
		return NIL;

	elog( DEBUG3, "IND ADV: merge_candidates: ENTER" );

	/* list1 and list2 are assumed to be sorted in ascending order */

	elog( DEBUG1, "IND ADV: ---merge_candidates---" );
	log_candidates( "idxcd-list1", list1 );
	log_candidates( "idxcd-list2", list2 );

	if( list_length( list1 ) == 0 )
		return list2;

	if( list_length( list2 ) == 0 )
		return list1;

	ret = NIL;
	prev2 = NULL;

	for( cell1 = list_head(list1), cell2 = list_head(list2);
		(cell1 != NULL) && (cell2 != NULL); )
	{
		const int cmp = compare_candidates( (IndexCandidate*)lfirst( cell1 ),
											(IndexCandidate*)lfirst( cell2 ) );

		if( cmp <= 0 )
		{
			/* next candidate comes from list 1 */
			ret = lappend( ret, lfirst( cell1 ) );

			cell1 = lnext( cell1 );

			/* if we have found two identical candidates then we remove the
			 * candidate from list 2
			 */
			if( cmp == 0 )
			{
				ListCell *next = lnext( cell2 );

				pfree( (IndexCandidate*)lfirst( cell2 ) );
				list2 = list_delete_cell( list2, cell2, prev2 );

				cell2 = next;
			}
		}
		else
		{
			/* next candidate comes from list 2 */
			ret = lappend( ret, lfirst( cell2 ) );

			prev2 = cell2;
			cell2 = lnext( cell2 );
		}
	}

	/* Now append the leftovers from both the lists; only one of them should have any elements left */
	for( ; cell1; cell1 = lnext(cell1) )
		ret = lappend( ret, lfirst(cell1) );

	for( ; cell2; cell2 = lnext(cell2) )
		ret = lappend( ret, lfirst(cell2) );

	list_free( list1 );
	list_free( list2 );

	log_candidates( "merged-list", ret );

	elog( DEBUG3, "IND ADV: merge_candidates: EXIT" );

	return ret;
}

/**
 * build_composite_candidates.
 *
 * @param [IN] list1 is a sorted list of candidates in ascending order.
 * @param [IN] list2 is a sorted list of candidates in ascending order.
 *
 * @returns A new sorted list containing composite candidates.
 */
static List*
build_composite_candidates( List* list1, List* list2 )
{
	ListCell *cell1 = list_head( list1 );
	ListCell *cell2 = list_head( list2 );
	IndexCandidate *cand1;
	IndexCandidate *cand2;

	List* compositeCandidates = NIL;

	elog( DEBUG3, "IND ADV: build_composite_candidates: ENTER" );

	if( cell1 == NULL || cell2 == NULL )
		goto DoneCleanly;

	elog( DEBUG1, "IND ADV: ---build_composite_candidates---" );
	log_candidates( "idxcd-list1", list1 );
	log_candidates( "idxcd-list2", list2 );

		/* build list with composite candiates */
	while( ( cell1 != NULL ) && ( cell2 != NULL ) )
	{
		int cmp ;

		cand1 = ((IndexCandidate*)lfirst( cell1 ));
		cand2 = ((IndexCandidate*)lfirst( cell2 ));

		cmp = cand1->reloid - cand2->reloid;

		if( cmp != 0 )
		{
			Oid relOid;

			if( cmp < 0 )
			{
				/* advance in list 1 */
				relOid = cand2->reloid;

				do
					cell1 = lnext( cell1 );
				while( cell1 != NULL && (relOid > cand1->reloid));
			}
			else
			{
				/* advance in list 2 */
				relOid = cand1->reloid;

				do
					cell2 = lnext( cell2 );
				while( cell2 != NULL && ( relOid > cand2->reloid ));
			}
		}
		else
		{
			/* build composite candidates */
			Oid relationOid = ((IndexCandidate*)lfirst(cell1))->reloid;
			ListCell* l1b;

			do
			{
				cand2 = lfirst( cell2 );

				l1b = cell1;
				do
				{
					cand1 = lfirst( l1b );

					/* do not build a composite candidate if the number of
					 * attributes would exceed INDEX_MAX_KEYS
					*/
					if( ( cand1->ncols + cand2->ncols ) < INDEX_MAX_KEYS )
					{

						/* Check if candidates have any common attribute */
						int		i1, i2;
						bool	foundCommon = false;

						for(i1 = 0; i1 < cand1->ncols && !foundCommon; ++i1)
							for(i2 = 0; i2 < cand2->ncols && !foundCommon; ++i2)
								if(cand1->varattno[i1] == cand2->varattno[i2])
									foundCommon = true;

						/* build composite candidates if the previous test
						 * succeeded
						 */
						if( !foundCommon )
						{
							signed int cmp;

							/* composite candidate 1 is a combination of
							 * candidates 1,2 AND
							 * composite candidate 2 is a combination of
							 * candidates 2,1
							 */
							IndexCandidate* cic1
								= (IndexCandidate*)palloc(
													sizeof(IndexCandidate));
							IndexCandidate* cic2
								= (IndexCandidate*)palloc(
													sizeof(IndexCandidate));

							/* init some members of composite candidate 1 */
							cic1->varno			= -1;
							cic1->varlevelsup	= -1;
							cic1->ncols			= cand1->ncols + cand2->ncols;
							cic1->reloid		= relationOid;
							cic1->idxused		= false;

							/* init some members of composite candidate 2 */
							cic2->varno			= -1;
							cic2->varlevelsup	= -1;
							cic2->ncols			= cand1->ncols + cand2->ncols;
							cic2->reloid		= relationOid;
							cic2->idxused		= false;

							/* copy attributes of candidate 1 to attributes of
							 * composite candidates 1,2
							 */
							for( i1 = 0; i1 < cand1->ncols; ++i1)
							{
								cic1->vartype[ i1 ]
									= cic2->vartype[cand2->ncols + i1]
									= cand1->vartype[ i1 ];

								cic1->varattno[ i1 ]
									= cic2->varattno[cand2->ncols + i1]
									= cand1->varattno[ i1 ];
							}

							/* copy attributes of candidate 2 to attributes of
							 * composite candidates 2,1
							 */
							for( i1 = 0; i1 < cand2->ncols; ++i1)
							{
								cic1->vartype[cand1->ncols + i1]
									= cic2->vartype[ i1 ]
									= cand2->vartype[ i1 ];

								cic1->varattno[cand1->ncols + i1]
									= cic2->varattno[ i1 ]
									= cand2->varattno[ i1 ];
							}

							/* set remaining attributes to null */
							for( i1 = cand1->ncols + cand2->ncols;
									i1 < INDEX_MAX_KEYS;
									++i1 )
							{
								cic1->varattno[ i1 ] = 0;
								cic2->varattno[ i1 ] = 0;
							}

							/* add new composite candidates to list */
							cmp = compare_candidates(cic1, cic2);

							if( cmp == 0 )
							{
								compositeCandidates =
									merge_candidates( list_make1( cic1 ),
													compositeCandidates );
								pfree( cic2 );
							}
							else
							{
								List* l;

								if( cmp < 0 )
									l = lcons( cic1, list_make1( cic2 ) );
								else
									l = lcons( cic2, list_make1( cic1 ) );

								compositeCandidates =
									merge_candidates(l, compositeCandidates);
							}
						}
					}

					l1b = lnext( l1b );

				} while( ( l1b != NULL ) &&
					( relationOid == ((IndexCandidate*)lfirst( l1b ))->reloid));

				cell2 = lnext( cell2 );

			} while( ( cell2 != NULL ) &&
				( relationOid == ((IndexCandidate*)lfirst( cell2 ))->reloid ) );
			cell1 = l1b;
		}
	}

	log_candidates( "composite-l", compositeCandidates );

DoneCleanly:
	elog( DEBUG3, "IND ADV: build_composite_candidates: EXIT" );

	return compositeCandidates;
}

#if CREATE_V_INDEXES
/**
 * create_virtual_indexes
 *    creates an index for every entry in the index-candidate-list.
 *
 * It may delete some candidates from the list passed in to it.
 */
static List*
create_virtual_indexes( List* candidates )
{
	ListCell	*cell;					  /* an entry from the candidate-list */
	ListCell	*prev, *next;						 /* for list manipulation */
	char		idx_name[ 16 ];		/* contains the name of the current index */
	int			idx_count = 0;				   /* number of the current index */
	IndexInfo*	indexInfo;
	Oid			op_class[INDEX_MAX_KEYS];/* needed for creating indexes */

	elog( DEBUG3, "IND ADV: create_virtual_indexes: ENTER" );

	/* fill index-info */
	indexInfo = makeNode( IndexInfo );

	indexInfo->ii_Expressions		= NIL;
	indexInfo->ii_ExpressionsState	= NIL;
	indexInfo->ii_Predicate			= NIL;
	indexInfo->ii_PredicateState	= NIL;
	indexInfo->ii_Unique			= false;
	indexInfo->ii_Concurrent		= true;

	/* create index for every list entry */
	/* TODO: simplify the check condition of the loop; it is basically
	 * advancing the 'next' pointer, so maybe this will work
	 * (next=lnext(), cell()); Also, advance the 'prev' pointer in the loop
	 */
	for( prev = NULL, cell = list_head(candidates);
			(cell && (next = lnext(cell))) || cell != NULL;
			cell = next)
	{
		int			i;

		IndexCandidate* const cand = (IndexCandidate*)lfirst( cell );

		indexInfo->ii_NumIndexAttrs = cand->ncols;

		for( i = 0; i < cand->ncols; ++i )
		{
			/* prepare op_class[] */
			op_class[i] = GetDefaultOpClass( cand->vartype[ i ], BTREE_AM_OID );

			if( op_class[i] == InvalidOid )
				/* don't create this index if couldn't find a default operator*/
				break;

			/* ... and set indexed attribute number */
			indexInfo->ii_KeyAttrNumbers[i] = cand->varattno[i];
		}

		/* if we decided not to create the index above, try next candidate */
		if( i < cand->ncols )
		{
			candidates = list_delete_cell( candidates, cell, prev );
			continue;
		}

		/* generate indexname */
		/* FIXME: This index name can very easily collide with any other index
		 * being created simultaneously by other backend running index adviser.
		*/
		sprintf( idx_name, "idx_adv_%d", idx_count );

		/* create the index without data */
		cand->idxoid = index_create( cand->reloid, idx_name,
										InvalidOid, indexInfo, BTREE_AM_OID,
										InvalidOid, op_class, NULL, (Datum)0,
										false, false, false, true, false );

		elog( DEBUG1, "IND ADV: virtual index created: oid=%d name=%s size=%d",
					cand->idxoid, idx_name, cand->pages );

		/* increase count for the next index */
		++idx_count;
		prev = cell;
	}

	pfree( indexInfo );

	/* do CCI to make the new metadata changes "visible" */
	CommandCounterIncrement();

	elog( DEBUG3, "IND ADV: create_virtual_indexes: EXIT" );

	return candidates;
}
#endif

#if CREATE_V_INDEXES
/**
 * drop_virtual_indexes
 *    drops all virtual-indexes
 */
static void
drop_virtual_indexes( List* candidates )
{
	ListCell* cell;		/* a entry from the index-candidate-list */

	elog( DEBUG3, "IND ADV: drop_virtual_indexes: ENTER" );

	/* drop index for every list entry */
	foreach( cell, candidates )
	{
		/* TODO: have a look at implementation of index_drop! citation:
		 * "NOTE: this routine should now only be called through
		 * performDeletion(), else associated dependencies won't be cleaned up."
		 */

		/* disabling index_drop() call, since it acquires AccessExclusiveLock
		 * on the base table, and hence causing a deadlock when multiple
		 * clients are running the same query
		 */

/*		IndexCandidate* cand = (IndexCandidate*)lfirst( cell );

		index_drop( cand->idxoid );
		elog( DEBUG1, "IND ADV: virtual index dropped: oid=%d", cand->idxoid );
*/	}

	/* do CCI to make the new metadata changes "visible" */
	CommandCounterIncrement();

	elog( DEBUG3, "IND ADV: drop_virtual_indexes: EXIT" );
}
#endif

static int4
estimate_index_pages(Oid rel_oid, Oid ind_oid )
{
	Size	data_length;
	int		i;
	int		natts;
	int2	var_att_count;
	int4	rel_pages;						/* diskpages of heap relation */
	float4	rel_tuples;						/* tupes in the heap relation */
	double	idx_pages;					   /* diskpages in index relation */

	TupleDesc			ind_tup_desc;
	Relation			base_rel;
	Relation			index_rel;
	Form_pg_attribute	*atts;

	base_rel	= heap_open( rel_oid, AccessShareLock );
	index_rel	= index_open( ind_oid, AccessShareLock );

	rel_pages = base_rel->rd_rel->relpages;
	rel_tuples = base_rel->rd_rel->reltuples;

	ind_tup_desc = RelationGetDescr( index_rel );

	atts = ind_tup_desc->attrs;
	natts = ind_tup_desc->natts;

	/*
	 * These calculations are heavily borrowed from index_form_tuple(), and
	 * heap_compute_data_size(). The only difference is that, that they have a
	 * real tuple being inserted, and hence all the VALUES are available,
	 * whereas, we don't have any of them available here.
	 */

	/*
	 * First, let's calculate the contribution of fixed size columns to the size
	 * of index tuple
	 */
	var_att_count = 0;
	data_length = 0;
	for( i = 0; i < natts; ++i)
	{
		/* the following is based on att_addlength() macro */
		if( atts[i]->attlen > 0 )
		{
			/* No need to do +=; RHS is incrementing data_length by including it in the sum */
			data_length = att_align_nominal(data_length, atts[i]->attalign);

			data_length += atts[i]->attlen;
		}
		else if( atts[i]->attlen == -1 )
		{
			data_length += atts[i]->atttypmod + VARHDRSZ;
		}
		else
		{	/* null terminated data */
			Assert( atts[i]->attlen == -2 );
			++var_att_count;
		}
	}

	/*
	 * Now, estimate the average space occupied by variable-length columns, per
	 * tuple. This is calculated as:
	 *     Total 'available' space
	 *       minus space consumed by ItemIdData
	 *       minus space consumed by fixed-length columns
	 *
	 * This calculation is very version specific, so do it for every major release.
	 * TODO: Analyze it for at least 1 major release and document it (perhaps
	 * 			branch the code if it deviates to a later release).
	 */
	if( var_att_count )
		data_length += (((float)rel_pages * (BLCKSZ - (sizeof(PageHeaderData)
														- sizeof(ItemIdData)
							)				)			)
							- (rel_tuples * sizeof(ItemIdData))
							- (data_length * rel_tuples)
						)
						/rel_tuples;

	/* Take into account the possibility that we might have NULL values */
	data_length += IndexInfoFindDataOffset( INDEX_NULL_MASK );

	idx_pages = (rel_tuples * (data_length + sizeof(ItemIdData)))
				/((BLCKSZ - SizeOfPageHeaderData
						- sizeof(BTPageOpaqueData)
					)
					* ((float)BTREE_DEFAULT_FILLFACTOR/100));

	idx_pages = ceil( idx_pages );

	heap_close( base_rel, AccessShareLock );
	index_close( index_rel, AccessShareLock );

	return (int4)idx_pages;
}
