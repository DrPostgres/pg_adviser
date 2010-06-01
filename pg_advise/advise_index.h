
#ifndef ADVISE_INDEX_H
#define ADVISE_INDEX_H

typedef unsigned char bool;
#define true	1
#define false	0

typedef struct {
	char	*table;
	char	*col_ids;	/* space saparated column numbers */
	int		size;		/* in KBs */
	double	benefit;
	bool	used;
} AdvIndexInfo;

typedef AdvIndexInfo** AdvIndexList;

extern long compute_config_size(AdvIndexList index_list, int len);

extern void find_optimal_configuration_greedy(AdvIndexList index_list, int len,
												long size_limit);

extern void find_optimal_configuration_dp(AdvIndexList index_list, int len,
											long size_limit);

#endif /* ADVISE_INDEX_H */
