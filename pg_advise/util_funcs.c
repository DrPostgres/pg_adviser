/*
 * util_funcs.c
 *
 */

#include <stdlib.h>
#include <stdio.h>
#include "advise_index.h"

long compute_config_size(AdvIndexList index_list, int len)
{
	int size = 0;
	int i;

	for (i = 0; i < len; ++i)
		size += index_list[i]->size;

	return size;
}

/* Note: it sets the 'used' member */
void find_optimal_configuration_greedy(AdvIndexList index_list, int len,
										long size_limit)
{
	int current_size = 0;
	double sum_benefit = 0.0;
	int i = 0;

	for (i = 0; i < len && current_size <= size_limit; ++i)
		if (current_size + index_list[i]->size <= size_limit)
		{
			index_list[i]->used = true;
			sum_benefit += index_list[i]->benefit;
			current_size += index_list[i]->size;
		}
}

/* TODO: not called from anywhere. Find a use for it */
void find_optimal_configuration_dp(AdvIndexList index_list, int len,
									long size_limit)
{
	int **cost;
	int w, i;

	printf("w = %ld\n", size_limit);
	cost = (int **)malloc(sizeof(int) * (len + 1));

	for (i = 0; i < len+1; ++i)
		cost[i] = (int *)malloc(sizeof(int *) * (size_limit * 1));
	for (w = 0; w <= size_limit; ++w)
		cost[0][w] = 0;
	for (i = 1; i <= len; ++i)
	{
		cost[i][0] = 0;

		for (w = 1; w <= size_limit; ++w)
		{
			if (index_list[i-1]->size <= w)
			{
				if (index_list[i-1]->benefit
					+ cost[i-1][w - index_list[i-1]->size]
					> cost[i-1][w])
				{
					cost[i][w] = index_list[i-1]->benefit +
									cost[i-1][w - index_list[i-1]->size];
				}
				else
					cost[i][w] = cost[i-1][w];
			}
			else
				cost[i][w] = cost[i-1][w];
		}
	}

	for (	i = len, w = size_limit;
			i > 0 && w > 0;
			--i)
	{
		if (cost[i][w] != cost[i-1][w])
		{
			index_list[i-1]->used = true;
			w -= index_list[i-1]->size;
		}
	}
	/*
	for (i = 0; i < len+1; ++i)
		free(cost[i]);
	free(cost);
	*/
}

#if DEBUG
void test_optimize() {
  AdvIndexList conf = (AdvIndexInfo **)malloc(3 * sizeof(AdvIndexInfo *));

  conf[0] = (AdvIndexInfo *)malloc(sizeof(AdvIndexInfo));
  conf[0]->benefit = 60;
  conf[0]->size = 1;
  conf[0]->used = false;
  conf[1] = (AdvIndexInfo *)malloc(sizeof(AdvIndexInfo));
  conf[1]->benefit = 100;
  conf[1]->size = 2;
  conf[1]->used = false;
  conf[2] = (AdvIndexInfo *)malloc(sizeof(AdvIndexInfo));
  conf[2]->benefit = 120;
  conf[2]->size = 3;
  conf[2]->used = false;
  optimize_indexset(conf, 3, 5);
}
#endif
