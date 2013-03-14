/****************************************************************************\
 * LU decomposition - Cilk.
 * lu.cilk
 * Robert Blumofe
 *
 * Copyright (c) 1996, Robert Blumofe.  All rights reserved.
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
\****************************************************************************/
static const char *ident __attribute__((__unused__))
     = "$HeadURL: https://bradley.csail.mit.edu/svn/repos/cilk/5.4.3/examples/lu.cilk $ $LastChangedBy: sukhaj $ $Rev: 517 $ $Date: 2003-10-27 10:05:37 -0500 (Mon, 27 Oct 2003) $";

#include <stdlib.h>
#include <memory.h>
#include <stdio.h>
#include <math.h>
#include <assert.h>
#include "getoptions.h"
#include "pp_time.h"

/* Define the size of a block. */
#ifndef BLOCK_SIZE
#define BLOCK_SIZE 16
#endif

/* Define the default matrix size. */
#ifndef DEFAULT_SIZE
#define DEFAULT_SIZE (16 * BLOCK_SIZE)
#endif

/* A block is a 2D array of doubles. */
typedef double Block[BLOCK_SIZE][BLOCK_SIZE];
#define BLOCK(B,I,J) (B[I][J])

/* A matrix is a 1D array of blocks. */
typedef Block *Matrix;
#define MATRIX(M,I,J) ((M)[(I)*nBlocks+(J)])

/* Matrix size in blocks. */
static int nBlocks;

/****************************************************************************\
 * Utility routines.
\****************************************************************************/

/*
 * init_matrix - Fill in matrix M with random values.
 */
static void init_matrix(Matrix M, int nb)
{
     int I, J, K, i, j, k;

     /* Initialize random number generator. */
     srand(1);

     /* For each element of each block, fill in random value. */
     for (I = 0; I < nb; I++)
	  for (J = 0; J < nb; J++)
	       for (i = 0; i < BLOCK_SIZE; i++)
		    for (j = 0; j < BLOCK_SIZE; j++)
			 BLOCK(MATRIX(M, I, J), i, j) = 
			      ((double)rand()) / (double)RAND_MAX;

     /* Inflate diagonal entries. */
     for (K = 0; K < nb; K++)
	  for (k = 0; k < BLOCK_SIZE; k++)
	       BLOCK(MATRIX(M, K, K), k, k) *= 10.0;
}

/*
 * print_matrix - Print matrix M.
 */
static void print_matrix(Matrix M, int nb)
{
     int i, j;

     /* Print out matrix. */
     for (i = 0; i < nb * BLOCK_SIZE; i++) {
	  for (j = 0; j < nb * BLOCK_SIZE; j++)
	       printf(" %6.4f",
		      BLOCK(MATRIX(M, i / BLOCK_SIZE, j / BLOCK_SIZE),
			    i % BLOCK_SIZE, j % BLOCK_SIZE));
	  printf("\n");
     }
}

/*
 * test_result - Check that matrix LU contains LU decomposition of M.
 */
static int test_result(Matrix LU, Matrix M, int nb)
{
     int I, J, K, i, j, k;
     double diff, max_diff;
     double v;

     /* Initialize test. */
     max_diff = 0.0;

     /* Find maximum difference between any element of LU and M. */
     for (i = 0; i < nb * BLOCK_SIZE; i++)
	  for (j = 0; j < nb * BLOCK_SIZE; j++) {
	       I = i / BLOCK_SIZE;
	       J = j / BLOCK_SIZE;
	       v = 0.0;
	       for (k = 0; k < i && k <= j; k++) {
		    K = k / BLOCK_SIZE;
		    v += BLOCK(MATRIX(LU, I, K), i % BLOCK_SIZE,
			       k % BLOCK_SIZE) *
			BLOCK(MATRIX(LU, K, J), k % BLOCK_SIZE,
			      j % BLOCK_SIZE);
	       }
	       if (k == i && k <= j) {
		    K = k / BLOCK_SIZE;
		    v += BLOCK(MATRIX(LU, K, J), k % BLOCK_SIZE,
			       j % BLOCK_SIZE);
	       }
	       diff = fabs(BLOCK(MATRIX(M, I, J), i % BLOCK_SIZE,
				 j % BLOCK_SIZE) - v);
	       if (diff > max_diff)
		    max_diff = diff;
	  }

     /* Check maximum difference against threshold. */
     if (max_diff > 0.00001)
	  return 0;
     else
	  return 1;
}

/*
 * count_flops - Return number of flops to perform LU decomposition.
 */
static double count_flops(int n)
{
     return ((4.0 * (double) n - 3.0) * (double) n -
	     1.0) * (double) n / 6.0;
}

/****************************************************************************\
 * Element operations.
 \****************************************************************************/
/*
 * elem_daxmy - Compute y' = y - ax where a is a double and x and y are
 * vectors of doubles.
 */
static void elem_daxmy(double a, double *x, double *y, int n)
{
     for (n--; n >= 0; n--)
	  y[n] -= a * x[n];
}

/****************************************************************************\
 * Block operations.
 \****************************************************************************/

/*
 * block_lu - Factor block B.
 * lu0 in smpss
 */
static void block_lu(Block B)
{
     int i, k;

     /* Factor block. */
     for (k = 0; k < BLOCK_SIZE; k++)
	  for (i = k + 1; i < BLOCK_SIZE; i++) {
	       BLOCK(B, i, k) /= BLOCK(B, k, k);
	       elem_daxmy(BLOCK(B, i, k), &BLOCK(B, k, k + 1),
			  &BLOCK(B, i, k + 1), BLOCK_SIZE - k - 1);
	  }
}

/*
 * block_lower_solve - Perform forward substitution to solve for B' in
 * LB' = B.
 * fwd in smpss?
 */
static void block_lower_solve(Block B, Block L)
{
     int i, k;

     /* Perform forward substitution. */
     for (i = 1; i < BLOCK_SIZE; i++)
	  for (k = 0; k < i; k++)
	       elem_daxmy(BLOCK(L, i, k), &BLOCK(B, k, 0),
			  &BLOCK(B, i, 0), BLOCK_SIZE);
}

/*
 * block_upper_solve - Perform forward substitution to solve for B' in
 * B'U = B.
 * bdiv in smpss
 */
static void block_upper_solve(Block B, Block U)
{
     int i, k;

     /* Perform forward substitution. */
     for (i = 0; i < BLOCK_SIZE; i++)
	  for (k = 0; k < BLOCK_SIZE; k++) {
	       BLOCK(B, i, k) /= BLOCK(U, k, k);
	       elem_daxmy(BLOCK(B, i, k), &BLOCK(U, k, k + 1),
			  &BLOCK(B, i, k + 1), BLOCK_SIZE - k - 1);
	  }
}

/*
 * block_schur - Compute Schur complement B' = B - AC.
 * bmod in smpss
 */
static void block_schur(Block B, Block A, Block C)
{
     int i, k;

     /* Compute Schur complement. */
     for (i = 0; i < BLOCK_SIZE; i++)
	  for (k = 0; k < BLOCK_SIZE; k++)
	       elem_daxmy(BLOCK(A, i, k), &BLOCK(C, k, 0),
			  &BLOCK(B, i, 0), BLOCK_SIZE);
}

/****************************************************************************\
 * Divide-and-conquer matrix LU decomposition.
 \****************************************************************************/

/*
 * schur - Compute M' = M - VW.
 */

void schur(Matrix M, Matrix V, Matrix W, int nb)
{
     Matrix M00, M01, M10, M11;
     Matrix V00, V01, V10, V11;
     Matrix W00, W01, W10, W11;
     int hnb;

     /* Check base case. */
     if (nb == 1) {
	  block_schur(*M, *V, *W);
	  return;
     }
     /* Break matrices into 4 pieces. */
     hnb = nb / 2;
     M00 = &MATRIX(M, 0, 0);
     M01 = &MATRIX(M, 0, hnb);
     M10 = &MATRIX(M, hnb, 0);
     M11 = &MATRIX(M, hnb, hnb);
     V00 = &MATRIX(V, 0, 0);
     V01 = &MATRIX(V, 0, hnb);
     V10 = &MATRIX(V, hnb, 0);
     V11 = &MATRIX(V, hnb, hnb);
     W00 = &MATRIX(W, 0, 0);
     W01 = &MATRIX(W, 0, hnb);
     W10 = &MATRIX(W, hnb, 0);
     W11 = &MATRIX(W, hnb, hnb);

     /* Form Schur complement with recursive calls. */
     cilk_spawn schur(M00, V00, W00, hnb);
     cilk_spawn schur(M01, V00, W01, hnb);
     cilk_spawn schur(M10, V10, W00, hnb);
     cilk_spawn schur(M11, V10, W01, hnb);
     cilk_sync;

     cilk_spawn schur(M00, V01, W10, hnb);
     cilk_spawn schur(M01, V01, W11, hnb);
     cilk_spawn schur(M10, V11, W10, hnb);
     cilk_spawn schur(M11, V11, W11, hnb);
     cilk_sync;

     return;
}

/*
 * lower_solve - Compute M' where LM' = M.
 */

void lower_solve(Matrix M, Matrix L, int nb);

void aux_lower_solve(Matrix Ma, Matrix Mb, Matrix L, int nb)
{
     Matrix L00, L01, L10, L11;

     /* Break L matrix into 4 pieces. */
     L00 = &MATRIX(L, 0, 0);
     L01 = &MATRIX(L, 0, nb);
     L10 = &MATRIX(L, nb, 0);
     L11 = &MATRIX(L, nb, nb);

     /* Solve with recursive calls. */
     cilk_spawn lower_solve(Ma, L00, nb);
     cilk_sync;

     cilk_spawn schur(Mb, L10, Ma, nb);
     cilk_sync;

     cilk_spawn lower_solve(Mb, L11, nb);
     cilk_sync;
}

void lower_solve(Matrix M, Matrix L, int nb)
{
     Matrix M00, M01, M10, M11;
     int hnb;

     /* Check base case. */
     if (nb == 1) {
	  block_lower_solve(*M, *L);
	  return;
     }
     /* Break matrices into 4 pieces. */
     hnb = nb / 2;
     M00 = &MATRIX(M, 0, 0);
     M01 = &MATRIX(M, 0, hnb);
     M10 = &MATRIX(M, hnb, 0);
     M11 = &MATRIX(M, hnb, hnb);

     /* Solve with recursive calls. */
     cilk_spawn aux_lower_solve(M00, M10, L, hnb);
     cilk_spawn aux_lower_solve(M01, M11, L, hnb);
     cilk_sync;

     return;
}

/*
 * upper_solve - Compute M' where M'U = M.
 */

void upper_solve(Matrix M, Matrix U, int nb);

void aux_upper_solve(Matrix Ma, Matrix Mb, Matrix U, int nb)
{
     Matrix U00, U01, U10, U11;

     /* Break U matrix into 4 pieces. */
     U00 = &MATRIX(U, 0, 0);
     U01 = &MATRIX(U, 0, nb);
     U10 = &MATRIX(U, nb, 0);
     U11 = &MATRIX(U, nb, nb);

     /* Solve with recursive calls. */
     cilk_spawn upper_solve(Ma, U00, nb);
     cilk_sync;

     cilk_spawn schur(Mb, Ma, U01, nb);
     cilk_sync;

     cilk_spawn upper_solve(Mb, U11, nb);
     cilk_sync;

     return;
}

void upper_solve(Matrix M, Matrix U, int nb)
{
     Matrix M00, M01, M10, M11;
     int hnb;

     /* Check base case. */
     if (nb == 1) {
	  block_upper_solve(*M, *U);
	  return;
     }
     /* Break matrices into 4 pieces. */
     hnb = nb / 2;
     M00 = &MATRIX(M, 0, 0);
     M01 = &MATRIX(M, 0, hnb);
     M10 = &MATRIX(M, hnb, 0);
     M11 = &MATRIX(M, hnb, hnb);

     /* Solve with recursive calls. */
     cilk_spawn aux_upper_solve(M00, M01, U, hnb);
     cilk_spawn aux_upper_solve(M10, M11, U, hnb);
     cilk_sync;

     return;
}

/*
 * lu - Perform LU decomposition of matrix M.
 */

void lu(Matrix M, int nb)
{
     Matrix M00, M01, M10, M11;
     int hnb;

     /* Check base case. */
     if (nb == 1) {
	  block_lu(*M);
	  return;
     }
     /* Break matrix into 4 pieces. */
     hnb = nb / 2;
     M00 = &MATRIX(M, 0, 0);
     M01 = &MATRIX(M, 0, hnb);
     M10 = &MATRIX(M, hnb, 0);
     M11 = &MATRIX(M, hnb, hnb);

     /* Decompose upper left. */
     cilk_spawn lu(M00, hnb);
     cilk_sync;

     /* Solve for upper right and lower left. */
     cilk_spawn lower_solve(M01, M00, hnb);
     cilk_spawn upper_solve(M10, M00, hnb);
     cilk_sync;

     /* Compute Schur complement of lower right. */
     cilk_spawn schur(M11, M10, M01, hnb);
     cilk_sync;

     /* Decompose lower right. */
     cilk_spawn lu(M11, hnb);
     cilk_sync;

     return;
}

/****************************************************************************\
 * Mainline.
 \****************************************************************************/

/*
 * check_input - Check that the input is valid.
 */
int usage(void)
{
     printf("\nUsage: lu <options>\n\n");
     printf("Options:\n");
     printf
	 ("  -n N : Decompose NxN matrix, where N is at least 16 and power of 2.\n");
     printf("  -o   : Print matrix before and after decompose.\n");
     printf("  -c   : Check result.\n\n");
     printf("Default: lu -n %d\n\n", DEFAULT_SIZE);

     return 1;
}

int invalid_input(int n)
{
     int v = n;

     /* Check that matrix is not smaller than a block. */
     if (n < BLOCK_SIZE)
	  return usage();

     /* Check that matrix is power-of-2 sized. */
     while (!((unsigned) v & (unsigned) 1))
	  v >>= 1;
     if (v != 1)
	  return usage();

     return 0;
}

/*
 * main
 */

char *specifiers[] = { "-n", "-o", "-c", "-benchmark", "-h", 0 };
int opt_types[] = { INTARG, BOOLARG, BOOLARG, BENCHMARK, BOOLARG, 0 };

int cilk_main(int argc, char *argv[])
{
     int print, test, n, benchmark, help, failed;
     Matrix M, Msave = 0;
     pp_time_t tm;
     memset( &tm, 0, sizeof(tm) );

     n = DEFAULT_SIZE;
     print = 0;
     test = 0;

     /* Parse arguments. */
     get_options(argc, argv, specifiers, opt_types, &n, &print, &test,
		 &benchmark, &help);

     if (help)
	  return usage();

     if (benchmark) {
	  switch (benchmark) {
	      case 1:		/* short benchmark options -- a little work */
	       n = 16;
	       break;
	      case 2:		/* standard benchmark options */
	       n = DEFAULT_SIZE;
	       break;
	      case 3:		/* long benchmark options -- a lot of work */
	       n = 2048;
	       break;
	  }
     }

     if (invalid_input(n))
	  return 1;
     nBlocks = n / BLOCK_SIZE;

     /* Allocate matrix. */
     M = (Matrix) malloc(n * n * sizeof(double));
     if (!M) {
	  fprintf(stderr, "Allocation failed.\n");
	  return 1;
     }

     /* Initialize matrix. */
     init_matrix(M, nBlocks);

     if (print)
	  print_matrix(M, nBlocks);

     if (test) {
	  Msave = (Matrix) malloc(n * n * sizeof(double));
	  if (!Msave) {
	       fprintf(stderr, "Allocation failed.\n");
	       return 1;
	  }
	  memcpy((void *) Msave, (void *) M, n * n * sizeof(double));
     }

     /* Timing. "Start" timers */
     cilk_sync;
     pp_time_start( &tm );

     cilk_spawn lu(M, nBlocks);
     cilk_sync;

     /* Timing. "Stop" timers */
     pp_time_end( &tm );

     /* Test result. */
     if (print)
	  print_matrix(M, nBlocks);
     failed = ((test) && (!(test_result(M, Msave, nBlocks))));

     if (failed)
	  printf("WRONG ANSWER!\n");
     else {
	  printf("\nCilk Example: lu\n");
	  printf("Options: (n x n matrix) n = %d\n\n", n);
          printf("Running time = %g %s\n", pp_time_read(&tm), pp_time_unit() );
     }

     /* Free matrix. */
     free(M);
     if (test)
	  free(Msave);

     return 0;
}
