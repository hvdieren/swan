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
#include "wf_interface.h"
#include "pp_time.h"

/* Define the size of a block. */
#ifndef BLOCK_SIZE
#define BLOCK_SIZE 16
#endif

/* Define the default matrix size. */
#ifndef DEFAULT_SIZE
#define DEFAULT_SIZE (16 * BLOCK_SIZE)
#endif

using obj::object_t;
using obj::indep;
using obj::inoutdep;

/* A block is a 2D array of doubles. */
typedef double UBlock[BLOCK_SIZE][BLOCK_SIZE];
#define BLOCK(B,I,J) ((B)[I][J])
typedef object_t<UBlock> Block;
typedef indep<UBlock> Bin;
typedef inoutdep<UBlock> Binout;

/* A matrix is a 1D array of blocks. */
typedef Block *Matrix;
#define HMATRIX(M,I,J) ((M)[(I)*nBlocks+(J)])
#define MATRIX(M,I,J) (*(M)[(I)*nBlocks+(J)])

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
 */
static void block_lu(Binout B)
{
   int i, j, k;

   for (k=0; k<BLOCK_SIZE; k++)
      for (i=k+1; i<BLOCK_SIZE; i++) {
	  (*B)[i][k] = (*B)[i][k] / (*B)[k][k];
         for (j=k+1; j<BLOCK_SIZE; j++)
             (*B)[i][j] -= (*B)[i][k] * (*B)[k][j];
      }

}

/*
 * block_lower_solve - Perform forward substitution to solve for B' in
 * LB' = B.
 */
static void block_lower_solve(Binout B, Bin L)
{
     int i, k;

     /* Perform forward substitution. */
     for (i = 1; i < BLOCK_SIZE; i++)
	  for (k = 0; k < i; k++)
	       elem_daxmy(BLOCK(*L, i, k), &BLOCK(*B, k, 0),
			  &BLOCK(*B, i, 0), BLOCK_SIZE);
}

/*
 * block_upper_solve - Perform forward substitution to solve for B' in
 * B'U = B.
 */
static void block_upper_solve(Binout B, Bin U)
{
     int i, k;

     /* Perform forward substitution. */
     for (i = 0; i < BLOCK_SIZE; i++)
	  for (k = 0; k < BLOCK_SIZE; k++) {
	       BLOCK(*B, i, k) /= BLOCK(*U, k, k);
	       elem_daxmy(BLOCK(*B, i, k), &BLOCK(*U, k, k + 1),
			  &BLOCK(*B, i, k + 1), BLOCK_SIZE - k - 1);
	  }
}

/*
 * block_schur - Compute Schur complement B' = B - AC.
 */
static void block_schur(Binout B, Bin A, Bin C)
{
     int i, k;

     /* Compute Schur complement. */
     for (i = 0; i < BLOCK_SIZE; i++)
	  for (k = 0; k < BLOCK_SIZE; k++)
	       elem_daxmy(BLOCK(*A, i, k), &BLOCK(*C, k, 0),
			  &BLOCK(*B, i, 0), BLOCK_SIZE);
}

/*
 * lu - Perform LU decomposition of matrix M.
 */

void lu(Matrix M, int nb)
{
    int ii, jj, kk;

    for (kk=0; kk<nBlocks; kk++) {
	// lu0( A[kk][kk]);
	spawn( block_lu, (Binout)HMATRIX(M, kk, kk) );

	for (jj=kk+1; jj<nBlocks; jj++)
	    // fwd(A[kk][kk], A[kk][jj]);
	    spawn( block_lower_solve, (Binout)HMATRIX(M, kk, jj),
		   (Bin)HMATRIX(M, kk, kk) );

	for (ii=kk+1; ii<nBlocks; ii++) 
	    // bdiv (A[kk][kk], A[ii][kk]);
	    spawn( block_upper_solve, (Binout)HMATRIX(M, ii, kk),
		   (Bin)HMATRIX(M, kk, kk) );

	for (ii=kk+1; ii<nBlocks; ii++) {
	    for (jj=kk+1; jj<nBlocks; jj++) {
		// bmod(A[ii][kk], A[kk][jj], A[ii][jj]);
		spawn( block_schur, (Binout)HMATRIX(M, ii, jj),
		       (Bin)HMATRIX(M, ii, kk),
		       (Bin)HMATRIX(M, kk, jj) );
	    }
	}
    }

    ssync();
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

int main(int argc, char *argv[])
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
     M = new Block[nBlocks*nBlocks];
     // M = (Matrix) malloc(n * n * sizeof(double));
     if (!M) {
	  fprintf(stderr, "Allocation failed.\n");
	  return 1;
     }

     /* Initialize matrix. */
     init_matrix(M, nBlocks);

     if (print)
	  print_matrix(M, nBlocks);

     if (test) {
	 // Msave = (Matrix) malloc(n * n * sizeof(double));
	 Msave = new Block[n*n];
	  if (!Msave) {
	       fprintf(stderr, "Allocation failed.\n");
	       return 1;
	  }
	  // memcpy((void *) Msave, (void *) M, n * n * sizeof(double));
	  for( int i=0; i < nBlocks*nBlocks; ++i )
	      memcpy((void *) *Msave[i], (void *)*M[i], sizeof(UBlock));
     }

     /* Timing. "Start" timers */
     pp_time_start( &tm );

     run(lu, M, nBlocks);

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
	  printf("Running time  = %g %s\n", pp_time_read(&tm), pp_time_unit() );
     }

     /* Free matrix. */
     // free(M);
     delete[] M;
     if (test)
	 //free(Msave);
	 delete[] Msave;

     return 0;
}
