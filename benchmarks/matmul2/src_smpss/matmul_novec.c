/*
* Copyright (c) 2008, BSC (Barcelon Supercomputing Center)
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*     * Redistributions of source code must retain the above copyright
*       notice, this list of conditions and the following disclaimer.
*     * Redistributions in binary form must reproduce the above copyright
*       notice, this list of conditions and the following disclaimer in the
*       documentation and/or other materials provided with the distribution.
*     * Neither the name of the <organization> nor the
*       names of its contributors may be used to endorse or promote products
*       derived from this software without specific prior written permission.
*
* THIS SOFTWARE IS PROVIDED BY BSC ''AS IS'' AND ANY
* EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
* WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED. IN NO EVENT SHALL <copyright holder> BE LIABLE FOR ANY
* DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
* (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
* LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
* ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
* SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#include <stdio.h>

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include "clapack_used.h"
#include "pp_time.h"

#define BSIZE 64

//-------------------------------------------------------------------------------------------------------------------------

#pragma css task input(A, B) inout(C)
void matmul (float A[BSIZE][BSIZE], float B[BSIZE][BSIZE], float C[BSIZE][BSIZE])
{

  int i, j, k;

  for (i = 0; i < BSIZE; i++)
    {
      for (j = 0; j < BSIZE; j++)
	{
	for (k=0; k < BSIZE; k++)
	C[i][j] += A[i][k]*B[k][j];
	}
    }
}

//---------------------------------------------------------------------------------------------------------------------------

void compute(pp_time_t * tm, int DIM, float *A[DIM][DIM], float *B[DIM][DIM], float *C[DIM][DIM])
{
 int i, j, k;

#pragma css start
  pp_time_start(tm);

  for (i = 0; i < DIM; i++)
    for (j = 0; j < DIM; j++)
      for (k = 0; k < DIM; k++)
        matmul (A[i][k], B[k][j], C[i][j]);

#pragma css finish
  pp_time_end(tm);

}

float **A;
float **B;
float **C;

void initialize (int argc, char **argv, int * N_p, int * DIM_p);

int main (int argc, char **argv)
{
  // local vars
  int i, j, k;
  int N,DIM;
  unsigned long elapsed;
  pp_time_t tm;
  memset( &tm, 0, sizeof(tm) );


  // application inicializations
  initialize (argc, argv, &N, &DIM);

  // compute with CellSs
  compute(&tm, DIM, (void *)A, (void *)B, (void *)C);

  elapsed = pp_time_read(&tm);
  printf("Matrix dimension: %d\n",N);
  printf ("Time %lu microsecs\n", elapsed);
  printf ("Perf %lu MFlops\n", (unsigned)(((double)N*N*N*2)/((double)elapsed)));
  printf("Running time = %g %s\n", pp_time_read(&tm), pp_time_unit() );

  return 0;
}

static void convert_to_blocks(int DIM, int N, float *Alin, float *A[DIM][DIM])
{
  int i, j;
  for (i = 0; i < N; i++)
  {
    for (j = 0; j < N; j++)
    {
      A[i/BSIZE][j/BSIZE][(i%BSIZE)*BSIZE+j%BSIZE] = Alin[j*N+i];
    }
  }

}

void initialize (int argc, char **argv, int * N_p, int * DIM_p)
{
  int ISEED[4] = {0,0,0,1};
  int IONE=1;
  int DIM;
  char UPLO='n';
  float FZERO=0.0;
  int i;

  if (argc==2)
  {
    DIM=atoi(argv[1]);
  }
  else
  {
    printf("usage: %s DIM\n",argv[0]);
    exit(0);
  }

  // matrix init
  int N=BSIZE*DIM;
  int NN=N*N;

  *N_p=N;
  *DIM_p=DIM;

  // linear matrix
  float *Alin = (float *) malloc(NN * sizeof(float));
  float *Blin = (float *) malloc(NN * sizeof(float));
  float *Clin = (float *) malloc(NN * sizeof(float));

  // fill the matrix with random values
  slarnv_(&IONE, ISEED, &NN, Alin);
  slarnv_(&IONE, ISEED, &NN, Blin);
  slaset_(&UPLO,&N,&N,&FZERO,&FZERO,Clin,&N);

  A = (float **) malloc(DIM*DIM*sizeof(float *));
  B = (float **) malloc(DIM*DIM*sizeof(float *));
  C = (float **) malloc(DIM*DIM*sizeof(float *));

#if 1
#if 1
  for (i = 0; i < DIM*DIM; i++)
  {
      A[i] = (float *) malloc(BSIZE*BSIZE*sizeof(float));
      B[i] = (float *) malloc(BSIZE*BSIZE*sizeof(float));
      C[i] = (float *) malloc(BSIZE*BSIZE*sizeof(float));

      // printf( "A[%d]=%p %p %p\n", i, A[i], B[i], C[i] );
  }
#else
  for (i = 0; i < DIM*DIM; i++) {
      A[i] = (float *) malloc(BSIZE*BSIZE*sizeof(float));
      // printf( "A[%d]=%p\n", i, A[i] );
  }
  for (i = 0; i < DIM*DIM; i++)
      B[i] = (float *) malloc(BSIZE*BSIZE*sizeof(float));
  for (i = 0; i < DIM*DIM; i++)
      C[i] = (float *) malloc(BSIZE*BSIZE*sizeof(float));
#endif
#else
  float * A_scratch = (float *) malloc(DIM*DIM*BSIZE*BSIZE*sizeof(float));
  float * B_scratch = (float *) malloc(DIM*DIM*BSIZE*BSIZE*sizeof(float));
  float * C_scratch = (float *) malloc(DIM*DIM*BSIZE*BSIZE*sizeof(float));

  for (i = 0; i < DIM*DIM; i++)
  {
      A[i] = &A_scratch[i*BSIZE*BSIZE];
      B[i] = &B_scratch[i*BSIZE*BSIZE];
      C[i] = &C_scratch[i*BSIZE*BSIZE];

      // printf( "A[%d]=%p\n", i, A[i] );
  }
#endif

  convert_to_blocks(DIM, N, Alin, (void *)A);
  convert_to_blocks(DIM, N, Blin, (void *)B);
  convert_to_blocks(DIM, N, Clin, (void *)C);

  free(Alin);
  free(Blin);
  free(Clin);
}

