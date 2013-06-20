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

#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include "cblas.h"

#define blasint int

#include "wf_interface.h"
#include "pp_time.h"

using obj::object_t;
using obj::indep;
using obj::outdep;
using obj::inoutdep;

typedef float *block_t;
typedef inoutdep<float[]> binout;
typedef indep<float[]> bin;
typedef outdep<float[]> bout;

//----------------------------------------------------------------------------------------------

extern "C" {
    extern int spotrf_(unsigned char *,int *, float *, int *, int *);
}


// #pragma css task input(NB) inout(A[NB][NB]) highpriority 
void smpSs_spotrf_tile(binout Adep, unsigned long NB)
{
unsigned char LO='L';
int INFO;
int nn=NB;
float * A = (block_t)Adep;
leaf_call(spotrf_,&LO,
          &nn,
          A,&nn,
          &INFO);
	
}

//#pragma css task input(A[NB][NB], B[NB][NB], NB) inout(C[NB][NB])
void smpSs_sgemm_tile(bin Adep, bin Bdep, binout Cdep, unsigned long NB)
{
// unsigned char TR='T', NT='N';
// float DONE=1.0, DMONE=-1.0;

//    sgemm_2(&NT, &TR,       /* TRANSA, TRANSB */
//           &NB, &NB, &NB,   /* M, N, K        */
//           &DMONE,          /* ALPHA          */
//           A, &NB,          /* A, LDA         */
//           B, &NB,          /* B, LDB         */
//           &DONE,           /* BETA           */
//           C, &NB);         /* C, LDC         */

    const float * A = (block_t)Adep;
    const float * B = (block_t)Bdep;
    float * C = (block_t)Cdep;

 // using CBLAS
    leaf_call(cblas_sgemm,
        CblasColMajor,
        CblasNoTrans, CblasTrans,
	      (blasint)NB, (blasint)NB, (blasint)NB,
	      (float)-1.0, A, (blasint)NB,
              B, (int)NB,
	      (float)1.0, C, (blasint)NB);


}

//#pragma css task input(T[NB][NB], NB) inout(B[NB][NB])
void smpSs_strsm_tile(bin Tdep, binout Bdep, unsigned long NB)
{
//unsigned char LO='L', TR='T', NU='N', RI='R';
//float DONE=1.0;

//  strsm_2(&RI, &LO, &TR, &NU,  /* SIDE, UPLO, TRANSA, DIAG */
//          &NB, &NB,             /* M, N                     */
//         &DONE,                /* ALPHA                    */
//         T, &NB,               /* A, LDA                   */
//         B, &NB);              /* B, LDB                   */

const float * T = (block_t)Tdep;
float * B = (block_t)Bdep;

 // using CBLAS
leaf_call(cblas_strsm,
        CblasColMajor,
        CblasRight, CblasLower, CblasTrans, CblasNonUnit,
	  (blasint)NB, (blasint)NB,
	  (float)1.0, T, (blasint)NB,
             B, (blasint)NB);

}

//#pragma css task input(A[NB][NB], NB) inout(C[NB][NB])
void smpSs_ssyrk_tile( bin Adep, binout Cdep, unsigned long NB)
{
//unsigned char LO='L', NT='N';
//float DONE=1.0, DMONE=-1.0;

//    ssyrk_2(&LO, &NT,          /* UPLO, TRANSA */
//           &NB, &NB,           /* M, N         */
//           &DMONE,             /* ALPHA        */
//           A, &NB,             /* A, LDA       */
//           &DONE,              /* BETA         */
//           C, &NB);            /* C, LDC       */

const float * A = (block_t)Adep;
float * C = (block_t)Cdep;

 // using CBLAS
leaf_call(cblas_ssyrk,
        CblasColMajor,
        CblasLower,CblasNoTrans,
	    (blasint)NB, (blasint)NB,
	  (float)-1.0, A, (blasint)NB,
	  (float)1.0, C, (blasint)NB);


}

//----------------------------------------------------------------------------------------------


void compute(pp_time_t *timer, unsigned long NB, long DIM, object_t<float[]> A[])
{
    // #pragma css start 
      // gettimeofday(start,NULL);
    pp_time_start(timer);
  for (long j = 0; j < DIM; j++)
  {
    for (long k= 0; k< j; k++)
    {
      for (long i = j+1; i < DIM; i++) 
      {
        // A[i,j] = A[i,j] - A[i,k] * (A[j,k])^t
	  spawn(smpSs_sgemm_tile, (bin)A[i+DIM*k], (bin)A[j+DIM*k],
		(binout)A[i+DIM*j], NB);
      }

    }

    for (long i = 0; i < j; i++)
    {
      // A[j,j] = A[j,j] - A[j,i] * (A[j,i])^t
	spawn(smpSs_ssyrk_tile, (bin)A[j+DIM*i], (binout)A[j+DIM*j], NB);
    }

    // Cholesky Factorization of A[j,j]
    spawn(smpSs_spotrf_tile, (binout)A[j+DIM*j], NB);
      
    for (long i = j+1; i < DIM; i++)
    {
      // A[i,j] <- A[i,j] = X * (A[j,j])^t
	spawn(smpSs_strsm_tile, (bin)A[j+DIM*j], (binout)A[i+DIM*j], NB);
    }
   
  }	
      // #pragma css finish
      // gettimeofday(stop,NULL);
  ssync();
    pp_time_end(timer);
}

//--------------------------------------------------------------------------------


static void  init(int argc, char **argv, unsigned long *NB_p, unsigned long *N_p, unsigned long *DIM_p);

object_t<float[]> *A;
float * Alin; // private in init

unsigned long NB, N, DIM; // private in main

int
main(int argc, char *argv[])
{
  // local vars
 
// unsigned char LO='L';
// int  INFO;
 
  pp_time_t timer;
  unsigned long elapsed;

  memset( &timer, 0, sizeof(timer) );

  // application inicializations
  init(argc, argv, &NB, &N, &DIM);
  // compute with CellSs

  run(compute, &timer, NB, (long)DIM, A);

// int nn=N;
// compute with library
  
//  spotrf(&LO, &nn, Alin, &nn, &INFO);

  elapsed = pp_time_read(&timer);

// time in usecs
  printf ("%lu;\t", elapsed);
// perfonrmance in MFLOPS
  printf("%d\n", (int)((0.33*N*N*N+0.5*N*N+0.17*N)/elapsed));
  printf("Running time  = %g %s\n", pp_time_read(&timer), pp_time_unit() );

	return 0;
}


static void convert_to_blocks(long NB,long DIM, long N, float *Alin, object_t<float[]> A[])
{
  for (long i = 0; i < N; i++)
  {
    for (long j = 0; j < N; j++)
    {
	((float*)A[(j/NB)+DIM*(i/NB)])[(i%NB)*NB+j%NB] = Alin[i*N+j];
    }
  }

}


//void slarnv_(long *idist, long *iseed, long *n, float *x);

void fill_random(float *Alin, int NN)
{
  int i;
  for (i = 0; i < NN; i++)
  {
    Alin[i]=((float)rand())/((float)RAND_MAX);
  }
}


static void init(int argc, char **argv, unsigned long *NB_p, unsigned long *N_p, unsigned long *DIM_p)
{
  // long ISEED[4] = {0,0,0,1};
  // long IONE=1;
  long DIM;
  long NB;

  
  if (argc==3)
  {
    NB=(long)atoi(argv[1]);
    DIM=(long)atoi(argv[2]);
  }
  else
  {
    printf("usage: %s NB DIM\n",argv[0]);
    exit(0);
  }

  // matrix init
  
  long N = NB*DIM;
  long NN = N * N;

  *NB_p=NB;
  *N_p = N;
  *DIM_p = DIM;
  
  // linear matrix
   Alin = (float *) malloc(NN * sizeof(float));

  // fill the matrix with random values
//  slarnv_(&IONE, ISEED, &NN, Alin);
  fill_random(Alin,NN);

  // make it positive definite
  for(long i=0; i<N; i++)
  {
    Alin[i*N + i] += N;
  }
  
  // blocked matrix
  // A = (float **) malloc(DIM*DIM*sizeof(float *));
  // for (long i = 0; i < DIM*DIM; i++)
     // A[i] = (float *) malloc(NB*NB*sizeof(float));
  A = new object_t<float[]>[DIM*DIM];
  for (long i = 0; i < DIM*DIM; i++)
      A[i] = object_t<float[]>(NB*NB); // resize

  convert_to_blocks(NB, DIM, N, Alin, A);
  
}

