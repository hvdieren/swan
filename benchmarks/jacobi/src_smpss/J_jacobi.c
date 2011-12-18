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
#include <stdlib.h> 
#include <string.h> 
#include <sys/time.h>
#include <time.h>
#include "pp_time.h"

#define NB 128
#define B 32
#define FALSE (0)
#define TRUE (1)
float *A[NB][NB];

void alloc_and_genmat ()
{
   int init_val, i, j, ii, jj;
   float *p;

   init_val = 1325;

   for (ii = 0; ii < NB; ii++) 
     for (jj = 0; jj < NB; jj++)
     {
        A[ii][jj] = (float *)malloc(B*B*sizeof(float));
       if (A[ii][jj]==NULL) { printf("Out of memory\n"); exit(1); }
	p=A[ii][jj];
        for (i = 0; i < B; i++) 
           for (j = 0; j < B; j++) {
              init_val = (3125 * init_val) % 65536;
      	      (*p) = (float)((init_val - 32768.0) / 16384.0);
	      p++;
           }
     }
}



long usecs (void)
{
  struct timeval t;

  gettimeofday(&t,NULL);
  return t.tv_sec*1000000+t.tv_usec;
}

#pragma css task output(v)
void clear(float v[B])
{
  int i, j, k;
                                                                               
  for (i=0; i<B; i++)
    v[i] = 0.0 ;
}

#pragma css task input(A[32][32]) output(v[32])
void getlastrow(float *A, float *v)
{
   int j;
   for (j=0; j<B; j++) v[j]=A[B-1*B+j];
}

#pragma css task input(A[32][32]) output(v[32])
void getlastcol(float *A, float *v)
{
   int i;
   for (i=0; i<B; i++) v[i]=A[i*B+B-1];
}

#pragma css task input(A[32][32]) output(v[32])
void getfirstrow(float *A, float *v)
{
   int j;
   for (j=0; j<B; j++) v[j]=A[j];
}

#pragma css task input(A[32][32]) output(v[32])
void getfirstcol(float *A, float *v)
{
   int i;
   for (i=0; i<B; i++) v[i]=A[i*B+B-1];
}

#pragma css task input (lefthalo[32], tophalo[32], righthalo[32], bottomhalo[32]) inout(A[32][32]) highpriority
void jacobi(float *lefthalo, float *tophalo,
            float *righthalo, float *bottomhalo,
            float *A )
{
   int i,j;
   float tmp;
   float left,top, right, bottom;

   for (i=0;(i<B); i++)
     for (j=0;j<B; j++)
     {
       tmp = A[i*B+j];
       left=(j==0? lefthalo[j]:A[i*B+j-1]);
       top=(i==0? tophalo[i]:A[(i-1)*B+j]);
       right=(j==B-1? righthalo[i]:A[i*B+j+1]);
       bottom=(i==B-1? bottomhalo[i]:A[(i+1)*B+j]);

       A[i*B+j] = 0.2*(A[i*B+j] + left + top + right + bottom);
     }

}

int main(int argc, char* argv[])
{
   int iters, niters;
   int ii, jj;
   float lefthalo[B], tophalo[B], righthalo[B], bottomhalo[B];
   pp_time_t tm;
   memset( &tm, 0, sizeof(tm) );

   if( argc > 1 ) {
      niters = atoi( argv[1] );
   } else
      niters = 1;

#pragma css start
   alloc_and_genmat();

   pp_time_start( &tm );
   for (iters=0; iters<niters; iters++)
      for (ii=0; ii<NB; ii++) 
         for (jj=0; jj<NB; jj++) {
            if (ii>0) {
		getlastrow(A[ii-1][jj], tophalo);
		}
            else {
		clear(tophalo);
		}
            if (jj>0) {
		getlastcol(A[ii][jj-1], lefthalo);
		}
            else {
		clear (lefthalo);
		}
            if (ii<NB-1) {
		getfirstrow(A[ii+1][jj], bottomhalo);
		}
            else {
		clear(bottomhalo);
		}
            if (jj<NB-1) {
		getfirstcol(A[ii][jj+1], righthalo);
		}
            else {
		clear (lefthalo);
		}

            jacobi (lefthalo, tophalo, righthalo, bottomhalo, A[ii][jj]);
         }

#pragma css finish
   pp_time_end( &tm );

    printf("Running time  = %g %s\n", pp_time_read(&tm), pp_time_unit() );
}

