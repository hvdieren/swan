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
#include "wf_interface.h"
#include "pp_time.h"

#define NB 32
#define B 128
#define FALSE (0)
#define TRUE (1)

using obj::object_t;
using obj::indep;
using obj::outdep;
using obj::inoutdep;

typedef float (*vector_t);
typedef object_t<float[B]> h_vector_t;
typedef indep<float[B]> vin;
typedef outdep<float[B]> vout;

typedef float (*block_t)[B];
typedef object_t<float[B][B]> h_block_t;
typedef indep<float[B][B]> bin;
typedef outdep<float[B][B]> bout;
typedef inoutdep<float[B][B]> binout;

h_block_t A[NB][NB];
h_block_t Ashadow[NB][NB];


void init_block(bout block)
{
    unsigned int i, j;
    block_t p=(block_t)block;
    for(i=0; i<B; i++) {
	for(j=0; j<B; j++) {
	    p[i][j] = (i*j) % 1345;
	}
    }
}



void alloc_and_genmat (int x)
{
   int ii, jj;

   for (ii = 0; ii < NB; ii++) 
     for (jj = 0; jj < NB; jj++)
     {
	 // A[ii][jj] = (float *)malloc(B*B*sizeof(float));
       	 // if (A[ii][jj]==NULL) { printf("Out of memory\n"); exit(1); }
	//p=(block_t)A[ii][jj];
        /*for (i = 0; i < B; i++) 
           for (j = 0; j < B; j++) {
              init_val = (3125 * init_val) % 65536;
      	      p[i][j] = (float)((init_val - 32768.0) / 16384.0);
           }*/
	spawn( init_block, (bout)( A[ii][jj] ) );
	spawn( init_block, (bout)( Ashadow[ii][jj] ) );
     }

   ssync();
}



long usecs (void)
{
  struct timeval t;

  gettimeofday(&t,NULL);
  return t.tv_sec*1000000+t.tv_usec;
}

//#pragma css task output(v)
void clear(vector_t v)
{
  int i;
                                                                               
  for (i=0; i<B; i++)
      v[i] = (float)0.0;
}

void clear(block_t A) {
   int ii, jj;

   for (ii = 0; ii < B; ii++) 
     for (jj = 0; jj < B; jj++)
	 A[ii][jj] = 0;
}

//#pragma css task input(A[B][B]) output(v[B])
void getlastrow(block_t A, vector_t v)
{
   int j;
   for (j=0; j<B; j++) v[j]=A[B-1][j];
}

//#pragma css task input(A[32][32]) output(v[32])
void getlastcol(block_t A, vector_t v)
{
   int i;
   for (i=0; i<B; i++) v[i]=A[i][B-1];
}

//#pragma css task input(A[32][32]) output(v[32])
void getfirstrow(block_t A, vector_t v)
{
   int j;
   for (j=0; j<B; j++) v[j]=A[0][j];
}

//#pragma css task input(A[32][32]) output(v[32])
void getfirstcol(block_t A, vector_t v)
{
   int i;
   for (i=0; i<B; i++) v[i]=A[i][0];
}

//#pragma css task input (lefthalo[32], tophalo[32], righthalo[32], bottomhalo[32]) inout(A[32][32]) highpriority
void jacobi(bin left_block, bin top_block,
            bin right_block, bin bottom_block,
            bin A, bout A2 )
{
   int i,j;
   float lefthalo[B], tophalo[B], righthalo[B], bottomhalo[B];
   float left, top, right, bottom;

   getlastrow( *top_block, tophalo );
   getlastcol( *left_block, lefthalo );
   getfirstrow( *bottom_block, bottomhalo );
   getfirstcol( *right_block, righthalo );

   for (i=0;(i<B); i++)
     for (j=0;j<B; j++)
     {
       left=(j==0? lefthalo[j]:(*A)[i][j-1]);
       top=(i==0? tophalo[i]:(*A)[(i-1)][j]);
       right=(j==B-1? righthalo[i]:(*A)[i][j+1]);
       bottom=(i==B-1? bottomhalo[i]:(*A)[(i+1)][j]);

       (*A2)[i][j] = 0.2*((*A)[i][j] + left + top + right + bottom);
     }

}

void compute(int niters)
{
   int iters;
   int ii, jj;
   h_vector_t lefthalo, tophalo, righthalo, bottomhalo;
   h_block_t (*As[2])[NB][NB];
   h_block_t zero;

   As[0] = &A;
   As[1] = &Ashadow;

   clear( *zero );

   for (iters=0; iters<niters; iters++) {
      for (ii=0; ii<NB; ii++) 
         for (jj=0; jj<NB; jj++) {
	    spawn( jacobi,
		   (bin)(ii>0?(*As[iters&1])[ii-1][jj]:zero),
		   (bin)(jj>0?(*As[iters&1])[ii][jj-1]:zero),
		   (bin)(jj<NB-1?(*As[iters&1])[ii][jj+1]:zero),
		   (bin)(ii<NB-1?(*As[iters&1])[ii+1][jj]:zero),
		   (bin)(*As[iters&1])[ii][jj],
		   (bout)(*As[1-(iters&1)])[ii][jj]);
         }
     }

   ssync();
}

int main(int argc, char* argv[])
{
    int niters;
    pp_time_t tm;
    memset( &tm, 0, sizeof(tm) );

    if( argc > 1 ) {
       niters = atoi( argv[1] );
    } else
       niters = 1;

    run(alloc_and_genmat, 0);

    pp_time_start( &tm );
    run(compute,niters);
    pp_time_end( &tm );

    printf("Running time  = %g %s\n", pp_time_read(&tm), pp_time_unit() );
}
