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
#include <string.h>
#include <stdlib.h> 
#include <math.h>

#include <sys/time.h>
#include <time.h>

#include "wf_interface.h"
#include "pp_time.h"

#ifndef NB
#define NB 100 
#endif
#ifndef BS
#define BS 32 
#endif
#define FLOATS_PER_LINE 15

/* Debug control*/
#define D_print_mat(X,Y)
/*
#define PRINT_VALUES
#define D_print_mat(X,Y)  print_mat(X,Y)
*/
#define FALSE (0)
#define TRUE (1)

using obj::object_t;
using obj::indep;
using obj::outdep;
using obj::inoutdep;

// typedef float (*p_block_t)[BS];
typedef float (*block_t)[BS];
typedef object_t<float[BS][BS]> * p_block_t;
typedef inoutdep<float[BS][BS]> binout;
typedef indep<float[BS][BS]> bin;
typedef outdep<float[BS][BS]> bout;

p_block_t A[NB][NB];
p_block_t origA[NB][NB];
p_block_t L[NB][NB];
p_block_t U[NB][NB];

p_block_t allocate_clean_block()
{
  int i,j;
  p_block_t p;

  // p=(p_block_t)malloc(BS*BS*sizeof(float));
  p = new object_t<float[BS][BS]>;
  if( p != NULL ) {
      block_t q = (block_t)*p;
     for (i = 0; i < BS; i++) 
        for (j = 0; j < BS; j++) 
	    q[i][j]=(float)0.0; 
  }
  else {
     printf ("OUT OF MEMORY!!!!!!!!!!!!!!!\n");
     exit (-1);
  }
  return (p);
}

void  print_pointer_structure(char *s, p_block_t A[NB][NB])
{
   int ii, jj;
   p_block_t p;

   printf ("\nStructure for matrix %s\n", s);

   for (ii = 0; ii < NB; ii++) {
     for (jj = 0; jj < NB; jj++) {
        p =  A[ii][jj];
        if (p!=NULL) printf ("%p ",p);
        else printf ("    *    ");
     }
     printf ("\n");
   }
}
void  print_structure(char *s, p_block_t A[NB][NB])
{
   int ii, jj;
   p_block_t p;

   printf ("\nStructure for matrix %s\n", s);

   for (ii = 0; ii < NB; ii++) {
     for (jj = 0; jj < NB; jj++) {
        p =  A[ii][jj];
        if (p!=NULL) printf ("x");
        else printf (" ");
     }
     printf ("\n");
   }
}


void print_mat (char *s, p_block_t mat[NB][NB])
{
   if (NB<14) print_pointer_structure (s, mat);
   else print_structure (s, mat);

#ifdef PRINT_VALUES
   int ii, jj, i, j, maxNB=NB;
   p_block_t p;

   printf ("values of matrix %s\n", s);
   if (NB*BS>FLOATS_PER_LINE) {
       maxNB=FLOATS_PER_LINE/BS;
       printf ("very large matrix. Only printing %d blocks\n",maxNB);
   }

   for (ii = 0; ii < maxNB; ii++) {
      for (i = 0; i < BS; i++) {
         for (jj = 0; jj < maxNB; jj++) {
            p=mat[ii][jj];
            for (j = 0; j < BS; j++) {
               if (p != NULL) printf ("%f ", p[i][j]);
                else printf ("%f ", 0.0);
            }              
         }
     printf ("\n");
     }
   }
#endif
}

void genmat ()
{
    int  i, j, ii, jj;
    int null_entry;

    long int init_val;

    init_val = 1325;

/* structure */
    for (ii=0; ii<NB; ii++)
	for (jj=0; jj<NB; jj++){
	    null_entry=FALSE;
	    if ((ii<jj) && (ii%3 !=0)) null_entry =TRUE;
	    if ((ii>jj) && (jj%3 !=0)) null_entry =TRUE;
	    if (ii%2==1) null_entry=TRUE;
	    if (jj%2==1) null_entry=TRUE;
	    if (ii==jj) null_entry=FALSE;
	    if (ii==jj-1) null_entry=FALSE;
	    if (ii-1 == jj) null_entry=FALSE; 
	    if (null_entry==FALSE){
		A[ii][jj] = allocate_clean_block();
		if (A[ii][jj]==NULL) {
		    printf("Out of memory\n");
		    exit(1);
		}
	    }
	    else A[ii][jj] = NULL;
	}

/* Initialization */ /* preguntar Cela valores razonable */
    for (ii = 0; ii < NB; ii++)
	for (jj = 0; jj < NB; jj++)
	{
	    p_block_t p;
	    p = A[ii][jj];
	    if (p!=NULL) {
		block_t q = (block_t)*p;
		for (i = 0; i < BS; i++)
		    for (j = 0; j < BS; j++) {
			init_val = (3125 * init_val) % 65536;
			q[i][j] = (float)0.0001;
			if (ii == jj){
			    if (i==j) q[i][j] = (float)-20000 ;
			    if (((i-1)==j) || (i==(j-1))) q[i][j] = (float)10000 ;
			}
		    }
	    }
	}
}


#pragma css task input (ref_block, to_comp) output (mse)
void are_blocks_equal (float ref_block[BS][BS], float to_comp[BS][BS], float *mse)
{
   int i,j;
   float diff;

   *mse = 0.0;
   for (i = 0; i < BS; i++) 
     for (j = 0; j < BS; j++) {
        diff = ref_block[i][j]-to_comp[i][j];
        *mse += diff*diff;
     }
}

void compare_mat (p_block_t X[NB][NB], p_block_t Y[NB][NB])
{
   int ii, jj;
   static float sq_error[NB][NB];
   p_block_t Zero_block;
   int some_difference = FALSE;

   Zero_block = allocate_clean_block();

   for (ii = 0; ii < NB; ii++) 
     for (jj = 0; jj < NB; jj++) {
       if (X[ii][jj] == NULL) 
          if (Y[ii][jj] == NULL)
              sq_error[ii][jj] = 0.0f;
          else
              are_blocks_equal((block_t)*Zero_block, (block_t)*Y[ii][jj],&sq_error[ii][jj]);
       else
          are_blocks_equal((block_t)*X[ii][jj], (block_t)*Y[ii][jj],&sq_error[ii][jj]);
     }

/* Alternative to put wait on */
#pragma css finish 
   printf ("\nComparison of matrices at %p and %p\n",X,Y);
   for (ii = 0; ii < NB; ii++) 
     for (jj = 0; jj < NB; jj++) 
//#pragma css wait on (&sq_error[ii][jj])
         if (sq_error[ii][jj] >0.0000001L) {
             printf ("block [%d, %d]: detected mse = %.20lf\n",ii,jj,sq_error[ii][jj]);
              some_difference =TRUE;
              exit(1);
         }
     if ( some_difference == FALSE) printf ("matrices are identical\n");

}


//#pragma css task inout(diag)
void lu0(binout diagdep)
{
   block_t diag = (block_t)diagdep;
   int i, j, k;

   for (k=0; k<BS; k++)
      for (i=k+1; i<BS; i++) {
         diag[i][k] = diag[i][k] / diag[k][k];
         for (j=k+1; j<BS; j++)
             diag[i][j] -= diag[i][k] * diag[k][j];
      }

}

// #pragma css task input(diag) inout(row)
void bdiv(bin diagdep, binout rowdep)
{
    block_t diag = (block_t)diagdep;
    block_t row = (block_t)rowdep;
   int i, j, k;

   for (i=0; i<BS; i++)
      for (k=0; k<BS; k++) {
         row[i][k] = row[i][k] / diag[k][k];
         for (j=k+1; j<BS; j++)
            row[i][j] -= row[i][k]*diag[k][j];
      }

}


//#pragma css task input(row,col) inout(inner)
void bmod(bin rowdep, bin coldep, binout innerdep)
{
    block_t row = (block_t)rowdep;
    block_t col = (block_t)coldep;
    block_t inner = (block_t)innerdep;
  int i, j, k;


  for (i=0; i<BS; i++)
     for (j=0; j<BS; j++)
        for (k=0; k<BS; k++) 
           inner[i][j] -= row[i][k]*col[k][j];

}

// #pragma css task input(a,b) inout(c)
void block_mpy_add(bin adep, bin bdep, binout cdep)
{
    block_t a = (block_t)adep;
    block_t b = (block_t)bdep;
    block_t c = (block_t)cdep;
  int i, j, k;

  for (i=0; i<BS; i++)
     for (j=0; j<BS; j++)
        for (k=0; k<BS; k++) 
           c[i][j] += a[i][k]*b[k][j];
}


//#pragma css task input(diag) inout(col)
void fwd(bin diagdep, binout coldep)
{
    block_t diag = (block_t)diagdep;
    block_t col = (block_t)coldep;
  int i, j, k;

  for (j=0; j<BS; j++)
     for (k=0; k<BS; k++) 
        for (i=k+1; i<BS; i++)
           col[i][j] -= diag[i][k]*col[k][j];

}

//#pragma css task input(A) output(L, U)
void split_block (bin Adep, bout Ldep, bout Udep)
{
    block_t A = (block_t)Adep;
    block_t L = (block_t)Ldep;
    block_t U = (block_t)Udep;
  int i, j;

  for (i=0; i<BS; i++)
     for (j=0; j<BS; j++) {
        if (i==j)     { L[i][j]=1.0;      U[i][j]=A[i][j]; }
        else if (i>j) { L[i][j]=A[i][j];  U[i][j]=0.0;}
       else           { L[i][j]=0.0;      U[i][j]=A[i][j]; }
     }
}


//#pragma css task input(Src) output(Dst)
void copy_block (bin Srcdep, bout Dstdep)
{
    block_t Src = (block_t)Srcdep;
    block_t Dst = (block_t)Dstdep;
  int i, j;

  for (i=0; i<BS; i++)
     for (j=0; j<BS; j++) 
        Dst[i][j]=Src[i][j];
}
//#pragma css task output(Dst)
void clean_block (bout Dstdep )
{
    block_t Dst = (block_t)Dstdep;
  int i, j;

  for (i=0; i<BS; i++)
     for (j=0; j<BS; j++) 
        Dst[i][j]=0.0;
}

void LU(p_block_t A[NB][NB])
{
   int ii, jj, kk;

   for (kk=0; kk<NB; kk++) {
       spawn(lu0, (binout)*A[kk][kk]);
      for (jj=kk+1; jj<NB; jj++)
	  if (A[kk][jj] != NULL) spawn(fwd, (bin)*A[kk][kk], (binout)*A[kk][jj]);

      for (ii=kk+1; ii<NB; ii++) 
         if (A[ii][kk] != NULL)
	     spawn(bdiv, (bin)*A[kk][kk], (binout)*A[ii][kk]);

      for (ii=kk+1; ii<NB; ii++) {
         if (A[ii][kk] != NULL) {
            for (jj=kk+1; jj<NB; jj++) {
               if (A[kk][jj] != NULL) {
                  if (A[ii][jj]==NULL) A[ii][jj]=allocate_clean_block();
                  spawn(bmod, (bin)*A[ii][kk], (bin)*A[kk][jj], (binout)*A[ii][jj]);
               }
            }
         }
      }
   }
   ssync();
}

void split_mat (p_block_t LU[NB][NB],p_block_t L[NB][NB],p_block_t U[NB][NB])
{
  int ii, jj;
  p_block_t block;

  for (ii=0; ii<NB; ii++) 
     for (jj=0; jj<NB; jj++){
        if (ii==jj) {                               /* split diagonal block */
           L[ii][ii] = allocate_clean_block(); 
           U[ii][ii] = allocate_clean_block(); 
           spawn(split_block,(bin)*LU[ii][ii],(bout)*L[ii][ii],(bout)*U[ii][ii]);
        } else {                                    /* copy non diagonal block to ... */
            if (LU[ii][jj] != NULL) {
              block = allocate_clean_block(); 
              spawn(copy_block,(bin)*LU[ii][jj],(bout)*block);
           } else block = NULL;
           if (ii>jj) {                             /*         ...either L ... */
              L[ii][jj]=block;
              U[ii][jj]=NULL;
           } else {                                /*         ... or U */
              L[ii][jj]=NULL;
              U[ii][jj]=block;
          }
        }
      }
  ssync();
}

void copy_mat (p_block_t Src[NB][NB], p_block_t Dst[NB][NB])
{
  int ii, jj;
  p_block_t block;

  for (ii=0; ii<NB; ii++)
     for (jj=0; jj<NB; jj++)
        if (Src[ii][jj] != NULL) {
           block = allocate_clean_block();
           spawn(copy_block,(bin)*Src[ii][jj],(bout)*block);
           Dst[ii][jj] = block;
        } else
           Dst[ii][jj]=NULL;
  ssync();
}

void clean_mat (p_block_t Src[NB][NB])
{
  int ii, jj;

  for (ii=0; ii<NB; ii++)
     for (jj=0; jj<NB; jj++)
        if (Src[ii][jj] != NULL) {
/* this will work in sequential, but would require waiting for all uses of Src[ii][jj] in parallel
           free (Src[ii][jj]);
           Src[ii][jj]=NULL;
*/
	    spawn(clean_block,(bout)*Src[ii][jj]);
        }
  ssync();
}

/* C = A*B */
void sparse_matmult (p_block_t A[NB][NB], p_block_t B[NB][NB], p_block_t C[NB][NB])
{
  int ii, jj, kk;

  for (ii=0; ii<NB; ii++) 
     for (jj=0; jj<NB; jj++)
        for (kk=0; kk<NB; kk++)
           if ((A[ii][kk] != NULL) && (B[kk][jj] !=NULL )) {
              if (C[ii][jj] == NULL) C[ii][jj] = allocate_clean_block();
              spawn(block_mpy_add, (bin)*A[ii][kk], (bin)*B[kk][jj], (binout)*C[ii][jj]);
           }
  ssync();
}

int main(int argc, char* argv[])
{
   pp_time_t tm;
   memset( &tm, 0, sizeof(tm) );

#pragma css start

   genmat();
   D_print_mat("A", A);

   run(copy_mat,A, origA); 
   D_print_mat("reference A", origA);

   pp_time_start( &tm );
   run( LU, A );
   pp_time_end( &tm );
   D_print_mat("A=LxU", A);

   run(split_mat, A, L, U);
   D_print_mat("L", L);
   D_print_mat("U", U);

   run(clean_mat,A);
   D_print_mat("Zero A", A);

   run(sparse_matmult, L, U, A); 
   D_print_mat("LxU", A);
   compare_mat(origA, A);

#pragma css finish

   printf("Running time  = %g %s\n", pp_time_read(&tm), pp_time_unit() );
}

