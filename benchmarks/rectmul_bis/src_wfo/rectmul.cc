/* 
 * Program to multiply two rectangualar matrizes A(n,m) * B(m,n), where
 * (n < m) and (n mod 16 = 0) and (m mod n = 0). (Otherwise fill with 0s 
 * to fit the shape.)
 *
 * written by Harald Prokop (prokop@mit.edu) Fall 97.
 */

static const char *ident __attribute__((__unused__))
     = "$HeadURL: https://bradley.csail.mit.edu/svn/repos/cilk/5.4.3/examples/rectmul.cilk $ $LastChangedBy: bradley $ $Rev: 517 $ $Date: 2005-10-12 12:58:40 -0400 (Wed, 12 Oct 2005) $";

/*
 * Copyright (c) 2003 Massachusetts Institute of Technology
 *
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
 */

#include <stdlib.h>
#include <stdio.h>
#include "getoptions.h"
#include "wf_interface.h"
#include "pp_time.h"

#ifndef ALGO
#define ALGO 1
#endif

#if ALGO == 0
#include "cblas.h"
#endif

#ifndef BLOCK_EDGE
#define BLOCK_EDGE 16
#endif
#define BLOCK_SIZE (BLOCK_EDGE*BLOCK_EDGE)
typedef double block[BLOCK_SIZE];

typedef obj::object_t<block> hyper_block;
typedef obj::indep<block> bin;
typedef obj::outdep<block> bout;
typedef obj::inoutdep<block> binout;

double min;
double max;
int count;
double sum;

#include "basic_ops.c"

void
mult_add_block_dep(block * A, block * B, binout R)
{
    leaf_call( mult_add_block, A, B, &*R );
}


/* Checks if each A[i,j] of a martix A of size nb x nb blocks has value v 
 */

int 
check_block (block *R, double v)
{
  int i;
  
  for (i = 0; i < BLOCK_SIZE; i++) 
      if (((double *) R)[i] != v)  {
	  assert( 0 );
	  return 1;
      }
  
  return 0;
}

int 
check_matrix(hyper_block *R, long x, long y, long o, double v)
{
    chandle<int> a,b;

  if ((x*y) == 1) 
    return check_block(&**R,v);
  else {
    if (x>y) {
	spawn(check_matrix, a, R,x/2,y,o,v);
	spawn(check_matrix, b, R+(x/2)*o,(x+1)/2,y,o,v);
	ssync();
    } 
    else {
	spawn(check_matrix, a, R,x,y/2,o,v);
	spawn(check_matrix, b, R+(y/2),x,(y+1)/2,o,v);
	ssync();
    }
  }  
  assert( !(a+b) );
  return (a+b);
}

long long 
add_block(block *T, block *R)
{
  long i;
  
  for (i = 0; i < BLOCK_SIZE; i += 4) {
    ((double *) R)[i] += ((double *) T)[i];
    ((double *) R)[i + 1] += ((double *) T)[i + 1];
    ((double *) R)[i + 2] += ((double *) T)[i + 2];
    ((double *) R)[i + 3] += ((double *) T)[i + 3];
  }
  
  return BLOCK_SIZE;
}
 
/* Add matrix T into matrix R, where T and R are bl blocks in size 
 *
 */
 
long long 
add_matrix(block *T, long ot, block *R, long or_, long x, long y)
{
    chandle<long long> flops1, flops2;
  
  if ((x+y)==2) {
    return add_block(T,R);
  } 
  else {
    if (x>y) {
	spawn(add_matrix, flops1, T,ot,R,or_,x/2,y);
	spawn(add_matrix, flops2, T+(x/2)*ot,ot,R+(x/2)*or_,or_,(x+1)/2,y);
    } 
    else {
	spawn(add_matrix, flops1, T,ot,R,or_,x,y/2);
	spawn(add_matrix, flops2, T+(y/2),ot,R+(y/2),or_,x,(y+1)/2);
    }
    
    ssync();
    return (long long)flops1 + (long long)flops2;
  }
}


void 
init_block(block* R, double v)
{
  int i;

  for (i = 0; i < BLOCK_SIZE; i++)
    ((double *) R)[i] = v;
}

void 
init_matrix(block *R, long x, long y, long o, double v)
{
  if ((x+y)==2) 
      init_block(R,v);
  else  {
    if (x>y) {
	spawn(init_matrix, R,x/2,y,o,v);
	spawn(init_matrix, R+(x/2)*o,(x+1)/2,y,o,v);
    } 
    else {
	spawn(init_matrix, R,x,y/2,o,v);
	spawn(init_matrix, R+(y/2),x,(y+1)/2,o,v);
    }
  }
  ssync();
}

void 
init_matrix_h(hyper_block *R, long x, long y, long o, double v)
{
  if ((x+y)==2) 
      init_block(&*R[0],v);
  else  {
    if (x>y) {
	spawn(init_matrix_h, R,x/2,y,o,v);
	spawn(init_matrix_h, R+(x/2)*o,(x+1)/2,y,o,v);
    } 
    else {
	spawn(init_matrix_h, R,x,y/2,o,v);
	spawn(init_matrix_h, R+(y/2),x,(y+1)/2,o,v);
    }
  }
  ssync();
}

long long 
multiply_matrix_ooo(block *A, long oa, block *B, long ob, long x, long y, long z, hyper_block *R, long or_, int add)
{
    if ((x+y+z) == 3) {
	if (add)
	    return mult_add_block(A, B, &**R);
	else
	    return multiply_block(A, B, &**R);
    } 
    else {
	chandle<long long> flops1, flops2;
	long long sflops = 0ULL;
	for( unsigned j=0; j < y; ++j ) {
	    for( unsigned i=0; i < x; ++i ) {
		for( unsigned k=0; k < z; ++k ) {
		    spawn(mult_add_block_dep, (A+i*oa+j),
			  (B+j*ob+k), (binout)*(R+i*or_+k));
		}
	    }
	}
	ssync();
	return sflops;
    }
}

#if 0
long long 
multiply_matrix(block *A, long oa, block *B, long ob, long x, long y, long z, hyper_block *R, long or_, int add, long threshold)
{
    if ((x+y+z) == 3) {
	if (add)
	    return mult_add_block(A, B, &**R);
	else
	    return multiply_block(A, B, &**R);
    } else if ((x+y+z) <= threshold) {
	return call(multiply_matrix_ooo, A,oa,B,ob,x,y,z,R,or_,add);
    } else {
	chandle<long long> flops1, flops2;
	long long sflops = 0ULL;
    
	if ((x>=y) && (x>=z)) {  
	    spawn(multiply_matrix, flops1, A,oa,B,ob,x/2,y,z,R,or_,add,threshold);
	    spawn(multiply_matrix, flops2, A+(x/2)*oa,oa,B,ob,(x+1)/2,y,z,R+(x/2)*or_,or_,add,threshold);
	} 
	else {
	    if ((y>x) && (y>z)){
		spawn(multiply_matrix, flops1, A+(y/2),oa,B+(y/2)*ob,ob,x,(y+1)/2,z,R,or_,add,threshold);

		if (SYNCHED() || true) {
		    ssync();
		    spawn(multiply_matrix, flops2, A,oa,B,ob,x,y/2,z,R,or_,1,threshold);
		}
#if 0
		else {
		    block *tmp = (block *)/*Cilk_*/alloca(x*z*sizeof(block));

		    spawn(multiply_matrix, flops2, A,oa,B,ob,x,y/2,z,tmp,z,0,threshold);
		    ssync();
	      
		    sflops += call(add_matrix,tmp,z,R,or_,x,z);
		}
#endif
	    }
	    else {
		spawn(multiply_matrix, flops1, A,oa,B,ob,x,y,z/2,R,or_,add,threshold);
		spawn(multiply_matrix, flops2, A,oa,B+(z/2),ob,x,y,(z+1)/2,(R+(z/2)),or_,add,threshold);
	    } 
	}
    
	ssync();
	return sflops + (long long)flops1 + (long long)flops2;
    }
}
#endif

int 
run_rectmul(long x, long y, long z, int check, long threshold)
{
  block *A, *B;
  hyper_block *R;
  long long flops;
  double f;
  pp_time_t tm;
  memset( &tm, 0, sizeof(tm) );
  
  // A = (block *)malloc(x*y * sizeof(block));
  // B = (block *)malloc(y*z * sizeof(block));
  // R = (block *)malloc(x*z * sizeof(block));
  A = new block[x*y];
  B = new block[y*z];
  R = new hyper_block[x*z];
  
  run(init_matrix, A, x, y, y, 1.0);
  run(init_matrix, B, y, z, z, 1.0); 
  run(init_matrix_h, R, x, z, z, 0.0);
       // sync;
  
  /* Timing. "Start" timers */
  pp_time_start( &tm );
  
  // flops = run(multiply_matrix,A,y,B,z,x,y,z,R,z,0,threshold);
  flops = run(multiply_matrix_ooo,A,y,B,z,x,y,z,R,z,0);

  /* Timing. "Stop" timers */
  pp_time_end( &tm );
  
  f = (double) flops / pp_time_read(&tm);
  
  if (check) {
      check = run(check_matrix, R, x, z, z, (double)(y*BLOCK_EDGE));
  }

  if (min>f) min=f;
  if (max<f) max=f;
  sum+=f;
  count++;
  
  if (check) {
    printf("WRONG RESULT!\n");
    exit( 1 );
  } else {	
    printf("\nCilk Example: rectmul\n");
    printf("Options: x = %ld\n", BLOCK_EDGE*x);
    printf("         y = %ld\n", BLOCK_EDGE*y);
    printf("         z = %ld\n\n", BLOCK_EDGE*z);
    printf("Mflops        = %4f \n", (double) flops / pp_time_read(&tm) );
    printf("Running time  = %g %s\n", pp_time_read(&tm), pp_time_unit() );
  }
  
  // free(A);
  // free(B);
  // free(R);
  delete[] A;
  delete[] B;
  delete[] R;
  
  return 0;
}


int usage(void) {
  fprintf(stderr, "\nUsage: rectmul [<cilk-options>] [<options>]\n\n");
  return 1;
}

char *specifiers[] = { "-x", "-y", "-z", "-c", "-t", "-benchmark", "-h", 0};
int opt_types[] = {INTARG, INTARG, INTARG, BOOLARG, INTARG, BENCHMARK, BOOLARG, 0 };

int 
main(int argc, char *argv[])
{
  int x, y, z, benchmark, help, t, check, threshold;

  /* standard benchmark options */
  x = 512;
  y = 512;
  z = 512;
  check = 0;
  threshold = 0;

  get_options(argc, argv, specifiers, opt_types, &x, &y, &z, &check, &threshold, &benchmark, &help);

  if (help) return usage();

  if (benchmark) {
    switch (benchmark) {
    case 1:      /* short benchmark options -- a little work*/
      x = 256;
      y = 256;
      z = 256;
     break;
    case 2:      /* standard benchmark options*/
      x = 512;
      y = 512;
      z = 512;
      break;
    case 3:      /* long benchmark options -- a lot of work*/
      x = 2048;
      y = 2048;
      z = 2048;
      break;
    }
  }

  x = x/BLOCK_EDGE;
  y = y/BLOCK_EDGE;
  z = z/BLOCK_EDGE;

  if (x<1) x=1;
  if (y<1) y=1;
  if (z<1) z=1;
  
  t = run_rectmul((long)x,(long)y,(long)z,(int)check,(long)threshold);

  return t; 
}
