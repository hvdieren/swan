/* 
 * Based on a Cilk benchmark:
 *
 * Program to multiply two rectangualar matrizes A(n,m) * B(m,n), where
 * (n < m) and (n mod 16 = 0) and (m mod n = 0). (Otherwise fill with 0s 
 * to fit the shape.)
 *
 * written by Harald Prokop (prokop@mit.edu) Fall 97.
 *
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

#include <cstdlib>
#include <cstdio>
#include <cassert>
#include "getoptions.h"
#include <swan/wf_interface.h>
#include "pp_time.h"

#ifndef BLOCK_EDGE
#define BLOCK_EDGE 16
#endif
#define BLOCK_SIZE (BLOCK_EDGE*BLOCK_EDGE)
typedef double block[BLOCK_SIZE];

double min;
double max;
int count;
double sum;

void
mult_add_block(block *A, block *B, block *R)
{
  int i, j;
  
  for (j = 0; j < 16; j += 2) {	/* 2 columns at a time */
    double *bp = &((double *) B)[j];
    for (i = 0; i < 16; i += 2) {		/* 2 rows at a time */
      double *ap = &((double *) A)[i * 16];
      double *rp = &((double *) R)[j + i * 16];
      register double s0_0, s0_1;
      register double s1_0, s1_1;
      s0_0 = rp[0];
      s0_1 = rp[1];
      s1_0 = rp[16];
      s1_1 = rp[17];
      s0_0 += ap[0] * bp[0];
      s0_1 += ap[0] * bp[1];
      s1_0 += ap[16] * bp[0];
      s1_1 += ap[16] * bp[1];
      s0_0 += ap[1] * bp[16];
      s0_1 += ap[1] * bp[17];
      s1_0 += ap[17] * bp[16];
      s1_1 += ap[17] * bp[17];
      s0_0 += ap[2] * bp[32];
      s0_1 += ap[2] * bp[33];
      s1_0 += ap[18] * bp[32];
      s1_1 += ap[18] * bp[33];
      s0_0 += ap[3] * bp[48];
      s0_1 += ap[3] * bp[49];
      s1_0 += ap[19] * bp[48];
      s1_1 += ap[19] * bp[49];
      s0_0 += ap[4] * bp[64];
      s0_1 += ap[4] * bp[65];
      s1_0 += ap[20] * bp[64];
      s1_1 += ap[20] * bp[65];
      s0_0 += ap[5] * bp[80];
      s0_1 += ap[5] * bp[81];
      s1_0 += ap[21] * bp[80];
      s1_1 += ap[21] * bp[81];
      s0_0 += ap[6] * bp[96];
      s0_1 += ap[6] * bp[97];
      s1_0 += ap[22] * bp[96];
      s1_1 += ap[22] * bp[97];
      s0_0 += ap[7] * bp[112];
      s0_1 += ap[7] * bp[113];
      s1_0 += ap[23] * bp[112];
      s1_1 += ap[23] * bp[113];
      s0_0 += ap[8] * bp[128];
      s0_1 += ap[8] * bp[129];
      s1_0 += ap[24] * bp[128];
      s1_1 += ap[24] * bp[129];
      s0_0 += ap[9] * bp[144];
      s0_1 += ap[9] * bp[145];
      s1_0 += ap[25] * bp[144];
      s1_1 += ap[25] * bp[145];
      s0_0 += ap[10] * bp[160];
      s0_1 += ap[10] * bp[161];
      s1_0 += ap[26] * bp[160];
      s1_1 += ap[26] * bp[161];
      s0_0 += ap[11] * bp[176];
      s0_1 += ap[11] * bp[177];
      s1_0 += ap[27] * bp[176];
      s1_1 += ap[27] * bp[177];
      s0_0 += ap[12] * bp[192];
      s0_1 += ap[12] * bp[193];
      s1_0 += ap[28] * bp[192];
      s1_1 += ap[28] * bp[193];
      s0_0 += ap[13] * bp[208];
      s0_1 += ap[13] * bp[209];
      s1_0 += ap[29] * bp[208];
      s1_1 += ap[29] * bp[209];
      s0_0 += ap[14] * bp[224];
      s0_1 += ap[14] * bp[225];
      s1_0 += ap[30] * bp[224];
      s1_1 += ap[30] * bp[225];
      s0_0 += ap[15] * bp[240];
      s0_1 += ap[15] * bp[241];
      s1_0 += ap[31] * bp[240];
      s1_1 += ap[31] * bp[241];
      rp[0] = s0_0;
      rp[1] = s0_1;
      rp[16] = s1_0;
      rp[17] = s1_1;
    }
  }
}

/* compute R = AB, where R,A,B are BLOCK_EDGE x BLOCK_EDGE matricies 
 */
void
multiply_block(block *A, block *B, block *R)
{
  int i, j;
  
  for (j = 0; j < 16; j += 2) {	/* 2 columns at a time */
    double *bp = &((double *) B)[j];
    for (i = 0; i < 16; i += 2) {		/* 2 rows at a time */
      double *ap = &((double *) A)[i * 16];
      double *rp = &((double *) R)[j + i * 16];
      register double s0_0, s0_1;
      register double s1_0, s1_1;
      s0_0 = ap[0] * bp[0];
      s0_1 = ap[0] * bp[1];
      s1_0 = ap[16] * bp[0];
      s1_1 = ap[16] * bp[1];
      s0_0 += ap[1] * bp[16];
      s0_1 += ap[1] * bp[17];
      s1_0 += ap[17] * bp[16];
      s1_1 += ap[17] * bp[17];
      s0_0 += ap[2] * bp[32];
      s0_1 += ap[2] * bp[33];
      s1_0 += ap[18] * bp[32];
      s1_1 += ap[18] * bp[33];
      s0_0 += ap[3] * bp[48];
      s0_1 += ap[3] * bp[49];
      s1_0 += ap[19] * bp[48];
      s1_1 += ap[19] * bp[49];
      s0_0 += ap[4] * bp[64];
      s0_1 += ap[4] * bp[65];
      s1_0 += ap[20] * bp[64];
      s1_1 += ap[20] * bp[65];
      s0_0 += ap[5] * bp[80];
      s0_1 += ap[5] * bp[81];
      s1_0 += ap[21] * bp[80];
      s1_1 += ap[21] * bp[81];
      s0_0 += ap[6] * bp[96];
      s0_1 += ap[6] * bp[97];
      s1_0 += ap[22] * bp[96];
      s1_1 += ap[22] * bp[97];
      s0_0 += ap[7] * bp[112];
      s0_1 += ap[7] * bp[113];
      s1_0 += ap[23] * bp[112];
      s1_1 += ap[23] * bp[113];
      s0_0 += ap[8] * bp[128];
      s0_1 += ap[8] * bp[129];
      s1_0 += ap[24] * bp[128];
      s1_1 += ap[24] * bp[129];
      s0_0 += ap[9] * bp[144];
      s0_1 += ap[9] * bp[145];
      s1_0 += ap[25] * bp[144];
      s1_1 += ap[25] * bp[145];
      s0_0 += ap[10] * bp[160];
      s0_1 += ap[10] * bp[161];
      s1_0 += ap[26] * bp[160];
      s1_1 += ap[26] * bp[161];
      s0_0 += ap[11] * bp[176];
      s0_1 += ap[11] * bp[177];
      s1_0 += ap[27] * bp[176];
      s1_1 += ap[27] * bp[177];
      s0_0 += ap[12] * bp[192];
      s0_1 += ap[12] * bp[193];
      s1_0 += ap[28] * bp[192];
      s1_1 += ap[28] * bp[193];
      s0_0 += ap[13] * bp[208];
      s0_1 += ap[13] * bp[209];
      s1_0 += ap[29] * bp[208];
      s1_1 += ap[29] * bp[209];
      s0_0 += ap[14] * bp[224];
      s0_1 += ap[14] * bp[225];
      s1_0 += ap[30] * bp[224];
      s1_1 += ap[30] * bp[225];
      s0_0 += ap[15] * bp[240];
      s0_1 += ap[15] * bp[241];
      s1_0 += ap[31] * bp[240];
      s1_1 += ap[31] * bp[241];
      rp[0] = s0_0;
      rp[1] = s0_1;
      rp[16] = s1_0;
      rp[17] = s1_1;
    }
  }
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
check_matrix(block *R, long x, long y, long o, double v)
{
    chandle<int> a,b;

  if ((x*y) == 1) 
    return check_block(R,v);
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
init_matrix_h(block *R, long x, long y, long o, double v)
{
  if ((x+y)==2) 
      init_block(R,v);
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

void
multiply_matrix(block *A, long oa, block *B, long ob, long x, long y, long z,
		block *R, long or_, int add)
{
    if ((x+y+z) == 3) {
	if (add)
	    mult_add_block(A, B, R);
	else
	    multiply_block(A, B, R);
    } else {
	for( unsigned k=0; k < z; ++k )
	    for( unsigned j=0; j < y; ++j )
		for( unsigned i=0; i < x; ++i )
		    mult_add_block((A+i*oa+j), (B+j*ob+k), (R+i*or_+k));
    }
}

int 
run_rectmul(long x, long y, long z, int check, long threshold)
{
  block *A, *B, * R;
  double f;
  pp_time_t tm;
  memset( &tm, 0, sizeof(tm) );
  
  A = new block[x*y];
  B = new block[y*z];
  R = new block[x*z];
  
  run(init_matrix, A, x, y, y, 1.0);
  run(init_matrix, B, y, z, z, 1.0); 
  run(init_matrix_h, R, x, z, z, 0.0);
  
  /* Timing. "Start" timers */
  pp_time_start( &tm );
  
  multiply_matrix(A,y,B,z,x,y,z,R,z,0);

  /* Timing. "Stop" timers */
  pp_time_end( &tm );
  f = pp_time_read( &tm );
  
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
    printf("Running time  = %g %s\n", pp_time_read(&tm), pp_time_unit() );
  }
  
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
