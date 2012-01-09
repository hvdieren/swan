/* compute R = R+AB, where R,A,B are BLOCK_EDGE x BLOCK_EDGE matricies 
 */
long long 
mult_add_block(block *A, block *B, block *R)
#if ALGO == 0
{
    double * pA = A[0];
    double * pB = B[0];
    double * pC = R[0];

 // using CBLAS
    cblas_dgemm(
        CblasColMajor,
        CblasNoTrans, CblasTrans,
	      (int)BLOCK_EDGE, (int)BLOCK_EDGE, (int)BLOCK_EDGE,
	      (float)1.0, pA, (int)BLOCK_EDGE,
              pB, (int)BLOCK_EDGE,
	      (float)1.0, pC, (int)BLOCK_EDGE);

    return 0;
}
#else
#if ALGO == 1
{
  int i, j, k;

  for (i = 0; i < BLOCK_EDGE; i++)
    {
      for (j = 0; j < BLOCK_EDGE; j++)
        {
        for (k=0; k < BLOCK_EDGE; k++)
        (*R)[i*BLOCK_EDGE+j] += (*A)[i*BLOCK_EDGE+k]*(*B)[k*BLOCK_EDGE+j];
        }
    }

  return BLOCK_EDGE * BLOCK_EDGE * BLOCK_EDGE * 2;
}
#else
{
  int i, j;
  long long flops = 0LL;
  
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
      flops += 128;
    }
  }
  
  return flops;
}
#endif
#endif


/* compute R = AB, where R,A,B are BLOCK_EDGE x BLOCK_EDGE matricies 
 */
long long 
multiply_block(block *A, block *B, block *R)
#if ALGO == 0
{
    double * pA = A[0];
    double * pB = B[0];
    double * pC = R[0];

 // using CBLAS
    cblas_dgemm(
        CblasColMajor,
        CblasNoTrans, CblasTrans,
	      (int)BLOCK_EDGE, (int)BLOCK_EDGE, (int)BLOCK_EDGE,
	      (float)1.0, pA, (int)BLOCK_EDGE,
              pB, (int)BLOCK_EDGE,
	      (float)0.0, pC, (int)BLOCK_EDGE);

    return 0;
}
#else
#if ALGO == 1
{
  int i, j, k;

  for (i = 0; i < BLOCK_EDGE; i++)
    {
      for (j = 0; j < BLOCK_EDGE; j++)
        {
        for (k=0; k < BLOCK_EDGE; k++)
        (*R)[i*BLOCK_EDGE+j] = (*A)[i*BLOCK_EDGE+k]*(*B)[k*BLOCK_EDGE+j];
        }
    }

  return BLOCK_EDGE * BLOCK_EDGE * BLOCK_EDGE;
}
#else
{
  int i, j;
  long long flops = 0LL;
  
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
      flops += 124;
    }
  }
  
  return flops;
}
#endif
#endif
