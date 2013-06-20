
/*-------------------------------------------------------------*/
/*--- Compression machinery (not incl block sorting)        ---*/
/*---                                            compress.c ---*/
/*-------------------------------------------------------------*/

/* ------------------------------------------------------------------
   This file is part of bzip2/libbzip2, a program and library for
   lossless, block-sorting data compression.

   bzip2/libbzip2 version 1.0.5 of 10 December 2007
   Copyright (C) 1996-2007 Julian Seward <jseward@bzip.org>

   Please read the WARNING, DISCLAIMER and PATENTS sections in the 
   README file.

   This program is released under the terms of the license contained
   in the file LICENSE.
   ------------------------------------------------------------------ */


/* CHANGES
    0.9.0    -- original version.
    0.9.0a/b -- no changes in this file.
    0.9.0c   -- changed setting of nGroups in sendMTFValues() 
                so as to do a bit better on small files
*/

#include "bzlib_private.h"

#ifdef __cplusplus
#include <cassert>
#include <list>
#include <inttypes.h>
#ifdef __unix
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <sys/mman.h>
#ifdef __sun
#include <sys/stat.h>
#endif
#endif
#ifdef __APPLE__ /* using __APPLE__ to mean Darwin which is unixy */
#include <errno.h>
#include <sys/mman.h>
#include <sys/stat.h>
#endif
#ifdef __linux
#include <sys/types.h>
#include <sys/stat.h>
#endif
#ifdef __cilkplusplus
#include <cilk.h>
#endif

extern "C"
{
#endif
    static void bsFinishWrite ( EState* s );
    static void bsW ( EState* s, Int32 n, UInt32 v );
    static void bsPutUInt32 ( EState* s, UInt32 u );
    static void bsPutUChar ( EState* s, UChar c );
    static void makeMaps_e ( EState* s );
    static void generateMTFValues ( EState* s );
    static void sendMTFValues ( EState* s );
#ifdef BZLIB_CILK
    static EState *BZ2_bzCompressInitCilk(int, int, int);
#endif
#ifdef __cplusplus
}
#endif

/*---------------------------------------------------*/
/*--- Bit stream I/O                              ---*/
/*---------------------------------------------------*/

/*---------------------------------------------------*/
void BZ2_bsInitWrite ( EState* s )
{
   s->bsLive = 0;
   s->bsBuff = 0;
}


/*---------------------------------------------------*/
static
void bsFinishWrite ( EState* s )
{
   while (s->bsLive > 0) {
      s->zbits[s->numZ] = (UChar)(s->bsBuff >> 24);
      s->numZ++;
      s->bsBuff <<= 8;
      s->bsLive -= 8;
   }
}


/*---------------------------------------------------*/
#define bsNEEDW(nz)                           \
{                                             \
   while (s->bsLive >= 8) {                   \
      s->zbits[s->numZ]                       \
         = (UChar)(s->bsBuff >> 24);          \
      s->numZ++;                              \
      s->bsBuff <<= 8;                        \
      s->bsLive -= 8;                         \
   }                                          \
}


/*---------------------------------------------------*/
static
__inline__
void bsW ( EState* s, Int32 n, UInt32 v )
{
   bsNEEDW ( n );
   s->bsBuff |= (v << (32 - s->bsLive - n));
   s->bsLive += n;
}


/*---------------------------------------------------*/
static
void bsPutUInt32 ( EState* s, UInt32 u )
{
   bsW ( s, 8, (u >> 24) & 0xffL );
   bsW ( s, 8, (u >> 16) & 0xffL );
   bsW ( s, 8, (u >>  8) & 0xffL );
   bsW ( s, 8,  u        & 0xffL );
}


/*---------------------------------------------------*/
static
void bsPutUChar ( EState* s, UChar c )
{
   bsW( s, 8, (UInt32)c );
}


/*---------------------------------------------------*/
/*--- The back end proper                         ---*/
/*---------------------------------------------------*/

/*---------------------------------------------------*/
static
void makeMaps_e ( EState* s )
{
   Int32 i;
   s->nInUse = 0;
   for (i = 0; i < 256; i++)
      if (s->inUse[i]) {
         s->unseqToSeq[i] = s->nInUse;
         s->nInUse++;
      }
}


/*---------------------------------------------------*/
static
void generateMTFValues ( EState* s )
{
   UChar   yy[256];
   Int32   i, j;
   Int32   zPend;
   Int32   wr;
   Int32   EOB;

   /* 
      After sorting (eg, here),
         s->arr1 [ 0 .. s->nblock-1 ] holds sorted order,
         and
         ((UChar*)s->arr2) [ 0 .. s->nblock-1 ] 
         holds the original block data.

      The first thing to do is generate the MTF values,
      and put them in
         ((UInt16*)s->arr1) [ 0 .. s->nblock-1 ].
      Because there are strictly fewer or equal MTF values
      than block values, ptr values in this area are overwritten
      with MTF values only when they are no longer needed.

      The final compressed bitstream is generated into the
      area starting at
         (UChar*) (&((UChar*)s->arr2)[s->nblock])

      These storage aliases are set up in bzCompressInit(),
      except for the last one, which is arranged in 
      compressBlock().
   */
   UInt32* ptr   = s->ptr;
   UChar* block  = s->block;
   UInt16* mtfv  = s->mtfv;

   makeMaps_e ( s );
   EOB = s->nInUse+1;

   for (i = 0; i <= EOB; i++) s->mtfFreq[i] = 0;

   wr = 0;
   zPend = 0;
   for (i = 0; i < s->nInUse; i++) yy[i] = (UChar) i;

   for (i = 0; i < s->nblock; i++) {
      UChar ll_i;
      AssertD ( wr <= i, "generateMTFValues(1)" );
      j = ptr[i]-1; if (j < 0) j += s->nblock;
      ll_i = s->unseqToSeq[block[j]];
      AssertD ( ll_i < s->nInUse, "generateMTFValues(2a)" );

      if (yy[0] == ll_i) { 
         zPend++;
      } else {

         if (zPend > 0) {
            zPend--;
            while (True) {
               if (zPend & 1) {
                  mtfv[wr] = BZ_RUNB; wr++; 
                  s->mtfFreq[BZ_RUNB]++; 
               } else {
                  mtfv[wr] = BZ_RUNA; wr++; 
                  s->mtfFreq[BZ_RUNA]++; 
               }
               if (zPend < 2) break;
               zPend = (zPend - 2) / 2;
            };
            zPend = 0;
         }
         {
            register UChar  rtmp;
            register UChar* ryy_j;
            register UChar  rll_i;
            rtmp  = yy[1];
            yy[1] = yy[0];
            ryy_j = &(yy[1]);
            rll_i = ll_i;
            while ( rll_i != rtmp ) {
               register UChar rtmp2;
               ryy_j++;
               rtmp2  = rtmp;
               rtmp   = *ryy_j;
               *ryy_j = rtmp2;
            };
            yy[0] = rtmp;
            j = ryy_j - &(yy[0]);
            mtfv[wr] = j+1; wr++; s->mtfFreq[j+1]++;
         }

      }
   }

   if (zPend > 0) {
      zPend--;
      while (True) {
         if (zPend & 1) {
            mtfv[wr] = BZ_RUNB; wr++; 
            s->mtfFreq[BZ_RUNB]++; 
         } else {
            mtfv[wr] = BZ_RUNA; wr++; 
            s->mtfFreq[BZ_RUNA]++; 
         }
         if (zPend < 2) break;
         zPend = (zPend - 2) / 2;
      };
      zPend = 0;
   }

   mtfv[wr] = EOB; wr++; s->mtfFreq[EOB]++;

   s->nMTF = wr;
}


/*---------------------------------------------------*/
#define BZ_LESSER_ICOST  0
#define BZ_GREATER_ICOST 15

static
void sendMTFValues ( EState* s )
{
   Int32 v, t, i, j, gs, ge, totc, bt, bc, iter;
   Int32 nSelectors, alphaSize, minLen, maxLen, selCtr;
   Int32 nGroups, nBytes;

   /*--
   UChar  len [BZ_N_GROUPS][BZ_MAX_ALPHA_SIZE];
   is a global since the decoder also needs it.

   Int32  code[BZ_N_GROUPS][BZ_MAX_ALPHA_SIZE];
   Int32  rfreq[BZ_N_GROUPS][BZ_MAX_ALPHA_SIZE];
   are also globals only used in this proc.
   Made global to keep stack frame size small.
   --*/


   UInt16 cost[BZ_N_GROUPS];
   Int32  fave[BZ_N_GROUPS];

   UInt16* mtfv = s->mtfv;

   if (s->verbosity >= 3)
      VPrintf3( "      %d in block, %d after MTF & 1-2 coding, "
                "%d+2 syms in use\n", 
                s->nblock, s->nMTF, s->nInUse );

   alphaSize = s->nInUse+2;
   for (t = 0; t < BZ_N_GROUPS; t++)
      for (v = 0; v < alphaSize; v++)
         s->len[t][v] = BZ_GREATER_ICOST;

   /*--- Decide how many coding tables to use ---*/
   AssertH ( s->nMTF > 0, 3001 );
   if (s->nMTF < 200)  nGroups = 2; else
   if (s->nMTF < 600)  nGroups = 3; else
   if (s->nMTF < 1200) nGroups = 4; else
   if (s->nMTF < 2400) nGroups = 5; else
                       nGroups = 6;

   /*--- Generate an initial set of coding tables ---*/
   { 
      Int32 nPart, remF, tFreq, aFreq;

      nPart = nGroups;
      remF  = s->nMTF;
      gs = 0;
      while (nPart > 0) {
         tFreq = remF / nPart;
         ge = gs-1;
         aFreq = 0;
         while (aFreq < tFreq && ge < alphaSize-1) {
            ge++;
            aFreq += s->mtfFreq[ge];
         }

         if (ge > gs 
             && nPart != nGroups && nPart != 1 
             && ((nGroups-nPart) % 2 == 1)) {
            aFreq -= s->mtfFreq[ge];
            ge--;
         }

         if (s->verbosity >= 3)
            VPrintf5( "      initial group %d, [%d .. %d], "
                      "has %d syms (%4.1f%%)\n",
                      nPart, gs, ge, aFreq, 
                      (100.0 * (float)aFreq) / (float)(s->nMTF) );
 
         for (v = 0; v < alphaSize; v++)
            if (v >= gs && v <= ge) 
               s->len[nPart-1][v] = BZ_LESSER_ICOST; else
               s->len[nPart-1][v] = BZ_GREATER_ICOST;
 
         nPart--;
         gs = ge+1;
         remF -= aFreq;
      }
   }

   /*--- 
      Iterate up to BZ_N_ITERS times to improve the tables.
   ---*/
   for (iter = 0; iter < BZ_N_ITERS; iter++) {

      for (t = 0; t < nGroups; t++) fave[t] = 0;

      for (t = 0; t < nGroups; t++)
         for (v = 0; v < alphaSize; v++)
            s->rfreq[t][v] = 0;

      /*---
        Set up an auxiliary length table which is used to fast-track
	the common case (nGroups == 6). 
      ---*/
      if (nGroups == 6) {
         for (v = 0; v < alphaSize; v++) {
            s->len_pack[v][0] = (s->len[1][v] << 16) | s->len[0][v];
            s->len_pack[v][1] = (s->len[3][v] << 16) | s->len[2][v];
            s->len_pack[v][2] = (s->len[5][v] << 16) | s->len[4][v];
	 }
      }

      nSelectors = 0;
      totc = 0;
      gs = 0;
      while (True) {

         /*--- Set group start & end marks. --*/
         if (gs >= s->nMTF) break;
         ge = gs + BZ_G_SIZE - 1; 
         if (ge >= s->nMTF) ge = s->nMTF-1;

         /*-- 
            Calculate the cost of this group as coded
            by each of the coding tables.
         --*/
         for (t = 0; t < nGroups; t++) cost[t] = 0;

         if (nGroups == 6 && 50 == ge-gs+1) {
            /*--- fast track the common case ---*/
            register UInt32 cost01, cost23, cost45;
            register UInt16 icv;
            cost01 = cost23 = cost45 = 0;

#           define BZ_ITER(nn)                \
               icv = mtfv[gs+(nn)];           \
               cost01 += s->len_pack[icv][0]; \
               cost23 += s->len_pack[icv][1]; \
               cost45 += s->len_pack[icv][2]; \

            BZ_ITER(0);  BZ_ITER(1);  BZ_ITER(2);  BZ_ITER(3);  BZ_ITER(4);
            BZ_ITER(5);  BZ_ITER(6);  BZ_ITER(7);  BZ_ITER(8);  BZ_ITER(9);
            BZ_ITER(10); BZ_ITER(11); BZ_ITER(12); BZ_ITER(13); BZ_ITER(14);
            BZ_ITER(15); BZ_ITER(16); BZ_ITER(17); BZ_ITER(18); BZ_ITER(19);
            BZ_ITER(20); BZ_ITER(21); BZ_ITER(22); BZ_ITER(23); BZ_ITER(24);
            BZ_ITER(25); BZ_ITER(26); BZ_ITER(27); BZ_ITER(28); BZ_ITER(29);
            BZ_ITER(30); BZ_ITER(31); BZ_ITER(32); BZ_ITER(33); BZ_ITER(34);
            BZ_ITER(35); BZ_ITER(36); BZ_ITER(37); BZ_ITER(38); BZ_ITER(39);
            BZ_ITER(40); BZ_ITER(41); BZ_ITER(42); BZ_ITER(43); BZ_ITER(44);
            BZ_ITER(45); BZ_ITER(46); BZ_ITER(47); BZ_ITER(48); BZ_ITER(49);

#           undef BZ_ITER

            cost[0] = cost01 & 0xffff; cost[1] = cost01 >> 16;
            cost[2] = cost23 & 0xffff; cost[3] = cost23 >> 16;
            cost[4] = cost45 & 0xffff; cost[5] = cost45 >> 16;

         } else {
	    /*--- slow version which correctly handles all situations ---*/
            for (i = gs; i <= ge; i++) { 
               UInt16 icv = mtfv[i];
               for (t = 0; t < nGroups; t++) cost[t] += s->len[t][icv];
            }
         }
 
         /*-- 
            Find the coding table which is best for this group,
            and record its identity in the selector table.
         --*/
         bc = 999999999; bt = -1;
         for (t = 0; t < nGroups; t++)
            if (cost[t] < bc) { bc = cost[t]; bt = t; };
         totc += bc;
         fave[bt]++;
         s->selector[nSelectors] = bt;
         nSelectors++;

         /*-- 
            Increment the symbol frequencies for the selected table.
          --*/
         if (nGroups == 6 && 50 == ge-gs+1) {
            /*--- fast track the common case ---*/

#           define BZ_ITUR(nn) s->rfreq[bt][ mtfv[gs+(nn)] ]++

            BZ_ITUR(0);  BZ_ITUR(1);  BZ_ITUR(2);  BZ_ITUR(3);  BZ_ITUR(4);
            BZ_ITUR(5);  BZ_ITUR(6);  BZ_ITUR(7);  BZ_ITUR(8);  BZ_ITUR(9);
            BZ_ITUR(10); BZ_ITUR(11); BZ_ITUR(12); BZ_ITUR(13); BZ_ITUR(14);
            BZ_ITUR(15); BZ_ITUR(16); BZ_ITUR(17); BZ_ITUR(18); BZ_ITUR(19);
            BZ_ITUR(20); BZ_ITUR(21); BZ_ITUR(22); BZ_ITUR(23); BZ_ITUR(24);
            BZ_ITUR(25); BZ_ITUR(26); BZ_ITUR(27); BZ_ITUR(28); BZ_ITUR(29);
            BZ_ITUR(30); BZ_ITUR(31); BZ_ITUR(32); BZ_ITUR(33); BZ_ITUR(34);
            BZ_ITUR(35); BZ_ITUR(36); BZ_ITUR(37); BZ_ITUR(38); BZ_ITUR(39);
            BZ_ITUR(40); BZ_ITUR(41); BZ_ITUR(42); BZ_ITUR(43); BZ_ITUR(44);
            BZ_ITUR(45); BZ_ITUR(46); BZ_ITUR(47); BZ_ITUR(48); BZ_ITUR(49);

#           undef BZ_ITUR

         } else {
	    /*--- slow version which correctly handles all situations ---*/
            for (i = gs; i <= ge; i++)
               s->rfreq[bt][ mtfv[i] ]++;
         }

         gs = ge+1;
      }
      if (s->verbosity >= 3) {
         VPrintf2 ( "      pass %d: size is %d, grp uses are ", 
                   iter+1, totc/8 );
         for (t = 0; t < nGroups; t++)
            VPrintf1 ( "%d ", fave[t] );
         VPrintf0 ( "\n" );
      }

      /*--
        Recompute the tables based on the accumulated frequencies.
      --*/
      /* maxLen was changed from 20 to 17 in bzip2-1.0.3.  See 
         comment in huffman.c for details. */
      for (t = 0; t < nGroups; t++)
         BZ2_hbMakeCodeLengths ( &(s->len[t][0]), &(s->rfreq[t][0]), 
                                 alphaSize, 17 /*20*/ );
   }


   AssertH( nGroups < 8, 3002 );
   AssertH( nSelectors < 32768 &&
            nSelectors <= (2 + (900000 / BZ_G_SIZE)),
            3003 );


   /*--- Compute MTF values for the selectors. ---*/
   {
      UChar pos[BZ_N_GROUPS], ll_i, tmp2, tmp;
      for (i = 0; i < nGroups; i++) pos[i] = i;
      for (i = 0; i < nSelectors; i++) {
         ll_i = s->selector[i];
         j = 0;
         tmp = pos[j];
         while ( ll_i != tmp ) {
            j++;
            tmp2 = tmp;
            tmp = pos[j];
            pos[j] = tmp2;
         };
         pos[0] = tmp;
         s->selectorMtf[i] = j;
      }
   };

   /*--- Assign actual codes for the tables. --*/
   for (t = 0; t < nGroups; t++) {
      minLen = 32;
      maxLen = 0;
      for (i = 0; i < alphaSize; i++) {
         if (s->len[t][i] > maxLen) maxLen = s->len[t][i];
         if (s->len[t][i] < minLen) minLen = s->len[t][i];
      }
      AssertH ( !(maxLen > 17 /*20*/ ), 3004 );
      AssertH ( !(minLen < 1),  3005 );
      BZ2_hbAssignCodes ( &(s->code[t][0]), &(s->len[t][0]), 
                          minLen, maxLen, alphaSize );
   }

   /*--- Transmit the mapping table. ---*/
   { 
      Bool inUse16[16];
      for (i = 0; i < 16; i++) {
          inUse16[i] = False;
          for (j = 0; j < 16; j++)
             if (s->inUse[i * 16 + j]) inUse16[i] = True;
      }
     
      nBytes = s->numZ;
      for (i = 0; i < 16; i++)
         if (inUse16[i]) bsW(s,1,1); else bsW(s,1,0);

      for (i = 0; i < 16; i++)
         if (inUse16[i])
            for (j = 0; j < 16; j++) {
               if (s->inUse[i * 16 + j]) bsW(s,1,1); else bsW(s,1,0);
            }

      if (s->verbosity >= 3) 
         VPrintf1( "      bytes: mapping %d, ", s->numZ-nBytes );
   }

   /*--- Now the selectors. ---*/
   nBytes = s->numZ;
   bsW ( s, 3, nGroups );
   bsW ( s, 15, nSelectors );
   for (i = 0; i < nSelectors; i++) { 
      for (j = 0; j < s->selectorMtf[i]; j++) bsW(s,1,1);
      bsW(s,1,0);
   }
   if (s->verbosity >= 3)
      VPrintf1( "selectors %d, ", s->numZ-nBytes );

   /*--- Now the coding tables. ---*/
   nBytes = s->numZ;

   for (t = 0; t < nGroups; t++) {
      Int32 curr = s->len[t][0];
      bsW ( s, 5, curr );
      for (i = 0; i < alphaSize; i++) {
         while (curr < s->len[t][i]) { bsW(s,2,2); curr++; /* 10 */ };
         while (curr > s->len[t][i]) { bsW(s,2,3); curr--; /* 11 */ };
         bsW ( s, 1, 0 );
      }
   }

   if (s->verbosity >= 3)
      VPrintf1 ( "code lengths %d, ", s->numZ-nBytes );

   /*--- And finally, the block data proper ---*/
   nBytes = s->numZ;
   selCtr = 0;
   gs = 0;
   while (True) {
      if (gs >= s->nMTF) break;
      ge = gs + BZ_G_SIZE - 1; 
      if (ge >= s->nMTF) ge = s->nMTF-1;
      AssertH ( s->selector[selCtr] < nGroups, 3006 );

      if (nGroups == 6 && 50 == ge-gs+1) {
            /*--- fast track the common case ---*/
            UInt16 mtfv_i;
            UChar* s_len_sel_selCtr 
               = &(s->len[s->selector[selCtr]][0]);
            Int32* s_code_sel_selCtr
               = &(s->code[s->selector[selCtr]][0]);

#           define BZ_ITAH(nn)                      \
               mtfv_i = mtfv[gs+(nn)];              \
               bsW ( s,                             \
                     s_len_sel_selCtr[mtfv_i],      \
                     s_code_sel_selCtr[mtfv_i] )

            BZ_ITAH(0);  BZ_ITAH(1);  BZ_ITAH(2);  BZ_ITAH(3);  BZ_ITAH(4);
            BZ_ITAH(5);  BZ_ITAH(6);  BZ_ITAH(7);  BZ_ITAH(8);  BZ_ITAH(9);
            BZ_ITAH(10); BZ_ITAH(11); BZ_ITAH(12); BZ_ITAH(13); BZ_ITAH(14);
            BZ_ITAH(15); BZ_ITAH(16); BZ_ITAH(17); BZ_ITAH(18); BZ_ITAH(19);
            BZ_ITAH(20); BZ_ITAH(21); BZ_ITAH(22); BZ_ITAH(23); BZ_ITAH(24);
            BZ_ITAH(25); BZ_ITAH(26); BZ_ITAH(27); BZ_ITAH(28); BZ_ITAH(29);
            BZ_ITAH(30); BZ_ITAH(31); BZ_ITAH(32); BZ_ITAH(33); BZ_ITAH(34);
            BZ_ITAH(35); BZ_ITAH(36); BZ_ITAH(37); BZ_ITAH(38); BZ_ITAH(39);
            BZ_ITAH(40); BZ_ITAH(41); BZ_ITAH(42); BZ_ITAH(43); BZ_ITAH(44);
            BZ_ITAH(45); BZ_ITAH(46); BZ_ITAH(47); BZ_ITAH(48); BZ_ITAH(49);

#           undef BZ_ITAH

      } else {
	 /*--- slow version which correctly handles all situations ---*/
         for (i = gs; i <= ge; i++) {
            bsW ( s, 
                  s->len  [s->selector[selCtr]] [mtfv[i]],
                  s->code [s->selector[selCtr]] [mtfv[i]] );
         }
      }


      gs = ge+1;
      selCtr++;
   }
   AssertH( selCtr == nSelectors, 3007 );

   if (s->verbosity >= 3)
      VPrintf1( "codes %d\n", s->numZ-nBytes );
}


/*---------------------------------------------------*/
void BZ2_compressBlock ( EState* s, Bool is_last_block )
{
   if (s->nblock > 0) {

      BZ_FINALISE_CRC ( s->blockCRC );
      s->combinedCRC = (s->combinedCRC << 1) | (s->combinedCRC >> 31);
      s->combinedCRC ^= s->blockCRC;
      if (s->blockNo > 1) s->numZ = 0;

      if (s->verbosity >= 2)
         VPrintf4( "    block %d: crc = 0x%08x, "
                   "combined CRC = 0x%08x, size = %d\n",
                   s->blockNo, s->blockCRC, s->combinedCRC, s->nblock );

      BZ2_blockSort ( s );
   }

   s->zbits = (UChar*) (&((UChar*)s->arr2)[s->nblock]);

   /*-- If this is the first block, create the stream header. --*/
   if (s->blockNo == 1) {
      BZ2_bsInitWrite ( s );
      bsPutUChar ( s, BZ_HDR_B );
      bsPutUChar ( s, BZ_HDR_Z );
      bsPutUChar ( s, BZ_HDR_h );
      bsPutUChar ( s, (UChar)(BZ_HDR_0 + s->blockSize100k) );
   }

   if (s->nblock > 0) {

      bsPutUChar ( s, 0x31 ); bsPutUChar ( s, 0x41 );
      bsPutUChar ( s, 0x59 ); bsPutUChar ( s, 0x26 );
      bsPutUChar ( s, 0x53 ); bsPutUChar ( s, 0x59 );

      /*-- Now the block's CRC, so it is in a known place. --*/
      bsPutUInt32 ( s, s->blockCRC );

      /*-- 
         Now a single bit indicating (non-)randomisation. 
         As of version 0.9.5, we use a better sorting algorithm
         which makes randomisation unnecessary.  So always set
         the randomised bit to 'no'.  Of course, the decoder
         still needs to be able to handle randomised blocks
         so as to maintain backwards compatibility with
         older versions of bzip2.
      --*/
      bsW(s,1,0);

      bsW ( s, 24, s->origPtr );
      generateMTFValues ( s );
      sendMTFValues ( s );
   }


   /*-- If this is the last block, add the stream trailer. --*/
   if (is_last_block) {

      bsPutUChar ( s, 0x17 ); bsPutUChar ( s, 0x72 );
      bsPutUChar ( s, 0x45 ); bsPutUChar ( s, 0x38 );
      bsPutUChar ( s, 0x50 ); bsPutUChar ( s, 0x90 );
      bsPutUInt32 ( s, s->combinedCRC );
      if (s->verbosity >= 2)
         VPrintf1( "    final combined CRC = 0x%08x\n   ", s->combinedCRC );
      bsFinishWrite ( s );
   }
}

/* Cilk API */
#ifdef BZLIB_CILK

#ifndef __cplusplus
#error "Cilk++ mode requires a C++ or Cilk++ compiler"
#endif

/* Flush all full bytes.  The caller will deal with the extra 0-7 bits. */
extern "C" { static void bsFinishWriteCilk(EState* s); }

static void bsFinishWriteCilk(EState* s)
{
    while (s->bsLive > 7) {
        s->zbits[s->numZ] = (UChar)(s->bsBuff >> 24);
        s->numZ++;
        s->bsBuff <<= 8;
        s->bsLive -= 8;
    }
    return;
}


static EState *BZ2_bzCompressInitCilk
                    (int        blockSize100k,
                     int        verbosity,
                     int        workFactor)
{
    EState* s = new EState;

    s->strm = 0;

    const int n   = 100000 * blockSize100k;
    s->arr1 = (UInt32 *)malloc( n                  * sizeof(UInt32) );
    s->arr2 = (UInt32 *)malloc( (n+BZ_N_OVERSHOOT) * sizeof(UInt32) );
    s->ftab = (UInt32 *)malloc( 65537              * sizeof(UInt32) );

    if (s->arr1 == NULL || s->arr2 == NULL || s->ftab == NULL) {
        free(s->arr1);
        free(s->arr2);
        free(s->ftab);
        return 0;
    }

    s->blockNo           = 0;
    s->state             = BZ_S_INPUT;
    s->mode              = BZ_M_RUNNING;
    BZ_INITIALISE_CRC (s->blockCRC);
    s->blockSize100k     = blockSize100k;
    s->nblockMAX         = 100000 * blockSize100k - 19;
    s->verbosity         = verbosity;
    s->workFactor        = workFactor;

    s->block             = (UChar*)s->arr2;
    s->mtfv              = (UInt16*)s->arr1;
    s->zbits             = NULL;
    s->ptr               = (UInt32*)s->arr1;

    s->bsBuff = 0;
    s->bsLive = 0;

    /* inlined init_RL */
    s->state_in_ch  = 256;
    s->state_in_len = 0;

    /* inlined prepare_new_block */
    s->nblock = 0;
    s->numZ = 0;
    s->state_out_pos = 0;
    /*BZ_INITIALISE_CRC (s->blockCRC);*/
    memset(s->inUse, 0, sizeof s->inUse);
    /*memset(s->unseqToSeq, 0, sizeof s->unseqToSeq);*/
    s->blockNo++;

    return s;
}


/* A bzip stream is a stream of bits.  The memblock structure
   represents a sequence of bytes (which may or may not ultimately
   land on a byte boundary) plus 0 to 7 extra bits.  The bitblock
   structure represents those extra bits. */

extern "C++"
struct bitblock
{
    unsigned char bits, numbits;
    bitblock(unsigned char bits0, int numbits0)
      : bits(bits0), numbits(numbits0)
      {
          AssertD(numbits < 8, "bitblock bits");
      }
    bitblock() : bits(0), numbits(0) { }
    void reset() { bits = 0; numbits = 0; }
};

extern "C++"
struct memblock
{
    memblock(unsigned int bytes, unsigned char *data, bitblock overflow)
        : bytes(bytes), overflow(overflow), data(data)
    {
    }
    void reset() { bytes = 0; data = 0; overflow.reset(); }
    unsigned int bytes;
    bitblock overflow;
    unsigned char *data;
};

/* This is a Cilk++ reducer.  The leftmost value is connected
   to a file.  Additional views accumulate output that can not
   yet be written in order. */
extern "C++"
struct writer
{
    writer(FILE *); /* make a writer that writes to a file */
    writer();       /* make a writer that accumulates output */
    FILE *out;
    bitblock carry;
    uint64_t total; /* total BITS written */
    uint32_t crc;
    bool failed;
    typedef std::list<memblock> memblock_list;
    memblock_list blocks;
    void reduce(writer *);
    void write(const unsigned char *, unsigned int, bitblock, uint32_t);
    void flush();
private:
    void write(const unsigned char *, unsigned int, bitblock);
};

writer::writer(FILE *out)
  /* A 32 bit header precedes construction of this object */
  : out(out), carry(), total(32), crc(0), failed(false)
{
}

writer::writer()
  : out(0), carry(), total(0), crc(0), failed(false)
{
}

extern "C++" { static bool fwrite_force(FILE *, const unsigned char *, unsigned int); }

static bool fwrite_force(FILE *out, const unsigned char *data, unsigned int length)
{
    const unsigned char *end = data + length;
    while (data < end) {
        size_t n = fwrite(data, 1, end - data, out);
        if (n == 0)
            return false;
        data += n;
    }   
    return true;
}

/* Form a byte string in out by inserting [bits/bit_count] in front of /in/.
   Return the extra bits. */
extern "C" { static unsigned char copy_shift(unsigned char *, bitblock, const unsigned char *in, unsigned int bytes); }

static unsigned char
copy_shift(unsigned char *out, bitblock carry_in,
           const unsigned char *in, unsigned int bytes)
{
    const int numbits = carry_in.numbits;
    assert(numbits >= 0 && numbits < 8);
    /* By convention the bits are stored at the most significant
       part of the byte, and unused bits are 0 instead of undefined. */
    unsigned char carry = carry_in.bits;
    for (unsigned int i = 0; i < bytes; i++)
    {
        unsigned char in_i = in[i];
        out[i] = carry | (in_i >> numbits);
        carry = in_i << (8-numbits);
    }
    return carry;
}
                       
void writer::write(const unsigned char *data, unsigned int length,
                   const bitblock overflow)
{
    assert(out);
    const int align_in = carry.numbits;
    if (align_in == 0) {
        fwrite_force(out, data, length);
        carry = overflow;
        return;
    }
    unsigned char buf[1024];
    int c = carry.bits;
    for (unsigned int i = 0; i < length / sizeof buf; i++) {
        c = copy_shift(buf, bitblock(c, align_in), data, sizeof buf);
        fwrite_force(out, buf, sizeof buf);
	data += sizeof buf;
    }
    if (length % sizeof buf) {
        c = copy_shift(buf, bitblock(c, align_in), data, length % sizeof buf);
        fwrite_force(out, buf, length % sizeof buf);
    }
    /* Now merge the carry out of the shift with the extra
       bits passed to this function.  Both values are aligned
       at the high end of a byte. */
    c = (c << 8) | (overflow.bits << (8 - align_in));
    const int new_align = align_in + overflow.numbits;
    if (new_align > 7) {
        fputc(c >> 8, out);
        carry = bitblock(c & 255, new_align & 7);
    } else {
        carry = bitblock(c >> 8, new_align & 7);
    }
    
    return;
}

void writer::write(const unsigned char *data, unsigned int length,
                   const bitblock overflow, uint32_t new_crc)
{
    total += length * 8 + overflow.numbits;
    crc = ((crc << 1) | (crc >> 31)) ^ new_crc;
    if (out) {
        write(data, length, overflow);
        return;
    }
    unsigned char *copy = new unsigned char[length];
    memcpy(copy, data, length);
    blocks.push_back(memblock(length, copy, overflow));
}

void writer::flush()
{
    assert(out);
    if (carry.numbits)
        fputc(carry.bits, out);
}

void writer::reduce(writer *right)
{
    total += right->total;
    if (!out) {
        int count = right->blocks.size() & 31;
        crc = ((crc << count) | (crc >> (32 - count))) ^ right->crc;
        blocks.splice(blocks.end(), right->blocks);
        return;
    }
    failed |= ferror(out) | right->failed;
    int count = 0;
    for (memblock_list::iterator i = right->blocks.begin();
         i != right->blocks.end();
         ++i) {
        memblock &b = *i;
        if (!failed)
            write(b.data, b.bytes, b.overflow);
        delete [] b.data;
        b.reset();
        count++;
    }
    count &= 31;
    crc = ((crc << count) | (crc >> (32 - count))) ^ right->crc;
    return;
}

#ifdef __cilkplusplus
typedef cilk::hyperobject<writer> hyper_writer;
#define VIEW(x) (x)()
#else
typedef writer hyper_writer;
#define cilk_spawn
#define cilk_sync
#define VIEW(x) (x)
#endif

#ifdef _WIN32 /* Cilk++ for Windows can't spawn a C function */
#error "Windows?" /* not tested */
static void BZ2_compressBlockCilk(EState *s, hyper_writer &output);
static void BZ2_compressBlockCilk(EState *s, hyper_writer &output,
                                  const unsigned char *begin,
                                  const unsigned char *end);
#else
extern "C++"
{
    static int BZ2_compressBlockCilk(EState *s, hyper_writer &output);
    static int BZ2_compressBlockCilk(EState *s, hyper_writer &output,
                                      const unsigned char *begin,
                                      const unsigned char *end);
}
#endif

extern "C"
{
    static unsigned int run_length_encode(unsigned char *,
                                          uint32_t *crc_out,
                                          unsigned char *inUse,
                                          const unsigned char *begin,
                                          const unsigned char *end);
}

static void report(long long in, long long out)
{
    if (in == 0) {
        fputs(" no data compressed.\n", stderr);
        return;
    }
    const double in_d = in, out_d = out;
    fprintf(stderr, "%6.3f:1, %6.3f bits/byte, "
            "%5.2f%% saved, %lld in, %lld out.\n",
            in_d / out_d,
            8.0 * out_d / in_d,
            100.0 * (1.0 - out_d / in_d),
            in, out);
}  


/*extern "Cilk++"*/
int
BZ2_compressFileCilk (FILE *in, FILE *out,
                      int blockSize100k,
                      int verbosity,
                      int workFactor) throw()
{
    if (blockSize100k < 1 || blockSize100k > 9 ||
        workFactor < 0 || workFactor > 250)
        return 0;

    if (workFactor == 0) workFactor = 30;

    unsigned char header[4] = { BZ_HDR_B, BZ_HDR_Z, BZ_HDR_h, BZ_HDR_0 + blockSize100k };
    fwrite(header, 1, 4, out);


    {
        hyper_writer output(out);
        off_t bytes_in = 0;

        /* This can not be a cilk_for loop because block boundaries
           are not known in advance and there are interblock dependencies
           in the parse stage.  There is also a memory use penalty for
           highly out of order execution.

           Because nearly every spawn is stolen, no attempt is made to
           avoid reallocation when a call returns normally.

           The cost of a steal is small compared to the cost of
           compressing a block. */
        int prev_state_char = 256, prev_state_len = 0;
        while (!feof(in)) {

            if (ferror(in))
                return BZ_IO_ERROR;

            struct EState *s = BZ2_bzCompressInitCilk(blockSize100k, verbosity, workFactor);
            s->state_in_ch = prev_state_char;
            s->state_in_len = prev_state_len;

            /* add_char_to_block returns false on EOF */
            while (BZ2_add_char_to_block(s, getc(in)))
                bytes_in++;

            prev_state_char = s->state_in_ch;
            prev_state_len = s->state_in_len;
            /* TODO: conditional sync here if too out of order */
            if (s->nblock) {
                cilk_spawn BZ2_compressBlockCilk(s, output);
            } else {
                free(s->arr1);
                free(s->arr2);
                free(s->ftab);
                free(s);
            }
        }

        cilk_sync;

        uint32_t crc = VIEW(output).crc;

        unsigned char trailer[10];
        trailer[0] = 0x17;
        trailer[1] = 0x72;
        trailer[2] = 0x45;
        trailer[3] = 0x38;
        trailer[4] = 0x50;
        trailer[5] = 0x90;
        trailer[6] = crc >> 24;
        trailer[7] = crc >> 16;
        trailer[8] = crc >> 8;
        trailer[9] = crc;

        if (verbosity >= 2)
            VPrintf1("    final combined CRC = 0x%08x\n   ", crc);

        /* This write destroys the cumulative checksum CRC,
           which was already written. */           
        VIEW(output).write(trailer, 10, bitblock(), 0);
        VIEW(output).flush();

        if (verbosity)
            report(bytes_in, ((VIEW(output).total + 7) / 8));
    }

    fflush(out);

    return ferror(out) ? BZ_IO_ERROR : BZ_OK;
}

extern "C++"
int
BZ2_compressBlockCilk(EState *s, hyper_writer &output)
{
    BZ_FINALISE_CRC(s->blockCRC);
    s->numZ = 0; /* flushed after every block */

    if (s->verbosity >= 2)
        VPrintf2("    block crc = 0x%08x, size = %d\n", s->blockCRC, s->nblock);

    BZ2_blockSort(s);
    s->zbits = (UChar*) (&((UChar*)s->arr2)[s->nblock]);

    /* Write header, CRC, bits */
    bsPutUChar(s, 0x31);
    bsPutUChar(s, 0x41);
    bsPutUChar(s, 0x59);
    bsPutUChar(s, 0x26);
    bsPutUChar(s, 0x53);
    bsPutUChar(s, 0x59);
    bsPutUInt32(s, s->blockCRC);
    bsW(s,1,0);
    bsW(s, 24, s->origPtr);
    generateMTFValues(s);
    sendMTFValues(s);
    bsFinishWriteCilk(s);

    VIEW(output).write((const unsigned char *)s->zbits, s->numZ,
                       bitblock(s->bsBuff >> 24, s->bsLive),
                       s->blockCRC);

    int count = s->nblock;

    free(s->arr1);
    free(s->arr2);
    free(s->ftab);
    free(s);

    return count;
}

/* Copy the buffer accumulating CRC and doing run length
   encoding.  If there are four or more consecutive identical
   bytes, to a maximum of 255, they are encoded as four
   consecutive identical bytes followed by a count of bytes
   beyond four.  (So runs like AAAABBBBCCCC increase in length.) */

static unsigned int
run_length_encode(unsigned char *out,
		  uint32_t *crc_out,
		  unsigned char */*__restrict*/ inUse,
		  const unsigned char *begin,
		  const unsigned char *end)
{
    memset(inUse, 0, 256);

    uint32_t crc;
    BZ_INITIALISE_CRC(crc);
    unsigned int n = 0;

    /* Current run is 0 invalid characters. */
    int last_ch = 256, run_len = 0;
    /* Last run is invalid */
    unsigned char *last_run = 0;
    for (const unsigned char *cp = begin; cp < end; ++cp) {
        /* Copy the character to output.  We will back up
           later if we find a long run. */
        unsigned char c = *cp;
        BZ_UPDATE_CRC(crc, c);
        inUse[c] = 1;
        out[n++] = c;
        /* If the last character was a different singleton,
           or was different and no run length encoding is
           required, start a new run. */
        if (c != last_ch && run_len < 4) {
            last_ch = c;
            run_len = 1;
            last_run = 0;
            continue;
        }
        if (c == last_ch && run_len < 255) {
            run_len++;
            continue;
        }
        /* Here run length is greater than 4 and we need to back up,
           write the run length, and put the latest character in its
           proper place.

           AAAAB -> AAAA0B
           AAAAAB -> AAAA1B
           etc. */
        n -= (run_len - 4) + 1;
        last_run = &out[n];
        out[n++] = run_len - 4;
        out[n++] = c;
        inUse[run_len - 4] = 1;
        last_ch = c;
        run_len = 1;
    }
    if (run_len >= 4) {
        n -= (run_len - 4);
        out[n++] = run_len - 4;
        inUse[run_len - 4] = 1;
    } else if (end - begin > 10 && last_run) {
        /* Special case: Don't end a block with a partial run that is the
           same as a previous complete run.  Write two shorter runs.  */
        if (last_run[-1] == last_ch) {
            AssertD(*last_run == 251, "run_length_encode");
            /* Previous run length must be 255 encoded as 251 so we can
               take a few bytes. */
            while (run_len < 4) {
                --*last_run;
                out[n++] = last_ch;
                run_len++;
            }
            out[n++] = 0; /* 4 - 4 */
	    inUse[*last_run] = 1;
	    inUse[0] = 1;
        }
    }
    BZ_FINALISE_CRC(crc);
    *crc_out = crc;
    return n;
}

extern "C++"
int
BZ2_compressBlockCilk(EState *s, hyper_writer &output,
                      const unsigned char *begin,
                      const unsigned char *end)
{
    uint32_t crc;
    unsigned int length;

    length = run_length_encode(s->block, &crc, s->inUse, begin, end);
    s->nblock = length;
    s->blockCRC = crc;

    if (s->verbosity >= 2)
        VPrintf2("    crc = 0x%08x, size = %d\n", s->blockCRC, s->nblock);

    BZ2_blockSort(s);
    s->zbits = (UChar*) (&((UChar*)s->arr2)[s->nblock]);

    /* Write header, CRC, bits */
    bsPutUChar(s, 0x31);
    bsPutUChar(s, 0x41);
    bsPutUChar(s, 0x59);
    bsPutUChar(s, 0x26);
    bsPutUChar(s, 0x53);
    bsPutUChar(s, 0x59);
    bsPutUInt32(s, s->blockCRC);
    bsW(s, 1, 0);
    bsW(s, 24, s->origPtr);
    generateMTFValues(s);
    sendMTFValues(s);
    bsFinishWriteCilk(s);

    VIEW(output).write((const unsigned char *)s->zbits, s->numZ,
                       bitblock(s->bsBuff >> 24, s->bsLive),
                       s->blockCRC);

    int count = s->nblock;

    free(s->arr1);
    free(s->arr2);
    free(s->ftab);
    free(s);

    return count;
}

/*extern "Cilk++"*/
int BZ2_compressFileCilk(int fd, FILE *out,
                         int blockSize100k,
                         int verbosity, 
                         int workFactor) throw()
{
    struct stat st;

    if (fstat(fd, &st) != 0)
        return BZ_IO_ERROR;

    /*const long page = sysconf(_SC_PAGESIZE);*/
    const off_t size = st.st_size;
    /*off_t sizerounded = (size + (page - 1)) / page * page;*/
    void *addrv = (char *)mmap(0, size, PROT_READ, MAP_SHARED, fd, 0);
    if (addrv == MAP_FAILED)
    {
        if (verbosity)
            VPrintf1("Unable to map input as stream (%s)\n    ", strerror(errno));
        /*perror ("mmap");*/
        return BZ_STREAM_ERROR;
    }
    unsigned char *addr = (unsigned char *)addrv;

    /* start */

    /* Run length encoding may cause a block to grow by 25%.
       Ensure that the run length encoded length fits in the
       maximum block size.  Another approach is to assume the
       block won't grow and split any blocks that do grow.  */
    int block = 80000 * blockSize100k - 20;
    /*int fullblocks = size / block;
    int residue = size % block;*/

    {
        unsigned char hdr[4] = { BZ_HDR_B, BZ_HDR_Z, BZ_HDR_h, BZ_HDR_0 + blockSize100k };
        fwrite(hdr, 1, 4, out);
    }


    {
        hyper_writer output(out);

        /* This is a serial loop with a spawn.  Usually such
           loops are bad Cilk++ style.  Here memory use of the
           program is proportional to the amount of out of order
           execution.  Serial spawn keeps that to O(P) on average,
           consistent with the usual amount of memory use of a
           Cilk++ program.  */

        const unsigned char *base = addr;
        const unsigned char *const limit = addr + size;
        while (base < limit) {
            struct EState *s = BZ2_bzCompressInitCilk(blockSize100k, verbosity, workFactor); /* freed in compress block */
            const unsigned char *last = base + block;
            if (last >= limit) {
                last = limit;
            } else if (last - base > 10 && last[0] == last[-1]) {
                unsigned char last_val = last[-1];
                /* If the block ends in the middle of a run extend the
                   block so that run length encoding commutes with
                   block compression.  If the block is too short to
                   hold a compressed run it must be the last block and
                   this does not matter.  */
                int run = 2; /* we know two bytes are the same */
                /* Scan back.  If run length is greater than 8 the
                   encoder will make sure the block ends with a
                   length byte.  */
                while (run < 8 && last_val == last[-run])
                    run++;
                ++last;
                while (run < 8 && last < limit && *last++ == last_val)
                    run++;
            }
            cilk_spawn BZ2_compressBlockCilk(s, output, base, last);
            base = last;
        }

        cilk_sync;

        uint32_t crc = VIEW(output).crc;

        unsigned char trailer[10];
        trailer[0] = 0x17;
        trailer[1] = 0x72;
        trailer[2] = 0x45;
        trailer[3] = 0x38;
        trailer[4] = 0x50;
        trailer[5] = 0x90;
        trailer[6] = crc >> 24;
        trailer[7] = crc >> 16;
        trailer[8] = crc >> 8;
        trailer[9] = crc;

        if (verbosity >= 2)
            VPrintf1("    final combined CRC = 0x%08x\n   ", crc);

        /* This write destroys the cumulative checksum CRC,
           which was already written. */
        VIEW(output).write(trailer, 10, bitblock(), 0);
        VIEW(output).flush();

        if (verbosity)
            report(size, ((VIEW(output).total + 7) / 8));
    }

    munmap((char *)addr, size);

    fflush(out);
    if (ferror(out))
        return BZ_IO_ERROR;
    return BZ_OK;
}

#endif /* BZLIB_CILK */

/* WFO API */
#ifdef BZLIB_WFO

#ifndef __cplusplus
#error "WFO mode requires a C++ compiler"
#endif

#include "wf_interface.h"

/* Flush all full bytes.  The caller will deal with the extra 0-7 bits. */
extern "C" { static void bsFinishWriteWFO(EState* s); }

static void bsFinishWriteWFO(EState* s)
{
    while (s->bsLive > 7) {
        s->zbits[s->numZ] = (UChar)(s->bsBuff >> 24);
        s->numZ++;
        s->bsBuff <<= 8;
        s->bsLive -= 8;
    }
    return;
}


static EState *BZ2_bzCompressInitWFO
                    (int        blockSize100k,
                     int        verbosity,
                     int        workFactor)
{
    EState* s = new EState;

    s->strm = 0;

    const int n   = 100000 * blockSize100k;
    s->arr1 = (UInt32 *)malloc( n                  * sizeof(UInt32) );
    s->arr2 = (UInt32 *)malloc( (n+BZ_N_OVERSHOOT) * sizeof(UInt32) );
    s->ftab = (UInt32 *)malloc( 65537              * sizeof(UInt32) );

    if (s->arr1 == NULL || s->arr2 == NULL || s->ftab == NULL) {
        free(s->arr1);
        free(s->arr2);
        free(s->ftab);
        return 0;
    }

    s->blockNo           = 0;
    s->state             = BZ_S_INPUT;
    s->mode              = BZ_M_RUNNING;
    BZ_INITIALISE_CRC (s->blockCRC);
    s->blockSize100k     = blockSize100k;
    s->nblockMAX         = 100000 * blockSize100k - 19;
    s->verbosity         = verbosity;
    s->workFactor        = workFactor;

    s->block             = (UChar*)s->arr2;
    s->mtfv              = (UInt16*)s->arr1;
    s->zbits             = NULL;
    s->ptr               = (UInt32*)s->arr1;

    s->bsBuff = 0;
    s->bsLive = 0;

    /* inlined init_RL */
    s->state_in_ch  = 256;
    s->state_in_len = 0;

    /* inlined prepare_new_block */
    s->nblock = 0;
    s->numZ = 0;
    s->state_out_pos = 0;
    /*BZ_INITIALISE_CRC (s->blockCRC);*/
    memset(s->inUse, 0, sizeof s->inUse);
    /*memset(s->unseqToSeq, 0, sizeof s->unseqToSeq);*/
    s->blockNo++;

    return s;
}


/* A bzip stream is a stream of bits.  The memblock structure
   represents a sequence of bytes (which may or may not ultimately
   land on a byte boundary) plus 0 to 7 extra bits.  The bitblock
   structure represents those extra bits. */

extern "C++"
struct bitblock
{
    unsigned char bits, numbits;
    bitblock(unsigned char bits0, int numbits0)
      : bits(bits0), numbits(numbits0)
      {
          AssertD(numbits < 8, "bitblock bits");
      }
    bitblock() : bits(0), numbits(0) { }
    void reset() { bits = 0; numbits = 0; }
};

extern "C++"
struct memblock
{
    memblock(unsigned int bytes, unsigned char *data, bitblock overflow)
        : bytes(bytes), overflow(overflow), data(data)
    {
    }
    void reset() { bytes = 0; data = 0; overflow.reset(); }
    unsigned int bytes;
    bitblock overflow;
    unsigned char *data;
};

/* This is a WFO++ reducer.  The leftmost value is connected
   to a file.  Additional views accumulate output that can not
   yet be written in order. */
extern "C++"
struct writer
{
    writer(FILE *); /* make a writer that writes to a file */
    writer();       /* make a writer that accumulates output */
    FILE *out;
    bitblock carry;
    uint64_t total; /* total BITS written */
    uint32_t crc;
    bool failed;
    typedef std::list<memblock> memblock_list;
    memblock_list blocks;
    void reduce(writer *);
    void write(const unsigned char *, unsigned int, bitblock, uint32_t);
    void flush();
private:
    void write(const unsigned char *, unsigned int, bitblock);
};

writer::writer(FILE *out)
  /* A 32 bit header precedes construction of this object */
  : out(out), carry(), total(32), crc(0), failed(false)
{
}

writer::writer()
  : out(0), carry(), total(0), crc(0), failed(false)
{
}

extern "C++" { static bool fwrite_force(FILE *, const unsigned char *, unsigned int); }

static bool fwrite_force(FILE *out, const unsigned char *data, unsigned int length)
{
    const unsigned char *end = data + length;
    while (data < end) {
        size_t n = fwrite(data, 1, end - data, out);
        if (n == 0)
            return false;
        data += n;
    }   
    return true;
}

/* Form a byte string in out by inserting [bits/bit_count] in front of /in/.
   Return the extra bits. */
extern "C" { static unsigned char copy_shift(unsigned char *, bitblock, const unsigned char *in, unsigned int bytes); }

static unsigned char
copy_shift(unsigned char *out, bitblock carry_in,
           const unsigned char *in, unsigned int bytes)
{
    const int numbits = carry_in.numbits;
    assert(numbits >= 0 && numbits < 8);
    /* By convention the bits are stored at the most significant
       part of the byte, and unused bits are 0 instead of undefined. */
    unsigned char carry = carry_in.bits;
    for (unsigned int i = 0; i < bytes; i++)
    {
        unsigned char in_i = in[i];
        out[i] = carry | (in_i >> numbits);
        carry = in_i << (8-numbits);
    }
    return carry;
}
                       
void writer::write(const unsigned char *data, unsigned int length,
                   const bitblock overflow)
{
    assert(out);
    const int align_in = carry.numbits;
    if (align_in == 0) {
        fwrite_force(out, data, length);
        carry = overflow;
        return;
    }
    unsigned char buf[1024];
    int c = carry.bits;
    for (unsigned int i = 0; i < length / sizeof buf; i++) {
        c = copy_shift(buf, bitblock(c, align_in), data, sizeof buf);
        fwrite_force(out, buf, sizeof buf);
	data += sizeof buf;
    }
    if (length % sizeof buf) {
        c = copy_shift(buf, bitblock(c, align_in), data, length % sizeof buf);
        fwrite_force(out, buf, length % sizeof buf);
    }
    /* Now merge the carry out of the shift with the extra
       bits passed to this function.  Both values are aligned
       at the high end of a byte. */
    c = (c << 8) | (overflow.bits << (8 - align_in));
    const int new_align = align_in + overflow.numbits;
    if (new_align > 7) {
        fputc(c >> 8, out);
        carry = bitblock(c & 255, new_align & 7);
    } else {
        carry = bitblock(c >> 8, new_align & 7);
    }
    
    return;
}

void writer::write(const unsigned char *data, unsigned int length,
                   const bitblock overflow, uint32_t new_crc)
{
    total += length * 8 + overflow.numbits;
    crc = ((crc << 1) | (crc >> 31)) ^ new_crc;
    if (out) {
        write(data, length, overflow);
        return;
    }
    unsigned char *copy = new unsigned char[length];
    memcpy(copy, data, length);
    blocks.push_back(memblock(length, copy, overflow));
}

void writer::flush()
{
    assert(out);
    if (carry.numbits)
        fputc(carry.bits, out);
}

void writer::reduce(writer *right)
{
    total += right->total;
    if (!out) {
        int count = right->blocks.size() & 31;
        crc = ((crc << count) | (crc >> (32 - count))) ^ right->crc;
        blocks.splice(blocks.end(), right->blocks);
        return;
    }
    failed |= ferror(out) | right->failed;
    int count = 0;
    for (memblock_list::iterator i = right->blocks.begin();
         i != right->blocks.end();
         ++i) {
        memblock &b = *i;
        if (!failed)
            write(b.data, b.bytes, b.overflow);
        delete [] b.data;
        b.reset();
        count++;
    }
    count &= 31;
    crc = ((crc << count) | (crc >> (32 - count))) ^ right->crc;
    return;
}

static void BZ2_compressBlockWFO(EState *s, obj::pushdep<EState*>);
static void BZ2_compressBlockWFOr(EState *s,
				  const unsigned char *begin,
				  const unsigned char *end,
				  obj::pushdep<EState*>);
//static void BZ2_writeBlockWFO(EState *s, writer *output);

static unsigned int run_length_encode(unsigned char *,
				      uint32_t *crc_out,
				      unsigned char *inUse,
				      const unsigned char *begin,
				      const unsigned char *end);

static void report(long long in, long long out)
{
    if (in == 0) {
        fputs(" no data compressed.\n", stderr);
        return;
    }
    const double in_d = in, out_d = out;
    fprintf(stderr, "%6.3f:1, %6.3f bits/byte, "
            "%5.2f%% saved, %lld in, %lld out.\n",
            in_d / out_d,
            8.0 * out_d / in_d,
            100.0 * (1.0 - out_d / in_d),
            in, out);
}  


void finishCompressWFO(int verbosity, int bytes_in, writer * output)
{
    uint32_t crc = output->crc;

    unsigned char trailer[10];
    trailer[0] = 0x17;
    trailer[1] = 0x72;
    trailer[2] = 0x45;
    trailer[3] = 0x38;
    trailer[4] = 0x50;
    trailer[5] = 0x90;
    trailer[6] = crc >> 24;
    trailer[7] = crc >> 16;
    trailer[8] = crc >> 8;
    trailer[9] = crc;

    if (verbosity >= 2)
	VPrintf1("    final combined CRC = 0x%08x\n   ", crc);

    /* This write destroys the cumulative checksum CRC,
       which was already written. */           
    output->write(trailer, 10, bitblock(), 0);
    output->flush();

    if (verbosity)
	report(bytes_in, ((output->total + 7) / 8));
}

static void
BZ2_compressBlockWFO_leaf(EState *s)
{
    BZ_FINALISE_CRC(s->blockCRC);
    s->numZ = 0; /* flushed after every block */

    if (s->verbosity >= 2)
        VPrintf2("    block crc = 0x%08x, size = %d\n", s->blockCRC, s->nblock);

    BZ2_blockSort(s);
    s->zbits = (UChar*) (&((UChar*)s->arr2)[s->nblock]);

    /* Write header, CRC, bits */
    bsPutUChar(s, 0x31);
    bsPutUChar(s, 0x41);
    bsPutUChar(s, 0x59);
    bsPutUChar(s, 0x26);
    bsPutUChar(s, 0x53);
    bsPutUChar(s, 0x59);
    bsPutUInt32(s, s->blockCRC);
    bsW(s,1,0);
    bsW(s, 24, s->origPtr);
    generateMTFValues(s);
    sendMTFValues(s);
    bsFinishWriteWFO(s);
}

void
BZ2_compressBlockWFO(EState *s, obj::pushdep<EState *> out)
{
    leaf_call( BZ2_compressBlockWFO_leaf, s );
    out.push( s );
}

static int
BZ2_writeBlockWFO_leaf(EState *s, writer *output_) {
    writer &output = *output_;
    output.write((const unsigned char *)s->zbits, s->numZ,
		 bitblock(s->bsBuff >> 24, s->bsLive),
		 s->blockCRC);

    int count = s->nblock;

    free(s->arr1);
    free(s->arr2);
    free(s->ftab);
    free(s);

    return count;
}

/*
void
BZ2_writeBlockWFO(EState *s, writer *output_ ) {
    leaf_call( BZ2_writeBlockWFO_leaf, s, output_ );
}
*/


int
BZ2_compressFileWFO_stage1(FILE *in,
			   int blockSize100k,
			   int verbosity,
			   int workFactor,
			   obj::pushdep<EState*> queue,
			   off_t * bytes_in_p) throw()
{
    off_t bytes_in = 0;

    /* This can not be a cilk_for loop because block boundaries
       are not known in advance and there are interblock dependencies
       in the parse stage.  There is also a memory use penalty for
       highly out of order execution.

       Because nearly every spawn is stolen, no attempt is made to
       avoid reallocation when a call returns normally.

       The cost of a steal is small compared to the cost of
       compressing a block. */
    int prev_state_char = 256, prev_state_len = 0;
    while (!feof(in)) {

	if (ferror(in))
	    return BZ_IO_ERROR;

	// struct EState *s = leaf_call( BZ2_bzCompressInitWFO, blockSize100k, verbosity, workFactor);
	struct EState *s = BZ2_bzCompressInitWFO( blockSize100k, verbosity, workFactor);
	s->state_in_ch = prev_state_char;
	s->state_in_len = prev_state_len;

	/* add_char_to_block returns false on EOF */
	// while (leaf_call( BZ2_add_char_to_block, s, getc(in)))
	while (BZ2_add_char_to_block(s, getc(in)))
	    bytes_in++;

	prev_state_char = s->state_in_ch;
	prev_state_len = s->state_in_len;
	/* TODO: conditional sync here if too out of order */
	if (s->nblock) {
	    queue.push( s );
	} else {
	    free(s->arr1);
	    free(s->arr2);
	    free(s->ftab);
	    free(s);
	}
    }

    *bytes_in_p = bytes_in;

    return BZ_OK;
}

void
BZ2_compressFileWFO_stage2(obj::popdep<EState *> in, obj::pushdep<EState *>out)
{
    while( !in.empty() ) {
	EState * s = in.pop();
	spawn( BZ2_compressBlockWFO, s, out );
    }
    ssync();
}

static void
BZ2_compressFileWFO_stage3(obj::popdep<EState *> in, writer *output )
{
    while( !in.empty() ) {
	EState * s = in.pop();
	leaf_call( BZ2_writeBlockWFO_leaf, s, output );
    }
}

int
BZ2_compressFileWFO (FILE *in, FILE *out,
                      int blockSize100k,
                      int verbosity,
                      int workFactor) throw()
{
    if (blockSize100k < 1 || blockSize100k > 9 ||
        workFactor < 0 || workFactor > 250)
        return 0;

    if (workFactor == 0) workFactor = 30;

    unsigned char header[4] = { BZ_HDR_B, BZ_HDR_Z, BZ_HDR_h, (unsigned char)(BZ_HDR_0 + blockSize100k) };
    fwrite(header, 1, 4, out);


    {
        writer output(out);
        off_t bytes_in = 0;
	obj::queue_t<EState *> queue1, queue2;
	chandle<int> ret;

	spawn( BZ2_compressFileWFO_stage1, ret,
	       in, blockSize100k, verbosity, workFactor,
	       (obj::pushdep<EState*>) queue1, &bytes_in);

	spawn( BZ2_compressFileWFO_stage2,
	       (obj::popdep<EState*>) queue1,
	       (obj::pushdep<EState*>) queue2 );

	spawn( BZ2_compressFileWFO_stage3,
	       (obj::popdep<EState*>) queue2, &output );

        ssync();

	if( ret != BZ_OK )
	    return ret;

	leaf_call( finishCompressWFO, verbosity, (int)bytes_in, &output );
    }

    leaf_call(fflush, out);

    return ferror(out) ? BZ_IO_ERROR : BZ_OK;
}

/* Copy the buffer accumulating CRC and doing run length
   encoding.  If there are four or more consecutive identical
   bytes, to a maximum of 255, they are encoded as four
   consecutive identical bytes followed by a count of bytes
   beyond four.  (So runs like AAAABBBBCCCC increase in length.) */

static unsigned int
run_length_encode(unsigned char *out,
		  uint32_t *crc_out,
		  unsigned char */*__restrict*/ inUse,
		  const unsigned char *begin,
		  const unsigned char *end)
{
    memset(inUse, 0, 256);

    uint32_t crc;
    BZ_INITIALISE_CRC(crc);
    unsigned int n = 0;

    /* Current run is 0 invalid characters. */
    int last_ch = 256, run_len = 0;
    /* Last run is invalid */
    unsigned char *last_run = 0;
    for (const unsigned char *cp = begin; cp < end; ++cp) {
        /* Copy the character to output.  We will back up
           later if we find a long run. */
        unsigned char c = *cp;
        BZ_UPDATE_CRC(crc, c);
        inUse[c] = 1;
        out[n++] = c;
        /* If the last character was a different singleton,
           or was different and no run length encoding is
           required, start a new run. */
        if (c != last_ch && run_len < 4) {
            last_ch = c;
            run_len = 1;
            last_run = 0;
            continue;
        }
        if (c == last_ch && run_len < 255) {
            run_len++;
            continue;
        }
        /* Here run length is greater than 4 and we need to back up,
           write the run length, and put the latest character in its
           proper place.

           AAAAB -> AAAA0B
           AAAAAB -> AAAA1B
           etc. */
        n -= (run_len - 4) + 1;
        last_run = &out[n];
        out[n++] = run_len - 4;
        out[n++] = c;
        inUse[run_len - 4] = 1;
        last_ch = c;
        run_len = 1;
    }
    if (run_len >= 4) {
        n -= (run_len - 4);
        out[n++] = run_len - 4;
        inUse[run_len - 4] = 1;
    } else if (end - begin > 10 && last_run) {
        /* Special case: Don't end a block with a partial run that is the
           same as a previous complete run.  Write two shorter runs.  */
        if (last_run[-1] == last_ch) {
            AssertD(*last_run == 251, "run_length_encode");
            /* Previous run length must be 255 encoded as 251 so we can
               take a few bytes. */
            while (run_len < 4) {
                --*last_run;
                out[n++] = last_ch;
                run_len++;
            }
            out[n++] = 0; /* 4 - 4 */
	    inUse[*last_run] = 1;
	    inUse[0] = 1;
        }
    }
    BZ_FINALISE_CRC(crc);
    *crc_out = crc;
    return n;
}

static void
BZ2_compressBlockWFOr_leaf(EState *s,
			   const unsigned char *begin,
			   const unsigned char *end)
{
    uint32_t crc;
    unsigned int length;

    length = run_length_encode(s->block, &crc, s->inUse, begin, end);
    s->nblock = length;
    s->blockCRC = crc;

    if (s->verbosity >= 2)
        VPrintf2("    crc = 0x%08x, size = %d\n", s->blockCRC, s->nblock);

    BZ2_blockSort(s);
    s->zbits = (UChar*) (&((UChar*)s->arr2)[s->nblock]);

    /* Write header, CRC, bits */
    bsPutUChar(s, 0x31);
    bsPutUChar(s, 0x41);
    bsPutUChar(s, 0x59);
    bsPutUChar(s, 0x26);
    bsPutUChar(s, 0x53);
    bsPutUChar(s, 0x59);
    bsPutUInt32(s, s->blockCRC);
    bsW(s, 1, 0);
    bsW(s, 24, s->origPtr);
    generateMTFValues(s);
    sendMTFValues(s);
    bsFinishWriteWFO(s);
}

void
BZ2_compressBlockWFOr(EState *s,
                      const unsigned char *begin,
                      const unsigned char *end,
		      obj::pushdep<EState *> q)
{
    leaf_call( BZ2_compressBlockWFOr_leaf, s, begin, end );
    q.push( s );
}

struct istate {
    EState * s;
    const unsigned char * base;
    const unsigned char * last;
};

void BZ2_compressFileWFOi_stage2( obj::popdep<istate> in,
				  obj::pushdep<EState *> out )
{
    while( !in.empty() ) {
	istate is = in.pop();
	spawn( BZ2_compressBlockWFOr, is.s, is.base, is.last, out );
    }
    ssync();
}

void BZ2_compressFileWFOi_stage1(int fd,
				 int blockSize100k,
				 int verbosity, 
				 int workFactor,
				 const off_t size,
				 unsigned char * addr,
				 obj::pushdep<istate> queue ) throw()
{
    /* start */

    /* Run length encoding may cause a block to grow by 25%.
       Ensure that the run length encoded length fits in the
       maximum block size.  Another approach is to assume the
       block won't grow and split any blocks that do grow.  */
    int block = 80000 * blockSize100k - 20;
    /*int fullblocks = size / block;
    int residue = size % block;*/

    {
        /* This is a serial loop with a spawn.  Usually such
           loops are bad Cilk++ style.  Here memory use of the
           program is proportional to the amount of out of order
           execution.  Serial spawn keeps that to O(P) on average,
           consistent with the usual amount of memory use of a
           Cilk++ program.  */

        const unsigned char *base = addr;
        const unsigned char *const limit = addr + size;
        while (base < limit) {
            struct EState *s = BZ2_bzCompressInitWFO(blockSize100k, verbosity, workFactor); /* freed in write block */
            const unsigned char *last = base + block;
            if (last >= limit) {
                last = limit;
            } else if (last - base > 10 && last[0] == last[-1]) {
                unsigned char last_val = last[-1];
                /* If the block ends in the middle of a run extend the
                   block so that run length encoding commutes with
                   block compression.  If the block is too short to
                   hold a compressed run it must be the last block and
                   this does not matter.  */
                int run = 2; /* we know two bytes are the same */
                /* Scan back.  If run length is greater than 8 the
                   encoder will make sure the block ends with a
                   length byte.  */
                while (run < 8 && last_val == last[-run])
                    run++;
                ++last;
                while (run < 8 && last < limit && *last++ == last_val)
                    run++;
            }
	    istate is;
	    is.s = s;
	    is.base = base;
	    is.last = last;
	    queue.push( is );
            base = last;
        }
    }
}

int BZ2_compressFileWFOi(int fd, FILE *out,
                         int blockSize100k,
                         int verbosity, 
                         int workFactor) throw()
{
    struct stat st;

    if (fstat(fd, &st) != 0)
        return BZ_IO_ERROR;

    /*const long page = sysconf(_SC_PAGESIZE);*/
    const off_t size = st.st_size;
    /*off_t sizerounded = (size + (page - 1)) / page * page;*/
    void *addrv = (char *)mmap(0, size, PROT_READ, MAP_SHARED, fd, 0);
    if (addrv == MAP_FAILED)
    {
        if (verbosity)
            VPrintf1("Unable to map input as stream (%s)\n    ", strerror(errno));
        /*perror ("mmap");*/
        return BZ_STREAM_ERROR;
    }
    unsigned char *addr = (unsigned char *)addrv;

    /* start */

    /* Run length encoding may cause a block to grow by 25%.
       Ensure that the run length encoded length fits in the
       maximum block size.  Another approach is to assume the
       block won't grow and split any blocks that do grow.  */
    // int block = 80000 * blockSize100k - 20;
    /*int fullblocks = size / block;
    int residue = size % block;*/

    {
        unsigned char hdr[4] = { BZ_HDR_B, BZ_HDR_Z, BZ_HDR_h, (unsigned char)(BZ_HDR_0 + blockSize100k) };
        fwrite(hdr, 1, 4, out);
    }


    {
        writer output(out);
	obj::queue_t<istate> queue1;
	obj::queue_t<EState *> queue2;

	spawn( BZ2_compressFileWFOi_stage1, fd, blockSize100k, verbosity, 
	       workFactor, size, addr, (obj::pushdep<istate>) queue1 );

	spawn( BZ2_compressFileWFOi_stage2,
	       (obj::popdep<istate>) queue1, (obj::pushdep<EState*>) queue2 );

	spawn( BZ2_compressFileWFO_stage3,
	       (obj::popdep<EState*>) queue2, &output );

	ssync();

	leaf_call( finishCompressWFO, verbosity, (int)size, &output );
    }

    munmap((char *)addr, size);

    fflush(out);
    if (ferror(out))
        return BZ_IO_ERROR;
    return BZ_OK;
}

#endif /* BZLIB_WFO */


/*-------------------------------------------------------------*/
/*--- end                                        compress.c ---*/
/*-------------------------------------------------------------*/
