/*
 * fmref.c: C reference implementation of FM Radio
 * David Maze <dmaze@cag.lcs.mit.edu>
 * $Id: fmref.c,v 1.15 2003/11/05 18:13:10 dmaze Exp $
 */

#ifdef raw
#include <raw.h>
#else
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#endif
#include <math.h>

#include "wf_interface.h"

#define SAMPLING_RATE 250000000
#define CUTOFF_FREQUENCY 108000000
#define NUM_TAPS 64
#define MAX_AMPLITUDE 27000.0
#define BANDWIDTH 10000
#define DECIMATION 4
/* Must be at least NUM_TAPS+1: */
#define IN_BUFFER_LEN 10000

#define CHUNK 4096

void begin(void);

/* Reading data: */
float get_float();
void get_float_chunk(float * out, float * out_prev);
#define OBJ_IN_SIZE (NUM_TAPS+CHUNK*(DECIMATION+1))
void get_float_df(obj::outdep<float[OBJ_IN_SIZE]>, obj::indep<float[OBJ_IN_SIZE]>);

/* Low pass filter: */
typedef struct LPFData
{
  float coeff[NUM_TAPS];
  float freq;
  int taps, decimation;
} LPFData;
float lpf_coeff[NUM_TAPS];
void init_lpf_data(LPFData *data, float freq, int taps, int decimation);

#define OBJ_LPF_SIZE (2+CHUNK-1)
void run_lpf_df(obj::indep<float[OBJ_IN_SIZE]> in,
		obj::outdep<float[OBJ_LPF_SIZE]> out,
		obj::indep<float[OBJ_LPF_SIZE]> out_prev,
		LPFData *data);
void run_lpf_chunk(float *in, float * out, float * out_prev, LPFData *data);
float run_lpf(float *in, LPFData *data);

float run_demod(float *lpf);
#define OBJ_DM_SIZE (64+CHUNK-1)
void run_demod_df(obj::indep<float[OBJ_LPF_SIZE]> in,
		  obj::outdep<float[OBJ_DM_SIZE]> out,
		  obj::indep<float[OBJ_DM_SIZE]> out_prev);
void run_demod_chunk(float * in, float * out, float *out_prev);

#define EQUALIZER_BANDS 10
float eq_cutoffs[EQUALIZER_BANDS + 1] =
  { 55.000004, 77.78174, 110.00001, 155.56354, 220.00002, 311.12695,
    440.00003, 622.25415, 880.00006, 1244.5078, 1760.0001 };
typedef struct EqualizerData
{
  LPFData lpf[EQUALIZER_BANDS + 1];
  float gain[EQUALIZER_BANDS];
} EqualizerData;
void init_equalizer(EqualizerData *data);
float run_equalizer(float *in, EqualizerData *data);
#define OBJ_CSUM_SIZE CHUNK
void run_equalizer_df(obj::indep<float[OBJ_DM_SIZE]>,
		      obj::outdep<float[OBJ_CSUM_SIZE]>,
		      EqualizerData *data);
void run_equalizer_chunk(float *in, float *out, EqualizerData *data);

void write_floats( float v );
void write_floats_df( obj::indep<float[OBJ_CSUM_SIZE]> v);
void write_floats_chunk( float * v );

/* Globals: */
static int numiters = -1;

#ifndef raw
int main(int argc, char **argv)
{
  int option;

  while ((option = getopt(argc, argv, "i:")) != -1)
  {
    switch(option)
    {
    case 'i':
      numiters = atoi(optarg);
    }
  }

  run(begin);
  return 0;
}
#endif



void begin(void)
{
    int i;
    static float in[NUM_TAPS+CHUNK*(DECIMATION+1)];
    LPFData lpf_data;
    EqualizerData eq_data;
    float sum;
    static float lpf[2+CHUNK-1];
    static float dm[64+CHUNK];
    float dms;
    int dmi;

    init_lpf_data(&lpf_data, CUTOFF_FREQUENCY, NUM_TAPS, DECIMATION);
    init_equalizer(&eq_data);

    /* Startup: */
    for( i=0; i < NUM_TAPS; ++i )
	in[i] = get_float();

    /* LPF needs at least NUM_TAPS+1 inputs; get_floats is fine. */
    lpf[0] = run_lpf(in, &lpf_data);
    /* run_demod needs 1 input, OK here. */
    /* run_equalizer needs 51 inputs (same reason as for LPF).  This means
     * running the pipeline up to demod 50 times in advance: */
    for (i = 0; i < 63; i++)
    {
	for( dmi=0; dmi<NUM_TAPS-(DECIMATION+1); ++dmi )
	    in[dmi] = in[dmi+DECIMATION+1];
	for( ; dmi < NUM_TAPS; ++dmi )
	    in[dmi] = get_float();
	
	lpf[1] = run_lpf(in, &lpf_data);
	dms = run_demod(lpf);
	for( dmi=0; dmi<63; ++dmi )
	    dm[dmi] = dm[dmi+1];
	dm[63] = dms;
	lpf[0] = lpf[1];
    }

    for( i=0; i < CHUNK-1; ++i, numiters-- ) {
	/* The low-pass filter will need NUM_TAPS+1 items; read them if we
	 * need to. */
	for( dmi=0; dmi<NUM_TAPS-(DECIMATION+1); ++dmi )
	    in[dmi] = in[dmi+DECIMATION+1];
	for( ; dmi < NUM_TAPS; ++dmi )
	    in[dmi] = get_float();
	// run_lpf:
	// in: consume/move: data->decimation+1 = DECIMATION+1 = 5
	// in: read: NUM_TAPS=64
	// out: 1
	// lpf_data: in
	lpf[1] = run_lpf(in, &lpf_data);
	// in: consume 1, read 2
	// out: produce 1
	dms = run_demod(lpf);
	for( dmi=0; dmi<63; ++dmi )
	    dm[dmi] = dm[dmi+1];
	dm[63] = dms;
	lpf[0] = lpf[1];
	// run_equalize:
	// in: consume/move: data->decimation+1 = 0+1 = 1
	// in: read: NUM_TAPS=64
	// out: 1
	// eq_data is in
	sum = run_equalizer(dm, &eq_data);
	leaf_call(write_floats,sum);
    }

    obj::object_t<float[2+CHUNK-1]> lpf_obj[2];
    obj::object_t<float[64+CHUNK-1]> dm_obj[2];
    obj::object_t<float[NUM_TAPS+CHUNK*(DECIMATION+1)]> in_obj[2];
    obj::object_t<float[CHUNK]> csum_obj;
    int cur = 0;

    (*lpf_obj[1-cur])[CHUNK] = lpf[0];

    for( dmi=0; dmi<63; ++dmi )
	(*dm_obj[1-cur])[dmi+CHUNK] = dm[dmi+1];

    for( dmi=0; dmi<NUM_TAPS-(DECIMATION+1); ++dmi )
	(*in_obj[1-cur])[dmi+CHUNK*(DECIMATION+1)] = in[dmi+DECIMATION+1];

    /* Main loop: */
    while (numiters == -1 || numiters > 0)
    {
	if( numiters > 0 )
	    numiters -= CHUNK;

	/* The low-pass filter will need NUM_TAPS+1 items */
	spawn(get_float_df, (obj::outdep<float[OBJ_IN_SIZE]>)in_obj[cur],
	      (obj::indep<float[OBJ_IN_SIZE]>)in_obj[1-cur] );

	// in: consume/move: data->decimation+1 = DECIMATION+1 = 5
	// in: read: NUM_TAPS=64
	// out: 1
	// lpf_data: in
	spawn(run_lpf_df,
	      (obj::indep<float[OBJ_IN_SIZE]>)in_obj[cur],
	      (obj::outdep<float[OBJ_LPF_SIZE]>)lpf_obj[cur],
	      (obj::indep<float[OBJ_LPF_SIZE]>)lpf_obj[1-cur],
	      &lpf_data);

	// in: consume 1, read 2
	// out: produce 1
	spawn( run_demod_df, (obj::indep<float[OBJ_LPF_SIZE]>)lpf_obj[cur],
	       (obj::outdep<float[OBJ_DM_SIZE]>)dm_obj[cur],
	       (obj::indep<float[OBJ_DM_SIZE]>)dm_obj[1-cur] );

	// eq_data is in
	spawn(run_equalizer_df, (obj::indep<float[OBJ_DM_SIZE]>)dm_obj[cur],
	      (obj::outdep<float[OBJ_CSUM_SIZE]>)csum_obj, &eq_data);

	spawn(write_floats_df, (obj::indep<float[OBJ_CSUM_SIZE]>)csum_obj);

	cur = 1 - cur;
    }

    ssync();
}

// out: up to IN_BUFFER_LEN (fill as much as possible)
void get_float_df(obj::outdep<float[OBJ_IN_SIZE]> c,
		  obj::indep<float[OBJ_IN_SIZE]> p) {
    get_float_chunk( *c, *p );
}

void get_float_chunk(float * out, float * out_prev) {
    for( int i=0; i < NUM_TAPS - (DECIMATION+1); ++i )
	out[i] = out_prev[CHUNK*(DECIMATION+1)+i];
    for( int i=0; i < CHUNK * (DECIMATION+1); ++i )
	out[NUM_TAPS-(DECIMATION+1)+i] = get_float();
}
float get_float()
{
  static int x = 0;
  float v = (float)x;
  x++;
  return v;
}

void init_lpf_data(LPFData *data, float freq, int taps, int decimation)
{
  /* Assume that CUTOFF_FREQUENCY is non-zero.  See comments in
   * StreamIt LowPassFilter.java for origin. */
  float w = 2 * M_PI * freq / SAMPLING_RATE;
  int i;
  float m = taps - 1.0;

  data->freq = freq;
  data->taps = taps;
  data->decimation = decimation;

  for (i = 0; i < taps; i++)
  {
    if (i - m/2 == 0.0)
      data->coeff[i] = w / M_PI;
    else
      data->coeff[i] = sin(w * (i - m/2)) / M_PI / (i - m/2) *
        (0.54 - 0.46 * cos(2 * M_PI * i / m));
  }
}

// in: consume/move: data->decimation+1 = DECIMATION+1 = 5
// in: read: NUM_TAPS=64
// out: 1
// data: in
void run_lpf_df(obj::indep<float[OBJ_IN_SIZE]> in,
		obj::outdep<float[OBJ_LPF_SIZE]> out,
		obj::indep<float[OBJ_LPF_SIZE]> out_prev,
		LPFData *data) {
    run_lpf_chunk( *in, *out, *out_prev, data );
}
void run_lpf_chunk(float *in, float * out, float * out_prev, LPFData *data)
{
    for( int i=0, s=0; i < CHUNK; ++i, s+=DECIMATION+1 )
	out[i+1] = run_lpf( &in[s], data );
    out[0] = out_prev[CHUNK];
}

float run_lpf(float *in, LPFData *data)
{
  float sum = 0.0;
  int i = 0;

  for (i = 0; i < data->taps; i++)
    sum += in[i] * data->coeff[i];

  return sum;
}

// in: consume 1, read 2
// out: produce 1
void run_demod_df(obj::indep<float[OBJ_LPF_SIZE]> in,
		  obj::outdep<float[OBJ_DM_SIZE]> out,
		  obj::indep<float[OBJ_DM_SIZE]> out_prev)
{
    run_demod_chunk( *in, *out, *out_prev );
}

void run_demod_chunk(float *in, float *out, float *out_prev)
{
    for( int i=0; i < 63; ++i )
	out[i] = out_prev[i+CHUNK];
    for( int i=0; i < CHUNK; ++i )
	out[i+63] = run_demod( &in[i] );
}

float run_demod(float * lpf)
{
  float temp, gain;
  gain = MAX_AMPLITUDE * SAMPLING_RATE / (BANDWIDTH * M_PI);
  temp = lpf[0] * lpf[1];
  temp = gain * atan(temp);
  return temp;
}

void init_equalizer(EqualizerData *data)
{
  int i;
  
  /* Equalizer structure: there are ten band-pass filters, with
   * cutoffs as shown below.  The outputs of these filters get added
   * together.  Each band-pass filter is LPF(high)-LPF(low). */
  for (i = 0; i < EQUALIZER_BANDS + 1; i++)
    init_lpf_data(&data->lpf[i], eq_cutoffs[i], 64, 0);

  for (i = 0; i < EQUALIZER_BANDS; i++) {
    // the gain amplifies the middle bands the most
    float val = (((float)i)-(((float)(EQUALIZER_BANDS-1))/2.0f)) / 5.0f;
    data->gain[i] = val > 0 ? 2.0-val : 2.0+val;
  }
}

// in:
//  - each call to run_lpf produces 1 element on a private buffer,
//    which is immediately consumed
//  - we do as run_lpf otherwise
// in: consume/move: data->decimation+1 = 0+1 = 1
// in: read: NUM_TAPS=64
// out: 1
// data: in
void run_equalizer_df(obj::indep<float[OBJ_DM_SIZE]> in,
		      obj::outdep<float[OBJ_CSUM_SIZE]> out,
		      EqualizerData *data)
{
    run_equalizer_chunk(*in, *out, data);
}

void run_equalizer_chunk(float *in, float * out, EqualizerData *data)
{
    for( int i=0; i < CHUNK; ++i )
	out[i] = run_equalizer( &in[i], data );
}

float run_equalizer(float *in, EqualizerData *data)
{
    int i;
  float lpf_out[EQUALIZER_BANDS + 1];
  float sum = 0.0;

  /* Run the child filters. */
  for (i = 0; i < EQUALIZER_BANDS + 1; i++)
      lpf_out[i] = run_lpf(in, &data->lpf[i]);

  /* Now process the results of the filters.  Remember that each band is
   * output(hi)-output(lo). */
  // HV: lpf_out[EQUALIZER_BANDS] has not been initialized?
  for (i = 0; i < EQUALIZER_BANDS; i++)
    sum += (lpf_out[i+1] - lpf_out[i]) * data->gain[i];

  return sum;
}

// in: all, usually just 1 present
void write_floats( float v )
{
    /* Better to resort to some kind of checksum for checking correctness... */
    static float running = 0;
    running += v;
    // printf( "%f\n", v );
}

void write_floats_df( obj::indep<float[OBJ_CSUM_SIZE]> v )
{
    leaf_call( write_floats_chunk, *v );
}

void write_floats_chunk( float * v )
{
    for( int i=0; i < CHUNK; ++i )
	write_floats( v[i] );
}
