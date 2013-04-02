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

#define SAMPLING_RATE 250000000
#define CUTOFF_FREQUENCY 108000000
#define NUM_TAPS 64
#define MAX_AMPLITUDE 27000.0
#define BANDWIDTH 10000
#define DECIMATION 4
/* Must be at least NUM_TAPS+1: */
#define IN_BUFFER_LEN 10000

void begin(void);

typedef struct FloatBuffer
{
  float buff[IN_BUFFER_LEN];
  int rpos, rlen;
} FloatBuffer;

void fb_compact(FloatBuffer *fb);
int fb_ensure_writable(FloatBuffer *fb, int amount);

/* Reading data: */
float get_float();

/* Low pass filter: */
typedef struct LPFData
{
  float coeff[NUM_TAPS];
  float freq;
  int taps, decimation;
} LPFData;
float lpf_coeff[NUM_TAPS];
void init_lpf_data(LPFData *data, float freq, int taps, int decimation);
void run_lpf_fb(FloatBuffer *fbin, FloatBuffer *fbout, LPFData *data);
float run_lpf_fb1(FloatBuffer *fbin, LPFData *data);
float run_lpf(float *in, LPFData *data);

float run_demod(float lpf0, float lfp1);

#define EQUALIZER_BANDS 10
float eq_cutoffs[EQUALIZER_BANDS + 1] =
  { 55.000004, 77.78174, 110.00001, 155.56354, 220.00002, 311.12695,
    440.00003, 622.25415, 880.00006, 1244.5078, 1760.0001 };
typedef struct EqualizerData
{
  LPFData lpf[EQUALIZER_BANDS + 1];
    // FloatBuffer fb[EQUALIZER_BANDS + 1];
  float gain[EQUALIZER_BANDS];
} EqualizerData;
void init_equalizer(EqualizerData *data);
float run_equalizer(float *in, EqualizerData *data);

void write_floats( float v );

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

  begin();
  return 0;
}
#endif



void begin(void)
{
    int i;
    float in[NUM_TAPS];
    LPFData lpf_data;
    EqualizerData eq_data;
    float sum, lpf0, lpf1;
    float dm[64], dms;
    int dmi;

    init_lpf_data(&lpf_data, CUTOFF_FREQUENCY, NUM_TAPS, DECIMATION);
    init_equalizer(&eq_data);

    /* Startup: */
    for( i=0; i < NUM_TAPS; ++i )
	in[i] = get_float();

    /* LPF needs at least NUM_TAPS+1 inputs; get_floats is fine. */
    lpf1 = run_lpf(in, &lpf_data);
    /* run_demod needs 1 input, OK here. */
    /* run_equalizer needs 51 inputs (same reason as for LPF).  This means
     * running the pipeline up to demod 50 times in advance: */
    for (i = 0; i < 63; i++)
    {
	for( dmi=0; dmi<NUM_TAPS-(DECIMATION+1); ++dmi )
	    in[dmi] = in[dmi+DECIMATION+1];
	for( ; dmi < NUM_TAPS; ++dmi )
	    in[dmi] = get_float();
	
	lpf0 = run_lpf(in, &lpf_data);
	dms = run_demod(lpf0, lpf1);
	for( dmi=0; dmi<63; ++dmi )
	    dm[dmi] = dm[dmi+1];
	dm[63] = dms;
	lpf1 = lpf0;
    }

    /* Main loop: */
    while (numiters == -1 || numiters-- > 0)
    {
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
	lpf0 = run_lpf(in, &lpf_data);
	// in: consume 1, read 2
	// out: produce 1
	dms = run_demod(lpf0, lpf1);
	for( dmi=0; dmi<63; ++dmi )
	    dm[dmi] = dm[dmi+1];
	dm[63] = dms;
	lpf1 = lpf0;
	// run_equalize:
	// in: consume/move: data->decimation+1 = 0+1 = 1
	// in: read: NUM_TAPS=64
	// out: 1
	// eq_data is in
	sum = run_equalizer(dm, &eq_data);
	write_floats(sum);
    }
}

void fb_compact(FloatBuffer *fb)
{
  memmove(fb->buff, fb->buff+fb->rpos, fb->rlen - fb->rpos);
  fb->rlen -= fb->rpos;
  fb->rpos = 0;
}

int fb_ensure_writable(FloatBuffer *fb, int amount)
{
  int available = IN_BUFFER_LEN - fb->rlen;
  if (available >= amount)
    return 1;
  
  /* Nope, not enough room, move current contents back to the beginning. */
  fb_compact(fb);
  
  available = IN_BUFFER_LEN - fb->rlen;
  if (available >= amount)
    return 1;

  /* Hmm.  We're probably hosed in this case. */
#ifndef raw
  printf("fb_ensure_writable(%p): couldn't ensure %d bytes (only %d available)\n", fb, amount, available);
#endif
  return 0;
}

// out: up to IN_BUFFER_LEN (fill as much as possible)
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
void run_lpf_fb(FloatBuffer *fbin, FloatBuffer *fbout, LPFData *data)
{
  float sum = run_lpf_fb1( fbin, data );

  /* Check that there's room in the output buffer; move data if necessary. */
  fb_ensure_writable(fbout, 1);
  fbout->buff[fbout->rlen++] = sum;
}

float run_lpf_fb1(FloatBuffer *fbin, LPFData *data)
{
    float sum = run_lpf(&fbin->buff[fbin->rpos], data);
    fbin->rpos += data->decimation + 1;
    return sum;
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
float run_demod(float lpf0, float lpf1)
{
  float temp, gain;
  gain = MAX_AMPLITUDE * SAMPLING_RATE / (BANDWIDTH * M_PI);
  temp = lpf0 * lpf1;
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

  /* Also initialize member buffers. */
  // for (i = 0; i < EQUALIZER_BANDS + 1; i++)
    // data->fb[i].rpos = data->fb[i].rlen = 0;

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
float run_equalizer(float *in, EqualizerData *data)
{
    int i;
  float lpf_out[EQUALIZER_BANDS + 1];
  float sum = 0.0;

  /* Run the child filters. */
  for (i = 0; i < EQUALIZER_BANDS + 1; i++)
      // lpf_out[i] = run_lpf(&fbin->buff[fbin->rpos], &data->lpf[i]);
      lpf_out[i] = run_lpf(in, &data->lpf[i]);

  // fbin->rpos++;

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
    printf( "%f\n", v );
}
