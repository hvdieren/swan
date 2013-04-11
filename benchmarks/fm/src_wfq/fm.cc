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

#include "swan/wf_interface.h"

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

/* Reading data: */
void get_floats(obj::pushdep<float> fb, int n);

/* Low pass filter: */
typedef struct LPFData
{
  float coeff[NUM_TAPS];
  float freq;
  int taps, decimation;
} LPFData;
float lpf_coeff[NUM_TAPS];
void init_lpf_data(LPFData *data, float freq, int taps, int decimation);
void run_lpf_fb(obj::popdep<float> fbin, obj::pushdep<float>fbout, LPFData *data, int n);
float run_lpf(const float *in, LPFData *data);

void run_demod(obj::popdep<float> fbin, obj::pushdep<float> fbout, int N);

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
void run_equalizer(obj::popdep<float> fbin, obj::pushdep<float> fbout, EqualizerData *data, int N);

float write_floats(obj::prefixdep<float> fb, int N);

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
    obj::hyperqueue<float>
	fb1( 8191, NUM_TAPS+DECIMATION ), // lpf peeks NUM_TAPS, pops DECIMATION+1
	fb2( 8191, 1 ), // demod peeks 1 -- TODO: may be 0
	fb3( 16383, 64 ), // equalizer peeks 64
	fb4( 16383 ); // write does not peek
    LPFData lpf_data;
    EqualizerData eq_data;

    init_lpf_data(&lpf_data, CUTOFF_FREQUENCY, NUM_TAPS, DECIMATION);
    init_equalizer(&eq_data);

    /* The low-pass filter will need NUM_TAPS+1 items; read them if we
     * need to. */
    spawn(get_floats,(obj::pushdep<float>)fb1,(numiters+NUM_TAPS-1+3)*5+NUM_TAPS);
    // run_lpf:
    // in: consume/move: data->decimation+1 = DECIMATION+1 = 5
    // in: read: NUM_TAPS=64
    // out: 1
    // lpf_data: in
    spawn(run_lpf_fb, (obj::popdep<float>)fb1, (obj::pushdep<float>)fb2, &lpf_data, numiters+NUM_TAPS-1+2);
    // in: consume 1, read 2
    // out: produce 1
    // run_demod( fb2h, fb2p, fb3t );
    // where fb2p is fb2h output now.
    // where fb3t is a series of the previous 63 taps and the new tap
    spawn(run_demod, (obj::popdep<float>)fb2, (obj::pushdep<float>)fb3, numiters+NUM_TAPS);
    // run_equalize:
    // in: consume/move: data->decimation+1 = 0+1 = 1
    // in: read: NUM_TAPS=64
    // out: 1
    // eq_data is in
    spawn(run_equalizer, (obj::popdep<float>)fb3, (obj::pushdep<float>)fb4, &eq_data, numiters);
    chandle<float> w;
    spawn(write_floats, w, fb4.prefix(numiters), numiters);
    ssync();
}

// out: up to IN_BUFFER_LEN (fill as much as possible)
void get_floats_sub(obj::suffixdep<float> fb, int x, int n)
{
    if( n < 8192 ) {
	/* Fill the remaining space in fb with 1.0. */
	int i = n;
	while( i-- > 0 ) {
	    fb.push( (float)x );
	    x++;
	}
    } else {
	spawn( get_floats_sub, fb.suffix( n/2 ), x, n/2 );
	call( get_floats_sub, fb.suffix( n-n/2 ), x+n/2, n-n/2 );
	ssync();
    }
}

void get_floats(obj::pushdep<float> fb, int n)
{
    static int x = 0;
  
    //errs() << "get_float start\n";
    call( get_floats_sub, fb.suffix( n ), x, n );
    x += n;
    //errs() << "get_float end\n";
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
void run_lpf_fb_sub(obj::prefixdep<float> fbin, obj::suffixdep<float> fbout, LPFData *data, int s, int N)
{
    if( N < 8192 ) {
	while( N > 0 ) {
	    obj::read_slice<obj::queue_metadata, float> slice
		= fbin.get_slice_upto( (data->decimation+1)*N, NUM_TAPS+data->decimation );
	    for( size_t i=0; i < slice.get_npops(); i += data->decimation+1 ) {
		float sum = run_lpf( &slice.peek(i), data);
		fbout.push( sum );
		N--;
	    }
	    slice.commit();
	    if( size_t n = slice.get_npops() % (data->decimation+1) ) {
		while( n++ < (data->decimation+1) )
		    fbin.pop();
	    }
	}
    } else {
	spawn( run_lpf_fb_sub, fbin.prefix( (data->decimation+1)*(N/2) ),
	       fbout.suffix( N/2 ), data, s, N/2 );
	call( run_lpf_fb_sub, fbin.prefix( (data->decimation+1)*(N-N/2) ),
	      fbout.suffix( N-N/2 ), data, s+N/2, N-N/2 );
	ssync();
    }
}

void run_lpf_fb(obj::popdep<float> fbin, obj::pushdep<float> fbout, LPFData *data, int N)
{
    spawn( run_lpf_fb_sub, fbin.prefix( (data->decimation+1)*N ),
	   fbout.suffix( N ), data, 0, N );
    ssync();
}

float run_lpf(const float *in, LPFData *data)
{
  float sum = 0.0;
  int i = 0;

  for (i = 0; i < data->taps; i++)
    sum += in[i] * data->coeff[i];

  return sum;
}

// in: consume 1, read 2
// out: produce 1
void run_demod_sub(obj::prefixdep<float> fbin, obj::suffixdep<float> fbout, int N)
{
    //errs() << "run_demod_sub start " << N << "\n";
    if( N < 8192 ) {
	while( N > 0 ) {
	    obj::read_slice<obj::queue_metadata, float> slice
		= fbin.get_slice_upto( N, 1 );
	    for( size_t i=0; i < slice.get_npops(); i++ ) {
		float temp, gain;
		gain = MAX_AMPLITUDE * SAMPLING_RATE / (BANDWIDTH * M_PI);
		float fpl0 = slice.peek(i);
		float fpl1 = slice.peek(i+1);
		temp = fpl0 * fpl1;
		temp = gain * atan(temp);
		fbout.push( temp );
	    }
	    N -= slice.get_npops();
	    slice.commit();
	}
    } else {
	spawn( run_demod_sub, fbin.prefix( N/2 ), fbout.suffix( N/2 ), N/2 );
	call( run_demod_sub, fbin.prefix( N-N/2 ), fbout.suffix( N-N/2 ), N-N/2 );
	ssync();
    }
    //errs() << "run_demod_sub end " << N << "\n";
}

void run_demod(obj::popdep<float> fbin, obj::pushdep<float> fbout, int N)
{
    call( run_demod_sub, fbin.prefix( N ), fbout.suffix( N ), N );
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
void run_equalizer_sub(obj::prefixdep<float> fbin, obj::suffixdep<float> fbout, EqualizerData *data, int N) {
    if( N < 16384 ) {
	while( N > 0 ) {
	    obj::read_slice<obj::queue_metadata, float> slice
		= fbin.get_slice_upto( N, 64 );
	    for( size_t j=0; j < slice.get_npops(); j++ ) {
		int i;
		float lpf_out[EQUALIZER_BANDS + 1];
		float sum = 0.0;
		
		/* Run the child filters. */
		for (i = 0; i < EQUALIZER_BANDS + 1; i++)
		    lpf_out[i] = run_lpf(&slice.peek(j), &data->lpf[i]);

		/* Now process the results of the filters.  Remember that each band is
		 * output(hi)-output(lo). */
		// HV: lpf_out[EQUALIZER_BANDS] has not been initialized?
		for (i = 0; i < EQUALIZER_BANDS; i++)
		    sum += (lpf_out[i+1] - lpf_out[i]) * data->gain[i];

		fbout.push( sum );
	    }
	    N -= slice.get_npops();
	    slice.commit();
	}
    } else {
	spawn( run_equalizer_sub, fbin.prefix( N/2 ), fbout.suffix( N/2 ), data, N/2 );
	call( run_equalizer_sub, fbin.prefix( N-N/2 ), fbout.suffix( N-N/2 ), data, N-N/2 );
	ssync();
    }
}

void run_equalizer(obj::popdep<float> fbin, obj::pushdep<float> fbout, EqualizerData *data, int N) {
    call( run_equalizer_sub, fbin.prefix(N), fbout.suffix(N), data, N );
}


// in: all, usually just 1 present
float write_floats(obj::prefixdep<float> fb, int N)
{
  /* printf() any data that's available: */
#ifdef raw
  while (fb->rpos < fb->rlen)
    print_float(fb->buff[fb->rpos++]);
#else
/* Better to resort to some kind of checksum for checking correctness...
*/

  if( N < 16384 ) {
    float running = 0;
    for( int i=0; i < N; ++i ) {
	running += fb.pop();
	// printf("%f\n", fb.pop());
    }
    return running;
  } else {
      chandle<float> r;
      spawn( write_floats, r, fb.prefix( N/2 ), N/2 );
      float c = call( write_floats, fb.prefix( N-N/2 ), N-N/2 );
      ssync();
      return r + c;
  }
/*
  errs() << "write_floats start " << N << "\n";
  for( int i=0; i < N; ++i )
      printf("%f\n", fb.pop());
  errs() << "write_floats end\n";
*/
#endif
}
