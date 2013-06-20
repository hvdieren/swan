#include "swan/wf_interface.h"

#define RESERVE 128

void generate( obj::pushdep<float> out, int n ) {
    for( int i=0; i < n; ++i )
	out.push( i+1 );
}

void delay( obj::popdep<float> in, obj::pushdep<float> out, int n ) {
    float prev = 0;
    while( !in.empty() ) {
	float f = in.pop();
	out.push( f );
	out.push( prev );
	prev = f;
    }
}

void filter( obj::popdep<float> in, obj::pushdep<float> out, float k, int n ) {
    while( !in.empty() ) {
	float f0 = in.pop();
	float f1 = in.pop();
	float ei = f0 - k * f1;
	float ebari = f1 - k * f0;
	out.push( ei );
	out.push( ebari );
    }
}

void final( obj::popdep<float> in, int n ) {
    while( !in.empty() ) {
	float v = in.pop();
	printf( "%f\n", v );
    }
}

void work( int n ) {
    obj::hyperqueue<float> * q1[11];
    obj::hyperqueue<float> * q2[11];

    for( int i=1; i <= 10; ++i ) {
	q1[i] = new obj::hyperqueue<float>( 8191, 0 );
	q2[i] = new obj::hyperqueue<float>( 8191, 0 );
    }

    spawn( generate, (obj::pushdep<float>)*q1[2], n );
    for( int i=2; i < 10; ++i ) {
	spawn( delay, (obj::popdep<float>)*q1[i],
	       (obj::pushdep<float>)*q2[i], n );
	spawn( filter, (obj::popdep<float>)*q2[i],
	       (obj::pushdep<float>)*q1[i+1], (float)2, n );
    }
    spawn( final, (obj::popdep<float>)*q1[10], n );

    ssync();
}

int main( int argc, char * argv[] ) {
    int n = 10;
    if( argc > 1 )
	n = atoi( argv[1] );

    run( work, n );

    return 0;
}

