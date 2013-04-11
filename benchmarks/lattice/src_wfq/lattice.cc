#include "swan/wf_interface.h"

void generate( obj::pushdep<float> out, int n ) {
    out.push( 1 );
    for( int i=1; i < n; ++i )
	out.push( 0 );
}

void delay( obj::popdep<float> in, obj::pushdep<float> out, int n ) {
    float prev = 0;
    for( int i=0; i < n; ++i ) {
	float f = in.pop();
	out.push( f );
	out.push( prev );
	prev = f;
    }
}

void filter( obj::popdep<float> in, obj::pushdep<float> out, float k, int n ) {
    for( int i=0; i < n; ++i ) {
	float f0 = in.pop();
	float f1 = in.pop();
	float ei = f0 - k * f1;
	float ebari = f1 - k * f0;
	out.push( ei );
	out.push( ebari );
    }
}

void final( obj::popdep<float> in, int n ) {
    for( int i=0; i < n; ++i ) {
	float v = in.pop();
	printf( "%f\n", v );
    }
}

void work( int n ) {
    obj::hyperqueue<float> q1( 8191, 1 );
    obj::hyperqueue<float> * q2[11];

    for( int i=2; i < 10; ++i )
	q2[i] = new obj::hyperqueue<float>( 8191, 0 );

    spawn( generate, (obj::pushdep<float>)q1, n );
    for( int i=2; i < 10; ++i ) {
	spawn( delay, (obj::popdep<float>)q1, (obj::pushdep<float>)*q2[i], n<<(i-1) );
	spawn( filter, (obj::popdep<float>)*q2[i],
	       (obj::pushdep<float>)*q2[i+1], (float)2, n<<(i-1) );
    }
    spawn( final, (obj::popdep<float>)*q2[10], n<<9 );

    ssync();
}

int main( int argc, char * argv[] ) {
    int n = 10;
    if( argc > 1 )
	n = atoi( argv[1] );

    run( work, n );

    return 0;
}

