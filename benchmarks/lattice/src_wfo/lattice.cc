//
// Speedup described in:
// Application-Specific Customization and Scalability of Soft Multiprocessors
// http://www.ann.ece.ufl.edu/courses/eel6935_13spr/papers/Application_Specific_Customization_and_Scalability_of_Soft_Multiprocessors.pdf
// and is limited to 10-20% on 16 (soft-core) processors.
//
#include "swan/wf_interface.h"

#define CHUNK   16384

void generate( obj::outdep<float[CHUNK]> out, size_t i, size_t n ) {
    float * o = (float *)out;
    for( size_t j=0; j < n; ++j ) {
	float f = i+j+1;
	o[j] = f;
    }
}

#if 0
void delay( obj::popdep<float> in, obj::pushdep<float> out, size_t n ) {
    float prev = 0;
    while( !in.empty() ) {
	obj::read_slice<obj::queue_metadata, float> rs
	    = in.get_slice_upto( RESERVE, 0 );
	obj::write_slice<obj::queue_metadata, float> ws
	    = out.get_write_slice( 2*std::min(size_t(RESERVE),rs.get_npops()) );
	for( size_t i=0; i < rs.get_npops(); ++i ) {
	    float f = rs.pop();
	    ws.push( f );
	    ws.push( prev );
	    prev = f;
	}
	rs.commit();
	ws.commit();
    }
}

void filter( obj::popdep<float> in, obj::pushdep<float> out, float k, size_t n ) {
    while( !in.empty() ) {
	obj::read_slice<obj::queue_metadata, float> rs
	    = in.get_slice_upto( RESERVE, 0 );
	obj::write_slice<obj::queue_metadata, float> ws
	    = out.get_write_slice( std::min(size_t(RESERVE),rs.get_npops()) );
	for( size_t i=0; i < rs.get_npops()/2; ++i ) {
	    float f0 = rs.pop();
	    float f1 = rs.pop();
	    float ei = f0 - k * f1;
	    float ebari = f1 - k * f0;
	    ws.push( ei );
	    ws.push( ebari );
	}
	rs.commit();
	ws.commit();
    }
}
#endif

void delay_filter_segment( obj::indep<float[CHUNK]> in, obj::outdep<float[CHUNK]> out0, obj::outdep<float[CHUNK]> out1, float k, size_t n ) {
    float prev = 0;
    size_t n0 = std::min( size_t(CHUNK/2), n );
    for( size_t i=0; i < n0; ++i ) {
	float f = (*in)[i];
	float f0 = f;
	float f1 = prev;
	prev = f;
	float ei = f0 - k * f1;
	float ebari = f1 - k * f0;
	(*out0)[2*i] = ei;
	(*out0)[2*i+1] = ebari;
    }
    for( size_t i=0; i < n-n0; ++i ) {
	float f = (*in)[n0+i];
	float f0 = f;
	float f1 = prev;
	prev = f;
	float ei = f0 - k * f1;
	float ebari = f1 - k * f0;
	(*out1)[2*i] = ei;
	(*out1)[2*i+1] = ebari;
    }
}

struct add_monad {
    typedef float value_type;
    typedef obj::cheap_reduction_tag reduction_tag;
    static void identity( float * p ) { *p = 0; }
    static void reduce( float * left, float * right ) { *left += *right; }
};

size_t final_count = 0;

void final_segment( obj::indep<float[CHUNK]> in, obj::reduction<add_monad> sum, size_t n ) {
    float running = 0;
    for( size_t i=0; i < n; ++i ) {
	float v = (*in)[i];
	// printf( "%f\n", v );
	running += v;
    }
    sum += running;
    __sync_fetch_and_add( &final_count, n );
}

void work( size_t n ) {
    static obj::object_t<float[CHUNK]> * data[1+(1<<10)];
    obj::object_t<float> sum;

    for( size_t d=0; d < 1+(1<<10); ++d )
	data[d] = new obj::object_t<float[CHUNK]>();

    for( size_t j=0; j < n; j += CHUNK ) {
	size_t nj = std::min( size_t(CHUNK), n-j );
	spawn( generate, (obj::outdep<float[CHUNK]>)*data[0], j, nj );
	size_t sd = 0, nd = 1;
	for( size_t i=2; i < 10; ++i ) {
	    size_t nd_add = 0;
	    for( size_t d=sd; d < nd; ++d ) {
		spawn( delay_filter_segment,
		       (obj::indep<float[CHUNK]>)*data[d],
		       (obj::outdep<float[CHUNK]>)*data[nd+2*d],
		       (obj::outdep<float[CHUNK]>)*data[nd+2*d+1],
		       (float)i, d == nd-1 ? nj : CHUNK );
		sd++;
		nd_add += 2;
	    }
	    nd += nd_add;
	}
	for( size_t d=sd; d < nd; ++d ) {
	    spawn( final_segment, (obj::indep<float[CHUNK]>)*data[d],
		   (obj::reduction<add_monad>)sum,
		   d == nd - 1 ? nj : CHUNK );
	}
    }

    ssync();

    printf( "Values popped in final_segment(): %ld\n", final_count );
}

int main( int argc, char * argv[] ) {
    size_t n = 10;
    if( argc > 1 )
	n = atoi( argv[1] );

    run( work, n );

    return 0;
}

