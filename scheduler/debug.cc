#include "config.h"

#include <time.h>
#include <sys/time.h>

#include <cstring>

#include <fstream>
#include <string>

#include "debug.h"
#include "lock.h"

#if DEBUG_CERR
class parallel_cerr {
    std::fstream file;
    size_t id;
    size_t seq;
public:
    parallel_cerr( const char * fname, size_t id_ )
	: file( fname, std::ios_base::out | std::ios_base::trunc ),
	  id( id_ ), seq( 0 ) {
	file.setf( std::ios_base::boolalpha, std::ios_base::showpoint );
    }

    std::ostream & get_ostream() const {
	return const_cast<std::fstream &>(file);
    }

    size_t get_id() const { return id; }
    size_t bump_seq() { return seq++; }
};

__thread parallel_cerr * pcerr = 0;
static size_t glob_ctr = 0;
static struct timeval start;
static cas_mutex lock;

std::ostream & errs()
{
    if( !pcerr ) {
	char fname[20];
	lock.lock();
	if( glob_ctr == 0 )
	    gettimeofday(&start, 0);
	size_t id = glob_ctr++;
	snprintf( fname, sizeof(fname)-1, "cerr-%zu", id );
	pcerr = new parallel_cerr( fname, id );
	std::cerr << "Create " << fname << "\n";
	lock.unlock();
    }
    std::ostream & os = pcerr->get_ostream();
    struct timeval now;
    gettimeofday( &now, 0 );
    std::streamsize w = os.width( 14 );
    os << (long long)((double(now.tv_usec)+double(now.tv_sec)*1000000.0)-(double(start.tv_usec)+double(start.tv_sec)*1000000.0)) << ' ';
    os.width( 2 );
    os << pcerr->get_id() << ' ' << pcerr->bump_seq() << ' ';
    os.flush();
    os.width( w );
    return os;
}

#else // DEBUG_CERR

std::ostream & errs() {
    return std::cerr;
}

#endif // !DEBUG_CERR
