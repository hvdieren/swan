#ifndef PP_TIME_H
#define PP_TIME_H

#if defined(__cpluplus) || defined(__GNUG__)
extern "C" {
#endif

struct pp_time_t {
    unsigned long long total;
    unsigned long long last;
    unsigned long measurements;
    unsigned long drops;
    const char * region;
    struct pp_time_t * next;
};

typedef struct pp_time_t pp_time_t;

unsigned long long pp_time();

void pp_time_report( pp_time_t * t, const char * region );
void pp_time_start( pp_time_t * t );
void pp_time_end( pp_time_t * t );

char const * pp_time_unit();
double pp_time_read( pp_time_t * t );

void pp_time_print( pp_time_t * t, char * region );

void pp_time_max( pp_time_t * m, const pp_time_t * v );
void pp_time_add( pp_time_t * m, const pp_time_t * v );

#if defined(__cpluplus) || defined(__GNUG__)
}
#endif

#endif /* PP_TIME_H */
