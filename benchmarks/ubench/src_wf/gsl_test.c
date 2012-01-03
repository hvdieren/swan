#include <stdio.h>
#include <gsl/gsl_rng.h>
#include <gsl/gsl_cdf.h>
#include <gsl/gsl_randist.h>

#define MAX_VALUES 100000
#define DISTRO_MEAN 0
#define DISTRO_DEVIATION 10000

double values[MAX_VALUES];

int main (void)
{
  const gsl_rng_type *T;
  gsl_rng *r;

  int i, n = MAX_VALUES;
  double min=DISTRO_MEAN, max=DISTRO_MEAN;
  double sum = 0.0;
  unsigned int negatives = 0;

  gsl_rng_env_setup ();

  T = gsl_rng_default;
  r = gsl_rng_alloc (T);

#pragma omp parallel for firstprivate(r)
  for (i = 0; i < n; i++)
  {
      double u = gsl_ran_gaussian_ziggurat (r, DISTRO_DEVIATION)+DISTRO_MEAN;
      if(u<0.0) negatives+=1;
      if(u<min) min = u;
      if(u>max) max = u;
      values[i] = u;
      //printf ("%.5f\n", u);
  }

  for (i = 0; i < n; i++)
  {
      sum += values[i];
  }

  printf("avg = %lf\n", sum/MAX_VALUES);
  printf("negatives = %u\n", negatives);
  printf("min = %lf\n", min);
  printf("max = %lf\n", max);

//  gsl_rng_free (r);

  return 0;
}

