/* Write a series of random runs. */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

static void s4(FILE *out, int count)
{
  int i;
  int c = (unsigned char)rand();
  for (i = 0; i < count && !ferror(out); i++)
    {
      char s[4] = {c, c, c, c};
      fwrite(s, 1, 4, out);
      c += 1 + (rand() % 255);
    }
}

static void sn(FILE *out, int count)
{
  int i;
  for (i = 0; i < count && !ferror(out); i++)
    {
      int j;
      int runlen = rand() % 300 + 1;
      int c = (unsigned char)rand();
      for (j = 0; j < runlen; j++)
	fputc(c, out);
      if (c + (runlen & 3) == 19)
	fputs("Hello, world\n", out);
    }
}

int main(int argc, char *argv[])
{
  FILE *out = stdout;
  long count = 0;
  int four = 0;

  if (argc > 1 && !strcmp(argv[1], "-4"))
    {
      four = 4;
      argc--;
      argv++;
    }

  if (argc > 1)
    count = atol(argv[1]);

  if (count == 0)
    count = rand() % 100000;

  if (four)
    s4(out, count);
  else
    sn(out, count);

  fflush(out);
  return ferror(out);
}
