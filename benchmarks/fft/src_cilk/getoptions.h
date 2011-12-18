/*
 * Copyright (C) 2011 Hans Vandierendonck (hvandierendonck@acm.org)
 * Copyright (C) 2011 George Tzenakis (tzenakis@ics.forth.org)
 * Copyright (C) 2011 Dimitrios S. Nikolopoulos (dsn@ics.forth.org)
 * 
 * This file is part of Swan.
 * 
 * Swan is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * Swan is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with Swan.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * Function to evaluate argv[]. specs is a 0 terminated array of command 
 * line options and types an array that stores the type as one of 
 */
#define INTARG 1
#define DOUBLEARG 2
#define LONGARG 3
#define BOOLARG 4
#define STRINGARG 5
#define BENCHMARK 6
/*
 * for each specifier. Benchmark is specific for cilk samples. 
 * -benchmark or -benchmark medium sets integer to 2
 * -benchmark short returns 1
 * -benchmark long returns 2
 * a boolarg is set to 1 if the specifier appears in the option list ow. 0
 * The variables must be given in the same order as specified in specs. 
 */

void get_options(int argc, char *argv[], char *specs[], int *types,...);
