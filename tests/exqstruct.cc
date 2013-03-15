#include "wf_interface.h"

#define N 65000
#define NUM_PRODUCERS 336

using namespace obj;

//-------------- EXAMPLE --------------------
typedef struct test {
	int value;
	int value2;
	// int value3;
	// int value4;
	// int value5;
	char const * str;
	char ch1;
	char ch2;
	// int value6;
	// int value7;
	// int value8;
	// int value9;

	//char* str;
}test;

test **input;
test *output;

void producer(pushdep<test> q, int pos)
{
	for (int i = 0; i<N; i++)
	{
		//cout<<"pushing... "<<input[pos][i]<<"\n";
		q.push(input[pos][i]);
	}	
}

void rec_producer(pushdep<test> q, int pos, int i)
{
	if(i<N)	{
	//	cout<<"in rec producer pushing!!! "<<input[pos][i]<<"\n";
	usleep(10);
		q.push(input[pos][i]);
		call(rec_producer, (pushdep<test>)q, pos, ++i);
		//q2.clearBusy();
	}
	//cout<<"returning!!! "<<"\n";
	
	return;
}

void rec_producer_wrapper(pushdep<test> q, int pos, int i)
{
	//hyperqueue<int> q2(q);
	//q2 = q;
	call(rec_producer, (pushdep<test>)q, pos, i);
	return;
}
void rec_producer_wrapper2(pushdep<test> q, int pos, int i)
{
	//hyperqueue<int> q2(q);
	call(rec_producer_wrapper, (pushdep<test>)q, pos, i);
	return;
}

void consumer_producer(popdep<test> q, pushdep<test> q2)
{
	test j;
	int i = 0;
	while(i<N*NUM_PRODUCERS)
	{
		
		j = q.pop();
		//usleep(10);
		//output[i] = j;
		q2.push(j);
		//cout<<"consumer just popped "<<j<<"\n";	
		i++;
	}
	errs() << "LIST IS EMPTY!!! FINISH CONSUMER producer!!!!!!!!!!\n";
}

void consumer(popdep<test> q, int k)
{
	test j;
	int i = 0;
	while(i<N*NUM_PRODUCERS)
	//do
//	while(!q.empty())
	{
		
		j = q.pop();
		//usleep(10);
		output[i] = j;
		//cout<<"consumer just popped "<<j<<"\n";	
		i++;
	}//while(!q.empty());
	errs()<<"LIST IS EMPTY!!! FINISH CONSUMER!!!!!!!!!! "<<i<<"\n";
}

void fragment(outdep<int> k) {
	usleep(10);
	k = 1;
}

void fragmentrefine(indep<int> k, pushdep<test> a, inoutdep<int> wait, int pos) {
	for (int i = 0; i<N; i++)
	{
		//cout<<"pushing... "<<input[pos][i]<<"\n";
		a.push(input[pos][i]);
	}
}

void deduplicate(popdep<test> a, pushdep<test> b, inoutdep<int> wait) {
	for (int i = 0; i<N; i++)
	{
		//cout<<"pushing... "<<input[pos][i]<<"\n";
		test t;
		t = a.pop();
		b.push(t);
	}

}

void out(popdep<test> b, inoutdep<int> wait, int pos, FILE *fp) {
	for(int i = 0; i<N; i++) {
		output[pos*N+i] = b.pop();
		fprintf(fp, "%d\n", output[pos*N+i].value);
	}
}

void func()
{
	object_t<int> wait_ref;
	object_t<int> wait_dedup;
	object_t<int> wait_out;
	object_t<int> o;
	FILE *fp = fopen("test_structs.txt", "a");
	for(int i = 0; i<NUM_PRODUCERS; i++)
	{
		hyperqueue<test> a;
		hyperqueue<test> b;
		//spawn(rec_producer_wrapper2, (pushdep<test>)q, i, 0);
		call(fragment, (outdep<int>)o);
		spawn(fragmentrefine, (indep<int>)o, (pushdep<test>)a, (inoutdep<int>)wait_ref, i);
		spawn(deduplicate, (popdep<test>)a, (pushdep<test>)b, (inoutdep<int>)wait_dedup);
		spawn(out, (popdep<test>)b, (inoutdep<int>)wait_out, i, fp);
		//spawn(producer, (pushdep<test>)q, i);
	}
	// spawn(consumer_producer, (popdep<test>)q, (pushdep<test>)q2);
	 
//	for(int i = 0; i<N*NUM_PRODUCERS; i++)
		//spawn(consumer, (popdep<test>)q, 0);
	 
	ssync();
	fclose(fp);
	
	int k = 0;
	
	for(int i = 0; i<NUM_PRODUCERS; i++)
	{
		for(int j = 0; j<N; j++)
		{
			if(strcmp(output[k].str, "blablabla") != 0)
			{
			    errs()<<"POSITION "<< k <<": SELF TEST FAILED "<<output[k].str<<" != "<<"blablabla"<<"\n";//input[i][j].value<<"\n";
				return;
			
			}
			if(output[k].value != input[i][j].value)
			{
			    errs()<<"POSITION "<< k <<": SELF TEST FAILED "<<output[k].value<<" != "<<input[i][j].value<<"\n";
				return;
			}
			k++;
		}
	}
				    errs()<<"SELF TEST PASSED!"<<"\n";
	
	
	return;
}
void initInput()
{
    unsigned int i, j;
    unsigned int k = 0;
    output = new test[N*NUM_PRODUCERS];
    input = new test *[NUM_PRODUCERS];
    for(i = 0; i<NUM_PRODUCERS; i++) {
	input[i] = new test[N];
	for(j = 0; j<N; j++) {
	    input[i][j].value = k;
	    input[i][j].str = "blablabla";
	    output[k].value = 0;
	    k++;
	}
    }
}
int main()
{
    errs()<<"size of integer = "<<sizeof(int)<<" sizeof(char) = "<<sizeof(char)<<" sizeof test struct = "<<sizeof(test)<<" sizeof long = "<<sizeof(long)<<"\n";
	initInput();
	run(func);
	//hyperqueue<int> q;
	//queue_version * qv;
	//cout<<"size of q = "<<sizeof(q)<<" sizeof qversion * = "<<sizeof(qv)<<"\n";
	//cout<<" q has default constructor = "<<std::has_trivial_default_constructor<hyperqueue<int>>::value<<"\n";
	errs()<<"size of integer = "<<sizeof(int)<<" sizeof(char*) = "<<sizeof(char*)<<" sizeof test struct = "<<sizeof(test)<<" sizeof long = "<<sizeof(long)<<"\n";
	
	return 0;
}


