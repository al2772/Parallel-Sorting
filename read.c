#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/sysinfo.h>
#include <sys/stat.h>
#include <sys/mman.h>

struct record
{
    int key;
    int* value;
};


struct record* readFile(const char* arg){
	struct record* list;
	struct record record_size;

	// Open the file specified
	int fp = open(arg, O_RDONLY);
	struct stat fst;

	if(fstat(fp, &fst) == -1){
		printf("fstat error");
	}
	
	// Get mmap file and create a char array to hold separated records
	int* mem_file = mmap(0, fst.st_size, PROT_READ, MAP_PRIVATE, fp, 0);
	// Allocate memory
	list = malloc(sizeof(struct record) * (fst.st_size/100));

	// Create a list of records
	// Find number of records
	for(int i = 0; i < fst.st_size/100; i++){
	
		// Copy first 4 as integer to record key
		list[i].key = mem_file[i*25];	
			
		// Loop through 24 values to store in record value
				
		// Copy last 96 bytes (as 4 byte integers) into value
		list[i].value = &mem_file[i*25];
		
			
	}
	// close file and return
	for(int i = 0; i < 10; i++){
		printf("%d\n", list[i].value[0]);
	}
	close(fp);
	close(fd
	return list;
}

int main(int argc, char const *argv[])
{
	readFile(argv[1]);

    return 0;
}

