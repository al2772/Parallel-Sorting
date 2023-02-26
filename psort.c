#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/sysinfo.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <math.h>

char error_message[30] = "An error has occurred\n";

struct record
{
    int key;
    char *value;
};

struct threadData
{
    int threadNum;
    int size;
    int offset;
};

struct record** lists;
char *mem_file;
int record_size;
int nprocs;


void readFile(const char* arg)
{
    if(arg == NULL){
	    int i = write(STDERR_FILENO, error_message, strlen(error_message));
            if(i == 0){
                exit(0);
            }
        exit(0);
    }
    // Open the file specified
    int fp = open(arg, O_CREAT | O_RDONLY, 0777);
    struct stat fst;

    if(fstat(fp, &fst) == -1){
            perror("An error has occurred");
    }

    if(fst.st_size == 0){
	int i = write(STDERR_FILENO, error_message, strlen(error_message));
            if(i == 0){
                exit(0);
            }
        exit(0);
    }

    // Get mmap file and create a char array to hold separated records
    mem_file = mmap(0, fst.st_size, PROT_READ, MAP_SHARED, fp, 0);
    record_size = fst.st_size/100;
    // close file and return
    close(fp);
    return;
}


void *copyToList(void *t)
{
    struct threadData *ptr = t;
    //printf("\nThreadNum = %d, size = %d, offset = %d", ptr->threadNum, ptr->size, ptr->offset);
    lists[ptr->threadNum] = malloc(sizeof(struct record) * (ptr->size + 1));    //Extra 1 element for storing size of the list.
    (lists[ptr->threadNum][0]).key = ptr->size;                                //Store size in the first element
    int offset = ptr->offset;
    for(int i=1; i<ptr->size+1; i++)
    {   
        (lists[ptr->threadNum][i]).key = *(int*)(mem_file + offset + (i-1)*100);                 //Read first 4 bytes as key; i*25 is for indexing the current record - each record is 100 bytes (25 ints) 
        (lists[ptr->threadNum][i]).value = &mem_file[offset + (i-1)*100];           //Save a pointer to the remaining 24 bytes of the record.
    }
    pthread_exit(NULL);
    return NULL;
}

int compare(const void* p1, const void* p2)
{
    int key1 = ((struct record*)p1)->key;
    int key2 = ((struct record*)p2)->key;
    return key1-key2;
}

void* sort(void* threadNum)
{
    qsort( &lists[*(int*)threadNum][1], lists[*(int*)threadNum][0].key, sizeof(struct record), compare);
    /*
    //For debugging
    printf("\nThread %d\n", *(int*)threadNum);
    for(int i=1;i<lists[*(int*)threadNum][0].key+1; i++)
        printf("%d ", lists[*(int*)threadNum][i].key);
    */
    free(threadNum);
    pthread_exit(NULL);
    return NULL;
}

void mergeLists(struct record *sortedList, int nprocs)
{
    int min=0, minValArray=0, *startIndices = malloc(sizeof(int) * nprocs);
    for(int i=0; i<nprocs; i++)
        startIndices[i] = 1;    //first element is size so skipping it.
    for(int i=0; i<record_size; i++)
    {
        //Assume that the first element(identified by startIndex) of a valid list is the min element. List is valid if the startIndex is < size of list
        for(int j=0; j<nprocs; j++)
        {
            if(startIndices[j] < lists[j][0].key + 1)
            {
                min = lists[j][startIndices[j]].key;
                minValArray=j;
            }
        }

        //Iterate through all the lists and compare the elements at the respective start indices; whichever is minimum will be the next element in the sorted array.
        for(int j=0; j<nprocs; j++)
        {
            //if(arr[j][startIndices[j]] == -1) startIndices[j]++;
            if(startIndices[j] < lists[j][0].key + 1 && lists[j][startIndices[j]].key < min)
            {
                min = lists[j][startIndices[j]].key;
                minValArray = j;
            }
        }

        //Copy key and value pointer to the sorted list
        sortedList[i].key = lists[minValArray][startIndices[minValArray]].key;
        sortedList[i].value = lists[minValArray][startIndices[minValArray]].value;

        startIndices[minValArray]++;
    }
}
int main(int argc, const char *argv[])
{
    nprocs = get_nprocs();
    if(argv[1] == NULL){
            fprintf(stderr, "An error has occurred\n");
            exit(0);
    }
    readFile(argv[1]);


 //Copy data from file to struct arrays for sorting
    lists = malloc(sizeof(struct record*) * nprocs);

    struct threadData *t = malloc(sizeof(struct threadData) * nprocs);
    int segSize = (int)ceil((double)record_size/(double)nprocs);

    pthread_t *threadId = malloc(nprocs*sizeof(pthread_t));
    for(int i=0; i<nprocs; i++)
    {
        t[i].threadNum = i;
        t[i].offset = i*segSize*100;
        t[i].size = ((i+1)*segSize <= record_size)? segSize : (record_size - i*segSize);
        // Some threads may not have elements; Ex: 12 threads, 10 records. Each thread gets 1 record but threads 11 and 12 won't have any. Ensure that size does not go -ve. 
        if(t[i].size < 0) t[i].size = 0;
        pthread_create(&threadId[i], NULL, &copyToList, (void*)&t[i]);
    }

    //Wait for all struct arrays to be populated
    for(int i=0; i<nprocs; i++)
    {
        pthread_join(threadId[i], NULL);
    }

    //Sort struct arrays in parallel
    for(int i=0; i<nprocs; i++)
    {
        int *threadNum = malloc(sizeof(int));
        *threadNum = i;
        pthread_create(&threadId[i], NULL, &sort, (void*)threadNum);    //the sort function will free threadNum after use
    }

    for(int i=0; i<nprocs; i++)
    {
        pthread_join(threadId[i], NULL);
    }

    //Merge all records into one sorted array
    struct record *sortedList = malloc(sizeof(struct record) * record_size);
    mergeLists(sortedList, nprocs);

    printf("\nSanity check");
    /*
    //For debugging - prints contents of each list after sorting
    for(int i=0; i<nprocs; i++)
    {
        printf("\nList %d\n", i);
        for(int j=1; j<(lists[i][0]).key+1; j++)
            printf("%d ",lists[i][j].key);
    }
    */
    printf("\nActual File data: ");
    for(int i=0; i<record_size; i++)
    {
        int key = mem_file[i*25];
        printf("%d ", key);
    }
    printf("\nFinal Sorted Data: ");
    for(int i=0; i<record_size; i++)
    {
        int key = sortedList[i].key;
        printf("%d ", key);
    }
    printf("\n");

    //Write sortedList to output file 
    //For each element in sortedList, extract value array (dereference value pointer); combine key and value and write it to disk - look into memcpy
    int fdout;
    if ((fdout = open(argv[2], O_TRUNC | O_RDWR | O_CREAT, S_IRWXO | S_IRWXU | S_IRWXG)) < 0)
    {
        printf("File %s cannot be created for writing", argv[2]);
        exit(0);
    }

    char *write_file;
    int trunc = ftruncate(fdout, (record_size*100));
    if(trunc == -1){
        printf("truncate error");
    }
    write_file = mmap(0, (record_size*100), PROT_READ | PROT_WRITE, MAP_SHARED, fdout, 0);
    for(int i = 0; i < record_size; i++)
    {
        //size_t x = (size_t)write_file + i*100;
        //printf("\nStart Val = %d, Value Pointer = %p", (int)x, (void*)sortedList[i].value);
        memcpy(write_file + i*100, sortedList[i].value, 100);
        //printf("\nCopied 100B");
    }

    //Free memory
    for(int i=0; i<nprocs; i++)
        free(lists[i]);
    free(t);
    free(threadId);
    free(sortedList);
    //Unmap file;
    munmap(mem_file, (record_size*100));
    munmap(write_file, (record_size*100));
    fsync(fdout);
    close(fdout);
    exit(0);

}


