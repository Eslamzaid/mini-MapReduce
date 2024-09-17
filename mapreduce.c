#include <stdio.h>
#include "mapreduce.h"
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdlib.h>

//^ data structures 
typedef  struct __node {
    char* name;
    char* value;
    struct __node *next;
} node;

typedef struct __hash_entry {
    node *bucket;
    pthread_mutex_t add_lock;
} hash_table;

hash_table *my_Hash_Table = NULL;
char number_of_entries = 0;
Partitioner toUsePartitioner;
// pthread_mutex_t add_lock = PTHREAD_MUTEX_INITIALIZER;
//~ sem_t mappers_done;
//~ pthread_cond_t mappers_done = PTHREAD_COND_INITIALIZER;


//^ functions
unsigned long MR_DefaultHashPartition(char *key, int num_partitions);

void MR_Emit(char *key, char *value) {
    // create a new 
    int index = toUsePartitioner(key, number_of_entries);

    node *new_entry = malloc(sizeof(struct __node));
    new_entry->name = key;
    new_entry->value = value;

    pthread_mutex_lock(&my_Hash_Table[index].add_lock);
    if(my_Hash_Table[index].bucket == NULL) {
        new_entry->next = NULL;
        my_Hash_Table[index].bucket = new_entry;
    } else {
        node* pointer = my_Hash_Table[index].bucket;
        my_Hash_Table[index].bucket = new_entry;
        new_entry->next = pointer;
    }
    pthread_mutex_unlock(&my_Hash_Table[index].add_lock);
}


void MR_Run(int argc, char* argv[],
            Mapper map, int num_mappers,
            Reducer reduce, int num_reducers,
            Partitioner partitioner) {

    printf("%d, %p, \n",num_reducers, reduce);

    if(argc <= 1) {
        printf("Missing arguments\n");
        exit(EXIT_FAILURE);
    }
    
    my_Hash_Table = (hash_table*) malloc(num_mappers * sizeof(struct __hash_entry));
    if(my_Hash_Table == NULL) {
        perror("Got an error in init");
        exit(EXIT_FAILURE);
    } 

    for(int i = 0 ; i < num_mappers; i++) {
        pthread_mutex_init(&my_Hash_Table[i].add_lock, NULL);
        my_Hash_Table[i].bucket = NULL;
    }

    number_of_entries = num_mappers;
    toUsePartitioner = partitioner;
    pthread_t mapper_threads[num_mappers];

    for(int i = 1; i <= argc-1; i++) {
        if(pthread_create(&mapper_threads[i-1], NULL, (void*)map,(Mapper) argv[i]) != 0){
            perror("Couldn't initialize a thread\n");
            exit(EXIT_FAILURE);
        }
    }

    for(int i = 0; i < argc-1; i++) {
        pthread_join(mapper_threads[i], NULL);
    } 

    printf("Joined All\n");

    for(int i = 0; i < num_mappers; i++) {
        node *current_bucket = my_Hash_Table[i].bucket;
        if(current_bucket == NULL) {
            printf("THis is null\n");
        } else {
            while(current_bucket != NULL) {
                printf("\t%s with value %s\n", current_bucket->name, current_bucket->value);
                current_bucket = current_bucket->next;
            }
        }
    }

}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions-1;
}
