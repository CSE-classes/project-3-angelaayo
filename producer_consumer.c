#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

#define BUFFER_SIZE 5
#define MAX_MSG 256


typedef struct {
    char buffer[BUFFER_SIZE];
    int head;      
    int tail;      
    int count;     
} circular_queue_t;


circular_queue_t queue;
pthread_mutex_t mutex;
pthread_cond_t buffer_not_full;   
pthread_cond_t buffer_not_empty; 

char message[MAX_MSG];
int message_len;
int producer_done = 0;


void init_queue() {
    queue.head = 0;
    queue.tail = 0;
    queue.count = 0;
}

int is_full() {
    return queue.count == BUFFER_SIZE;
}

 
int is_empty() {
    return queue.count == 0;
}


void enqueue(char c) {
    queue.buffer[queue.tail] = c;
    queue.tail = (queue.tail + 1) % BUFFER_SIZE;
    queue.count++;
    printf("  [QUEUE] Added '%c' - Queue size: %d/%d\n", c, queue.count, BUFFER_SIZE);
}


char dequeue() {
    char c = queue.buffer[queue.head];
    queue.head = (queue.head + 1) % BUFFER_SIZE;
    queue.count--;
    printf("  [QUEUE] Removed '%c' - Queue size: %d/%d\n", c, queue.count, BUFFER_SIZE);
    return c;
}


void *producer(void *arg) {
    int i;
    
    printf("\n[PRODUCER] Starting...\n");
    
    for(i = 0; i < message_len; i++) {
        pthread_mutex_lock(&mutex);
        

        while(is_full()) {
            printf("[PRODUCER] Buffer full! Waiting for consumer...\n");
            pthread_cond_wait(&buffer_not_full, &mutex);
        }

        printf("[PRODUCER] Reading character '%c' from message\n", message[i]);
        enqueue(message[i]);
        
        
        pthread_cond_signal(&buffer_not_empty);
        pthread_mutex_unlock(&mutex);
        
        //usleep(100000);
    }
    

    pthread_mutex_lock(&mutex);
    producer_done = 1;
    pthread_cond_broadcast(&buffer_not_empty);
    pthread_mutex_unlock(&mutex);
    
    printf("[PRODUCER] Finished! Total characters produced: %d\n", message_len);
    pthread_exit(NULL);
}


void *consumer(void *arg) {
    char c;
    int consumed_count = 0;
    
    printf("\n[CONSUMER] Starting...\n");
    
    while(1) {
        pthread_mutex_lock(&mutex);
        
        
        while(is_empty()) {
            if(producer_done) {
                pthread_mutex_unlock(&mutex);
                printf("[CONSUMER] Producer is done and buffer is empty. Exiting...\n");
                pthread_exit(NULL);
            }
            printf("[CONSUMER] Buffer empty! Waiting for producer...\n");
            pthread_cond_wait(&buffer_not_empty, &mutex);
        }
        

        c = dequeue();
        consumed_count++;
        printf("[CONSUMER] Consumed character '%c'\n", c);
        
        
        pthread_cond_signal(&buffer_not_full);
        pthread_mutex_unlock(&mutex);
        
        
        //usleep(150000);
    }
}

int main() {
    pthread_t producer_thread, consumer_thread;
    FILE *fp;
    
    

    if((fp = fopen("message.txt", "r")) == NULL) {
        printf("ERROR: can't open message.txt!\n");
        printf("Please create message.txt with a message inside.\n");
        return 1;
    }
    
    if(fgets(message, MAX_MSG, fp) == NULL) {
        printf("ERROR: can't read message!\n");
        fclose(fp);
        return 1;
    }
    
    fclose(fp);

    message_len = strlen(message);
    if(message[message_len - 1] == '\n') {
        message[message_len - 1] = '\0';
        message_len--;
    }
    
    printf("\nMessage to process: \"%s\" (length: %d characters)\n", message, message_len);
    printf("Buffer size: %d characters\n", BUFFER_SIZE);

    init_queue();
    pthread_mutex_init(&mutex, NULL);
    pthread_cond_init(&buffer_not_full, NULL);
    pthread_cond_init(&buffer_not_empty, NULL);
    
    pthread_create(&producer_thread, NULL, producer, NULL);
    pthread_create(&consumer_thread, NULL, consumer, NULL);
    
    pthread_join(producer_thread, NULL);
    pthread_join(consumer_thread, NULL);
    
    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&buffer_not_full);
    pthread_cond_destroy(&buffer_not_empty);
   
    
    return 0;
}