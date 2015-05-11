#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <sys/time.h>
#include <string.h>
#include <czmq.h>
#include <errno.h>

/*
 * number of workers
 * do not change this
 */
#define NUMBER_OF_WORKERS 8
#define BIND_ADDRESS "0.0.0.0"
#define PORT 5656

/* 
 * this struct will be passed to zthread workers.
 * mashine_number is a mashine identifier for each server in the cluster.
 * zUID cluster support 16 server for a cluster.
 * mashine_number starts from 0 to 15
 * 
 * thread_number is unique number for each worker
 * number of workers is fixed with NUMBER_OF_WORKERS starting from 0 to 7
 */
typedef struct _setting {
	long mashine_number;
	long thread_number;
} setting_t;

/* print usage of program */
void print_usage(FILE *output) {
    fprintf(output, "Usage: zuid -m [mashine number] -b [bind address] -p [port]\n");
}

void worker(void *args, zctx_t *ctx, void *pipe) {
	long mashine_number =  ((setting_t *) args)->mashine_number;
	long thread_number = ((setting_t *) args)->thread_number;	
	long auto_increment = 0;
    struct timeval tp;
	
	void *worker = zsocket_new(ctx, ZMQ_DEALER);
	zsocket_connect(worker, "inproc://zid");
	
	while (!zctx_interrupted) {
		zmsg_t *request = zmsg_recv(worker);
		
		/* drop message if its size is less than 2 */
		if (zmsg_size(request) != 2) {
			zmsg_destroy(&request);
			continue;
		}
		/* sender id */
		zframe_t *sender = zmsg_pop(request);
		
		/* number of id to generate */
		char *num = zmsg_popstr(request);
		int n = atoi(num);
		free(num);
		
		if (n > 0) {
		
			/* response message */
			zmsg_t *response = zmsg_new();
		
			int i;
			for (i = 0; i < n; i++) {
				gettimeofday(&tp, NULL);
			
				zmsg_addstrf(response, "%ld", ((mashine_number << 59) | (thread_number << 56) | (auto_increment << 45) | (tp.tv_sec << 10) | (tp.tv_usec / 1000)));
			
				auto_increment++;
				if (auto_increment == 2048) {
					auto_increment = 0;
				}
			}
		
			/* push sender id */
			zmsg_push(response, sender);
			/* send back reply */
			zmsg_send(&response, worker);
		} else {
			zframe_destroy(&sender);
		}
		
		zmsg_destroy(&request);
	}
}

int main(int argc, char** argv) {
	long mashine_number = -1;
	char *bind_address = NULL;
	long port = -1;
	int opt;
	char *endptr;
	
	/* parse command line options */
	while ((opt = getopt(argc, argv, "m:b:p:h")) != -1) {
		switch (opt) {
			case 'm':
				errno = 0;
				mashine_number = strtol(optarg, &endptr, 10);
				if ((errno != 0 && mashine_number == 0) || 
					(*endptr != '\0') ||
					(mashine_number < 0 || mashine_number > 15)) {
						
						fprintf(stderr, "invalid mashine number\n");
						return EXIT_FAILURE;
				}
				break;
			case 'b':
				bind_address = strndup(optarg, 45);
				break;
			case 'p':
				errno = 0;
				port = strtol(optarg, &endptr, 10);
				if ((errno != 0) || 
					(*endptr != '\0') ||
					(port <= 0 || port > 65535)) {
						
						fprintf(stderr, "invalid port number\n");
					
						return EXIT_FAILURE;
				}
				break;
			case 'h':
				print_usage(stdout);
				return EXIT_SUCCESS;
			case '?':
				print_usage(stderr);
				return EXIT_FAILURE;
			default:
				return EXIT_FAILURE;
		}
	}
	
	if (mashine_number == -1) {
		print_usage(stderr);
		
		return EXIT_FAILURE;
	}
	
	if (bind_address == NULL) {
		fprintf(stdout, "No bind address specified. Using default address: %s\n", BIND_ADDRESS);
		bind_address = BIND_ADDRESS;
	}
	
	if (port == -1) {
		fprintf(stdout, "No port number specified. Using default port number: %d\n", PORT);
		port = PORT;
	}
	
	/* create context object */
	zctx_t *ctx = zctx_new();
	
	/* create socket objects */
	void *server = zsocket_new(ctx, ZMQ_ROUTER);
	void *dispatcher = zsocket_new(ctx, ZMQ_DEALER);
	
	/* bind server/dispatcher socket */
	zsocket_bind(server, "tcp://%s:%ld", bind_address, port);
	zsocket_bind(dispatcher, "inproc://zid");
	
	/* create worker threads */
	setting_t *s;
	int i;
	for (i = 0; i < NUMBER_OF_WORKERS; i++) {
		s = (setting_t *) malloc(sizeof(setting_t));
		s->mashine_number = mashine_number;
		s->thread_number = i;
		
		zthread_fork(ctx, worker, s);
	}
	
	/* zmq_proxy runs in current thread */
	zmq_proxy(server, dispatcher, NULL);

	zctx_destroy (&ctx);
	
	return EXIT_SUCCESS;
}
