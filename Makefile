CC = gcc
CFLAGS = -W -Wall -Werror

# Default target
all: clean build run

# Target to clean up build files
clean:
	rm -rf mapreduce

# Target to compile the code
build: mapreduce.c main.c
	$(CC) $(CFLAGS) mapreduce.c main.c -o mapreduce

# Target to run the executable
run: build
	./mapreduce text.txt
