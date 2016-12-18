CC = gcc

all: main.c
	$(CC) -g -o rdb main.c common.c actions.c args.h
