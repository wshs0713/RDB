CC = gcc

all: main.c
	$(CC) -g -Wall -Werror -Wextra -o rdb main.c common.c actions.c args.h
