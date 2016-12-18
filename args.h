#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>

typedef struct Config
{
	char dbName[20];
	char *createTime;
	int fileSize;
	int curFile;
	int maxBuffer;
	int patCnt;
	char pat[50][20]; //field pattern
	char titlePat[20];
} Conf;

typedef struct Result
{
	int rid;
	int del;
	int score;
	char url[500];
	char *title;
	char *content;
} RES;

void showHelp();
void writeConfing(Conf *config);
int readConfig(char *db, Conf *config);
void getPrev(int *prevRID, int *prevOffset, Conf *config);
void createDB(char *db, char *field, char *title);
void rput(char *rec, Conf *config);
void fput(char *recFile, char *recBeg, Conf *config);
void rget(char *field, char *val, int start, int end, Conf *config);
void rdel(char *id, Conf *config);
void rupdate(char *id, char *rec, Conf *config);

