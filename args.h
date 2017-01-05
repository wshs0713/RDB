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
	int maxBuffer;
	int patCnt;
	char pat[50][20]; //field pattern
	char titlePat[20];
} Conf;

typedef struct Info
{
	int recCnt;
	int curFile;
} INFO;

typedef struct Index
{
	int rid;
	int del;
	int fileID;
	int offset;
} INDEX;

typedef struct Result
{
	int rid;
	int score;
} RES;

void showHelp();
int readConfig(char *db, Conf *config);
void writeConfig(Conf *config);
void readInfo(char *db, INFO *info);
void writeInfo(INFO *info, Conf *config);
void readIndex(INDEX *index[], Conf *config);
void writeIndex(INDEX *index[], Conf *config, INFO *info);
void sort(RES result[200], int total, int rid, int score);
void createDB(char *db, char *field, char *title);
void rput(int RID, char *rec, Conf *config, INFO *info);
void fput(char *recFile, char *recBeg, Conf *config, INFO *info);
void rget(char *field, char *val, int start, int end, Conf *config, INFO *info);
void rdel(int rid, Conf *config, INFO *info);
void rupdate(int rid, char *rec, Conf *config, INFO *info);

