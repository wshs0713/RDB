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
	int recCnt;
	int fileSize;
	int curFile;
	int maxBuffer;
	int patCnt;
	char pat[50][20]; //field pattern
	char titlePat[20];
} Conf;

/*typedef struct Result
{
	int rid;
	int score;
	int fileID;
	int offset;
} RES;*/

typedef struct Data
{
	int rid;
	int del;
	int fileID;
	int offset;
} DATA;

void showHelp();
int readConfig(char *db, Conf *config);
void writeConfig(Conf *config);
void readIndex(DATA *data[], Conf *config);
void writeIndex(DATA *data[], Conf *config);
void createDB(char *db, char *field, char *title);
void rput(char *rec, Conf *config);
void fput(char *recFile, char *recBeg, Conf *config);
void rget(char *field, char *val, int start, int end, Conf *config);
void rdel(int rid, Conf *config);
void rupdate(int rid, char *rec, Conf *config);

