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
void readInfo(char *db, INFO *info);
void writeInfo(INFO *info, Conf *config);
void readIndex(DATA *data[], Conf *config);
void writeIndex(DATA *data[], Conf *config, INFO *info);
void createDB(char *db, char *field, char *title);
void rput(int RID, char *rec, Conf *config, INFO *info);
void fput(char *recFile, char *recBeg, Conf *config, INFO *info);
void rget(char *field, char *val, int start, int end, Conf *config, INFO *info);
void rdel(int rid, Conf *config, INFO *info);
void rupdate(int rid, char *rec, Conf *config, INFO *info);

