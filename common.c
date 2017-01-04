#include "args.h"

void showHelp()
{
	printf("command:\n");
	printf("\t-h: show command\n");
	printf("\t-create [db name] [field1,field2] [title field]: create db file and config file\n");
	printf("\t-rput [db name] [record] : put record into db\n");
	printf("\t-fput [db name] [record file] [record begin pattern]: read record file and put into db\n");
	printf("\t-rget [db name] [field=value] [start] [end]: get record by rid/key from db\n");
	printf("\t-rdel [db name] [rid]: delete record by rid\n");
	printf("\t-rupdate [db name] [rid] [record]: update record\n");
}
int readConfig(char *db, Conf *conf)
{
	//return pattern count
	FILE *fp;
	char fileName[40] = {'\0'}, line[2048] = {'\0'};
	char *ptr, *tok;
	int len = 0, patCnt = 0;

	sprintf(fileName, "./data/db/%s.conf", db);
	if(access(fileName, F_OK) != -1)
	{
		fp = fopen(fileName, "r");
		while(fgets(line, 2048, fp))
		{
			len = strlen(line);
			line[len-1] = '\0';
			ptr = line;
			if(strncmp(line, "dbName:", 7)==0)
			{
				ptr += 7;
				len = strlen(ptr);
				strncpy((*conf).dbName, ptr, len);
			}
			else if(strncmp(line, "createTime:", 11)==0)
			{
				ptr += 11;
				len = strlen(ptr);
				(*conf).createTime = (char *)malloc((len+1) * sizeof(char));
				memset((*conf).createTime, '\0', len+1);
				strncpy((*conf).createTime, ptr, len);
			}
			else if(strncmp(line, "fileSize:", 9)==0)
			{
				ptr += 9;
				(*conf).fileSize = atoi(ptr);
			}
			else if(strncmp(line, "maxBuffer:", 10)==0)
			{
				ptr += 10;
				(*conf).maxBuffer = atoi(ptr);
			}
			else if(strncmp(line, "patCnt:", 7)==0)
			{
				ptr += 7;
				(*conf).patCnt = atoi(ptr);
			}
			else if(strncmp(line, "field_pat:", 10)==0)
			{
				patCnt = 0;
				ptr += 10;
				tok = strtok(ptr, ",");
				while(tok != NULL)
				{
					strcpy((*conf).pat[patCnt], tok);
					patCnt++;
					tok = strtok(NULL, ",");
				}
			}
			else if(strncmp(line, "title_pat:", 10)==0)
			{
				ptr += 10;
				len = strlen(ptr);
				strncpy((*conf).titlePat, ptr, len);
			}
		}
		fclose(fp);
		return 1;
	}
	else
		return -1;
}

void writeConfig(Conf *conf)
{
	FILE *fp;
	char fileName[50] = {'\0'};
	int i;

	sprintf(fileName, "./data/db/%s.conf", (*conf).dbName);
	fp = fopen(fileName, "w+");
	fprintf(fp, "dbName:%s\n", (*conf).dbName);
	fprintf(fp, "createTime:%s\n", (*conf).createTime);
	fprintf(fp, "fileSize:%d\n", (*conf).fileSize);
	fprintf(fp, "maxBuffer:%d\n", (*conf).maxBuffer);
	fprintf(fp, "patCnt:%d\n", (*conf).patCnt);
	fprintf(fp, "field_pat:%s", (*conf).pat[0]);
	for(i = 1; i < (*conf).patCnt; i++)
		fprintf(fp, ",%s", (*conf).pat[i]);
	fprintf(fp, "\ntitle_pat:%s\n", (*conf).titlePat);
	fclose(fp);
}
void readInfo(char *db, INFO *info)
{
	FILE *fp;
	char fileName[40] = {'\0'}, line[2048] = {'\0'};
	char *ptr;
	int len = 0;

	sprintf(fileName, "./data/db/%s.info", db);
	if(access(fileName, F_OK) != -1)
	{
		fp = fopen(fileName, "r");
		while(fgets(line, 2048, fp))
		{
			len = strlen(line);
			line[len-1] = '\0';
			ptr = line;
			
			if(strncmp(line, "recCnt:", 7)==0)
			{
				ptr += 7;
				(*info).recCnt = atoi(ptr);
			}
			else if(strncmp(line, "currentFile:", 12)==0)
			{
				ptr += 12;
				(*info).curFile = atoi(ptr);
			}
		}
		fclose(fp);
	}
}
void writeInfo(INFO *info, Conf *conf)
{
	FILE *fp;
	char fileName[50] = {'\0'};

	sprintf(fileName, "./data/db/%s.info", (*conf).dbName);
	fp = fopen(fileName, "w+");
	fprintf(fp, "recCnt:%d\n", (*info).recCnt);
	fprintf(fp, "currentFile:%d\n", (*info).curFile);
	fclose(fp);
}

void readIndex(DATA *data[], Conf *config)
{
	FILE *fp_index;
	int count = 0;
	char line[1024], fileName[40] = {'\0'}, buf[1024] = {'\0'};
	char *idStart, *delStart, *fileStart, *offsetStart;

	sprintf(fileName, "./data/db/%s.index", (*config).dbName);
	fp_index = fopen(fileName, "r"); //index file
	while(fgets(line, 1024, fp_index))
	{
		idStart = line;
		while(*idStart != ':') //pointer to rid start
			idStart++;
		idStart++;
		
		delStart = idStart;
		while(*delStart != ',') //pointer to del start
			delStart++;
		delStart++;
		
		fileStart = delStart;
		while(*fileStart != ',') //pointer to file start
			fileStart++;
		fileStart++;

		offsetStart = fileStart;
		while(*offsetStart != ',') //pointer to offset start
			offsetStart++;
		offsetStart++;
		
		//rid
		strncpy(buf, idStart, (delStart - idStart)-1); //-1: ,
		(*data)[count].rid = atoi(buf);
		memset(buf, '\0', 1024);
		//del	
		strncpy(buf, delStart, (fileStart - delStart)-1); //-1: ,
		(*data)[count].del = atoi(buf);
		memset(buf, '\0', 1024);
		//fileID
		strncpy(buf, fileStart, (offsetStart - fileStart)-1); //-1: ,
		(*data)[count].fileID = atoi(buf);
		memset(buf, '\0', 1024);
		//offset
		(*data)[count].offset = atoi(offsetStart);

		count++;	
	}
	fclose(fp_index);
}

void writeIndex(DATA *data[], Conf *conf, INFO *info)
{
	FILE *fp;
	char fileName[50] = {'\0'};
	int i;

	sprintf(fileName, "./data/db/%s.index", (*conf).dbName);
	fp = fopen(fileName, "w+");
	for( i = 0; i < (*info).recCnt; i++)
	{
		 fprintf(fp, "@rid:%d,%d,%d,%d\n", (*data)[i].rid, (*data)[i].del, (*data)[i].fileID, (*data)[i].offset);
	}
	fclose(fp);
}
void sort(RES result[200], int total, int rid, int score)
{
	//sort result, order by score
	int i, end = 0, pos = 0;

	if(total > 200)
		end = 200;
	else
		end = total;

	if(score > result[end-1].score)
	{
		for(i = 0; i < end; i++)
		{
			if(score > result[i].score)
			{
				pos = i;
				break;
			}
		}
		for(i = end-1; i > pos; i--)
		{
			result[i].rid = result[i-1].rid;
			result[i].score = result[i-1].score;
		}
		result[pos].rid = rid;
		result[pos].score = score;
	}
}
