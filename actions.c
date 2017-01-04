#include "args.h"

void createDB(char *db, char *field, char *title)
{
	int flag = 0, openFile = 0, len = 0, patCnt = 0;
	char *ptr, *tok;
	char fileName[40] = {'\0'}, str[65536] = {'\0'}, buf[1000] = {'\0'};
	time_t current_time;
	char *timeStr;

	//get current time
	current_time = time(NULL);
	timeStr = ctime(&current_time);

	len = strlen(db);
	if(len > 30)
	{
		printf("DB name is too long.\n");
	}
	else
	{
		ptr = db;
		while(*ptr != '\0')
		{
			if(!isalpha(*ptr) && !isdigit(*ptr))
			{
				flag = 1;
				break;
			}
			ptr++;
		}
		ptr = field;
		while(*ptr != '\0')
		{
			if(!isalpha(*ptr) && !isdigit(*ptr) && (*ptr != '_') && (*ptr != '-') && (*ptr != ',') && (*ptr != '@') && (*ptr != ':'))
			{
				flag = 1;
				break;
			}
			ptr++;
		}
		if(flag == 1)
		{
			printf("Invalid DB name or field name!\n");
			printf("DB name allow to contain A~Z, a~z and 0~9\n");
			printf("field name allow to contain A~Z, a~z, 0~9, '-' and '_'\n");
			return;
		}
		else
		{
			sprintf(fileName, "./data/db/%s_0", db);
			if(access(fileName, F_OK) == -1) //check DB not exist 
			{
				//create db file
				openFile = open(fileName, O_CREAT | O_WRONLY, 0644);
				if(openFile != -1)
				{
					printf("Success to create DB:%s\n", db);
					close(openFile);
				}
				else
				{
					printf("Fail to create DB:%s\n", db);
					return;
				}

				//create index file
				sprintf(fileName, "./data/db/%s.index", db);
				openFile = open(fileName, O_CREAT | O_WRONLY, 0644);
				if(openFile != -1)
				{
					printf("Success to create index file of DB:%s\n", db);
					close(openFile);
				}
				else
				{
					printf("Fail to create index file of DB:%s\n", db);
					return;
				}

				//create info file
				sprintf(fileName, "./data/db/%s.info", db);
				openFile = open(fileName, O_CREAT | O_WRONLY, 0644);
				if(openFile != -1)
				{
					sprintf(str, "recCnt:-1\n");
					sprintf(str + strlen(str), "currentFile:0\n");
					write(openFile, str, strlen(str));
					close(openFile);
					printf("Success to create info file of DB:%s\n", db);
				}
				else
				{
					printf("Fail to create info file of DB:%s\n", db);
					return;
				}

				//create config file
				sprintf(fileName, "./data/db/%s.conf", db);
				openFile = open(fileName, O_CREAT | O_WRONLY, 0644);
				if(openFile != -1)
				{
					strncpy(buf, field, strlen(field));
					tok = strtok(buf, ",");
					while(tok != NULL)
					{
						patCnt++;
						tok = strtok(NULL, ",");
					}
					sprintf(str, "dbName:%s\n", db);
					sprintf(str + strlen(str), "createTime:%s", timeStr);
					sprintf(str + strlen(str), "fileSize:2048000000\n");
					sprintf(str + strlen(str), "maxBuffer:655360\n");
					sprintf(str + strlen(str), "patCnt:%d\n", patCnt);
					sprintf(str + strlen(str), "field_pat:%s\n", field);
					sprintf(str + strlen(str), "title_pat:%s\n", title);
					write(openFile, str, strlen(str));
					close(openFile);
					printf("Success to create config file of DB:%s\n", db);
				}
				else
				{
					printf("Fail to create config file of DB:%s\n", db);
				}
			}
			else
			{
				printf("DB:%s exist!\n", db);
				printf("Fail to create DB:%s\n", db);
			}
		}
	}
}

void rput(int RID, char *rec, Conf *config, INFO *info)
{
	FILE *fp, *fp_index;
	int i, j, dataLen = 0, dataCnt = 0;// patLen = 0;
	int rid = 0, offset = 0;
	char fileName[40] = {'\0'}, indexFile[40] = {'\0'};
	char *data[50], *tok;

	dataLen = strlen(rec);
	if(dataLen < (*config).maxBuffer)
	{
		sprintf(indexFile, "./data/db/%s.index", (*config).dbName);
		if(access(indexFile, F_OK) != -1)
		{
			//get previous rid and offset
			if(RID == -1)
				rid = (*info).recCnt + 1;
			else //update
				rid = RID;
			sprintf(fileName, "./data/db/%s_%d", (*config).dbName, (*info).curFile);
			fp = fopen(fileName, "r");
			fseek(fp, 0, SEEK_END);
			offset = ftell(fp)+2;
			fclose(fp);
			printf("rid:%d\n", rid);
			printf("offset:%d\n", offset);
			if(offset > (*config).fileSize)
			{
				(*info).curFile++;
				offset = 2; //@\n
				printf("curFile:%d\n", (*info).curFile);
				printf("new offset:%d", offset);
				writeInfo(info, config);
			}
			if(RID == -1) //new record
			{
				fp_index = fopen(indexFile, "a");
				fprintf(fp_index, "@rid:%d,0,%d,%d\n", rid, (*info).curFile, offset);
				fclose(fp_index);
			}

			//parse input record
			tok = strtok(rec, "|");
			while(tok != NULL)
			{
				dataLen = strlen(tok);
				data[dataCnt] = (char *)malloc((dataLen+1) * sizeof(char));
				memset(data[dataCnt], '\0', dataLen+1);
				strcpy(data[dataCnt], tok);

				dataCnt++;
				tok = strtok(NULL, "|");
			}

			//open rdb file and write
			sprintf(fileName, "./data/db/%s_%d", (*config).dbName, (*info).curFile);
			fp = fopen(fileName, "a");
			fprintf(fp, "@\n@rid:%d\n", rid);
			
			for(j = 0; j< dataCnt; j++)
				fprintf(fp, "%s\n", data[j]);
			
			if(RID == -1) //new record
			{
				(*info).recCnt++;
				writeInfo(info, config);
			}
			printf("Success to put record into %s\n", (*config).dbName);
			fclose(fp);
			for(i = 0; i < dataCnt; i++)
				free(data[i]);
		}
		else
		{
			printf("index file not exist!\n");
		}
	}
	else
	{
		printf("record length is too long.");
	}
}

void fput(char *recFile, char *recBeg, Conf *config, INFO *info)
{
	FILE *fpr, *fpw, *fp_index;
	int MAX = (*config).maxBuffer+1;
	int prevRID = 0, rid = 0, offset = 0;
	char fileName[40] = {'\0'}, indexFile[50] = {'\0'};
	char *line, *writeBuf, *ptr;
	clock_t t_start, t_end;
	double take = 0;

	line = (char *)malloc(MAX * sizeof(char));
	memset(line, '\0', MAX);
	writeBuf = (char *)malloc(MAX * sizeof(char));
	memset(writeBuf, '\0', MAX);
	
	sprintf(fileName, "./data/db/%s.conf", (*config).dbName);
	sprintf(indexFile, "./data/db/%s.index", (*config).dbName);

	if(access(fileName, F_OK) != -1)
	{
		fpr = fopen(recFile, "r"); //record file
		if(!fpr)
		{
			printf("record file not exist\n");
		}
		else
		{
			prevRID = (*info).recCnt;
			rid = (*info).recCnt + 1;
			sprintf(fileName, "./data/db/%s_%d", (*config).dbName, (*info).curFile);
			fpw = fopen(fileName, "r");
			fseek(fpw, 0, SEEK_END);
			offset = ftell(fpw);
			printf("rid:%d\n", rid);
			printf("offset:%d\n", offset);
			if(offset > (*config).fileSize)
			{
				(*info).curFile++;
				offset = 0;
				printf("curFile:%d\n", (*info).curFile);
				printf("new offset:%d", offset);
			}
			fclose(fpw);

			//read record file
			t_start = clock();
			sprintf(fileName, "./data/db/%s_%d", (*config).dbName, (*info).curFile);
			fpw = fopen(fileName, "a");
			fp_index = fopen(indexFile, "a");
			while(fgets(line, MAX-1, fpr))
			{
				ptr = line;
				if(strncmp(line, recBeg, strlen(recBeg)) == 0)
				{
					offset += 2; //start of record @\n
					if(offset > (*config).fileSize)
					{
						(*info).curFile++;
						offset = 2; //start of record @\n
						printf("curFile:%d\n", (*info).curFile);
						printf("new offset:%d", offset);
						fclose(fpw);
						sprintf(fileName, "./data/db/%s_%d", (*config).dbName, (*info).curFile);
						fpw = fopen(fileName, "a");
					}
					sprintf(writeBuf, "@\n@rid:%d\n", rid);
					fwrite(writeBuf, sizeof(char), strlen(writeBuf), fpw);

					//write to index file
					fprintf(fp_index, "@rid:%d,0,%d,%d\n", rid, (*info).curFile, offset);

					rid++;
					(*info).recCnt++;
					offset += strlen(writeBuf)-2; //@\n already added.
					memset(writeBuf, '\0', MAX);	
				}
				else
				{
					fprintf(fpw, "%s", ptr);
					offset += strlen(ptr);
				}
			}
			t_end = clock();
			take = (double)(t_end - t_start)/CLOCKS_PER_SEC;
			writeInfo(info, config);
			printf("Success to put file into %s\n", (*config).dbName);
			printf("Total record: %d\n", (rid-prevRID-1));
			printf("Take %.3lf seconds\n", take);
			fclose(fpr);
			fclose(fpw);
			fclose(fp_index);
		}
	}
	else
	{
		printf("DB not exist!\n");
	}
	free(line);
	free(writeBuf);
}

void rget(char *field, char *val, int start, int end, Conf *config, INFO *info)
{
	FILE *fp;
	int MAX = (*config).maxBuffer+1;
	int i, j, value = 0, len = 0, patLen = 0;
	int find = 0, total = 0, step = 0, score = 0;
	int rid = 0, offset = 0, recOffset = 0;
	int flag_must = 0, flag_not = 0, flag_ignore = 0;
	char fileName[40] = {'\0'}, indexFile[50] = {'\0'};
	char *line, buf[1000] = {'\0'}; //buf: temporally store field name of each line
	char *ptr, *valPtr, *findPtr;
	//char and[256] = {'\0'}, or[256] = {'\0'};
	char key[256] = {'\0'}, must[256] = {'\0'}, mustNot[256] = {'\0'};
	clock_t t_start, t_end;
	double take = 0;
	RES result[200];
	DATA *data;

	data = (DATA *)malloc(((*info).recCnt + 5)*sizeof(DATA));
	//initialize
	for(i = 0; i < 200; i++)
	{
		result[i].rid = -1;
		result[i].score = 0;
	}
	
	t_start = clock();
	readIndex(&data, config);
	t_end = clock();
	take = (double)(t_end - t_start)/CLOCKS_PER_SEC;
	//printf("Time:%.3lf\n", take);
	/*for(i = 0; i <= (*config).recCnt; i++)
	{
		printf("@rid:%d,%d,%d,%d\n", data[i].rid, data[i].del, data[i].fileID, data[i].offset);
	}*/

	line = (char *)malloc(MAX * sizeof(char));
	memset(line, '\0', MAX);

	sprintf(indexFile, "./data/db/%s.index", (*config).dbName);
	/* boolean search
		and: &
		or: ,
		must: +
		must not: !
	*/
	i = 0;
	ptr = val;
	while((*ptr != '&') && (*ptr != ',') && (*ptr != '+') && (*ptr != '!') && (*ptr != '\0'))
	{
		key[i] = *ptr;
		i++;
		ptr++;
	}
	printf("key:%s\n", key);
	if(strstr(val, "+") != NULL)
	{
		flag_must = 1;
		findPtr = strstr(val, "+"); //point to '+'
		findPtr++;
		i = 0;
		while((*findPtr != '&') && (*findPtr != ',') && (*findPtr != '+') && (*findPtr != '!') && (*findPtr != '\0'))
		{
			must[i] = *findPtr;
			i++;
			findPtr++;
		}
		printf("must:%s\n", must);
	}
	if(strstr(val, "!") != NULL)
	{
		flag_not = 1;
		findPtr = strstr(val, "!"); //point to '!'
		findPtr++;
		i = 0;
		while((*findPtr != '&') && (*findPtr != ',') && (*findPtr != '+') && (*findPtr != '!') && (*findPtr != '\0'))
		{
			mustNot[i] = *findPtr;
			i++;
			findPtr++;
		}
		printf("must not:%s\n", mustNot);
	}
	if(strcmp(field, "rid") == 0) //get rid
	{
		t_start = clock();
		value = atoi(val);
		if((value >= 0) && (value <= (*info).recCnt))
		{
			if(data[value].del != 1)
			{
				//get record
				sprintf(fileName, "./data/db/%s_%d", (*config).dbName, data[value].fileID);
				fp = fopen(fileName, "r");
				fseek(fp, data[value].offset, SEEK_SET);
			
				printf("{\"result\":[{");
				while(fgets(line, MAX-1, fp))
				{
					if(strcmp(line, "@\n") == 0)
						break;
	
					len = strlen(line);
					line[len-1] = '\0'; //replace '\n'
					find = 0;

					//output specific field(set in config file)
					for(j = 0; j < (*config).patCnt; j++)
					{
						patLen = strlen((*config).pat[j]);
						if(strncmp(line, (*config).pat[j], patLen) == 0)
						{
							find = 1;
							break;
						}
					}
					if(find == 1)
					{
						ptr = line;
						ptr++; //skip '@'
						valPtr = ptr;
						while((*valPtr != ':') && (*ptr != '\0'))
							valPtr++;
						valPtr++; //skip ':'
						memset(buf, '\0', 1000);
						strncpy(buf, ptr, (valPtr-ptr-1)); //-1: ':'
					
						printf("\"%s\":\"%s\"", buf,valPtr);
						ptr = (*config).pat[(*config).patCnt-1]; //record end pattern
						if(strstr(line, ptr) == NULL)
							printf(",");
					}
				}
				printf("}]}\n");
				printf("@Total:1\n");
				fclose(fp);
			}
			else
			{
				printf("{\"result\":[]}\n");
				printf("@Total:0\n");
			}
		}
		else
		{
			printf("{\"result\":[]}\n");
			printf("@Total:0\n");
		}
		t_end = clock();
		take = (double)(t_end - t_start)/CLOCKS_PER_SEC;
		printf("@Time:%.3lf\n", take);
	}
	else  //not get rid
	{
		t_start = clock();
		//read rdb file, use rgrep/strstr
		for( i = 0; i <= (*info).curFile; i++)
		{
			sprintf(fileName, "./data/db/%s_%d", (*config).dbName, i);
			fp = fopen(fileName, "r");
			find = 0;
			findPtr = NULL;
			offset = 0;
			while(fgets(line, MAX-1, fp))
			{
				offset += strlen(line);
				if(strcmp(line, "@\n") == 0) //record begin in rdb file
				{
					if(find == 1)
					{
						sort(result, total, rid, score); //result structure order by score, save top 200 results
						/*if((total >= start) && (total < end))
						{
							result[total-start] = rid;
						}*/
						total++;
					}
					flag_ignore = 0;
					find = 0;
					score = 0;
					findPtr = NULL;
					recOffset = offset;
					continue;
				}
				else if(strncmp(line, "@rid:", 5) == 0)
				{
					ptr = line;
					ptr += 5;
					rid = atoi(ptr);
					continue;
				}
				if(strncmp(line, (*config).titlePat, strlen((*config).titlePat)) == 0)
				{
					step = 100;
				}
				else
				{
					step = 10;
				}

				if((flag_ignore != 1) && (data[rid].del != 1) && (recOffset == data[rid].offset))
				{
					ptr = line;
					ptr++; //skip @
					if(strlen(field) == 0) //full text search
					{
						if(flag_not == 1)
						{
							findPtr = strstr(line, mustNot);
							if(findPtr != NULL)
							{
								findPtr = NULL;
								flag_ignore = 1; //ignore this record
								continue;
							}
						}
						if(flag_must == 1)
						{
							findPtr = strstr(line, must);
							while(findPtr != NULL)
							{
								find = 1;
								score += step;
								findPtr += strlen(must);
								findPtr = strstr(findPtr, must);
							}
						}
						findPtr = strstr(line, key);
						while(findPtr != NULL)
						{
							find = 1;
							score += step;
							findPtr += strlen(key);
							findPtr = strstr(findPtr, key);
						}
					}
					else
					{
						if(strncmp(ptr, field, strlen(field)) == 0)
						{
							ptr += strlen(field)+1; //skip field:
							findPtr = strstr(line, val);
							while(findPtr != NULL)
							{
								score += step;
								findPtr += strlen(val);
								findPtr = strstr(findPtr, val);
							}
						}
					}
				}
			}
			if(find == 1) //last record
			{
				sort(result, total, rid, score); //result structure order by score, save top 200 results
				total++;
				/*if((total >= start) && (total < end))
				{
					result[total-start] = rid;
				}
				total++;*/
			}
			fclose(fp);
		}

		if(end > total)
			end = total;
		
		printf("{\"result\":[");
		if(total > 0)
		{
			sprintf(fileName, "./data/db/%s_%d", (*config).dbName, data[result[0].rid].fileID);
			fp = fopen(fileName, "r");
			if(fp)
			{
				for(i = 0; i < (end-start); i++)
				{
					fseek(fp, data[result[i].rid].offset, SEEK_SET);
					printf("{");
					printf("\"score\":\"%d\",", result[i].score);
					while(fgets(line, MAX-1, fp))
					{
						if(strcmp(line, "@\n") == 0)
							break;

						len = strlen(line);
						line[len-1] = '\0';
						find = 0;
				
						//output specific field(set in config file)
						for(j = 0; j < (*config).patCnt; j++)
						{
							patLen = strlen((*config).pat[j]);
							if(strncmp(line, (*config).pat[j], patLen) == 0)
							{
								find = 1;
								break;
							}
						}
						if(find == 1)
						{
							ptr = line;
							ptr++;
							valPtr = line; //get value
							while((*valPtr != ':') && (*valPtr != '\0'))
								valPtr++;
							valPtr++; //skip ':'
								
							memset(buf, '\0', 1000);
							strncpy(buf, ptr, (valPtr-ptr-1)); //-1: ':', field name
							printf("\"%s\":\"%s\"", buf, valPtr);

							ptr = (*config).pat[(*config).patCnt-1]; //record end pattern
							if(strstr(line, ptr) == NULL)
								printf(",");
						}
						if(i < end-start-1)
						{
							if(data[result[i+1].rid].fileID != data[result[i].rid].fileID)
							{
								fclose(fp);
								sprintf(fileName, "./data/db/%s_%d", (*config).dbName, data[result[i+1].rid].fileID);
								fp = fopen(fileName, "r");
							}
						}
					}
					printf("}");
					if(i < end-start-1)
						printf(",");
				}
			}
			fclose(fp);
		}
		printf("]}\n");
		t_end = clock();
		take = (double)(t_end - t_start)/CLOCKS_PER_SEC;
		printf("@Total:%d\n", total);
		printf("@Time:%.3lf\n", take);
		free(line);
		free(data);
	}
}

void rdel(int rid, Conf *config, INFO *info)
{
	clock_t t_start, t_end;
	double take = 0;
	DATA *data;
	data = (DATA *)malloc(((*info).recCnt + 5)*sizeof(DATA));
	
	t_start = clock();
	readIndex(&data, config);
	if((rid >= 0) && (rid <= (*info).recCnt))
	{
		data[rid].del = 1;
		writeIndex(&data, config, info);
	}
	else
	{
		printf("record id not exist\n");
	}
	t_end = clock();
	take = (double)(t_end - t_start)/CLOCKS_PER_SEC;
	printf("@Time:%.3lf\n", take);
	free(data);
}

void rupdate(int rid, char *rec, Conf *config, INFO *info)
{
	FILE *fp;
	int offset = 0;
	char fileName[40] = {'\0'};
	clock_t t_start, t_end;
	double take = 0;
	DATA *data;
	
	if((rid >= 0) && (rid <= (*info).recCnt))
	{
		t_start = clock();
		data = (DATA *)malloc(((*info).recCnt + 5)*sizeof(DATA));
		readIndex(&data, config);
	
		//update offset
		sprintf(fileName, "./data/db/%s_%d", (*config).dbName, (*info).curFile);
		fp = fopen(fileName, "r");
		fseek(fp, 0, SEEK_END);
		offset = ftell(fp)+2;
		fclose(fp);
		if(offset > (*config).fileSize)
		{
			(*info).curFile++;
			offset = 2;
			writeInfo(info, config);
		}
		data[rid].del = 0;
		data[rid].fileID = (*info).curFile;
		data[rid].offset = offset;
		writeIndex(&data, config, info);
		
		//put rec
		rput(rid, rec, config, info);
		
		t_end = clock();
		take = (double)(t_end - t_start)/CLOCKS_PER_SEC;
		printf("@Time:%.3lf\n", take);
		free(data);
	}
	else
	{
		printf("rid not exist\n");
	}
}
