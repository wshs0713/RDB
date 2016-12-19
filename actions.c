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

					printf("Success to create config file of DB:%s\n", db);
					sprintf(str + strlen(str), "dbName:%s\n", db);
					sprintf(str + strlen(str), "createTime:%s", timeStr);
					sprintf(str + strlen(str), "recCnt:-1\n");
					sprintf(str + strlen(str), "fileSize:2048000000\n");
					sprintf(str + strlen(str), "currentFile:0\n");
					sprintf(str + strlen(str), "maxBuffer:655360\n");
					sprintf(str + strlen(str), "patCnt:%d\n", patCnt);
					sprintf(str + strlen(str), "field_pat:%s\n", field);
					sprintf(str + strlen(str), "title_pat:%s\n", title);
					write(openFile, str, strlen(str));
					close(openFile);
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

void rput(int RID, char *rec, Conf *config)
{
	FILE *fp, *fp_index;
	int i, j, dataLen = 0, patLen = 0, dataCnt = 0;
	int find = 0; //for checking record field
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
				rid = (*config).recCnt + 1;
			else //update
				rid = RID;
			sprintf(fileName, "./data/db/%s_%d", (*config).dbName, (*config).curFile);
			fp = fopen(fileName, "r");
			fseek(fp, 0, SEEK_END);
			offset = ftell(fp)+2;
			fclose(fp);
			printf("rid:%d\n", rid);
			printf("offset:%d\n", offset);
			if(offset > (*config).fileSize)
			{
				(*config).curFile++;
				offset = 2; //@\n
				printf("curFile:%d\n", (*config).curFile);
				printf("new offset:%d", offset);
			}
			if(RID == -1) //new record
			{
				fp_index = fopen(indexFile, "a");
				fprintf(fp_index, "@rid:%d,0,%d,%d\n", rid, (*config).curFile, offset);
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
			sprintf(fileName, "./data/db/%s_%d", (*config).dbName, (*config).curFile);
			fp = fopen(fileName, "a");
			fprintf(fp, "@\n@rid:%d\n", rid);
			for(i = 1; i < (*config).patCnt; i++)
			{
				find = 0;
				patLen = strlen((*config).pat[i]);
				for(j = 0; j < dataCnt; j++)
				{
					if(strncmp(data[j], (*config).pat[i], patLen) == 0)
					{
						find = 1;
						fprintf(fp, "%s\n", data[j]);
						break;
					}
				}
				if(find == 0)
				{
					fprintf(fp, "%s\n", (*config).pat[i]);
				}
			}
			if(RID == -1) //new record
			{
				(*config).recCnt++;
				writeConfig(config);
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

void fput(char *recFile, char *recBeg, Conf *config)
{
	FILE *fpr, *fpw, *fp_index;
	int MAX = (*config).maxBuffer+1;
	int i, patLen = 0;
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
			prevRID = (*config).recCnt;
			rid = (*config).recCnt + 1;
			sprintf(fileName, "./data/db/%s_%d", (*config).dbName, (*config).curFile);
			fpw = fopen(fileName, "r");
			fseek(fpw, 0, SEEK_END);
			offset = ftell(fpw);
			printf("rid:%d\n", rid);
			printf("offset:%d\n", offset);
			if(offset > (*config).fileSize)
			{
				(*config).curFile++;
				offset = 0;
				printf("curFile:%d\n", (*config).curFile);
				printf("new offset:%d", offset);
			}
			fclose(fpw);

			//read record file
			t_start = clock();
			sprintf(fileName, "./data/db/%s_%d", (*config).dbName, (*config).curFile);
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
						(*config).curFile++;
						offset = 2; //start of record @\n
						printf("curFile:%d\n", (*config).curFile);
						printf("new offset:%d", offset);
						fclose(fpw);
						sprintf(fileName, "./data/db/%s_%d", (*config).dbName, (*config).curFile);
						fpw = fopen(fileName, "a");
					}
					sprintf(writeBuf, "@\n@rid:%d\n", rid);
					fwrite(writeBuf, sizeof(char), strlen(writeBuf), fpw);

					//write to index file
					fprintf(fp_index, "@rid:%d,0,%d,%d\n", rid, (*config).curFile, offset);

					rid++;
					(*config).recCnt++;
					offset += strlen(writeBuf)-2; //@\n already added.
					memset(writeBuf, '\0', MAX);	
				}
				else
				{
					for(i = 0; i < (*config).patCnt; i++)
					{
						patLen = strlen((*config).pat[i]);
						if(strncmp(ptr, (*config).pat[i], patLen) == 0)
						{
							ptr += patLen; //ptr point to 'value'
							sprintf(writeBuf, "%s%s", (*config).pat[i], ptr);
							fwrite(writeBuf, sizeof(char), strlen(writeBuf), fpw);
							offset += strlen(writeBuf);

							memset(writeBuf, '\0', MAX);
							break;
						}
					}
				}
			}
			t_end = clock();
			take = (double)(t_end - t_start)/CLOCKS_PER_SEC;
			writeConfig(config);
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

void rget(char *field, char *val, int start, int end, Conf *config)
{
	FILE *fp;
	int MAX = (*config).maxBuffer+1;
	int i, value = 0, len = 0, find = 0, total = 0;
	char fileName[40] = {'\0'}, indexFile[50] = {'\0'};
	char *line, buf[1000] = {'\0'}; //buf: temporally store field name of each line
	char *ptr, *valPtr, *findPtr;
	clock_t t_start, t_end;
	double take = 0;
	int result[200];
	DATA *data;

	data = (DATA *)malloc(((*config).recCnt + 5)*sizeof(DATA));
	
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
	if((strlen(field) != 0) && (strcmp(field, "rid") == 0)) //get rid
	{
		t_start = clock();
		value = atoi(val);
		if(value <= (*config).recCnt)
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
				
					ptr = line;
					ptr++; //skip '@'
					valPtr = ptr;
					while((*valPtr != ':') && (*ptr != '\0'))
						valPtr++;
					valPtr++; //skip ':'
					memset(buf, '\0', 1000);
					strncpy(buf, ptr, (valPtr-ptr-1)); //-1: ':'
					
					printf("\"%s\":\"%s\"", buf,valPtr);
					if(strstr(line, "@B:") == NULL)
						printf(",");
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
	else
	{
		t_start = clock();
		//read rdb file, use rgrep/strstr
		//for(i = 0; i <= (*config).curFile; i++)
		sprintf(fileName, "./data/db/%s_%d", (*config).dbName, data[0].fileID);
		fp = fopen(fileName, "r");
		for( i = 0; i <= (*config).recCnt; i++)
		{
			fseek(fp, data[i].offset, SEEK_SET);
			find = 0;
			findPtr = NULL;
			while(fgets(line, MAX-1, fp))
			{
				if(strcmp(line, "@\n") == 0) //record begin in rdb file
				{
					if(find == 1)
					{
						if((total >= start) && (total < end))
						{
							result[total-start] = data[i].rid;
						}
						total++;
					}
					break;
				}
				/*else if(strncmp(line, "@rid:", 5) == 0)
				{
					ptr = line;
					ptr += 5;
					rid = atoi(ptr);
				}*/

				ptr = line;
				ptr++; //skip @
				if(strlen(field) == 0) //full text search
				{
					findPtr = strstr(line, val);
				}
				else
				{
					if(strncmp(ptr, field, strlen(field)) == 0)
					{
						ptr += strlen(field)+1; //skip field:
						findPtr = strstr(line, val);
					}
				}
				if((findPtr != NULL) && (data[i].del != 1))
				{
					find = 1;
				}
			}
			if( i < (*config).recCnt)
			{
				if(data[i+1].fileID != data[i].fileID)
				{
					fclose(fp);
					sprintf(fileName, "./data/db/%s_%d", (*config).dbName, data[i+1].fileID);
					fp = fopen(fileName, "r");
				}
			}
		}
		fclose(fp);

		if(end > total)
			end = total;
		
		printf("{\"result\":[");
		if(total > 0)
		{
			sprintf(fileName, "./data/db/%s_%d", (*config).dbName, data[result[0]].fileID);
			fp = fopen(fileName, "r");
			if(fp)
			{
				for(i = 0; i < (end-start); i++)
				{
					fseek(fp, data[result[i]].offset, SEEK_SET);
					printf("{");
					while(fgets(line, MAX-1, fp))
					{
						if(strcmp(line, "@\n") == 0)
							break;

						len = strlen(line);
						line[len-1] = '\0';
				
						ptr = line;
						ptr++;
						valPtr = line; //get value
						while((*valPtr != ':') && (*valPtr != '\0'))
							valPtr++;
						valPtr++; //skip ':'
								
						memset(buf, '\0', 1000);
						strncpy(buf, ptr, (valPtr-ptr-1)); //-1: ':', field name
						printf("\"%s\":\"%s\"", buf, valPtr);

						if(strstr(line, "@B:") == NULL)
							printf(",");

						if(data[result[i+1]].fileID != data[result[i]].fileID)
						{
							fclose(fp);
							sprintf(fileName, "./data/db/%s_%d", (*config).dbName, data[result[i+1]].fileID);
							fp = fopen(fileName, "r");
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

void rdel(int rid, Conf *config)
{
	clock_t t_start, t_end;
	double take = 0;
	DATA *data;
	data = (DATA *)malloc(((*config).recCnt + 5)*sizeof(DATA));
	
	t_start = clock();
	readIndex(&data, config);
	if(rid <= (*config).recCnt)
	{
		data[rid].del = 1;
		writeIndex(&data, config);
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

void rupdate(int rid, char *rec, Conf *config)
{
	FILE *fp;
	int offset = 0;
	char fileName[40] = {'\0'};
	clock_t t_start, t_end;
	double take = 0;
	DATA *data;
	
	if((rid >= 0) && (rid <= (*config).recCnt))
	{
	//	rdel(rid, config);
		t_start = clock();
		data = (DATA *)malloc(((*config).recCnt + 5)*sizeof(DATA));
		readIndex(&data, config);
	
		//update offset
		sprintf(fileName, "./data/db/%s_%d", (*config).dbName, (*config).curFile);
		fp = fopen(fileName, "r");
		fseek(fp, 0, SEEK_END);
		offset = ftell(fp)+2;
		fclose(fp);
		if(offset > (*config).fileSize)
		{
			(*config).curFile++;
			offset = 2;
			writeConfig(config);
		}
		data[rid].del = 0;
		data[rid].fileID = (*config).curFile;
		data[rid].offset = offset;
		writeIndex(&data, config);
		
		//put rec
		rput(rid, rec, config);
		
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
