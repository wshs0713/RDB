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
		//verify db name
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
		//verify field name
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
		if(flag == 1) //invalid
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
	int i, j, dataLen = 0, dataCnt = 0;
	int rid = 0, offset = 0;
	char fileName[40] = {'\0'}, indexFile[40] = {'\0'};
	char *data[50], *tok;

	dataLen = strlen(rec);
	if(dataLen < (*config).maxBuffer)
	{
		sprintf(indexFile, "./data/db/%s.index", (*config).dbName);
		if(access(indexFile, F_OK) != -1)
		{
			if(RID == -1)					//put new record
				rid = (*info).recCnt + 1;	//get current rid
			else							//update
				rid = RID;

			//get current offset
			sprintf(fileName, "./data/db/%s_%d", (*config).dbName, (*info).curFile);
			fp = fopen(fileName, "r");
			fseek(fp, 0, SEEK_END);
			offset = ftell(fp)+2; //+2: @\n
			fclose(fp);
			printf("rid:%d\n", rid);
			printf("offset:%d\n", offset);

			//exceed the file size limit, update to db.info
			if(offset > (*config).fileSize)
			{
				(*info).curFile++;
				offset = 2; //@\n
				printf("curFile:%d\n", (*info).curFile);
				printf("new offset:%d", offset);
				writeInfo(info, config);
			}

			//new record, update to db.index and db.info
			if(RID == -1)
			{
				fp_index = fopen(indexFile, "a");
				fprintf(fp_index, "@rid:%d,0,%d,%d\n", rid, (*info).curFile, offset);
				fclose(fp_index);
				
				(*info).recCnt++;
				writeInfo(info, config);
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
			fclose(fp);

			printf("Success to put record into %s\n", (*config).dbName);
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
		fpr = fopen(recFile, "r");
		if(!fpr)
		{
			printf("record file not exist\n");
		}
		else
		{
			//get current rid
			prevRID = (*info).recCnt;
			rid = (*info).recCnt + 1;

			//get current offset
			sprintf(fileName, "./data/db/%s_%d", (*config).dbName, (*info).curFile);
			fpw = fopen(fileName, "r");
			fseek(fpw, 0, SEEK_END);
			offset = ftell(fpw);
			printf("rid:%d\n", rid);
			printf("offset:%d\n", offset);
			
			//exceed the file size limit, change current file
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

					//exceed the file size limit, change current file
					if(offset > (*config).fileSize)
					{
						(*info).curFile++;
						offset = 2; //start of record: @\n
						printf("curFile:%d\n", (*info).curFile);
						printf("new offset:%d", offset);

						//open new db file
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

			//update db.info
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
	int find = 0, findMust = 0, total = 0, step = 0, score = 0;
	int rid = 0, offset = 0, recOffset = 0;
	int flag_or = 0, flag_must = 0, flag_not = 0, flag_ignore = 0;
	char fileName[40] = {'\0'}, indexFile[50] = {'\0'};
	char *line, buf[1000] = {'\0'}; //buf: temporally store field name of each line
	char *ptr, *valPtr, *findPtr;
	char key[256] = {'\0'}, or[256] = {'\0'}, must[256] = {'\0'}, mustNot[256] = {'\0'};
	clock_t t_start, t_end;
	double take = 0;
	RES result[200];
	INDEX *index;

	index = (INDEX *)malloc(((*info).recCnt + 5)*sizeof(INDEX));
	readIndex(&index, config);
	
	line = (char *)malloc(MAX * sizeof(char));
	memset(line, '\0', MAX);
	
	//initialize
	for(i = 0; i < 200; i++)
	{
		result[i].rid = -1;
		result[i].score = 0;
	}
	
	sprintf(indexFile, "./data/db/%s.index", (*config).dbName);
	if(strcmp(field, "rid") == 0) //get rid
	{
		t_start = clock();
		value = atoi(val);
		if((value >= 0) && (value <= (*info).recCnt))
		{
			if(index[value].del != 1)
			{
				//get record
				sprintf(fileName, "./data/db/%s_%d", (*config).dbName, index[value].fileID);
				fp = fopen(fileName, "r");
				fseek(fp, index[value].offset, SEEK_SET);
			
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
						ptr++;	//skip '@'
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
				printf("}],");
				printf("\"total\":1,");
				fclose(fp);
			}
			else
			{
				printf("{\"result\":[],");
				printf("\"total\":0,");
			}
		}
		else
		{
			printf("{\"result\":[],");
			printf("\"total\":0,");
		}
		t_end = clock();
		take = (double)(t_end - t_start)/CLOCKS_PER_SEC;
		printf("\"time\":%.3lf", take);
		printf("}\n");
	}
	else  //not get rid
	{
		/** parse value **/
		/* 
			boolean search
			or: ,
			must: ^
			must not: !
		*/
		i = 0;
		ptr = val;
		while((*ptr != ',') && (*ptr != '^') && (*ptr != '!') && (*ptr != '\0'))
		{
			key[i] = *ptr;
			i++;
			ptr++;
		}
		if(strstr(val, ",") != NULL)
		{
			flag_or = 1;
			findPtr = strstr(val, ","); //point to ','
			findPtr++;
			i = 0;
			while((*findPtr != ',') && (*findPtr != '^') && (*findPtr != '!') && (*findPtr != '\0'))
			{
				or[i] = *findPtr;
				i++;
				findPtr++;
			}
		}
		if(strstr(val, "^") != NULL)
		{
			flag_must = 1;
			findPtr = strstr(val, "^"); //point to '^'
			findPtr++;
			i = 0;
			while((*findPtr != ',') && (*findPtr != '^') && (*findPtr != '!') && (*findPtr != '\0'))
			{
				must[i] = *findPtr;
				i++;
				findPtr++;
			}
		}
		if(strstr(val, "!") != NULL)
		{
			flag_not = 1;
			findPtr = strstr(val, "!"); //point to '!'
			findPtr++;
			i = 0;
			while((*findPtr != ',') && (*findPtr != '^') && (*findPtr != '!') && (*findPtr != '\0'))
			{
				mustNot[i] = *findPtr;
				i++;
				findPtr++;
			}
		}
		/** end parse value **/
		
		t_start = clock();
		for( i = 0; i <= (*info).curFile; i++)
		{
			sprintf(fileName, "./data/db/%s_%d", (*config).dbName, i);
			fp = fopen(fileName, "r");
			offset = 0;
			find = 0;
			findPtr = NULL;
			while(fgets(line, MAX-1, fp))
			{
				offset += strlen(line);
				if(strcmp(line, "@\n") == 0)	//record begin in db file
				{
					if(flag_must == 1)			//must have keyword
					{
						if((find != 0) && (findMust != 0))
						{
							//add to result array of structure, order by score, save top 200 results
							sort(result, total, rid, score);
							total++;
						}
					}
					else if(find != 0)
					{
						//add to result array of structure, order by score, save top 200 results
						sort(result, total, rid, score);
						total++;
					}
					flag_ignore = 0;
					find = 0;
					findMust = 0;
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

				if((flag_ignore != 1) && (index[rid].del != 1) && (recOffset == index[rid].offset))
				{
					ptr = line;
					ptr++; //skip @
					if(strlen(field) == 0) //full text search
					{
						/*priority: ! > + > ,*/
						if(flag_not == 1)
						{
							//if find mustNot keyword, ignore this record
							findPtr = strstr(line, mustNot);
							if(findPtr != NULL)
							{
								findPtr = NULL;
								flag_ignore = 1;
								find = 0;
								findMust = 0;
								continue;
							}
						}
						if(flag_must == 1)
						{
							findPtr = strstr(line, must);
							if(findPtr != NULL)
								findMust++;
							while(findPtr != NULL)
							{
								score += step;
								findPtr += strlen(must);
								findPtr = strstr(findPtr, must);
							}
						}
						if(flag_or == 1)
						{
							findPtr = strstr(line, or);
							if(findPtr != NULL)
								find++;
							while(findPtr != NULL)
							{
								score += step;
								findPtr += strlen(or);
								findPtr = strstr(findPtr, or);
							}
						}
						findPtr = strstr(line, key);
						if(findPtr != NULL)
							find++;
						while(findPtr != NULL)
						{
							score += step;
							findPtr += strlen(key);
							findPtr = strstr(findPtr, key);
						}
					}
					else
					{
						//specific field search
						if(strncmp(ptr, field, strlen(field)) == 0)
						{
							ptr += strlen(field)+1; //skip field:
							findPtr = strstr(line, val);
							while(findPtr != NULL)
							{
								find = 1;
								score += step;
								findPtr += strlen(val);
								findPtr = strstr(findPtr, val);
							}
						}
					}
				}
			}
			//check last record
			if(flag_must == 1) //must have keyword
			{
				if((find != 0) && (findMust != 0))
				{
					//add to result array of structure, order by score, save top 200 results
					sort(result, total, rid, score);
					total++;
				}
			}
			else if(find != 0)
			{
				//add to result array of structure, order by score, save top 200 results
				sort(result, total, rid, score);
				total++;
			}
			fclose(fp);
		}//end of for loop, check all db file

		if(end > total)
			end = total;
		
		printf("{\"result\":[");
		if(total > 0)
		{
			sprintf(fileName, "./data/db/%s_%d", (*config).dbName, index[result[0].rid].fileID);
			fp = fopen(fileName, "r");
			if(fp)
			{
				for(i = 0; i < (end-start); i++)
				{
					fseek(fp, index[result[i].rid].offset, SEEK_SET);
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
						//if match field, output the result
						if(find == 1)
						{
							ptr = line;
							ptr++;

							//get value
							valPtr = line;
							while((*valPtr != ':') && (*valPtr != '\0'))
								valPtr++;
							valPtr++; //skip ':'
								
							memset(buf, '\0', 1000);
							strncpy(buf, ptr, (valPtr-ptr-1));			//-1: ':', field name
							printf("\"%s\":\"%s\"", buf, valPtr);		//buf is field name

							ptr = (*config).pat[(*config).patCnt-1];	//record end pattern
							if(strstr(line, ptr) == NULL)				//not end of record
								printf(",");
						}
						if(i < end-start-1)
						{
							if(index[result[i+1].rid].fileID != index[result[i].rid].fileID)
							{
								fclose(fp);
								sprintf(fileName, "./data/db/%s_%d", (*config).dbName, index[result[i+1].rid].fileID);
								fp = fopen(fileName, "r");
							}
						}
					}
					printf("}");
					if(i < end-start-1) //not the last one result
						printf(",");
				} //end for
			} //end if(fp)
			fclose(fp);
		}
		printf("],");
		t_end = clock();
		take = (double)(t_end - t_start)/CLOCKS_PER_SEC;
		printf("\"total\":%d,", total);
		printf("\"time\":%.3lf", take);
		printf("}\n");
		free(line);
		free(index);
	}
}

void rdel(int rid, Conf *config, INFO *info)
{
	clock_t t_start, t_end;
	double take = 0;
	INDEX *index;
	index = (INDEX *)malloc(((*info).recCnt + 5)*sizeof(INDEX));
	
	t_start = clock();
	readIndex(&index, config);
	if((rid >= 0) && (rid <= (*info).recCnt))
	{
		index[rid].del = 1;
		writeIndex(&index, config, info);
	}
	else
	{
		printf("record id not exist\n");
	}
	t_end = clock();
	take = (double)(t_end - t_start)/CLOCKS_PER_SEC;
	printf("@Time:%.3lf\n", take);
	free(index);
}

void rupdate(int rid, char *rec, Conf *config, INFO *info)
{
	FILE *fp;
	int offset = 0;
	char fileName[40] = {'\0'};
	clock_t t_start, t_end;
	double take = 0;
	INDEX *index;
	
	if((rid >= 0) && (rid <= (*info).recCnt))
	{
		t_start = clock();
		index = (INDEX *)malloc(((*info).recCnt + 5)*sizeof(INDEX));
		readIndex(&index, config);
	
		//update offset
		sprintf(fileName, "./data/db/%s_%d", (*config).dbName, (*info).curFile);
		fp = fopen(fileName, "r");
		fseek(fp, 0, SEEK_END);
		offset = ftell(fp)+2; //+2: @\n
		fclose(fp);
		
		//exceed the file size limit, update to db.info
		if(offset > (*config).fileSize)
		{
			(*info).curFile++;
			offset = 2;
			writeInfo(info, config);
		}
		index[rid].del = 0;
		index[rid].fileID = (*info).curFile;
		index[rid].offset = offset;
		writeIndex(&index, config, info);
		
		//put record
		rput(rid, rec, config, info);
		
		t_end = clock();
		take = (double)(t_end - t_start)/CLOCKS_PER_SEC;
		printf("@Time:%.3lf\n", take);
		free(index);
	}
	else
	{
		printf("rid not exist\n");
	}
}
