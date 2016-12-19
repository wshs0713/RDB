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

void getPrev(int *prevRID, int *prevOffset, Conf *config)
{
	FILE *fp_index, *fpr;
	int MAX = (*config).maxBuffer+1;
	int len = 0;
	char *line, fileName[40] = {'\0'};
	char buf[1001] = {'\0'}, buf2[1001] = {'\0'};
	char *idStart, *fileStart, *offsetStart;
	/*index file
		field:id,file,offset
		@rid:1,1,2345
	*/
	line = (char *)malloc(MAX * sizeof(char));
	memset(line, '\0', MAX);

	sprintf(fileName, "./data/db/%s_%d", (*config).dbName, (*config).curFile);
	fpr = fopen(fileName, "r"); //rdb file

	sprintf(fileName, "./data/db/%s.index", (*config).dbName);
	fp_index = fopen(fileName, "r"); //index file

	fseek(fp_index, -1000, SEEK_END); //file cursor to end
	while(fgets(buf, 1000, fp_index)); //read to last line

	idStart = buf;
	if(*idStart != '\0')
	{
		while(*idStart != ':') //pointer to rid start
			idStart++;
		idStart++;
		
		fileStart = idStart;
		while(*fileStart != ',') //pointer to file start
			fileStart++;
		fileStart++;

		offsetStart = fileStart;
		while(*offsetStart != ',') //pointer to offset start
			offsetStart++;
		offsetStart++;
		
		strncpy(buf2, idStart, (fileStart - idStart)-1); //-1: ,
		*prevRID = atoi(buf2);
		*prevOffset = atoi(offsetStart);

		fseek(fpr, *prevOffset, SEEK_SET);
		while(fgets(line, MAX-1, fpr))
		{
			len = strlen(line);
			*prevOffset += len;
		}
	}
	else
	{
		*prevRID = -1;
		*prevOffset = 0;
	}
	free(line);
	fclose(fpr);
	fclose(fp_index);
}

void rput(char *rec, Conf *config)
{
	FILE *fp, *fp_index;
	int i, j, dataLen = 0, patLen = 0, dataCnt = 0;
	int find = 0; //for checking record field
	int prevRID = 0, prevOffset = 0, rid = 0, offset = 0;
	char fileName[40] = {'\0'}, indexFile[40] = {'\0'};
	char *data[50], *tok;

	if(strlen(rec) < (*config).maxBuffer)
	{
		sprintf(indexFile, "./data/db/%s.index", (*config).dbName);
		if(access(indexFile, F_OK) != -1)
		{
			//get previous rid and offset
			getPrev(&prevRID, &prevOffset, config);
			rid = prevRID + 1;
			offset = prevOffset+2; //start of record: @\n
			printf("rid:%d\n", rid);
			printf("offset:%d\n", offset);
			if(offset > (*config).fileSize)
			{
				(*config).curFile++;
				offset = 2; //@\n
				printf("curFile:%d\n", (*config).curFile);
				printf("new offset:%d", offset);
				writeConfig(config);
			}
			fp_index = fopen(indexFile, "a");
			fprintf(fp_index, "@rid:%d,%d,%d\n", rid, (*config).curFile, offset);
			fclose(fp_index);

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
			fprintf(fp, "@\n@rid:%d\n@del:0\n", rid);
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
	int prevRID = 0, rid = 0, prevOffset = 0, offset = 0;
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
			//get previous rid and offset
			t_start = clock();
			getPrev(&prevRID, &prevOffset, config);
			t_end = clock();
			take = (double)(t_end - t_start)/CLOCKS_PER_SEC;
			printf("Take %.3lf seconds\n", take);

			rid = prevRID + 1;
			offset = prevOffset;
			printf("rid:%d\n", rid);
			printf("offset:%d\n", offset);
			if(offset > (*config).fileSize)
			{
				(*config).curFile++;
				offset = 0;
				printf("curFile:%d\n", (*config).curFile);
				printf("new offset:%d", offset);
				writeConfig(config);
			}

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
						writeConfig(config);
						fclose(fpw);
						sprintf(fileName, "./data/db/%s_%d", (*config).dbName, (*config).curFile);
						fpw = fopen(fileName, "a");
					}
					sprintf(writeBuf, "@\n@rid:%d\n@del:0\n", rid);
					fwrite(writeBuf, sizeof(char), strlen(writeBuf), fpw);

					//write to index file
					fprintf(fp_index, "@rid:%d,%d,%d\n", rid, (*config).curFile, offset);

					rid++;
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
	FILE *fp, *fp2;
	int MAX = (*config).maxBuffer+1;
	int i, len = 0, find = 0, del = 0, total = 0;
	int fileID = 0, offset = 0, recStart = 0;
	int score = 0, count = 0;
	char fileName[40] = {'\0'}, indexFile[50] = {'\0'};
	char *line, *line2, buf[1000] = {'\0'}; //buf: temporally store field name of each line
	char *ptr, *valPtr, *findPtr, *scorePtr, *filePtr;
	clock_t t_start, t_end;
	double take = 0;
	RES result[20];

	line = (char *)malloc(MAX * sizeof(char));
	memset(line, '\0', MAX);
	line2 = (char *)malloc(MAX * sizeof(char));
	memset(line2, '\0', MAX);
	for(i = 0; i < (end-start); i++)
	{
		result[i].title = (char *)malloc(MAX * sizeof(char));
		memset(result[i].title, '\0', MAX);
		result[i].content = (char *)malloc(MAX * sizeof(char));
		memset(result[i].content, '\0', MAX);
	}

	sprintf(indexFile, "./data/db/%s.index", (*config).dbName);
	if((strlen(field) != 0) && (strcmp(field, "rid") == 0)) //get rid
	{
		t_start = clock();
		//read index file
		fp = fopen(indexFile, "r");
		while(fgets(line, MAX-1, fp))
		{
			ptr = line;
			ptr += 5; //skip @rid:
			if(strncmp(val, ptr, strlen(val)) == 0)
			{
				find = 1;
				//index file: @rid:123,1,123123
				ptr += strlen(val)+1; //skip rid and ','
				filePtr = ptr;
				while(*ptr != ',')
					ptr++;
				ptr++; //skip ','
				strncpy(buf, filePtr, (ptr-filePtr-1)); //-1: ,
				fileID = atoi(buf);
				offset = atoi(ptr);
				break;
			}
		}
		fclose(fp);

		if(find == 1)
		{
			//get record
			sprintf(fileName, "./data/db/%s_%d", (*config).dbName, fileID);
			fp = fopen(fileName, "r");
			fseek(fp, offset, SEEK_SET);
			
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
		t_end = clock();
		take = (double)(t_end - t_start)/CLOCKS_PER_SEC;
		printf("@Time:%.3lf\n", take);
	}
	else
	{
		t_start = clock();
		//read rdb file, use rgrep/strstr
		for(i = 0; i <= (*config).curFile; i++)
		{
			sprintf(fileName, "./data/db/%s_%d", (*config).dbName, i);
			fp = fopen(fileName, "r");
			fp2 = fopen(fileName, "r"); //for seeking to result record
			offset = 0;
			find = 0;
			while(fgets(line, MAX-1, fp))
			{
				findPtr = NULL;
				if(strcmp(line, "@\n") == 0) //record begin in rdb file
				{
					if(find == 1)
					{
						//printf("total:%d\n", total);
						if((total >= start) && (total < end))
						{
							fseek(fp2, recStart, SEEK_SET);
							count = 0;
							result[total-start].score = 0;
							while(fgets(line2, MAX-1, fp2))
							{
								if(strcmp(line2, "@\n") == 0)
									break;
								
								len = strlen(line2);
								line2[len-1] = '\0'; //replace '\n'
								
								if(strncmp(line2, (*config).titlePat, strlen((*config).titlePat)) == 0)
									score = 100;
								else
									score = 10;
															
								/*computing score
								scorePtr = line2;
								while(strstr(scorePtr, val) != NULL)
								{
									count++;
									result[total-start].score += score;
									scorePtr += strlen(val);
								}*/
								//printf("count:%d\n", count);
								//printf("result[%d] score:%d\n", total-start, result[total-start].score);
				
								ptr = line2;
								ptr++; //skip '@'
								valPtr = line2; //get value
								while((*valPtr != ':') && (*valPtr != '\0'))
									valPtr++;
								valPtr++; //skip ':'
							
								memset(buf, '\0', 1000);
								strncpy(buf, ptr, (valPtr-ptr-1)); //-1: ':', field name

								switch(buf[0])
								{
									case 'r':
										result[total-start].rid = atoi(valPtr);
										break;
									case 'd':
										result[total-start].del = atoi(valPtr);
										break;
									case 'U':
										len = strlen(valPtr);
										strncpy(result[total-start].url, valPtr, len);
										break;
									case 'T':
										len = strlen(valPtr);
										strncpy(result[total-start].title, valPtr, len);
										break;
									case 'B':
										len = strlen(valPtr);
										strncpy(result[total-start].content, valPtr, len);
										break;
								}
							} //end while
						} //end if start <= total < end
						total++;
					}
					del = 0;
					find = 0;
					count = 0;
					recStart = offset+2; //skip @\n
				}
				else if(strncmp(line, "@del:", 5) == 0)
				{
					ptr = line;
					ptr += 5;
					del = atoi(ptr);
				}

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
				if((findPtr != NULL) && (del == 0))
				{
					find = 1;
				}
				offset += strlen(line);
			}
			fclose(fp);
			fclose(fp2);
		}
		t_end = clock();

		if(end > total)
			end = total;
		
		printf("{\"result\":[");
		for( i = 0; i < (end-start); i++)
		{
			printf("{");
			printf("\"rid\":\"%d\",", result[i].rid);
			printf("\"del\":\"%d\",", result[i].del);
			printf("\"score\":\"%d\",", result[i].score);
			printf("\"U\":\"%s\",", result[i].url);
			printf("\"T\":\"%s\",", result[i].title);
			printf("\"B\":\"%s\"", result[i].content);
			printf("}");
			if(i < end-start-1)
				printf(",");
			free(result[i].title);
			free(result[i].content);
		}
		printf("]}\n");

		take = (double)(t_end - t_start)/CLOCKS_PER_SEC;
		printf("@Total:%d\n", total);
		printf("@Time:%.3lf\n", take);
		free(line);
		free(line2);
	}
}

void rdel(char *id, Conf *config)
{
	printf("rdel\n");
	printf("id: %s\n", id);
	printf("db: %s\n", (*config).dbName);
}

void rupdate(char *id, char *rec, Conf *config)
{
	printf("rupdate\n");
	printf("id: %s\n", id);
	printf("rec: %s\n", rec);
	printf("db: %s\n", (*config).dbName);
}
