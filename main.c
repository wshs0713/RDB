#include "args.h"

int main(int argc, char *argv[])
{
	/* command:
		-create [db name] [field1,field2] [title field]: create db file and config file
		-rput [db name] [record] : put record into db
		-fput [db name] [record file] [record begin pattern]: read record file and put into db
		-rget [db name] [field=value] [start] [end]: get record by rid/key from db
		-rdel [db name] [rid]: delete record by rid
		-rupdate [db name] [rid] [record]: update record
	*/
	char *field, *val, *ptr;
	int i, len = 0, fieldLen = 0, valLen = 0;
	int status = 0, start = 0, end = 0;
	int value = 0;
	Conf config;
	INFO info;
	
	//initialize
	memset(config.dbName, '\0', 20);
	config.fileSize = 0;
	config.maxBuffer = 65536;
	config.patCnt = 0;
	for(i = 0; i < 50; i++)
		memset(config.pat[i], '\0', 20);
	memset(config.titlePat, '\0', 20);

	info.recCnt = -1;
	info.curFile = 0;
	
	if(argc > 1)
	{
		if(argv[1][0] == '-')
		{
			if(strcmp(argv[1], "-h") == 0)
				showHelp();
			else if(strcmp(argv[1], "-create") == 0)
			{
				if(argc != 5) //argument number is wrong
				{
					printf("Error!\n");
					showHelp();
				}
				else
				{
					createDB(argv[2], argv[3], argv[4]);
				}
			}
			else 
			{
				status = readConfig(argv[2], &config);
				if(status == 1)
				{
					readInfo(argv[2], &info);
					if(strcmp(argv[1], "-rput") == 0)
					{
						if(argc != 4) //argument number is wrong
						{
							printf("Error!\n");
							showHelp();
						}
						else
							rput(-1, argv[3], &config, &info);
					}
					else if(strcmp(argv[1], "-fput") == 0)
					{
						if(argc != 5) //argument number is wrong
						{
							printf("Error!\n");
							showHelp();
						}
						else
							fput(argv[3], argv[4], &config, &info);
					}
					else if(strcmp(argv[1], "-rget") == 0)
					{
						if(argc != 4 && argc != 6) //argument number is wrong
						{
							printf("Error!\n");
							showHelp();
						}
						else
						{
							if(argc == 4)
							{
								start = 0;
								end = 10;
							}
							else
							{
								start = atoi(argv[4]);
								end = atoi(argv[5]);
							}

							ptr = argv[3];
							len = strlen(ptr);
							if(strstr(ptr, "=") == NULL) //full text search
							{
								rget("", argv[3], start, end, &config, &info);
							}
							else
							{
								while(*ptr != '=')
								{
									ptr++;
									fieldLen++;
								}
								ptr++; //skip '='
								field = (char *)malloc((fieldLen+1) * sizeof(char));
								memset(field, '\0', fieldLen+1);
								strncpy(field, argv[3], fieldLen);
				
								valLen = len - fieldLen - 1; //-1: =
								val = (char *)malloc((valLen+1) * sizeof(char));
								memset(val, '\0', valLen+1);
								strncpy(val, ptr, valLen);

								rget(field, val, start, end, &config, &info);
							}
						}
					}
					else if(strcmp(argv[1], "-rdel") == 0)
					{
						if(argc != 4) //argument number is wrong
						{
							printf("Error!\n");
							showHelp();
						}
						else
						{
							ptr = argv[3];
							len = strlen(ptr);
							if(strstr(ptr, "=") != NULL)
							{
								while(*ptr != '=')
								{
									ptr++;
									fieldLen++;
								}
								ptr++; //skip '='
							
								valLen = len - fieldLen - 1; //-1: =
								val = (char *)malloc((valLen+1) * sizeof(char));
								memset(val, '\0', valLen+1);
								strncpy(val, ptr, valLen);
								value = atoi(val);
							}
							else
								value = atoi(argv[3]);

							rdel(value, &config, &info);
						}
					}
					else if(strcmp(argv[1], "-rupdate") == 0)
					{
						//./rdb -rupdate dbname rid=123 rec="____"
						if(argc != 5) //argument number is wrong
						{
							printf("Error!\n");
							showHelp();
						}
						else
						{
							ptr = argv[3];
							len = strlen(ptr);
							if(strstr(ptr, "=") != NULL)
							{
								while(*ptr != '=')
								{
									ptr++;
									fieldLen++;
								}
								ptr++; //skip '='
							
								valLen = len - fieldLen - 1; //-1: =
								val = (char *)malloc((valLen+1) * sizeof(char));
								memset(val, '\0', valLen+1);
								strncpy(val, ptr, valLen);
								value = atoi(val);
							}
							else
								value = atoi(argv[3]);

							ptr = argv[4];
							if(strstr(ptr, "=") != NULL)
							{
								while(*ptr != '=')
								{
									ptr++;
								}
								ptr++; //skip '='
							}

							rupdate(value, ptr, &config, &info);
						}
					}
				}
				else
					printf("DB not exist!\n");
			}
		}
	}
	else
	{
		printf("Error! Please input command.\n");
		showHelp();
	}
	return 0;
}
