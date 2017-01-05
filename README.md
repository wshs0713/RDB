# RDB
### RDB Backend (C)
+ Setup
	+ Create database path: RDB/data/db
	+ Makefile
	```
	make
	```
+ Usage
	+ help
	```C
	./rdb -h
	```
	+ create
	```C
	./rdb -create [db name] [fields] [title field]
	/* 
		Fields format: 
			1. field start with '@'
			2. field end with ':'
			3. fields are separated by ','
	*/
	/*
		For example: ./rdb -create "rdb" "@rid:,@U:,@T:,@B:" "@T:"
	*/
	```
	+ rput: put a record
	```C
	./rdb -rput [db name] [record]
	/* 
		Record format:
			1. field start with '@'
			2. field and value are separated by ':'
			3. field-value pairs are seprated by '|'
	*/
	/*
		For example: ./rdb -rput "rdb" "@U:http:www.cs.ccu.edu.tw|@T:CCU CSIE|@B:Web page"
	*/
	```
	+ fput
	```C
	./rdb -fput [db name] [record file] [record begin pattern]
	/*
		For example: ./rdb -fput "rdb" "data.rec" "@GAISRec:"
	*/
	```
	+ rget
	```C
	./rdb -rget [db name] [query] [start] [end]
	/*
		Default start is 0, end is 10
		Query format:
			1. search specific field: field=value
			2. full text search:
				a. single keyword: keyword
				b. must: ^keyword
				c. must not: !keyword
				d. or: ,keyword
	*/
	/*
		For example: 
			1. ./rdb -rget "rdb" "蔡英文" 0 10
			2. ./rdb -rget "rdb" "蔡英文,柯文哲" 0 10
			3. ./rdb -rget "rdb" "蔡英文^柯文哲" 0 10
			4. ./rdb -rget "rdb" "蔡英文^柯文哲!連勝文" 0 10
			5. ./rdb -rget "rdb" "蔡英文,柯文哲^馬英九!連勝文" 0 10
	*/
	```
	+ rdelete
	```C
	./rdb -rdel [db name] [rid]
	/*
		For example: ./rdb -rdel "rdb" "rid=123"
	*/
	```
	+ rupdate
	```C
	./rdb -rupdate [db name] [rid] [record]
	//record format is the same with rput.
	/*
		For example: ./rdb -rupdate "rdb" "rid=123" "@U:http:www.cs.ccu.edu.tw|@T:CCU CSIE|@B:Web page"
	*/
	```

### Webpage (Node.js)
+ Setup
```
npm install -l
```
+ Modify default ip and port in:
	+ public/search.html
	+ views/result.ejs
	+ Default port: 2888

+ Start
```
node server.js
```
