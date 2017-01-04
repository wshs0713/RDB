# RDB
### RDB Backend (C)
+ Setup
	+ Create database path: [root_dir]/data/db
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
		fields format: 
			1. field start with '@'
			2. field end with ':'
			3. fields are separated by ','
			for example:
				@rid:,@U:,@T:
	*/
	```
	+ rput: put a record
	```C
	./rdb -rput [db name] [record]
	/* 
		record format:
			1. field start with '@'
			2. field and value are separated by ':'
			3. field-value pairs are seprated by '|'
			for example:
				@field1:value1|@field2:value2
	*/
	```
	+ fput
	```C
	./rdb -fput [db name] [record file] [record begin pattern]
	```
	+ rget
	```C
	./rdb -rget [db name] [query] [start] [end]
	/*
		query format:
			1. search specific field: field=value
			2. full text search:
				a. single keyword: value
				b. must: +value
				c. must not: !value
	*/
	```
	+ rdelete
	```C
	./rdb -rdel [db name] [rid]
	```
	+ rupdate
	```C
	./rdb -rupdate [db name] [rid] [record]
	//record format is the same with rput.
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
