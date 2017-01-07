var express = require('express');
var bodyParser = require('body-parser');
var exec = require('child_process').exec;

var app = express();
var urlencodeParser = bodyParser.urlencoded({ extended: false });

app.set('views', __dirname + '/views');
app.set('view engine', 'ejs');
app.use(express.static(__dirname +'/public'));

app.get('/', function(req, res) {
    res.sendFile(__dirname + "/public/search.html");
});

app.get('/page', function(req, res) {
    if (typeof req.query.page !== 'undefined') {
        var key = req.query.key;
        var currentPage = req.query.page;
        var from = (Number(currentPage) - 1) * 10;
        var to = Number(currentPage)*10;
        
        getRecord(res, key, from, to, currentPage);
    }
});

app.post('/search', urlencodeParser, function(req, res) {

    var key = req.body.key;
    var pageSize = 10;
    var currentPage = 1;
    var from = Number(0);
    var to = Number(pageSize);

    getRecord(res, key, from, to, currentPage);
})

var server = app.listen(2888, "127.0.0.1", function() {
    var host = server.address().address;
    var port = server.address().port;
    console.log("Listening at http://%s:%s", host, port);
})

function getRecord(res, key, from, to, currentPage) {
    var record = [];
    var line, size, total, time;
    var pageCount, pageSize = 10;  
    var i, buf, ocntent;

    var cmd = './rdb -rget "rdb" "' + key + '" "' + from + '" "' + to + '"';
    console.log("rdb: " + cmd);

    exec(cmd, function(err, out, code) {
        /*
            result JSON format
        */
        record = JSON.parse(out);
		var time = record["time"];
		var total = record["total"];
        pageCount = Math.ceil(Number(total) / Number(pageSize));
        
		console.log("Total:" + total);
        console.log("Time:" + time);
        console.log("pageCount:" + pageCount);
                
		var len = record["result"].length;
        for(i = 0; i < len; i++)
        {
			content = (record["result"][i].B).replace(/\s+/gi, " ");
            if (content.length > 300) 
			{
                content = content.substring(0, 300);
                content += "...";
                record["result"][i].B = content;
            }
			if(i >= len-1)
				res.render('result', { keyword: key, record: record, pageCount: pageCount, currentPage: currentPage});
        }
    });
}
