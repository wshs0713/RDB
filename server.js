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
        //var currentPage = Number(to) / Number(pageSize);
        
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

var server = app.listen(2888, function() {
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
            result JSON
            @Total:
            @Time:
        */
        line = out.split(/\n/);
        size = line.length;
        for( i = 0; i < size-1; i++)
        {
            if(line[i].indexOf("@Total:")!= -1)
            {
                buf = line[i].split("@Total:");
                total = Number(buf[1]);
            }
            else if(line[i].indexOf("@Time:")!= -1)
            {
                buf = line[i].split("@Time:");
                time = buf[1];
            }
            else
            {
                record = JSON.parse(line[i]);
                var len = record["result"].length;
                //console.log(len);
                for(j = 0; j < len; j++)
                {
                    content = (record["result"][j].B).replace(/\s+/gi, " ");
                    if (content.length > 300) {
                        content = content.substring(0, 300);
                        content += "...";
                        record["result"][j].B = content;
                    }
                }
            }
            if( i >= size - 2)
            {
                pageCount = Math.ceil(Number(total) / Number(pageSize));
                console.log("Total:" + total);
                console.log("Time:" + time);
                console.log("pageCount:" + pageCount);
                res.render('result', { keyword: key, record: record, total: total, pageCount: pageCount, currentPage: currentPage, time: time });
            }
        }
        
    });
}
