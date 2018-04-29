var express = require('express')
var path = require('path')
var bodyParser = require('body-parser');
var stats = require("./routes/stats");

// console.log(stats)


var port = process.env.PORT || 3002
var app = express()

app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());


app.use(express.static('./')); 		// set the static files location /
app.use('/stats', stats);


app.get('/', function(req, res) {
    res.sendFile("index.html", {
        root: path.join(__dirname, '/views')
    });
});


var listenPort = 3002;
if (process.env.hasOwnProperty('IISNODE_VERSION') && process.env.hasOwnProperty('PORT')) {
    listenPort = process.env.PORT
}

app.listen(listenPort, function() {
    console.log('Tweets Cloud app listening on port ' + listenPort + '!');
});

