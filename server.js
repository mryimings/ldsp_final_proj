var express = require('express')
var path = require('path')

var port = process.env.PORT || 3002
var app = express()


app.get('/', function(req, res) {
    res.sendFile("index.html", {
        root: __dirname
    });
});

// app.get('/', function(req, res) {
//     res.sendFile("test.html", {
//         root: path.join(__dirname, '/views')
//     });
// });

var listenPort = 3002;
if (process.env.hasOwnProperty('IISNODE_VERSION') && process.env.hasOwnProperty('PORT')) {
    listenPort = process.env.PORT
}

app.listen(listenPort, function() {
    console.log('Tweets Cloud app listening on port ' + listenPort + '!');
});

