var express = require('express');
var router = express.Router();
var fs = require('fs');


router.post("/filepipline", function(req, res) {

    // console.log(req.body);
    var json = JSON.stringify(req.body);
    console.log(json);
    fs.writeFile('./file_pipline/streaming_args.json', json, 'utf8', function (err) {
        if (err) throw 'error writing file: ' + err;
        else
            console.log("Writing streaming arguments done");
    });
    res.send("success");
});

module.exports = router;
