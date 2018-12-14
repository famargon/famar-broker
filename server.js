var express = require('express');
var app = express();

var brokerFactory = require('./broker');

var properties = {};//TODO

var broker = brokerFactory(properties);

app.get('/add/:topic/:message', function(req, res){
    broker.produce({topic:req.params.topic, message:Buffer.from(req.params.message)})
    .then(()=>{
        res.end(req.params.message);
    })
    .catch((err)=>{
        console.error(err);
        res.end(new String(err));
    })
});

app.get('/read/:topic/:partition', function (req, res) {
    broker.fetch({topic:req.params.topic, partition:new Number(req.params.partition), fetchOffset:new Number(req.query.offset), maxBytes:req.query.maxBytes})
    .then((recordsStream)=>{
        recordsStream.pipe(res);
    })
    .catch((err)=>{
        console.error(err);
        res.end(new String(err));
    })
});

app.put('/topic', function(req, res){
    broker.createTopic({topic:req.query.topic, partitionsCount:req.query.partitionsCount})
    .then(()=>{
        res.end();
    })
    .catch((err)=>{
        console.error(err);
        res.end(err);
    });
});

broker.init()
.then(()=>{
    app.listen(3000, function () {
        console.log('Welcome to famar-broker. Server listening on port 3000');
    });
})
.catch((err)=>{
    console.error(err);
})


