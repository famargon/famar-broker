const express = require('express');
const bodyParser = require('body-parser');

const brokerFactory = require('./broker');
let properties = {dataPath:__dirname+"/data/"};//TODO
const broker = brokerFactory(properties);

const app = express();
app.use(bodyParser.json());

app.get('/add/:topic/:message', function(req, res){
    broker.produce({topic:req.params.topic, message:Buffer.from(req.params.message)})
    .then((produceResponse)=>{
        res.end(JSON.stringify(produceResponse));
    })
    .catch((err)=>{
        console.error(err);
        res.end(new String(err));
    })
});

app.get('/read/:topic/:partition', function (req, res) {
    broker.fetch({topic:req.params.topic, partition:req.params.partition, fetchOffset:req.query.offset, maxBytes:req.query.maxBytes})
    .then((recordsStream)=>{
        recordsStream.pipe(res);
    })
    .catch((err)=>{
        console.error(err);
        res.end(JSON.stringify(err));
    })
});

app.post('/subscription', (req, res)=>{
    broker.subscribe(req.body)
    .then(result=>{
        res.end(JSON.stringify(result));
    })
    .catch(err=>{
        console.error(err);
        res.end(JSON.stringify(err));
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


