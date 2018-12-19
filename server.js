const express = require('express');
const bodyParser = require('body-parser');

const brokerFactory = require('./broker');
let properties = {dataPath:__dirname+"/data/", serverPort:3000};//TODO
const broker = brokerFactory(properties);

const app = express();

app.use('/produce/', bodyParser.raw({type:'application/json'}));
app.post('/produce/:topic', (req,res)=>{
    let partition = req.headers['partition'];
    broker.produce({topic:req.params.topic, partition, message:req.body})
    .then((produceResponse)=>{
        res.end(JSON.stringify(produceResponse));
    })
    .catch((err)=>{
        console.error(err);
        res.end(JSON.stringify(err));
    });
});

app.get('/fetch/:topic', (req, res)=>{
    let consumerGroup = req.headers['consumer-group'];
    let consumerId = req.headers['consumer-id'];
    let topic = req.params.topic;
    let maxFetchBytes = req.headers['maxFetchBytes'];
    broker.fetch({consumerGroup, consumerId, topic, maxFetchBytes})
    .then((recordsStream)=>{
        recordsStream.pipe(res);
    })
    .catch((err)=>{
        console.error(err);
        res.end(JSON.stringify(err));
    });

});

app.use(['/subscription', '/topic'], bodyParser.json());

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

app.post('/topic', function(req, res){
    broker.createTopic({topic:req.query.topic, partitionsCount:req.query.partitionsCount})
    .then(()=>{
        res.end();
    })
    .catch((err)=>{
        console.error(err);
        res.end(err);
    });
});

console.log('Welcome to famar-broker.');
broker.init()
.then(()=>{
    const serverPort = properties.serverPort == null ? 3000 : properties.serverPort;
    app.listen(serverPort, function () {
        console.log('Initialization finished. Server listening on port '+serverPort);
    });
})
.catch((err)=>{
    console.error(err);
})


