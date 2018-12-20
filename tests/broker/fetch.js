var brokerFactory = require('../../broker');

var properties = {dataPath:__dirname+"/testBrokerData/"};//TODO

var broker = brokerFactory(properties);

broker.init()
.then(()=>{
    let topic = "topic1";
    broker.fetch({consumerGroup:"test", consumerId:"test", topic})
    .then((readStream)=>{
        readStream.pipe(process.stdout);
        // var recordsChunks = [];
        // readStream.on('data', function(d){ recordsChunks.push(d); });
        // readStream.on('end', function(){
        //     var buf = Buffer.concat(recordsChunks);
        //     let topicChangelog = JSON.parse(buf);
        //     console.log(JSON.stringify(topicChangelog));
        // });
    })
    .catch((err)=>{
        console.error(err);
    });

})
.catch((err)=>{
    console.error(err);
})