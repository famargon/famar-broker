var brokerFactory = require('../../broker');

var properties = {dataPath:__dirname+"/testBrokerData/"};//TODO

var broker = brokerFactory(properties);

broker.init()
.then(()=>{
    let topic = "topic1";
    let partition;
    if(process.argv[2] == null){
        partition = 0;
    }else{
       partition = new Number(process.argv[2]); 
    }
    broker.fetch({topic, partition, fetchOffset:0, maxBytes:4096})
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