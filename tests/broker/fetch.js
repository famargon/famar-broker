var brokerFactory = require('../../broker');

var properties = {dataPath:__dirname+"/testBrokerData/"};//TODO

let transform = require('../../json/jsonRecordsTransform')()

var broker = brokerFactory(properties);

broker.init()
.then(()=>{
    let topic = "topic1";
    if(process.argv[2] == null){
        console.error("partition is needed")
        return;
    }
    broker.fetch({topic, partition:new Number(process.argv[2]), fetchOffset:0, maxBytes:4096})
    .then((recordsStream1)=>{
        // recordsStream1.pipe(process.stdout);
        recordsStream1.pipe(transform).pipe(process.stdout);
    })
    .catch((err)=>{
        console.error(err);
    });

})
.catch((err)=>{
    console.error(err);
})