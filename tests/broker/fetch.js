var brokerFactory = require('../../broker');

var properties = {dataPath:__dirname+"/testBrokerData/"};//TODO

var broker = brokerFactory(properties);

broker.init()
.then(()=>{
    let topic = "topic1";

    broker.fetch({topic, partition:0, fetchOffset:0, maxBytes:4096})
    .then((recordsStream1)=>{
        recordsStream1.pipe(console.log);
    })
    .catch((err)=>{
        console.error(err);
    });

})
.catch((err)=>{
    console.error(err);
})