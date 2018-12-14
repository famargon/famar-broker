var brokerFactory = require('../../broker');

var properties = {dataPath:__dirname+"/testBrokerData/"};//TODO

var broker = brokerFactory(properties);

broker.init()
.then(()=>{
    let topic = "topic1";
    broker.createTopic({topic, partitionsCount:2})
    .then(()=>{
        console.log(topic+" created");
    })
    .catch((err)=>{
        console.error(err);
    });



})
.catch((err)=>{
    console.error(err);
})