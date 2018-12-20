var brokerFactory = require('../../broker');

var properties = {dataPath:__dirname+"/testBrokerData/"};//TODO

var broker = brokerFactory(properties);

broker.init()
.then(()=>{
    let topic = "topic1";
    broker.subscribe({consumerGroup:"test", consumerId:"test", topic})
    .then((res)=>{
        console.log(res);
    })
    .catch((err)=>{
        console.error(err);
    });

})
.catch((err)=>{
    console.error(err);
})