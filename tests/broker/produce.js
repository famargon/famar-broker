var brokerFactory = require('../../broker');

var properties = {dataPath:__dirname+"/testBrokerData/"};//TODO

var broker = brokerFactory(properties);

broker.init()
.then(()=>{
    let topic = "topic1";
    let count = 1;
    if(process.argv[2] != null){
        count = new Number(process.argv[2]);
    }

    for(let i=0; i<count; i++){
        broker.produce({topic, partition:0, message:Buffer.from(JSON.stringify({text:"Hello broker", i}))})
        .then((res)=>{
            console.log(res);
        })
        .catch((err)=>{
            console.error(err);
        });
    }

})
.catch((err)=>{
    console.error(err);
})