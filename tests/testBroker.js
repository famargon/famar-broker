var brokerFactory = require('../broker');

var properties = {dataPath:__dirname+"/testBrokerData/"};//TODO

var broker = brokerFactory(properties);

broker.init()
.then(()=>{
    let topic = "topic1";
    // broker.createTopic({topic, partitionsCount:2})
    // .then(()=>{
    //     broker.produce({topic, message:Buffer.from("test broker topic1")})
    //     .then(()=>{
    //         broker.fetch({topic, partition:0, fetchOffset:0, maxBytes:4096})
    //         .then((recordsStream0)=>{
    //             recordsStream0.pipe(console.log);
    //             broker.fetch({topic, partition:1, fetchOffset:0, maxBytes:4096})
    //             .then((recordsStream1)=>{
    //                 recordsStream1.pipe(console.log);
    //             })
    //             .catch((err)=>{
    //                 console.error(err);
    //             });
    //         })
    //         .catch((err)=>{
    //             console.error(err);
    //         });

    //     })
    //     .catch((err)=>{
    //         console.error(err);
    //     });
    // })
    // .catch((err)=>{
    //     console.error(err);
    // });

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