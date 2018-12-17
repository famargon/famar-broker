    //broker
    //-api :
    //  create, delete. topic
    //  produce
    //  fetch(consume)
    //  offset commit

    //topics
    //  partitions(log manager)
    //      -segments
    //      -active segment
    //      -opts: maxSegmentBytes, retention policy //TODO

    //segment
    //  -append
    //  -read


    // produce -> getTopic().getPartition().activeSegment().append(message)

var fs = require('fs');

var partitionFactory = require('./commitlog/partition');
let recordsParser = require('./commitlog/record').jsonParser;
const fetchTransformStreamFactory = require('./commitlog/fetchTransformStream');
const constants = require('./constants');

module.exports = function(properties){

    const DATA_PATH = properties.dataPath || constants.DEFAULT_DATA_PATH;
    const TOPIC_STORE_NAME = "_topic_changelog";
    const ERROR_TOPIC_MISSING = "ETM";
    const ERROR_TOPIC_ALREADY_EXISTS = "ETAE";

    var topicsPool = {};

    let createTopic = function({topic, partitionsCount, parameters}, internal){
        return new Promise((resolve, reject)=>{
            if(topicsPool[topic]){
                reject("Topic already exists");
                return;
            }
            if(!partitionsCount){
                partitionsCount = 1;
            }
            if(parameters==null){
                parameters = {};
            }
            var topicObject = {
                topic,
                partitionsCount,
                parameters,
                partitions: []
            };
            var topicDirectory = DATA_PATH + topic + "/";
            fs.mkdir(topicDirectory,(mkdirErr)=>{
                if(mkdirErr){
                    if(internal && mkdirErr.code === 'EEXIST'){
                        //do nothing and load topic in memory
                    }else{
                        reject({code:ERROR_TOPIC_ALREADY_EXISTS, msg:mkdirErr});
                        return;
                    }
                }
                let tempPartitions = [];
                for(let i=0; i<partitionsCount; i++){
                    let path = topicDirectory+i+"/";
                    let partition = partitionFactory({topic, partitionId:i, path, parameters});
                    tempPartitions.push(partition);
                }
                let initResults = [];
                for(let i=0; i<partitionsCount; i++){
                    let res = tempPartitions[i].init();
                    initResults.push(res);
                }
                Promise.all(initResults)
                .then(()=>{
                    topicObject.partitions = tempPartitions;
                    topicsPool[topic] = topicObject;
                    if(internal){
                        resolve();
                        return;
                    }
                    let topicMetadata = {topic, partitionsCount, parameters};
                    produce({topic:TOPIC_STORE_NAME, partition:0, message:Buffer.from(JSON.stringify(topicMetadata))})
                    .then(()=>{
                        resolve();
                    })
                    .catch(err=>{
                        reject(err);
                    })
                })
                .catch(err=>{
                    reject(err);
                });
            });
        });
    }

    let produce = function({topic, partition, message}){
        return new Promise((resolve,reject)=>{
            if(!topic || topic.length==0){
                reject("Topic is empty");
                return;
            }
            if(!message || message.length==0){
                reject("Message is empty");
                return;
            }
            var topicObj = topicsPool[topic];
            if(topicObj){
                if(partition && partition>=topicObj.partitions.length){
                    reject("Partition is not a valid index");
                    return;
                }
                if(partition == null){
                    let min = 0;
                    let max = topicObj.partitions.length;
                    partition = Math.floor(Math.random() * (max - min) ) + min;
                }
                topicObj.partitions[partition].append(message)
                .then((result)=>{
                    resolve(result);
                })
                .catch((err)=>{
                    reject(err);
                });
            }else{
                reject("Topic "+topic+" doesn't exists");
            }
        });
    }

    let fetch = function({topic, partition, fetchOffset, maxBytes}){
        return new Promise((resolve,reject)=>{
            if(!topic || topic.length==0){
                reject("Topic is empty");
                return;
            }
            if(partition==null || partition<0){
                reject("Partition is missing or is not valid");
                return;
            }else{
               partition = new Number(partition);
            }
            if(fetchOffset==null || fetchOffset<0){
                reject("fetchOffset is missing or is not valid");
                return;
            }else{
                fetchOffset = new Number(fetchOffset);
            }
            if(maxBytes == null){
                maxBytes = 4096;
            }else{
                maxBytes = new Number(maxBytes);
            }
            var topicObj = topicsPool[topic];
            if(topicObj){
                if(partition>=topicObj.partitions.length){
                    reject("Partition is not a valid index");
                    return;
                }
                topicObj.partitions[partition].read(fetchOffset, maxBytes)
                .then((recordsStream)=>{
                    let fetchTransform = fetchTransformStreamFactory();
                    resolve(recordsStream.pipe(fetchTransform));
                })
                .catch((err)=>{
                    reject(err);
                });
            }else{
                reject({code:ERROR_TOPIC_MISSING, msg:"Topic "+topic+" doesn't exists"});
            }
        });
    }

    function createDataDirectory(){
        return new Promise((resolve,reject)=>{
            fs.mkdir(DATA_PATH,(mkdirErr)=>{
                if(mkdirErr){
                    reject(mkdirErr);
                    return;
                }
                fs.open(DATA_PATH, 'r', (err, fd) => {
                    if(err){
                        reject(err);
                    }else{
                        resolve();
                    }
                });
            });
        });
    }

    function checkDataDirectory(){
        return new Promise((resolve,reject)=>{
            fs.open(DATA_PATH, 'r', (err, fd) => {
                if(err){
                    if (err.code === 'ENOENT') {
                        createDataDirectory()
                        .then(()=>resolve())
                        .catch(dirErr=>{
                            reject(dirErr);
                        });
                    }else{
                        reject(err);
                    }
                }else{
                    resolve();
                }
            });
        });
    }

    let loadTopics = function(){
        return new Promise((resolve, reject)=>{
            fetch({topic:TOPIC_STORE_NAME, partition:0, fetchOffset:0})
            .then(readStream=>{
                var recordsChunks = [];
                readStream.on('data', function(d){ recordsChunks.push(d); });
                readStream.on('end', function(){
                    var buf = Buffer.concat(recordsChunks);
                    let topicChangelog = JSON.parse(buf);
                    let results = [];
                    for(let i in topicChangelog){
                        let topicMetadata = topicChangelog[i];
                        let creation = createTopic(topicMetadata, true);
                        results.push(creation);
                    }
                    Promise.all(results)
                    .catch(err=>reject(err))
                    .then(()=>resolve());
                });
            })
            .catch(err=>{
                if(err.code && err.code === "EONF"){
                    //do nothing we tried to fetch from a empty internal topic
                    console.log("Nothing to load from "+TOPIC_STORE_NAME)
                    resolve();
                }else{
                    reject(err);
                }
            });
        });
    }

    function initTopics(){
        return new Promise((resolve, reject)=>{
            createTopic({topic:TOPIC_STORE_NAME, partitionsCount:1}, true)
            .catch(errCreateTopic=>{
                let code = errCreateTopic.code;
                if(code && code === ERROR_TOPIC_ALREADY_EXISTS){
                    loadTopics()
                    .then(()=>resolve())
                    .catch(err=>reject(err));
                }else{
                    reject(errCreateTopic);
                }
            })
            .then(()=>{
                loadTopics()
                .then(()=>resolve())
                .catch(err=>reject(err));
            });
        });
    }

    function init(){
        return new Promise((resolve, reject)=>{
            checkDataDirectory()
            .catch(err=>reject(err))
            .then(()=>{
                initTopics()
                .then(()=>{
                    resolve();
                })
                .catch(err=>reject(err));
            });
        });
    }

    return {
        init,
        createTopic,
        produce,
        fetch
    };

}
