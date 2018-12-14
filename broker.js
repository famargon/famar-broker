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
const constants = require('./constants');

module.exports = function(properties){

    const DATA_PATH = properties.dataPath || constants.DEFAULT_DATA_PATH;

    var topicsPool = {};

    let createTopic = function({topic, partitionsCount, parameters}){
        return new Promise((resolve, reject)=>{
            if(topicsPool[topic]){
                reject("Topic already exists");
            }
            if(!partitionsCount){
                partitionsCount = 1;
            }
            var topicObject = {
                topic,
                partitionsCount,
                parameters,
                partitions: []
            };
            var topicDirectory = DATA_PATH + topic + "/";
            fs.mkdir(topicDirectory,(mkdirErr)=>{
                if(mkdirErr)reject(mkdirErr);
            });
            for(var i=0; i<partitionsCount; i++){
                var path = topicDirectory+i+"/";
                var partition = partitionFactory({topic, partitionId:i, path, ...parameters});
                partition.init().catch(err=>reject(err));
                topicObject.partitions.push(partition);
            }
            topicsPool[topic] = topicObject;
            resolve();
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
                if(!partition){
                    /**
                     * This JavaScript function always returns a random number between min (included) and max (excluded)
                     */
                    function getRndInteger(min, max) {
                        return Math.floor(Math.random() * (max - min) ) + min;
                    }
                    partition = getRndInteger(0, topicObj.partitions.length)
                }
                topicObj.partitions[partition].append(message)
                .then(()=>{
                    resolve();
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
            if(!partition || partition<0){
                reject("Partition is missing or is not valid");
                return;
            }
            if(!maxBytes){
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
                    resolve(recordsStream);
                })
                .catch((err)=>{
                    reject(err);
                });
            }else{
                reject("Topic "+topic+" doesn't exists");
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

    function initTopics(){
        return new Promise((resolve, reject)=>{
            fs.readdir(DATA_PATH, (err, files) => {
                if(err){
                    reject(err);
                    return;
                }
                for(var file in files){
                    
                }
                resolve();
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