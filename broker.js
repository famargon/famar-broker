const fs = require('fs');
// const mergeStream = require("merge-stream");

const partitionFactory = require('./commitlog/partition');
const fetchTransformFactory = require('./commitlog/fetch').fetchTransformFactory;

const {
    DEFAULT_DATA_PATH,
    AUTO_COMMIT_POLICY,
    COMMIT_POLICIES,
    ERROR_TOPIC_ALREADY_EXISTS,
    ERROR_TOPIC_MISSING,
    TOPIC_STORE_NAME,
    SUBSCRIPTIONS_STORE_NAME,
    COMMITS_STORE_NAME
} = require('./constants');

const {checkDirectory} = require('./utils');

module.exports = function(properties){

    const environment = properties.environment || 'dev';
    const DATA_PATH = properties.dataPath || DEFAULT_DATA_PATH;

    const topicsPool = {};
    const subscriptions = {};

    const createTopic = function({topic, partitionsCount, parameters}, internal){
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
                    });
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
                    //TODO round robin to load balance producers in partitions
                    let min = 0;
                    let max = topicObj.partitions.length;
                    partition = Math.floor(Math.random() * (max - min) ) + min;
                }
                topicObj.partitions[partition].append(message)
                .then(({offset})=>{
                    resolve({offset, topic, partition});
                })
                .catch((err)=>{
                    reject(err);
                });
            }else{
                reject("Topic "+topic+" doesn't exists");
            }
        });
    }

    let read = function({topic, partition, fetchOffset, maxBytes}){
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
                maxBytes = 512000; //512KB
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
                    let fetchTransform = fetchTransformFactory();
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

    let commit = function({consumerGroup, consumerId, topic, partition, offset}, cache, internal){
        cache = cache == null ? true : cache;
        const commitMetadata = {consumerGroup, consumerId, topic, partition, offset};
        return new Promise((resolve, reject)=>{
            if(consumerGroup == null){
                reject("ConsumerGroup is missing");
                return;
            }
            if(consumerId == null){
                reject("ConsumerId is missing");
                return;
            }
            if(topic == null){
                reject("Topic is missing");
                return;
            }
            if(partition == null){
                reject("partition is missing");
                return;
            }
            if(offset == null){
                reject("offset is missing");
                return;
            }
            var topicObj = topicsPool[topic];
            if(topicObj == null){
                reject("Topic "+topic+" doesn't exists");
                return;
            }
            if(topicObj.partitions[partition] == null){
                reject("Partition "+partition+" doesn't exists in topic "+topic);
                return;
            }
            let topicSubscriptions = subscriptions[topic];
            if(topicSubscriptions == null){
                reject("Topic subscription not found");
                return;
            }
            let group = topicSubscriptions[consumerGroup];
            if(group == null){
                reject("Consumer group not found");
                return;
            }
            let subscriptionMetadata = group.consumers[consumerId];
            if(subscriptionMetadata == null){
                reject("Consumer id not found");
                return;
            }
            let assignedPartitions = subscriptionMetadata.assignedPartitions;
            if(assignedPartitions == null){
                reject("Consumer has no assigned partitions on this topic");
                return;
            }
            let partitionAssigned = assignedPartitions[partition];
            if(partitionAssigned == null){
                reject("consumer "+consumerId+" is not assigned to partition "+partition);
                return;
            }
            if(cache){
                partitionAssigned.lastCommitedOffset = offset;
            }
            if(internal){
                resolve(commitMetadata);
                return;
            }
            produce({topic:COMMITS_STORE_NAME, partition:0, message:Buffer.from(JSON.stringify(commitMetadata))})
            .then(()=>{
                resolve(commitMetadata);
            })
            .catch(err=>{
                reject(err);
            });
        });
    }

    let fetch = function({consumerGroup, consumerId, topic, maxFetchBytes}){
        return new Promise((resolve,reject)=>{
            if(consumerGroup == null){
                reject("ConsumerGroup is missing");
                return;
            }
            if(consumerId == null){
                reject("ConsumerId is missing");
                return;
            }
            if(topic == null){
                reject("Topic is missing");
                return;
            }
            var topicObj = topicsPool[topic];
            if(topicObj == null){
                reject("Topic "+topic+" doesn't exists");
                return;
            }
            let topicSubscriptions = subscriptions[topic];
            if(topicSubscriptions == null){
                reject("Topic subscription not found");
                return;
            }
            let group = topicSubscriptions[consumerGroup];
            if(group == null){
                reject("Consumer group not found");
                return;
            }
            let subscriptionMetadata = group.consumers[consumerId];
            if(subscriptionMetadata == null){
                reject("Consumer id not found");
                return;
            }
            let assignedPartitions = subscriptionMetadata.assignedPartitions;
            if(assignedPartitions == null){
                reject("Consumer has no assigned partitions on this topic");
                return;
            }
            assignedPartitions = Object.keys(assignedPartitions).map(partitionId=>assignedPartitions[partitionId]);
            let fetchCounter = subscriptionMetadata.fetchCounter;
            if(fetchCounter == null){
                fetchCounter = 0;
                subscriptionMetadata.fetchCounter = fetchCounter;
            }
            //round robin for choosing partition to fetch
            let fetchPartition = assignedPartitions[fetchCounter % Object.keys(assignedPartitions).length];
            subscriptionMetadata.fetchCounter ++;

            let lastCommitedOffset = fetchPartition.lastCommitedOffset;
            if(lastCommitedOffset == null){
                lastCommitedOffset = 0;
            }else{
                lastCommitedOffset++;
            }

            read({topic, partition:fetchPartition.id, fetchOffset:lastCommitedOffset, maxFetchBytes})
            .then(readStream=>{
                resolve(readStream);
                if(group.commitPolicy === AUTO_COMMIT_POLICY){
                    readStream.on('end', ()=>{
                        fetchPartition.lastCommitedOffset = readStream.lastOffset;
                        commit({consumerGroup, consumerId, topic, partition:fetchPartition.id, offset:readStream.lastOffset}, false)
                        .catch(err=>console.error(err));
                    });
                }
            })
            .catch(err=>{
                if(err.code && err.code === 'EONF' && lastCommitedOffset === 0){
                    reject([]);
                }else{
                    reject(err);
                }
            });

            // let readResults = [];
            // for(let partition in assignedPartitions){
            //     readResults.push(read({topic, partition, lastCommitedOffset, maxFetchBytes}));
            // }
            // Promise.all(readResults)
            // .catch(err=>reject(err))
            // .then(readStreams=>{
            //     let mergedReadStream = mergeStream(readStreams);
            //     mergedReadStream.pipe()
            // });
        });
    }

    let subscribe = function(subscriptionMetadata, internal){
        internal = internal == null ? false : internal;
        return new Promise((resolve,reject)=>{
            let {consumerGroup, consumerId, topic, commitPolicy} = subscriptionMetadata;
            if(consumerGroup == null){
                reject("ConsumerGroup is missing");
                return;
            }
            if(consumerId == null){
                reject("ConsumerId is missing");
                return;
            }
            if(topic == null){
                reject("Topic is missing");
                return;
            }
            var topicObj = topicsPool[topic];
            if(topicObj == null){
                reject("Topic "+topic+" doesn't exists");
                return;
            }
            if(commitPolicy==null){
                commitPolicy = AUTO_COMMIT_POLICY;
            }
            if(!COMMIT_POLICIES.includes(commitPolicy)){
                reject("Invalid commit policy "+commitPolicy+" permitted values:"+COMMIT_POLICIES);
                return;
            }
            let topicSubscriptions = subscriptions[topic];
            if(topicSubscriptions == null){
                topicSubscriptions = {};
                subscriptions[topic] = topicSubscriptions;
            }
            let group = topicSubscriptions[consumerGroup];
            if(group == null){
                group = {commitPolicy, consumers:{}};
                topicSubscriptions[consumerGroup] = group;
            }
            if(group.consumers[consumerId] != null && !internal){
                reject("ConsumerId "+consumerId+" already has a subscription in this topic");
                return;
            }

            if(subscriptionMetadata.assignedPartitions == null){
                for(let auxConsumerId in group.consumers){
                    group.consumers[auxConsumerId].assignedPartitions = {};
                }
                group.consumers[consumerId] = {assignedPartitions:{}};
                let arrayConsumerIds = Object.keys(group.consumers);
                for(let partitionId=0; partitionId < topicObj.partitionsCount; partitionId++){
                    let consumerIndex = partitionId % arrayConsumerIds.length;
                    let consumerAtIndex = arrayConsumerIds[consumerIndex];
                    group.consumers[consumerAtIndex].assignedPartitions[partitionId] = {id:partitionId};
                }
            }else{
                let assignedPartitions = subscriptionMetadata.assignedPartitions;
                group.consumers[consumerId] = {assignedPartitions};
            }

            if(internal){
                resolve(subscriptionMetadata);
                return;
            }
            produce({topic:SUBSCRIPTIONS_STORE_NAME, partition:0, message:Buffer.from(JSON.stringify(subscriptionMetadata))})
            .then(()=>{
                resolve(subscriptionMetadata);
            })
            .catch(err=>{
                reject(err);
            });

        });
    }

    let internalStoreInitializer = function(internalTopicName, loader){
        return new Promise((resolve, reject)=>{
            createTopic({topic:internalTopicName, partitionsCount:1}, true)
            .catch(errCreateTopic=>{
                let code = errCreateTopic.code;
                if(code && code === ERROR_TOPIC_ALREADY_EXISTS){
                    loader()
                    .then(()=>resolve())
                    .catch(err=>reject(err));
                }else{
                    reject(errCreateTopic);
                }
            })
            .then(()=>{
                loader()
                .then(()=>resolve())
                .catch(err=>reject(err));
            });
        });
    }

    let internalStoreLoader = function(internalTopicName, factory){
        return new Promise((resolve, reject)=>{
            read({topic:internalTopicName, partition:0, fetchOffset:0})
            .then(readStream=>{
                let recordsChunks = [];
                readStream.on('data', function(d){ recordsChunks.push(d); });
                readStream.on('end', function(){
                    let buf = Buffer.concat(recordsChunks);
                    let changelogRecords = JSON.parse(buf);
                    let results = [];
                    for(let i in changelogRecords){
                        let metadata = changelogRecords[i].payload;
                        let creation = factory(metadata);
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
                    resolve();
                }else{
                    reject(err);
                }
            });
        });   
    }

    const loadTopics = function(){
        return internalStoreLoader(TOPIC_STORE_NAME, (topicMetadata)=>createTopic(topicMetadata, true));
    }

    const initTopics = function(){
        return internalStoreInitializer(TOPIC_STORE_NAME, loadTopics);
    }

    const loadSubscriptions = function(){
        return internalStoreLoader(SUBSCRIPTIONS_STORE_NAME, (metadata)=>subscribe(metadata, true));
    }

    const initSubscriptions = function(){
        return internalStoreInitializer(SUBSCRIPTIONS_STORE_NAME, loadSubscriptions);
    }

    const loadCommits = function(){
        return internalStoreLoader(COMMITS_STORE_NAME, (commitMetadata)=>commit(commitMetadata, true, true));
    }

    const initCommits = function(){
        return internalStoreInitializer(COMMITS_STORE_NAME, loadCommits);
    }

    const checkDataDirectory = ()=>checkDirectory(DATA_PATH);

    let init = function(){
        return new Promise((resolve, reject)=>{
            //runs secuentialy this functions
            [checkDataDirectory, 
            initTopics, 
            initSubscriptions, 
            initCommits]
            .reduce((prev,current)=>{
                return prev.catch(err=>console.error(err)).then(()=>current());
            },Promise.resolve())
            .then(() => {
                    console.log("----------------------------------------------------");
                    console.log("Topics: "+JSON.stringify(topicsPool, 1, 2));
                    console.log("----------------------------------------------------");
                    console.log("Subscriptions: "+JSON.stringify(subscriptions, 1, 2));
                    console.log("----------------------------------------------------");
                    resolve();
            })
            .catch(err=>reject(err));
        });
    }

    return {
        init,
        createTopic,
        produce,
        fetch,
        subscribe
    };

}
