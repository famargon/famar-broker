var fs = require('fs');
var segmentFactory = require('./segment');
var createRecord = require('./record').createRecord;

const constants = require("../constants");

function partitionFactory (options){

    const topic = options.topic;
    const partitionId = options.partitionId;
    let segments = [];

    function activeSegment(){
        return segments[segments.length-1];
    }

    //choose the segments that stores the record with fetchOffset
    function chooseSegment(fetchOffset) {
        let choosen;
        for(let i in segments){
            let segment = segments[i];
            if(fetchOffset >= segment.firstOffset){
                choosen = segment;
            }else{
                break;
            }
        }
        return choosen;
        // return segments.find((segment)=>fetchOffset >= segment.firstOffset);
    }

    function validateOptions(resolve,reject){
        if(!options.path){
            reject("Partition path is missing");
        }
        if(options.path.length==0){
            reject("Partition path is empty");
        }
    }

    function createPartitionDirectory(){
        return new Promise((resolve,reject)=>{
            fs.mkdir(options.path,(mkdirErr)=>{
                if(mkdirErr){
                    reject(mkdirErr);
                    return;
                }
                fs.open(options.path, 'r', (err, fd) => {
                    if(err){
                        reject(err);
                    }else{
                        resolve();
                    }
                });
            });
        });
    }

    function loadSegments(){
        return new Promise((resolve,reject)=>{
            fs.readdir(options.path, (err, files) => {
                if(err){
                    reject(err);
                    return;
                }
                for(var file in files){
                    if(file.endsWith(constants.INDEX_FILE_SUFFIX)){
                        //check if corresponding log file exists
                        let logFile = file.replace(constants.INDEX_FILE_SUFFIX, constants.LOG_FILE_SUFFIX);
                        if(!files.includes(logFile)){
                            reject("Partition inconsistency, missing log file "+logFile);
                            return;
                        }
                    }else if(file.endsWith(constants.LOG_FILE_SUFFIX)){
                        //check existence of index file
                        let indexFile = file.replace(constants.LOG_FILE_SUFFIX, constants.INDEX_FILE_SUFFIX);
                        if(!files.includes(indexFile)){
                            reject("Partition inconsistency, missing index file "+logFile);
                            return;
                        }
                        var baseOffset = file.replace(constants.LOG_FILE_SUFFIX,"");//filename is base offset
                        var segment = segmentFactory(options.path, new Number(baseOffset))
                        segments.push(segment);
                    }
                }
                if(segments.length==0){
                    //init first segment
                    var segment = segmentFactory(options.path, 0)
                    segments.push(segment);
                }
                resolve();
            });
        });
    }

    function load(resolve, reject){
        loadSegments()
        .then(()=>{
            let initResults = [];
            for(let i in segments){
                let segment = segments[i];
                initResults.push(segment.init());
            }
            Promise.all(initResults)
            .then(()=>{
                resolve();
            })
            .catch(err=>{
                reject(err);
            })
        })
        .catch((err)=>{
            reject(err);
        })
    }

    function init(){
        return new Promise((resolve,reject)=>{
            validateOptions(resolve,reject);
            fs.open(options.path, 'r', (err, fd) => {
                if (err) {
                    if (err.code === 'ENOENT') {
                        createPartitionDirectory()
                        .then(()=>load(resolve,reject))
                        .catch(dirErr=>reject(dirErr));
                    }else{
                        reject(err);
                    }
                }else{
                    load(resolve,reject);
                }
            });
        });
    }

    function checkSegment(){
        return new Promise((resolve,reject)=>{
            //TODO check segment length and swap if needed to a new segment
            resolve();
        });
    }

    function append(message){
        return new Promise((resolve, reject)=>{
            checkSegment()
            .then(()=>{
                var record = createRecord(message);
                let segment = activeSegment();
                var offset = segment.getNextOffset();
                record.setOffset(offset);
                segment.append(record.buffer())
                .then((position)=>{
                    segment.index.index(offset, position, record.size());
                    resolve({offset});
                })
                .catch(err=>{
                    reject(err);
                });
            })
            .catch(err=>{
                reject(err);
            });
        });
    }

    let read = function(fetchOffset, maxBytes){
        return new Promise((resolve, reject)=>{
            let segment = chooseSegment(fetchOffset);
            if(segment){
                let lookupResult = segment.index.lookup(fetchOffset);
                if(lookupResult.err){
                    reject(lookupResult.err);
                    return;
                }
                let translatedPosition = lookupResult.entry.position;
                let bytesToRead = Math.min(segment.size()-translatedPosition, maxBytes);
                resolve(segment.read(translatedPosition, bytesToRead));
            }else{
                reject("Error choosing segment for offset "+fetchOffset+" on topic "+topic+" and partition "+partitionId);
            }
        });
    }

    return {
        init,
        append,
        read
    }

}

module.exports = partitionFactory;