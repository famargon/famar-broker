const fs = require('fs');
const EventEmitter = require('events');

const createDirectory = require('../utils').createDirectory;
const segmentFactory = require('./segment');
const createRecord = require('./record').createRecord;
const constants = require("../constants");

function partitionFactory (options){

    const topic = options.topic;
    const partitionId = options.partitionId;
    const partitionPath = options.path;
    const maxSegmentBytes = options.parameters.maxSegmentBytes;

    let segments = [];

    const loadSegments = function(){
        return new Promise((resolve,reject)=>{
            fs.readdir(partitionPath, (err, files) => {
                if(err){
                    reject(err);
                    return;
                }
                for(let i in files){
                    let file = files[i];
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
                        var segment = segmentFactory(partitionPath, new Number(baseOffset), maxSegmentBytes)
                        segments.push(segment);
                    }
                }
                if(segments.length==0){
                    //init first segment
                    var segment = segmentFactory(partitionPath, 0, maxSegmentBytes);
                    segments.push(segment);
                }else{
                    //sort
                    segments = segments.sort((a,b)=>{
                        if(a.firstOffset < b.firstOffset) {return -1}
                        if(a.firstOffset > b.firstOffset) {return 1}
                        return 0;
                    });
                }
                resolve();
            });
        });
    }

    const load = function(resolve, reject){
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

    const init = function(){
        return new Promise((resolve,reject)=>{
            if(!partitionPath){
                reject("Partition path is missing");
            }
            if(partitionPath.length==0){
                reject("Partition path is empty");
            }
            fs.open(partitionPath, 'r', (err, fd) => {
                if (err) {
                    if (err.code === 'ENOENT') {
                        createDirectory(partitionPath)
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

    const activeSegment = function(){
        return segments[segments.length-1];
    }

    let swapping = false;
    const coordinator = new EventEmitter().setMaxListeners(0);
    
    const checkAndSwapSegment = function(){
        return new Promise((resolve,reject)=>{
            let active = activeSegment();
            if(active.needsSwap()){
                if(swapping){
                    let listener = (err)=>{
                        if(err){
                            reject(err);
                            coordinator.removeListener('error', listener);
                        }else{
                            resolve();
                            coordinator.removeListener('swapped', listener);
                        }
                    }
                    coordinator.on('swapped', listener);
                    coordinator.on('error', listener);
                }else{
                    swapping = true;
                    let baseOffset = active.getNextOffset();
                    var segment = segmentFactory(partitionPath, baseOffset, maxSegmentBytes);
                    segment.init()
                    .then(()=>{
                        segments.push(segment);
                        coordinator.emit('swapped');
                        resolve();
                        swapping = false;
                    })
                    .catch(err=>{
                        coordinator.emit('error',err);
                        reject(err);
                        swapping = false;
                    });
                }
            }else{
                resolve();
            }
        });
    }

    const internalAppend = function(resolve, reject, message){
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
    }

    const append = function(message){
        return new Promise((resolve, reject)=>{
            let active = activeSegment();
            if(active.needsSwap()){
                checkAndSwapSegment()
                .then(()=>{
                    append(message)
                    .then(res=>resolve(res))
                    .catch(err=>reject(err));
                })
                .catch(err=>reject(err));
            }else{
                internalAppend(resolve, reject, message);
            }
        });
    }

    //choose the segments that stores the record with fetchOffset
    const chooseSegment = function(fetchOffset) {
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
        id: partitionId,
        init,
        append,
        read
    }

}

module.exports = partitionFactory;