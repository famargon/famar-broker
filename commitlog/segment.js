var fs = require('fs');
const indexFactory = require('./index');
const {LOG_FILE_SUFFIX} = require("../constants");

module.exports = function(path, baseOffset, maxSegmentBytes){
    if(maxSegmentBytes == null){
        maxSegmentBytes = 5000000; //5 MBytes
    }

    const segmentFilePath = path+baseOffset+LOG_FILE_SUFFIX;

    let firstOffset = baseOffset;
    let nextOffset = baseOffset;
    let segmentLength = 0;

    const index = indexFactory(path, baseOffset);

    let append = function(record){
        return new Promise((resolve, reject)=>{
            let writeStream = fs.createWriteStream(segmentFilePath, {flags:"a"});
            let recordPosition = segmentLength;
            nextOffset++;
            segmentLength += record.length;
            writeStream.write(record,e=>{
                if(e){
                    reject(e);
                }else{
                    resolve(recordPosition);
                }
            });
        });
    }

    let read = function(position, bytesToRead){
        if(bytesToRead == null){
            bytesToRead = segmentLength-position;
        }
        return fs.createReadStream(segmentFilePath, {start:position, end:position+bytesToRead});
    }

    let size = function(){
        return segmentLength;
    }

    let getNextOffset = function(){
        return nextOffset;
    }

    let init = function(){
        return new Promise((resolve, reject)=>{
            index.init()
            .then((indexStatus)=>{
                segmentLength = indexStatus.segmentLength;
                nextOffset = indexStatus.nextOffset;
                resolve();
            })
            .catch(err=>{
                reject(err);
            });
        });
    }

    let needsSwap = function(){
        return segmentLength>=maxSegmentBytes;
    }

    return {
        init,
        firstOffset,
        append,
        read,
        getNextOffset,
        index,
        size,
        needsSwap
    }

}