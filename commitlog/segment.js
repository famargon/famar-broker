var fs = require('fs');
const indexFactory = require('./index');
const constants = require("../constants");

module.exports = function(path, baseOffset, maxSegmentBytes){
    if(!maxSegmentBytes){
        maxSegmentBytes = 5000000; //5 MBytes
    }

    const segmentFilePath = path+baseOffset+constants.LOG_FILE_SUFFIX;

    let firstOffset = baseOffset;
    let nextOffset = baseOffset;
    let segmentLength = 0;

    const index = indexFactory(path, baseOffset);

    let append = function(record){
        return new Promise((resolve, reject)=>{
            let writeStream = fs.createWriteStream(segmentFilePath, {flags:"a"});
            let recordPosition = segmentLength;
            writeStream.write(record,e=>{
                if(e){
                    reject(e);
                }else{
                    nextOffset++;
                    segmentLength += record.length;
                    resolve(recordPosition);
                }
            });
        });
    }

    let read = function(position, bytesToRead){
        if(!bytesToRead){
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

    return {
        firstOffset,
        append,
        read,
        getNextOffset,
        index,
        size
    }

}