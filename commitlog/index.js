let fs = require('fs');
let Int64 = require('node-int64')
const constants = require("../constants");

const entryFactory = function(offset, position, size){
    //offset(8B)position(8B)size(4B)
    let buffer = Buffer.allocUnsafe(8+8+4);
    let intOffset = new Int64(offset);
    intOffset.copy(buffer, 0);
    let intPosition = new Int64(position);
    intPosition.copy(buffer, 8);
    const buffSize = Buffer.allocUnsafe(4);
    buffSize.writeInt32BE(size, 0);
    buffSize.copy(buffer, 8+8);
    return buffer;
}

const indexFactory = function(path, baseOffset){

    const indexFilePath = path+baseOffset+constants.INDEX_FILE_SUFFIX;

    let indexMap = {};

    let lookup = function(offset){
        let entry = indexMap[offset];
        if(entry){
            return {entry};
        }else{
            return {err:"offset not found"}
        }
    }

    let index = function(offset, position, size){
        return new Promise((resolve, reject)=>{
            let entry = entryFactory(offset, position, size);
            let writeStream = fs.createWriteStream(indexFilePath, {flags:"a"});
            writeStream.write(entry, e=>{
                if(e){
                    reject(e);
                }else{
                    indexMap[offset] = {offset, position, size};
                    resolve();
                }
            });
        });
    }

    return {
        lookup,
        index
    };

}

module.exports = indexFactory;