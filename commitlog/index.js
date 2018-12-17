let fs = require('fs');
let Int64 = require('node-int64')
const constants = require("../constants");
const indexEntrySize = 8+8+4;
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

    const filePath = path+baseOffset+constants.INDEX_FILE_SUFFIX;

    const indexCache = {};

    let cache = function(offset, position, size){
        indexCache[offset] = {offset, position, size};
    }

    let loadCache = function(){
        return new Promise((resolve, reject)=>{
            let reader = fs.createReadStream(filePath);
            var bufs = [];
            reader.on('error',(err)=>{
                reject(err);
            })
            reader.on('data', function(d){ bufs.push(d); });
            reader.on('end', function(){
                var buffer = Buffer.concat(bufs);
                var position = 0;
                let segmentLength = 0;
                let lastOffset = 0;
                do{
                    var intOffset = new Int64(buffer.slice(position, position+8));
                    let offset = intOffset.toNumber();
                    var intFilePosition = new Int64(buffer.slice(position+8, position+8+8));
                    var filePosition = intFilePosition.toNumber();
                    var size = buffer.readInt32BE(position+8+8);//lee 4bytes
                    cache(offset, filePosition, size);
                    position = position + indexEntrySize;
                    segmentLength += size;
                    lastOffset = offset;
                }while(position<buffer.length);	  
                let nextOffset = lastOffset + 1;  
                resolve({segmentLength, nextOffset});
            });
        });
    }

    let init = function(){
        return new Promise((resolve, reject)=>{
            fs.stat(filePath, (err)=>{
                if(err == null) {
                    loadCache()
                    .then((status)=>{
                        resolve(status);
                    })
                    .catch(err=>{
                        reject(err);
                    })
                } else if(err.code === 'ENOENT') {
                    // file does not exist
                    resolve({segmentLength:0, nextOffset:baseOffset});
                } else {
                    reject(err);
                }
            });
        });
    }

    let index = function(offset, position, size){
        return new Promise((resolve, reject)=>{
            fs.createWriteStream(filePath, {flags:"a"})
                .write(entryFactory(offset, position, size), (err)=>{
                    if(err){
                        reject(err);
                        return;
                    }
                    cache(offset, position, size);
                    resolve();
                });
        });
    }

    let lookup = function(offset) {
        let entry = indexCache[offset];
        if(!entry){
            return {err:{code:"EONF", msg:"Offset not found"}};
        }
        return {entry};
    }

    return {
        init,
        index,
        lookup
    };

}

module.exports = indexFactory;