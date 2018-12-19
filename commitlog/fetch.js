const { Transform } = require('stream');
const recordParser = require('./record').parser;

let fetchTransformFactory = function(){

    let first = true;
    const incompleteChunks = [];

    const recordsToJson = new Transform({
        // writableObjectMode: true,

        transform(chunk,encoding, callback){
            if(first){
                this.push('[');
            }
            let recordsChunk = chunk;
            if(incompleteChunks.length>0){
                let swapChunks = [];
                while(incompleteChunks.length !== 0){
                    let incomplete = incompleteChunks.shift();
                    swapChunks.push(incomplete);
                }
                swapChunks.push(chunk);
                recordsChunk = Buffer.concat(swapChunks)
            }
            let result = recordParser(recordsChunk);
            if(result.incomplete){
                incompleteChunks.push(result.incompleteSlice);
            }
            if(result.records.length>0){
                for(let i in result.records){
                    let record = result.records[i];
                    if(first){
                        first = false;
                    }else{
                        this.push(',');
                    }
                    let recordToPush = "{\"offset\":"+record.offset+", \"payload\":"+record.payload.toString()+"}";
                    this.push(recordToPush);
                }
            }
            callback();
        },
        final(callback){
            this.push(']');
            callback();
        }
    });

    return recordsToJson;
}

module.exports.fetchTransformFactory = fetchTransformFactory;