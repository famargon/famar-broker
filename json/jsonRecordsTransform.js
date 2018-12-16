const { Transform } = require('stream');
const recordParser = require('../commitlog/record').parser;;

let recordsToJsonTransformFactory = function(){

    let first = true;
    const incompleteChunks = [];

    const recordsToJson = new Transform({
        writableObjectMode: true,

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
                    if(first){
                        first = false;
                    }else{
                        this.push(',');
                    }
                    this.push(result.records[i].payload.toString())
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

module.exports = recordsToJsonTransformFactory;


// const commaSplitter = new Transform({
//   readableObjectMode: true,
//   transform(chunk, encoding, callback) {
//     this.push(chunk.toString().trim().split(','));
//     callback();
//   }
// });
// const arrayToObject = new Transform({
//   readableObjectMode: true,
//   writableObjectMode: true,
//   transform(chunk, encoding, callback) {
//     const obj = {};
//     for(let i=0; i < chunk.length; i+=2) {
//       obj[chunk[i]] = chunk[i+1];
//     }
//     this.push(obj);
//     callback();
//   }
// });
// const objectToString = new Transform({
//   writableObjectMode: true,
//   transform(chunk, encoding, callback) {
//     this.push(JSON.stringify(chunk) + '\n');
//     callback();
//   }
// });
// process.stdin
//   .pipe(commaSplitter)
//   .pipe(arrayToObject)
//   .pipe(objectToString)
//   .pipe(process.stdout)