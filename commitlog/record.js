const constants = require("../constants");
var Int64 = require('node-int64')
//8+4+content.length
//8 bytes para offset
//4 bytes para tamaño payload
//content.length para payload
const OFFSET_LENGTH = constants.OFFSET_LENGTH;
const SIZE_LENGTH = constants.SIZE_LENGTH;
const HEADER_LENGTH = constants.HEADER_LENGTH;
//content es un buffer
function createRecord(content){

    let payload = content;
    let offset;

    return {
        setPayload(content){
            payload = content;
        },
        setOffset(o){
            offset = o;
        },
        buffer(){
            //
            let buffer = Buffer.allocUnsafe(HEADER_LENGTH+payload.length);
            //header
            let intOffset = new Int64(offset);
            intOffset.copy(buffer, 0);
            //
            const buffSize = Buffer.allocUnsafe(SIZE_LENGTH);
            buffSize.writeInt32BE(payload.length, 0);
            buffSize.copy(buffer, OFFSET_LENGTH);
            //payload
            // buffer.write(payload, HEADER_LENGTH, payload.length);
            payload.copy(buffer, HEADER_LENGTH);
            //
            return buffer;
        },
        payloadLength(){
            return payload.length;
        },
        size(){
            return HEADER_LENGTH + payload.length;
        }
    }

}

let parser = function(buffer){
    // let position = 0;
    // let offset = new Int64(buffer.slice(position, position+OFFSET_LENGTH));
    // let size = buffer.readInt32BE(position+OFFSET_LENGTH);//lee 4bytes
    // let payload = buffer.slice(position+HEADER_LENGTH, position+HEADER_LENGTH+size);
    // return {record:{
    //     offset,
    //     size,
    //     payload
    // }};

    let records = [];
    let position = 0;
    let incomplete = false;
    do{
        if(buffer.length<=position+HEADER_LENGTH){
            incomplete = true;
            break;
        }
        let offset = new Int64(buffer.slice(position, position+OFFSET_LENGTH));
        let size = buffer.readInt32BE(position+OFFSET_LENGTH);//lee 4bytes
        if(buffer.length<position+HEADER_LENGTH+size){
            incomplete = true;
            break;
        }
        let payload = buffer.slice(position+HEADER_LENGTH, position+HEADER_LENGTH+size);
        records.push({
            offset,
            size,
            payload
        });
        position = position + HEADER_LENGTH + size;
    }while(position<buffer.length);

    let result = {records, incomplete};
    if(incomplete){
        let incompleteSlice = buffer.slice(position);
        result.incompleteSlice = incompleteSlice;
    }
    return result;
}

function parseRecords(data){
    let buffer = Buffer.from(data);
    let records = [];
    let position = 0;
    do{
        console.log("position is "+position)
        let offset = new Int64(buffer.slice(position, position+OFFSET_LENGTH));
        let size = buffer.readInt32BE(position+OFFSET_LENGTH);//lee 4bytes
        let payload = buffer.slice(position+HEADER_LENGTH, position+HEADER_LENGTH+size);
        records.push({
            offset,
            size,
            payload
        });
        position = position + HEADER_LENGTH + size;
        
    }while(position<buffer.length);

    return records;
}

let jsonParser = function(data) {
    let records = parseRecords(data);
    let json = [];
    for(let i in records){
        let record = records[i];
        json.push(JSON.parse(record.payload.toString('utf8')));
    }
    return json;
}

module.exports = {createRecord, parseRecords, jsonParser, parser}
