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
        size(){
            return payload.length;
        }
    }

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

module.exports = {createRecord, parseRecords}
