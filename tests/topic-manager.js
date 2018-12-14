var fs = require('fs');

const dataDirectory = __dirname+"/data/";

var writeStreams = {};
var deliveryTracking = {};
var indexes = {};

function add(topic, message, cb){
    let logStream;
    if(writeStreams[topic]){
        logStream = writeStreams[topic];
    }else{
        writeStreams[topic] = fs.createWriteStream(dataDirectory+topic+".log", {flags:"a"});
        logStream = writeStreams[topic];
    }
    if(!indexes[topic]){
        indexes[topic] = [];
    }
    var msgLength = message.length;
    logStream.write(message, () => {
        var msgIndex = indexes[topic].length;
        indexes[topic].push({index:msgIndex, length:msgLength});
        cb();
    });
}

function read(topic, messagesToRead) {
    if(!messagesToRead){
        messagesToRead = 1;
    }
    var startPosition;
    if(deliveryTracking[topic]){
        startPosition = deliveryTracking[topic];
    }else{
        startPosition = 0;
    }
    let stream = fs.createReadStream(dataDirectory+topic+".log");
    return stream;
}

process.on('exit',function(){
    // for(stream in writeStreams){
    //     stream.end();
    // }
});

module.exports = {add, read};
