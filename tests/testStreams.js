var fs = require("fs");

const DATA_PATH = "D:\\DEVELOP\\famar-broker\\data\\";
function testWrite(topic, msg1, msg2) {
    var path = DATA_PATH+topic+".log";
    var writeStream = fs.createWriteStream(path, {flags:"a"});
    writeStream.write(msg1,(e=>{
        if(e){
            console.error(e);
        }else{
            writeStream.write(msg2,()=>{
                var readStream = fs.createReadStream(path, {start:msg1.length, end:msg1.length+msg2.length});
                readStream.on("data",(data)=>{
                    console.log(data.toString());
                });
            })
        }
    }));
}

// testWrite("test1", Buffer.from("primera prueba mensage numero 1"), Buffer.from("este es el mensaje numero 2"))