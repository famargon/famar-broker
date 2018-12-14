var recordFactory = require("./record");

var record = recordFactory(JSON.stringify({text:"hello record"}));

record.setOffset(0);

console.log(record.buffer().toString());