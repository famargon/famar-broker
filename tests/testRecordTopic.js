var topicManager = require('./topic-manager');
var recordFactory = require("./record").createRecord;
var parseRecords = require('../commitlog/record').parseRecords;

var record = recordFactory(JSON.stringify({text:"hello record"}));
record.setOffset(0);

topicManager.add("testRecordTopic",record.buffer(),()=>{
	
	record = recordFactory(JSON.stringify({text:"hello record"}));
	record.setOffset(1);
	
	topicManager.add("testRecordTopic", record.buffer(), ()=>{
		
	    var reader = topicManager.read("testRecordTopic");
	    // var data = []
	    // reader.on("data",(chunk)=>{
	    //     console.log(chunk.toString());
	    // })
	    var bufs = [];
	    reader.on('data', function(d){ bufs.push(d); });
	    reader.on('end', function(){
	        var buf = Buffer.concat(bufs);
	        console.log(parseRecords(buf))
	    });
	    
	});
	
});