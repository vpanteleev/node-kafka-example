var fs = require('fs');
let kafka = require('kafka-node');
var StringDecoder = require('string_decoder').StringDecoder;
var decoder = new StringDecoder('utf8');

let kafkaAddress = '192.168.2.228:2181';
let fileName = 'recived.pdf';
let topic = 'topic1';
let partition = 0;
let finishMessage = 'TRANSMIT_IS_OVER';

let Consumer = kafka.Consumer;
let client = new kafka.Client(kafkaAddress);
let wstream = fs.createWriteStream(fileName);


let consumer = new Consumer( client, [{ topic, partition }], { autoCommit: true });

consumer.on('message', function (message) {
    console.log(`Message offset - ${message.offset}`);

    let data = new Buffer(message.value, 'base64')
    wstream.write(data);
});
