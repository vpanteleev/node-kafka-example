let kafka = require('kafka-node');
let fs = require("fs");
let crypto = require('crypto');
let Producer = kafka.Producer;
let KeyedMessage = kafka.KeyedMessage;

let finishMessage = 'TRANSMIT_IS_OVER';
let topic = 'topic1';
let partition = 0;
let attributes = 2;
let kafkaAddress = '192.168.2.228:2181';
let fileName = 'test.pdf';


let client = new kafka.Client(kafkaAddress);
let producer = new Producer(client, { requireAcks: 1 });
let readStream = fs.createReadStream(fileName);

client.once('connect', () => {
    client.loadMetadataForTopics([topic], (error, results) => {
      if (error) {
      	return console.error(error);
      }

      console.log(results[1].metadata);
    });
});

producer.on('ready', function () {
    readStream.on('data', function (chunk) {
        let messages = [chunk.toString('base64')];

        producer.send([{ topic, partition, messages, attributes }], (error, result) => {
            if (error) {
            	return console.error(error);
            }

            console.log(`Message offset - ${result[topic][partition]}`);
        });
    })
});

producer.on('error', function (error) {
  console.log('error', error);
});
