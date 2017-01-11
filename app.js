const fs = require('fs');
const parse = require('csv-parse');
const transform = require('stream-transform');
const request = require('request');
var options = { db: 'mongodb://localhost:27017/local', collection: 'services' }
var streamToMongo = require('stream-to-mongo')(options);

urlGenerator = function(record, callback){
      const odsCode = record[0];
      const active = record[12];
      const orgSubType = record[13];

      let url = "";
      if (active === 'A' && orgSubType === '1') {
        url = `https://api.nhs.uk/organisations/${odsCode}`;
        // console.log(url);
        callback(null, url + '\n');
      }
}

orgDetailsRetriever = function (url, callback) {
		request(url, (err, res, body) => {
			if (err) {
        console.log(`Download of ${url} returned ${res.statusCode}`);
			} else {
				switch (res.statusCode) {
					case 200:
            callback(null, JSON.parse(body.replace('\"identifier\"', '\"_id\"')));
            // callback(null, body.replace('\"identifier\"', '\"_id\"'));
						break;
					case 404:
            console.log(`404 ${url}`);
						break;
					case 500:
            console.log(`500 ${url}`);
						break;
					default:
						console.log(`Download of ${url} returned ${res.statusCode}`);
				}
			}
		});


}

const input = fs.createReadStream('./edispensary-50.csv');

const parser = parse({delimiter: ','})
const urlTransformer = transform(urlGenerator);
const orgTransformer = transform(orgDetailsRetriever);

// input.pipe(parser).pipe(urlTransformer).pipe(orgTransformer).pipe(fs.createWriteStream('org-list.json'));
input.pipe(parser).pipe(urlTransformer).pipe(orgTransformer).pipe(streamToMongo);

