const fs = require('fs');
const parse = require('csv-parse');
const transform = require('stream-transform');
const request = require('request');

urlGenerator = function(record, callback){
    setTimeout(function(){
      const odsCode = record[0];
      const active = record[12];
      const orgSubType = record[13];

      let url = "";
      if (active === 'A' && orgSubType === '1') {
        url = `https://api.nhs.uk/organisations/${odsCode}`;
        callback(null, url + '\n');
      }
    }, 500);
}

orgDetailsRetriever = function (url, callback) {
	setTimeout( function() {
		request(url, (err, res, body) => {
			if (err) {
        callback(null, `Download of ${url} encountered an error: ${err}\n`);
			} else {
				switch (res.statusCode) {
					case 200:
            callback(null, body);
						break;
					case 404:
            callback(null, `404 ${url}`);
						break;
					case 500:
            callback(null, `500 ${url}`);
						break;
					default:
						callback(null, `Download of ${url} returned ${res.statusCode}`);
				}
			}
		});

	}, 500);

}

const parser = parse({delimiter: ','})
const input = fs.createReadStream('./edispensary-50.csv');

const urlTransformer = transform(urlGenerator, {parallel: 5});
const pharmacyTransformer = transform(orgDetailsRetriever, {parallel: 5});

input.pipe(parser).pipe(urlTransformer).pipe(pharmacyTransformer).pipe(fs.createWriteStream('pharmacy-list.json'));

