
var elasticsearch = require('elasticsearch');

var optimist = require('optimist')
  .usage('Simple utility for estimating indexing overhead factor based on sample data.\n\nUsage: $0 [options]')
  .options({
    hosts: {
      alias: 'h',
      describe: 'Host name and port combinations to connect to, e.g. localhost:9200',
      default: 'localhost:9200'
    },
    protocol: {
      alias: 'p',
      describe: 'Protocol to use when connecting to Elasticsearch. [http/https]',
      default: 'http'
    },
    creds: {
      alias: 'c',
      describe: 'user:password credentials when you want to connect to a secured elasticsearch cluster over basic auth.'
    },
    index: {
      alias: 'i',
      describe: 'Name of index to estimate indexing overhead factor for. [Mandatory]'
    },
    samplesize: {
      alias: 's',
      type: 'integer',
      describe: 'Number of random documents the indexing overhead estimate is to be based on.',
      default: 10000
    },
    help: {
      describe: 'Display help message',
      type: 'boolean'
    }
  });

var argv = optimist.argv;

if (argv.help) {
  optimist.showHelp(console.log);
  process.exit();
}

if (!argv.index) {
  console.log('Error: index argument not provided.');
  process.exit();
}

// validate protocol
if (['http','https'].indexOf(argv.protocol) <= -1) {
  console.log('Error: Illegal protocol specified (%s).', argv.protocol);
  show_help();
}

// Create list of host strings
if(argv.creds) {
  argv.hosts = argv.hosts.split(',').map(function urlify(host) { return argv.protocol + '://' + argv.creds + '@' + host; });
} else {
  argv.hosts = argv.hosts.split(',').map(function urlify(host) { return argv.protocol + '://' + host; });
}

// Connect to Elasticsearch
try {
  var esClient = new elasticsearch.Client({
    hosts: argv.hosts,
    min_sockets: 1
  });

  esClient.ping({
    requestTimeout: 30000,
  }, function (error) {
    if (error) {
      console.log('Error connecting to Elasticsearch. Terminating.');
      process.exit();
    } 
  });
}
catch(e){
  console.log('Failed to start Elasticsearch Client: %s', e.message);
  process.exit();
}

estimate_indexing_overhead(esClient, argv.index, argv.samplesize, function() {process.exit();});


function estimate_indexing_overhead(esClient, index, samplesize, callback) {
  esClient.indices.stats({
    index: index,
    metric: 'store,docs'
  }, function (err, resp) {
    if(err) {
      console.log('Error determining average document size on disk: %s', err.message);
      process.exit();
    } else {
      if (index in resp['indices']) {
        var docs = (resp['indices'][index].primaries.docs.count - resp['indices'][index].primaries.docs.deleted);
        var primary_size = resp['indices'][index].primaries.store.size_in_bytes;
        var avg_size = primary_size / docs;
        calculate_and_print_results(esClient, index, samplesize, avg_size, callback);
      } else {
        console.log('Error finding index data for index: %s', index);
        process.exit();
      }
    }
  });
}

function calculate_and_print_results(esclient, index, samplesize, avg_size, callback) {
  esClient.search({
    index: index,
    body: {
      size: samplesize,
      query: {
        function_score: {
          query: {
            match_all: {}
          },
          functions: [
            {
              random_score: { 
                seed: "test" 
              }
            }
          ],
          score_mode: "sum"
        }
      }
    }
  }, function (err, resp) {
    if(err) {
      console.log('Error retrieving sample documents: %s', err.message);
      process.exit();
    } else {
      var json_count = 0;
      var json_size = 0;
      var raw_count = 0;
      var raw_size = 0;

      if(resp.hits.hits.length == 0) {
        console.log('No sample documents found for index: %s', index);
        process.exit();
      }

      for (var i in resp.hits.hits) {
        var obj = resp.hits.hits[i];
        
        if(obj['_source']) {
          json_size += JSON.stringify(obj['_source']).length;
          json_count += 1;

          if(obj['_source']['@message']) {
            raw_size += obj['_source']['@message'].length;
            raw_count += 1;
          }
        }
      }

      if(json_count == 0) {
        console.log('Sample documents did not have _source enabled. Aborting.');
        process.exit();
      }

      var results = {};

      results['target_sample_size'] = samplesize;
      results['real_sample_size'] = resp.hits.hits.length;
      results['json_sample_size'] = json_count;
      results['average_document_disk_size'] = avg_size;
      results['average_document_json_size'] = json_size / json_count;
      results['indexing_overhead_json_to_disk'] = results['average_document_disk_size'] / results['average_document_json_size'];

      if(raw_count > 0) {
        results['raw_sample_size'] = raw_count;
        results['average_document_raw_size'] = raw_size / raw_count;
        results['indexing_overhead_raw_to_json'] = results['average_document_json_size'] / results['average_document_raw_size'];
        results['indexing_overhead_raw_to_disk'] = results['indexing_overhead_raw_to_json'] * results['indexing_overhead_json_to_disk'];
      }

      console.log(JSON.stringify(results));

      callback();
    }
  });
}
