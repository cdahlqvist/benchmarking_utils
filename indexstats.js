
var elasticsearch = require('elasticsearch');

var optimist = require('optimist')
  .usage('Simple utility used to retrieve statistics for an index or alias.\n\nUsage: $0 [options]')
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
      describe: 'Name of index or alias to retrieve statistics for.'
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
  console.log('Error: index argumant not provided.');
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

log_stats(esClient, argv.index, function() {process.exit();});


function log_stats(esClient, alias, callback) {
  esClient.indices.stats({
    index: alias,
    metric: 'store,docs'
  }, function (err, resp) {
    log_aggregated_index_stats(resp, alias);
    callback();
  });
}

function log_aggregated_index_stats(response, alias) {
  var stats = response['indices'];
  var docs = 0;
  var primary_size = 0;
  var total_size = 0;

  for(var index in stats) {
    docs += (stats[index].primaries.docs.count - stats[index].primaries.docs.deleted) / 1000000;
    primary_size += stats[index].primaries.store.size_in_bytes / (1024 * 1024 * 1024);
    total_size += stats[index].total.store.size_in_bytes / (1024 * 1024 * 1024);
  }

  log('Alias/index ' + alias + ' => documents: ' + docs.toFixed(3) + 'M primary size: ' + primary_size.toFixed(3) + 'GB total size: ' + total_size.toFixed(3) + 'GB');
}

function log(msg) {
  console.log('%s %s', new Date().toISOString(), msg);
}








