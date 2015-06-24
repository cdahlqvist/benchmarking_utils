
var snapshot_utils = require('./lib/snapshot_utils');
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

snapshot_utils.log_alias_stats(esClient, argv.index, function() {process.exoit();});


