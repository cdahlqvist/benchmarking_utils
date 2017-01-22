var http = require('http');
var cluster = require('cluster');
var crypto = require('crypto');
var id_length = 20;
var pool = id_length * 128;
var r = crypto.randomBytes(pool);
var strs = [];
var j = 0;

strs.length = id_length;

var optimist = require('optimist')
  .usage('Simple utility used to efficiently respond successfully to bulk requests.\n\nUsage: $0 [options]')
  .options({
    port: {
      alias: 'p',
      describe: 'Port the process should bind to. [Default: 9200]',
      type: 'integer',
      default: 9200
    },
    interval: {
      alias: 'i',
      describe: 'Reporting interval for statistics in seconds. [Default: 60]',
      type: 'integer',
      default: 60
    },
    workers: {
      alias: 'w',
      describe: 'Number of workers. [Defaults to the number of CPU cores on the host]',
      type: 'integer'
    },
    help: {
      alias: 'h',
      describe: 'Display help message',
      type: 'boolean'
    }
  });

var argv = optimist.argv;

if (argv.help) {
  optimist.showHelp(console.log);
  process.exit();
}

var workers;

if (argv.w == undefined) {
  workers = require('os').cpus().length;
} else {
  workers = argv.w;
}

if(cluster.isMaster) {
    console.log('Master setting up ' + workers + ' workers...');

    for(var i = 0; i < workers; i++) {
        cluster.fork();
    }

    cluster.on('online', function(worker) {
        console.log('Worker ' + worker.process.pid + ' is online');
    });

    cluster.on('exit', function(worker, code, signal) {
        console.log('Worker ' + worker.process.pid + ' died with code: ' + code + ', and signal: ' + signal);
        console.log('Starting a new worker');
        cluster.fork();
    });
} else {
  // Handle web requests
  server = http.createServer( function(req, res) {
    if (req.method == 'POST') {
      var body = '';
      var index = extract_index_from_url(req.url);
      var type = extract_type_from_url(req.url);

      req.on('data', function (data) {
        body += data;
      });
      req.on('end', function () {
        var response = process_bulk(index, type, body);
        res.writeHead(200, {"Content-Type": "application/json"});
        res.end(JSON.stringify(response));       
      });
    } else if (req.method == 'HEAD') {
      res.writeHead(200, {'Content-Type': 'text/plain; charset=UTF-8'});
      res.end();
    } else {
      res.writeHead(500, {'Content-Type': 'text/plain; charset=UTF-8'});
      res.end("Not supported"); 
    }
  });

  server.listen(argv.port, function() {
    console.log('Listening on port: ' + argv.port);
  });
}

function process_bulk(index_default, type_default, bulk) {
  var op_array = bulk.split(/\r?\n/);

  var response = {took: getRandomInt(100,500), errors: false, items : []};

  var op_array_size = op_array.length - 1;
  var i = 0;
  var op;
  var resp;

  do {
    op = JSON.parse(op_array[i]);

    resp = generate_response(index_default, type_default, JSON.parse(op_array[i]));
    response.items.push(resp);

    if (op['index'] || op['create'] || op['update']) {
      i += 2;
    } else {
      i++;
    } 
  }
  while (i < op_array_size);

  return response;
}

function generate_response(index_default, type_default, index_op) {
  var index = index_default;
  var type = type_default;
  var id = "";
  var op;
  var response;

  if (index_op['index']) { 
    op = "index";
  } else if (index_op['create']) {
    op = "create"
  } else if (index_op['update']) {
    op = "update"
  } else if (index_op['delete']) {
    op = "delete"
  }

  if (index_op[op]['_index']) {
    index = index_op[op]['_index'];
  }

  if (index_op[op]['_type']) {
    type = index_op[op]['_type'];
  }

  if (index_op[op]['_id']) {
    id = index_op[op]['_id'];
  }

  if (id == "") {
    id = generate_id();
  }

  if (op == 'index') {
    response = {
      "index" : {
        "_index" : index,
        "_type" : type,
        "_id" : id,
        "_version" : 1,
        "result" : "created",
        "_shards" : {
          "total" : 2,
          "successful" : 1,
          "failed" : 0
        },
        "created" : true,
        "status" : 201
      }
    }
  } else if (op == "create") {
    response = {
      "create" : {
        "_index" : index,
        "_type" : type,
        "_id" : id,
        "_version" : 1,
        "result" : "created",
        "_shards" : {
          "total" : 2,
          "successful" : 1,
          "failed" : 0
        },
        "created" : true,
        "status" : 201
      }
    }
  } else if (op == "update") {
    response = {
      "update" : {
        "_index" : index,
        "_type" : type,
        "_id" : id,
        "_version" : 2,
        "result" : "updated",
        "_shards" : {
          "total" : 2,
          "successful" : 1,
          "failed" : 0
        },
        "status" : 200
      }
    }
  } else {
    response = {
      "delete" : {
        "found" : true,
        "_index" : index,
        "_type" : type,
        "_id" : id,
        "_version" : 3,
        "result" : "deleted",
        "_shards" : {
          "total" : 2,
          "successful" : 1,
          "failed" : 0
        },
        "status" : 200
      }
    }
  }

  return response;
}

function extract_index_from_url(url) {
  var components = url.split('/');

  if(components.length == 3 || components.length == 4) {
    return components[1];
  }

  return "";
}
  
function extract_type_from_url(url) {
  var components = url.split('/');

  if(components.length == 4) {
    return components[2];
  }

  return "";
}

function generate_id() {
  var chi;

  for (chi = 0; chi < id_length; chi++) {
    j++;
    if (j >= r.length) {
      r = crypto.randomBytes(pool);
      j = 0;
    }

    strs[chi] = (r[j] % 16).toString(16);
  }

  return strs.join('');
}

function getRandomInt(min, max) {
  min = Math.ceil(min);
  max = Math.floor(max);
  return Math.floor(Math.random() * (max - min)) + min;
}
