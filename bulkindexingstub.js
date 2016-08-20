var http = require('http');

var optimist = require('optimist')
  .usage('Simple utility used to duplicate indices based on snapshots.\n\nUsage: $0 [options]')
  .options({
    port: {
      alias: 'p',
      describe: 'Port the process should bind to. [Default: 9200]',
      type: 'integer',
      default: 9200
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
  } else {
      res.writeHead(500, {'Content-Type': 'text/plain'});
      res.end("Operation not supported.");
  }
});

server.listen(argv.port, function() {
    console.log('Listening on port: ' + argv.port);
});

function process_bulk(index_default, type_default, bulk) {
  var op_array = bulk.split(/\r?\n/);

  var response = {took: getRandomInt(5,30), errors: false, items : []};
 
  var op_array_size = op_array.length - 1;

  for(var i = 0; i < op_array_size; i += 2) {
    var r = generate_response(index_default, type_default, JSON.parse(op_array[i]), JSON.parse(op_array[i+1]));
    response.items.push(r);
  }

  return response;
}

function generate_response(index_default, type_default, index_op, data) {
  var index = index_default;
  var type = type_default;
  var id = "";
  var op;

  if (index_op['index']) { 
    op = "index";

    if (index_op['index']['_index']) {
      index = index_op['index']['_index'];
    }

    if (index_op['index']['_type']) {
      type = index_op['index']['_type'];
    }

    if (index_op['index']['_id']) {
      id = index_op['index']['_id'];
    }
  }

  if (index_op['create']) { 
    op = "create";

    if (index_op['create']['_index']) {
      index = index_op['create']['_index'];
    }

    if (index_op['create']['_type']) {
      type = index_op['create']['_type'];
    }

    if (index_op['create']['_id']) {
      id = index_op['create']['_id'];
    }
  }

  if (id == "") {
    id = generate_id();
  }

  var response = {
    create : {
      _version : 1,
      _index : index,
      status : 201,
      _id : id,
      _type : type,
      _shards : {
        total : 2,
        failed : 0,
        successful : 2
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
  var rnum = Math.random(0,100000000);
  var id = "AVam-tYRNa8_" + pad(rnum, 8);

  return id;
}

function pad(num, size) {
    var s = "0000000000000" + num;
    return s.substr(s.length-size);
}

function getRandomInt(min, max) {
  min = Math.ceil(min);
  max = Math.floor(max);
  return Math.floor(Math.random() * (max - min)) + min;
}
