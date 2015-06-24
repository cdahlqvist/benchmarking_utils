
module.exports.log_alias_stats = function(esClient, alias, callback) {
  esClient.indices.existsAlias({ name: alias }, function (err, resp) {   
    if(resp) {
      esClient.indices.stats({
        index: alias,
        metric: 'store,docs'
      }, function (err, resp) {
        log_aggregated_index_stats(resp, alias);
        callback();
      });
    } else {
    	var log_msg = 'Alias or index ' + alias + ' does not currently exist.';
    	log(log_msg);
    	callback();
    }
  });
}

module.exports.restore_snapshot = function(esClient, repository, snapshot_id, index, alias, callback) {
  var new_index_name = index + '_' + (new Date).getTime();

  esClient.snapshot.restore({ waitForCompletion: false,
                              repository: repository,
                              snapshot: snapshot_id,
                              verify: false,
                              body: {
                                indices: index,
                                include_global_state: false,
                                rename_pattern: index,
                                rename_replacement: new_index_name
                             }
  }, function (err, resp) {
    if (err) {
      log('Error restoring snapshot.');
      callback(true);
    } else {
      log('Wait for recovery to complete...');
      wait_for_completion(esClient, new_index_name, alias, callback);
    }
  });
}

function wait_for_completion(esClient, index_name, alias, callback) {
  esClient.indices.recovery({ index: index_name, detailed: true }, function(err, resp) {
    if(err) {
      log('Error checking recovery status. Terminating.');
      callback();
    } else {
      if(is_completed(resp)) {

      	add_index_to_alias(esClient, index_name, alias, callback);
      } else {
        setTimeout(wait_for_completion, 10000, esClient, index_name, alias, callback);
      }
    }
  });
}

function is_completed(response) {
  var completed = true;

  for (var index in response) {
    var shard_list = response[index].shards;
    shard_list.forEach(function(shard_status) {
    	if (shard_status.stage != 'DONE') {
    		completed = false;
    	}
    });
  }

  return completed;
}

function add_index_to_alias (esClient, index_name, alias, callback) {
  esClient.indices.putAlias({ index: index_name, name: alias }, function (err, resp) {
    if (err) {	
      log('Error adding newly restored index ' + index_name + ' to alias ' + alias);
      callback(true);
    } else {
      log('Successfully added newly restored index ' + index_name + ' to alias ' + alias);
      callback(false);
    }
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
