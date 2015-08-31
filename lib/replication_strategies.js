// node modules
var nm_ = require('underscore');

var exports = {};

exports.STRATEGIES = {
  SimpleStrategy: 'org.apache.cassandra.locator.SimpleStrategy',
  NetworkTopologyStrategy: 'org.apache.cassandra.locator.NetworkTopologyStrategy'
};

// ===========
// = Helpers =
// ===========

// expects: (replcation{Object})
// returns: replicationString{String}
exports.replicationToString = function(replication) {
  var obj = {};
  nm_.each(replication, function(value, key) {
    obj[key] = value.toString();
  });
  return JSON.stringify(obj).replace(/"/g, "'");
};

module.exports = exports;