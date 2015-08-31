// node modules
var nmCassandra = require('cassandra-driver');
var nmDakota = require('../index');
var nmKeyspace = require('../lib/keyspace');

// connection
var dakota = require('./connection');

// =================
// = Ensure Exists =
// =================

// copy connection contactPoints, ignore keyspace
var connection = {};
connection.contactPoints = dakota._options.connection.contactPoints;

var client = new nmCassandra.Client(connection);

// keyspace needs a client without a keyspace defined
var keyspace = new nmKeyspace(dakota, client, 'uproar_dev_test', dakota._options.keyspace.replication, dakota._options.keyspace.durableWrites);

// keyspace.create(function(err, result) {
//   console.log({
//     action: 'CREATE',
//     err: err,
//     result: result
//   });
// }, { ifNotExists: true });

keyspace.ensureExists(function(err, result) {
  console.log({
    action: 'ENSURE EXISTS',
    err: err,
    result: result
  });
  
  client.shutdown(function(err) {
    if (err) {
      console.log(err);
    }
  });
  
}, { alter: false });

module.exports = null;