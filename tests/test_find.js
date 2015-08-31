// node modules
var nmCassandra = require('cassandra-driver');
var nmDakota = require('../index');
var nmKeyspace = require('../lib/keyspace');

// connection
var dakota = require('./connection');

// ===========
// = Models =
// ===========
var User = require('./models/user');

User.all(function(err, result) {
  console.log(err);
  console.log(result);
});

User.first(function(err, result) {
  console.log(err);
  console.log(result);
});

module.exports = null;