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

// count
User.count(function(err, result){
  console.log(err);
  console.log(result);
});

module.exports = User;