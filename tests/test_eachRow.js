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

User.eachRow(
  function(n, row) {
    console.log('ROW');
    console.log(n);
    console.log(row);
  },
  function(err) {
    console.log('FINISHED');
    console.log(err);
  }
);

module.exports = null;