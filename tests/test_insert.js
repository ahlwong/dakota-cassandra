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

var u = new User({ email:'alex@uproar.xyz', id: nmDakota.generateUUID() });
u.save(function(err, result) {
  console.log(err);
  console.log(result);
  
  u.delete(function(err, result) {
    console.log(err);
    console.log(result);
  });
});

module.exports = null;