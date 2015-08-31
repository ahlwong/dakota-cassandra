// node modules
var nmDakota = require('../index');
var nmQuery = require('../lib/query');

// connection
var dakota = require('./connection');

// ===========
// = Queries =
// ===========
var User = require('./models/user');

var results = [];

// SELECT
var query = new nmQuery(User);
query = query.action('select').select('email', 'lucky_numbers').select(['ctime', 'utime']).where('true', 'false').where({ ilove: 'dakota', age: { '$gte' : 5 } }).orderBy('age', '$asc').orderBy({ 'age' : '$desc' }).limit(99).allowFiltering(true);
results.push(query.build());

// UPDATE
query = new nmQuery(User);
query = query.action('update').using('$ttl', 44300).using({'$timestamp':1337}).update('string', 'some string').update({'int': 1337}).update({ilove:{'$set' : 'dakota'}}).update({lucky_numbers:{'$prepend':'a', '$append' : 'b'}}).update({projects:{'$add':'element'}}).where({home:'is where the heart is'}).if({age:{'$gte':5}}).ifExists(true);
results.push(query.build());

// INSERT
query = new nmQuery(User);
query = query.action('insert').insert('email', 'fname').insert({'map':{asd:'1231'}, 'list':[1,2,3]}).ifNotExists(true).using('$ttl', 4430);
results.push(query.build());

// DELETE
query = new nmQuery(User);
query = query.action('delete').select('email', 'fname', 'lname').select(['ctime', 'utime']).where('true', 'false').where({ ilove: 'dakota', age: { '$gte' : 5 } }).using('$timestamp', 555);
results.push(query.build());

module.exports = results;