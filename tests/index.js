// node modules
var nmCassandra = require('cassandra-driver');
var nmDakota = require('../index');
var nmKeyspace = require('../lib/keyspace');
var nmSchema = require('../lib/schema');
var nmTable = require('../lib/table');
var nmWhen = require('when');
var nm_ = require('underscore');

// ================
// = Tests to Run =
// ================
var RUN_TESTS = {
  keyspaces: true,
  tables: false,
  models: false,
  queries: false
};

// ===========
// = Connect =
// ===========

var options = {
  
  // connection
  connection: {
    contactPoints: [
      '127.0.0.1'
    ],
    keyspace: 'dakota_test'
  },
  
  // keyspace
  keyspace: {
    replication: { 'class': 'SimpleStrategy', 'replication_factor': 1 },
    durableWrites: true
  }
  
};

var dakota = new nmDakota(options);

// =============
// = Keyspaces =
// =============
(function(run) {
  if (!run) {
    return;
  }
  
  // copy connection contactPoints, ignore keyspace
  var connection = {};
  connection.contactPoints = dakota._options.connection.contactPoints;
  
  var replication = dakota._options.keyspace.replication;
  var durableWrite = dakota._options.keyspace.durableWrites;
  
  // create client with no keyspace, keyspace queries need to run with no keyspace
  var client = new nmCassandra.Client(connection);
  
  // create keyspace object
  var keyspace = new nmKeyspace(dakota, client, 'dakota_test_keyspace', replication, durableWrite);
  
  // create if doesn't exist
  keyspace.create(function(err) {
    if (err) {
      console.log('Error creating keyspace: ' + err + '.');
    }
    else {
      console.log('Created keyspace successfully.');
      
      // alter keyspace's replication strategy and durable writes
      keyspace.alter({ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }, false, function(err) {
        if (err) {
          console.log('Error altering keyspace: ' + err + '.');
        }
        else {
          console.log('Altered keyspace successfully.');
          
          // drop keyspace if exists
          keyspace.drop(function(err) {
            if (err) {
              console.log('Error dropping keyspace: ' + err + '.');
            }
            else {
              console.log('Dropped keyspace successfully.');
              
              client.shutdown();
            }
          }, { ifExists: true });
        }
      });
    }
  }, { ifNotExists: true });
  
})(RUN_TESTS.keyspaces);

// ==========
// = Tables =
// ==========
(function(run) {
  if (!run) {
    return;
  }
  
  // get table instance from User model
  var schemaDefinition = require('./models/user.schema');
  var schema = new nmDakota.Schema(schemaDefinition, {});
  var table = new nmTable(dakota, 'user_tests', schema, {});
  
  // create table if not exists
  table.create(function(err) {
    if (err) {
      console.log('Error creating table: ' + err + '.');
    }
    else {
      console.log('Created table successfully.');
      
      // retrieve table schema from system table
      table.selectSchema(function(err) {
        if (err) {
          console.log('Error getting table schema: ' + err + '.');
        }
        else {
          console.log('Retrieved table schema successfully.');
          
          // alter table
          table.addColumn('new_column', 'map<text,text>', function(err) {
            if (err) {
              console.log('Error adding column: ' + err + '.');
            }
            else {
              console.log('Added column successfully.');
              
              // alter table
              table.renameColumn('new_column', 'old_column', function(err) {
                if (err) {
                  console.log('Error renaming column: ' + err + '.');
                }
                else {
                  console.log('Renamed column successfully.');
                  
                  // alter table
                  table.alterType('old_column', 'map<blob,blob>', function(err) {
                    if (err) {
                      console.log('Error changing type of column: ' + err + '.');
                    }
                    else {
                      console.log('Changed type of column successfully.');
                      
                      // alter table
                      table.dropColumn('old_column', function(err) {
                        if (err) {
                          console.log('Error dropping column: ' + err + '.');
                        }
                        else {
                          console.log('Dropped column successfully.');
                          
                          // drop table
                          table.drop(function(err, result) {
                            if (err) {
                              console.log('Error dropping table: ' + err + '.');
                            }
                            else {
                              console.log('Dropped table successfully.');
                            }
                          });
                        }
                      });
                    }
                  });
                }
              });
            }
          });
        }
      });
    }
  }, { ifNotExists: true });
  
})(RUN_TESTS.tables);

// ==========
// = Models =
// ==========
(function(run) {
  if (!run) {
    return;
  }
  
  var User = require('./models/user')(dakota);
  
  for (var i = 0; i < 5; i++) {
    (function(i) {
      var user = new User({ id: nmDakota.generateUUID(), name: 'test name', loc: 'San Francisco', email: 'test@test.test' });
      user.save(function(err) {
        if (err) {
          console.log('Error creating user: ' + err + '.');
        }
        else {
          console.log('Created user ' + i + ' successfully.');
        }
      });
    })(i);
  }
  
  // count
  User.count(function(err, count){
    if (err) {
      console.log('Error counting users.');
    }
    else {
      console.log('Successfully counted users: ' + count);
    }
  });
  
  User.first(function(err, result) {
    if (err) {
      console.log('Error retrieving first user.');
    }
    else {
      if (result && !(result instanceof User)) {
        console.log('Error result object not instance of User.');
      }
      console.log('Successfully retrieved first user.');
    }
  });
  
  User.all(function(err, result) {
    if (err) {
      console.log('Error retrieving all users.');
    }
    else {
      nm_.each(result, function(u, index) {
        if (!(u instanceof User)) {
          console.log('Error result object not instance of User.');
        }
      });
      console.log('Successfully retrieved all users: ' + result.length);
    }
  });
  
  User.where(email, 'test@test.test').allowFiltering(true).eachRow(
    function(n, row) {
      if (!(row instanceof User)) {
        console.log('Error result object not instance of User.');
      }
      console.log('Retrieved row: ' + n);
    },
    function(err) {
      if (err) {
        console.log('Error retrieving all users by each row: ' + err + '.');
      }
      else {
        console.log('Successfully retrieved users by each row.');
      }
    }
  );
  
  var user = new User({ id: nmDakota.generateUUID(), name: 'dakota user', loc: 'San Francisco', email: 'dakota@dakota.dakota' });
  user.save(function(err) {
    if (err) {
      console.log('Error creating user: ' + err + '.');
    }
    else {
      user.email = 'dakota@alexanderwong.me';
      user.name = 'Dakota Wong';
      user.save(function(err) {
        if (err) {
          console.log('Error updating user: ' + err + '.');
        }
        else {
          user.delete(function(err) {
            if (err) {
              console.log('Error deleting user: ' + err + '.');
            }
            else {
              console.log('Successfully deleted user.');
            }
          });
        }
      });
    }
  });
  
})(RUN_TESTS.models);

// ===========
// = Queries =
// ===========
(function(run) {
  if (!run) {
    return;
  }
  
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
  
})(RUN_TESTS.queries);
