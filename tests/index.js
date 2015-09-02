// node modules
var nmCassandra = require('cassandra-driver');
var nmDakota = require('../index');
var nmKeyspace = require('../lib/keyspace');
var nmLogger = require('../lib/logger');
var nmSchema = require('../lib/schema');
var nmTable = require('../lib/table');
var nmUserDefinedType = require('../lib/user_defined_type');
var nmWhen = require('when');
var nm_ = require('underscore');

// ================
// = Tests to Run =
// ================
var RUN_TESTS = {
  keyspaces: false,
  tables: false,
  models: false,
  queries: false,
  userDefinedTypes: false,
  complexTypes: true
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
  },
  
  // logging
  logging: {
    level: 'info'
  }
  
};

var userDefinedTypes = {
  address: require('./user_defined_types/address')
};

var dakota = new nmDakota(options, userDefinedTypes);

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
  var keyspace = new nmKeyspace(client, 'dakota_test_keyspace', replication, durableWrite);
  
  // create if doesn't exist
  keyspace.create(function(err) {
    if (err) {
      nmLogger.error('Error creating keyspace: ' + err + '.');
    }
    else {
      nmLogger.info('Created keyspace successfully.');
      
      // alter keyspace's replication strategy and durable writes
      keyspace.alter({ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }, false, function(err) {
        if (err) {
          nmLogger.error('Error altering keyspace: ' + err + '.');
        }
        else {
          nmLogger.info('Altered keyspace successfully.');
          
          // drop keyspace if exists
          keyspace.drop(function(err) {
            if (err) {
              nmLogger.error('Error dropping keyspace: ' + err + '.');
            }
            else {
              nmLogger.info('Dropped keyspace successfully.');
              
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
  var schema = new nmDakota.Schema(dakota, schemaDefinition, {});
  var table = new nmTable(dakota, 'user_tests', schema, {});
  
  // drop table
  table.drop(function(err, result) {
    if (err) {
      nmLogger.error('Error dropping table: ' + err + '.');
    }
    else {
      nmLogger.info('Dropped table successfully.');
      
      // create table if not exists
      table.create(function(err) {
        if (err) {
          nmLogger.error('Error creating table: ' + err + '.');
        }
        else {
          nmLogger.info('Created table successfully.');
          
          // retrieve table schema from system table
          table.selectSchema(function(err) {
            if (err) {
              nmLogger.error('Error getting table schema: ' + err + '.');
            }
            else {
              nmLogger.info('Retrieved table schema successfully.');
              
              // alter table
              table.addColumn('new_column', 'map<text,text>', function(err) {
                if (err) {
                  nmLogger.error('Error adding column: ' + err + '.');
                }
                else {
                  nmLogger.info('Added column successfully.');
                  
                  // alter table
                  table.renameColumn('id', 'id_new', function(err) {
                    if (err) {
                      nmLogger.error('Error renaming column: ' + err + '.');
                    }
                    else {
                      nmLogger.info('Renamed column successfully.');
                      
                      // alter table
                      table.alterType('new_column', 'map<blob,blob>', function(err) {
                        if (err) {
                          nmLogger.error('Error changing type of column: ' + err + '.');
                        }
                        else {
                          nmLogger.info('Changed type of column successfully.');
                          
                          // alter table
                          table.dropColumn('new_column', function(err) {
                            if (err) {
                              nmLogger.error('Error dropping column: ' + err + '.');
                            }
                            else {
                              nmLogger.info('Dropped column successfully.');
                              
                              // drop table
                              table.drop(function(err, result) {
                                if (err) {
                                  nmLogger.error('Error dropping table: ' + err + '.');
                                }
                                else {
                                  nmLogger.info('Dropped table successfully.');
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
    }
  }, { ifExists: true });
  
})(RUN_TESTS.tables);

// ==========
// = Models =
// ==========
(function(run) {
  if (!run) {
    return;
  }
  
  var User = require('./models/user')(dakota);
  
  User.findOne({ id: nmDakota.generateUUID(), name: 'asdf' }, function(err, user) {
    if (err) {
      nmLogger.error('Error finding one: ' + err + '.');
    }
    else {
      nmLogger.info('Successfully called findOne');
      
      User.find({ id: nmDakota.generateUUID(), name: 'asdf' }, function(err, users) {
        if (err) {
          nmLogger.error('Error finding: ' + err + '.');
        }
        else {
          nmLogger.info('Successfully called find');
        }
      });
    }
  });
  
  for (var i = 0; i < 5; i++) {
    (function(i) {
      var user = new User({ id: nmDakota.generateUUID(), name: 'test name', loc: 'San Francisco', email: 'test@test.test' });
      user.save(function(err) {
        if (err) {
          nmLogger.error('Error creating user: ' + err + '.');
        }
        else {
          nmLogger.info('Created user ' + i + ' successfully.');
          
          if (i == 4) {
            
            // test method and static methods
            user.greet();
            User.greet();
            
            // count
            User.count(function(err, count){
              if (err) {
                nmLogger.error('Error counting users.');
              }
              else {
                nmLogger.info('Successfully counted users: ' + count);
              }
            });
            
            User.all(function(err, result) {
              if (err) {
                nmLogger.error('Error retrieving all users.');
              }
              else {
                nm_.each(result, function(u, index) {
                  if (!(u instanceof User)) {
                    nmLogger.error('Error result object not instance of User.');
                  }
                });
                nmLogger.info('Successfully retrieved all users: ' + result.length);
              }
            });
            
            // first
            User.first(function(err, result) {
              if (err) {
                nmLogger.error('Error retrieving first user.');
              }
              else {
                if (result && !(result instanceof User)) {
                  nmLogger.error('Error result object not instance of User.');
                }
                nmLogger.info('Successfully retrieved first user.');
                
                // each row
                User.where({ id: result.id, name: result.name }).allowFiltering(true).eachRow(
                  function(n, row) {
                    if (!(row instanceof User)) {
                      nmLogger.error('Error result object not instance of User.');
                    }
                    nmLogger.info('Retrieved row: ' + n);
                  },
                  function(err) {
                    if (err) {
                      nmLogger.error('Error retrieving all users by each row: ' + err + '.');
                    }
                    else {
                      nmLogger.info('Successfully retrieved users by each row.');
                      
                      // user
                      var user = new User({ id: nmDakota.generateUUID(), name: 'dakota user', loc: 'San Francisco', email: 'dakota@dakota.dakota' });
                      user.save(function(err) {
                        if (err) {
                          nmLogger.error('Error creating user: ' + err + '.');
                        }
                        else {
                          user.email = 'dakota@alexanderwong.me';
                          user.age = 17;
                          user.save(function(err) {
                            if (err) {
                              nmLogger.error('Error updating user: ' + err + '.');
                            }
                            else {
                              user.delete(function(err) {
                                if (err) {
                                  nmLogger.error('Error deleting user: ' + err + '.');
                                }
                                else {
                                  nmLogger.info('Successfully deleted user.');
                                  
                                  // delete all
                                  User.deleteAll(function(err) {
                                    if (err) {
                                      nmLogger.error('Error deleting all users: ' + err + '.');
                                    }
                                    else {
                                      nmLogger.info('Successfully deleted all users.');
                                    }
                                  });
                                }
                              });
                            }
                          });
                        }
                      });
                      
                    }
                  }
                );
              }
            });
          }
        }
      });
    })(i);
  }
  
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

// ======================
// = User Defined Types =
// ======================
(function(run) {
  if (!run) {
    return;
  }
  
  var address = new nmUserDefinedType(dakota, 'address', require('./user_defined_types/address'), {});
  
  // delete
  address.drop(function(err, result) {
    if (err) {
      nmLogger.error(err);
    }
    else {
      nmLogger.info('Successfully deleted type.');
      
      // create
      address.create(function(err, result) {
        if (err) {
          nmLogger.error(err);
        }
        else {
          nmLogger.info('Successfully created type.');
          
          // add field
          address.addField('new_field', 'set<int>', function(err, result) {
            if (err) {
              nmLogger.error(err);
            }
            else {
              nmLogger.info('Successfully added field to type.');
              
              // rename
              address.renameField('new_field', 'old_field', function(err, result) {
                if (err) {
                  nmLogger.error(err);
                }
                else {
                  nmLogger.info('Successfully renamed field in type.');
                  
                  // select schema
                  address.selectSchema(function(err, result) {
                    if (err) {
                      nmLogger.error(err);
                    }
                    else {
                      nmLogger.info('Successfully selected schema for type.');
                      
                      // delete
                      address.drop(function(err, result) {
                        if (err) {
                          nmLogger.error(err);
                        }
                        else {
                          nmLogger.info('Successfully deleted type.');
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
    }
  }, { ifExists: true });
  
})(RUN_TESTS.userDefinedTypes);

// =================
// = Complex Types =
// =================
(function(run) {
  if (!run) {
    return;
  }
  
  var User = require('./models/user')(dakota);
  var user = new User({ name: 'Frank', email: 'dakota@dakota.dakota' });
  var address = {
    street: '123 Main Street',
    city: 'San Francisco',
    state: 'California',
    zip: 92210,
    phones: ['(123) 456-7890', '(123) 456-7890'],
    tenants: { 101: 'Bob', 505: 'Mary' }
  };
  user.set({
    address: address,
    tuples: ['my tuple', 77, 'is the best tuple of them all'],
    nestedTuple: [['my tuple', 77, 'is the best tuple of them all'],['my tuple', 77, 'is the best tuple of them all']]
  });
  user.save(function(err) {
    if (err) {
      nmLogger.error(err);
    }
    else {
      nmLogger.info('Saved user successfully.');
      
      // retrieve
      User.first(function(err, result) {
        if (err) {
          nmLogger.error(err);
        }
        else {
          nmLogger.info(result);
        }
      });
    }
  });
  
})(RUN_TESTS.complexTypes);
