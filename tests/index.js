// node modules
var nmCassandra = require('cassandra-driver');
var nmDakota = require('../index');
var nmKeyspace = require('../lib/keyspace');
var nmSchema = require('../lib/schema');
var nmTable = require('../lib/table');
var nmWhen = require('when');

// ================
// = Tests to Run =
// ================
var RUN_TESTS = {
  keyspaces: true,
  tables: false,
  queries: true
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

// ===========
// = Queries =
// ===========

