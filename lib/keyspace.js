// node modules
var nmCassandra = require('cassandra-driver');
var nm_ = require('underscore');

// lib
var lErrors = require('./errors');
var lHelpers = require('./helpers');
var lReplicationStrategies = require('./replication_strategies');

// expects: (dakota{Dakota}, client{Cassandra.Client}, name{String}, replication{Object}, durableWrites{Boolean}, options{Object}?)
// returns: undefined
function Keyspace(dakota, client, name, replication, durableWrites, options) {
  if (!(dakota instanceof require('./index'))) { // need to require here
    throw new lErrors.Keyspace.InvalidArgument('Argument should be a Dakota.');
  }
  else if (!(client instanceof nmCassandra.Client)) {
    throw new lErrors.Keyspace.InvalidArgument('Argument should be a Cassandra.Client.');
  }
  else if (!nm_.isString(name)) {
    throw new lErrors.Keyspace.InvalidArgument('Argument should be a string.');
  }
  else if (!lHelpers.isHash(replication)) {
    throw new lErrors.Keyspace.InvalidArgument('Argument should be a {}.');
  }
  else if (!nm_.isBoolean(durableWrites)) {
    throw new lErrors.Keyspace.InvalidArgument('Argument should be a boolean.');
  }
  else if (!nm_.isUndefined(options) && !lHelpers.isHash(options)) {
    throw new lErrors.Keyspace.InvalidArgument('Argument should be a {}.');
  }
  else {
    this._dakota = dakota;
    this._client = client;
    this._name = name;
    this._replication = replication;
    this._durableWrites = durableWrites;
    this._options = options;
  }
}

// =================
// = Ensure Exists =
// =================

// expects: (function(err{Error}), options{Object}?)
// options:
//          alter{Boolean} : alter keyspace if replication or durableWrites mismatch
// returns: undefined
Keyspace.prototype.ensureExists = function(callback, options) {
  var self = this;
  
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Keyspace.InvalidArgument('Argument should be a function.');
  }
  else if (!nm_.isUndefined(options) && !lHelpers.isHash(options)) {
    throw new lErrors.Keyspace.InvalidArgument('Argument should be a {}.');
  }
  options = nm_.extend({ alter: false }, options);
  
  this.selectSchema(function(err, result) {
    if (err) {
      throw new lErrors.Keyspace.SelectSchemaError('Error occurred trying to select schema: ' + err + '.');
    }
    else if (!result || !result.rows) {
      throw new lErrors.Keyspace.SelectSchemaError('Select schema returned no result or no rows.');
    }
    else {
      
      // create keyspace
      if (result.rows.length === 0) {
        console.log('Creating keyspace: ' + self._name + '.');
        self.create(function(err, result) {
          if (err) {
            throw new lErrors.Keyspace.CreateError('Create keyspace failed: ' + err + '.');
          }
          else {
            callback(err);
          }
        });
      }
      
      // compare schema to existing keyspace
      else {
        
        // diff replication strategy
        var row = result.rows[0];
        var differentReplicationStrategy = false;
        if (row.strategy_class !== lReplicationStrategies.STRATEGIES[self._replication.class]) {
          differentReplicationStrategy = true;
        }
        else {
          var strategyOptions = JSON.parse(row.strategy_options);
          nm_.each(self._replication, function(value, key) {
            if (key !== 'class') {
              if (nm_.isUndefined(strategyOptions[key])) {
                differentReplicationStrategy = true;
              }
              else if (strategyOptions[key] !== value.toString()) { // values are stored as strings in schema
                differentReplicationStrategy = true;
              }
            }
          });
        }
        
        // diff durable writes
        var differentDurableWrites = false;
        if (row.durable_writes !== self._durableWrites) {
          differentDurableWrites = true;
        }
        
        // log
        if (differentReplicationStrategy) {
          console.log('Different replication strategy found for existing keyspace: ' + self._name + '.');
        }
        if (differentDurableWrites) {
          console.log('Different durable writes value found for existing keyspace: ' + self._name + '.');
        }
        
        // fix
        if (options.alter && (differentReplicationStrategy || differentDurableWrites)) {
          console.log('Altering keyspace to match schema...');
          self.alter(self._replication, self._durableWrites, function(err) {
            if (err) {
              throw new lErrors.Keyspace.FixError('Alter keyspace failed: ' + err + '.');
            }
            else {
              callback();
            }
          });
        }
        else {
          callback();
        }
        
      }
      
    }
  });
};

// =================
// = Select Schema =
// =================

// expects: (function(err{Error}, result))
// returns: undefined
Keyspace.prototype.selectSchema = function(callback) {
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Keyspace.InvalidArgument('Argument should be a function.');
  }
  
  var query = {
    query: 'SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ? ALLOW FILTERING',
    params: [this._name],
    prepare: true
  };
  
  this._client.execute(query.query, query.params, { prepare: query.prepare }, function(err, result) {
    callback(err, result);
  });
};

// ==========
// = Create =
// ==========

// expects: (function(err{Error}, result), options{Object}?)
// options:
//          ifNotExists{Boolean} : add IF NOT EXISTS property to query
// returns: undefined
Keyspace.prototype.create = function(callback, options) {
  if (!nm_.isFunction(callback)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a function.');
  }
  else if (!nm_.isUndefined(options) && !lHelpers.isHash(options)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a {}.');
  }
  options = nm_.extend({ ifNotExists: false }, options);
  
  var query = {
    query: 'CREATE KEYSPACE',
    params: [],
    prepare: true
  };
  
  if (options.ifNotExists) {
    query.query += ' IF NOT EXISTS';
  }
  
  concatBuilders.call(this, [buildKeyspaceName, buildReplication, buildDurableWrites], query);
  
  this._client.execute(query.query, query.params, { prepare: query.prepare }, function(err, result) {
    callback(err, result);
  });
};

// ========
// = Drop =
// ========

// expects: (function(err{Error}, result), options{Object}?)
// options:
//          ifExists{Boolean} : add IF EXISTS property to query
// returns: undefined
Keyspace.prototype.drop = function(callback, options) {
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Keyspace.InvalidArgument('Argument should be a function.');
  }
  else if (!nm_.isUndefined(options) && !lHelpers.isHash(options)) {
    throw new lErrors.Keyspace.InvalidArgument('Argument should be a {}.');
  }
  options = nm_.extend({ ifExists: false }, options);
  
  var query = {
    query: 'DROP KEYSPACE',
    params: [],
    prepare: true
  };
  
  if (options.ifExists) {
    query.query += ' IF EXISTS';
  }
  
  concatBuilders.call(this, [buildKeyspaceName], query);
  
  this._client.execute(query.query, query.params, { prepare: query.prepare }, function(err, result) {
    callback(err, result);
  });
};

// =========
// = Alter =
// =========

// expects: (replication{Object}, durableWrites{Boolean}, function(err{Error}, result))
// returns: undefined
Keyspace.prototype.alter = function(replication, durableWrites, callback) {
  if (!nm_.isNull(replication) && !lHelpers.isHash(replication)) {
    throw new lErrors.Keyspace.InvalidArgument('Argument should be a {} or null.');
  }
  else if (!nm_.isNull(durableWrites) && !nm_.isBoolean(durableWrites)) {
    throw new lErrors.Keyspace.InvalidArgument('Argument should be a boolean or null.');
  }
  else if (!nm_.isFunction(callback)) {
    throw new lErrors.Keyspace.InvalidArgument('Argument should be a function.');
  }
  
  var query = {
    query: 'ALTER KEYSPACE',
    params: [],
    prepare: true
  };
  
  concatBuilders.call(this, [buildKeyspaceName], query);
  
  var clause = '';
  if (!nm_.isNull(replication)) {
    clause += ' WITH REPLICATION = ' + lReplicationStrategies.replicationToString(replication);
  }
  if (!nm_.isNull(durableWrites)) {
    if (clause.length > 0) {
      clause += ' AND';
    }
    else {
      clause += ' WITH';
    }
    clause += ' DURABLE_WRITES = ' + durableWrites;
  }
  query.query += clause;
  
  this._client.execute(query.query, query.params, { prepare: query.prepare }, function(err, result) {
    callback(err, result);
  });
};

// ============
// = Building =
// ============

// expects: ([{ clause: clause{String}, params: [] }, ...], *{ query: query{String}, params: [], prepare: prepare{Boolean} })
// returns: undefined
function concatBuilders(builders, query) {
  var self = this;
  
  nm_.each(builders, function(builder) {
    var result = builder.call(self);
    if (result.clause.length > 0) {
      query.query += ' ' + result.clause;
      query.params = query.params.concat(result.params);
    }
  });
}

function buildKeyspaceName() {
  var clause = this._name;
  var params = [];
  return { clause: clause, params: params };
}

function buildReplication() {
  var clause = 'WITH REPLICATION = ' + lReplicationStrategies.replicationToString(this._replication);
  var params = [];
  return { clause: clause, params: params };
}

function buildDurableWrites() {
  var clause = 'AND DURABLE_WRITES = ' + this._durableWrites;
  var params = [];
  return { clause: clause, params: params };
}

module.exports = Keyspace;