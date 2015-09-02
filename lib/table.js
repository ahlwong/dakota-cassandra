// node modules
var nmWhen = require('when');
var nm_ = require('underscore');

// lib
var lErrors = require('./errors');
var lHelpers = require('./helpers');
var lLogger = require('./logger');
var lSchema = require('./schema');
var lTableWithProperties = require('./table_with_properties');
var lTypes = require('./types');

// expects: (dakota{Dakota}, name{String}, schema{Schema}, options{Object}?)
// returns: undefined
function Table(dakota, name, schema, options) {
  if (!(dakota instanceof require('./index'))) { // need to require here
    throw new lErrors.Table.InvalidArgument('Argument should be a Dakota.');
  }
  else if (!nm_.isString(name)) {
    throw new lErrors.Table.InvalidArgument('Argument should be a string.');
  }
  else if (!(schema instanceof lSchema)) {
    throw new lErrors.Table.InvalidArgument('Argument should be a Schema.');
  }
  else if (options && !lHelpers.isHash(options)) {
    throw new lErrors.Table.InvalidArgument('Argument should be a {}.');
  }
  
  this._dakota = dakota;
  this._name = name;
  this._schema = schema;
  this._options = options;
}

// =================
// = Ensure Exists =
// =================

// expects: (function(err{Error}), options{Object}?)
// options:
//          recreate{Boolean}              : drop and recreate table on schema mismatch, takes precedence over following options
//          fixMismatch[$recreate]{String} : recreate columns where types don't match schema
//          removeExtra{Boolean}           : remove extra columns not in schema
//          addMissing{Boolean}            : add columns in schema that aren't in table
// returns: undefined
Table.prototype.ensureExists = function(callback, options) {
  var self = this;
  
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Table.InvalidArgument('Argument should be a function.');
  }
  else if (options && !lHelpers.isHash(options)) {
    throw new lErrors.Table.InvalidArgument('Argument should be a {}.');
  }
  options = nm_.extend({ recreate: false, fixMismatch: false, removeExtra: false, addMissing: false }, options);
  
  this.selectSchema(function(err, result) {
    if (err) {
      throw new lErrors.Table.SelectSchemaError('Error occurred trying to select schema: ' + err + '.');
    }
    else if (!result || !result.rows) {
      throw new lErrors.Table.SelectSchemaError('Select schema returned no result or no rows.');
    }
    else {
      
      // create table
      if (result.rows.length === 0) {
        lLogger.warn('Creating table: ' + self._dakota._keyspace + '.' + self._name + '.');
        self.create(function(err, result) {
          if (err) {
            throw new lErrors.Table.CreateError('Create table failed: ' + err + '.');
          }
          else {
            callback(err);
          }
        }, { ifNotExists: true });
      }
      
      // compare schema to existing table
      else {
        
        // create hash for diff
        var columns = {};
        nm_.each(self._schema.columns(), function(column, index) {
          columns[column] = true;
        });
        
        // diff
        var mismatched = [];
        var extra = [];
        nm_.each(result.rows, function(row, index) {
          var column = row.column_name;
          if (columns[column]) {
            if (lTypes.dbValidator(self._dakota, self._schema.columnType(column)) !== row.validator) {
              mismatched.push(row);
            }
            delete columns[column];
          }
          else {
            extra.push(row);
          }
        });
        var missing = nm_.keys(columns);
        
        // log
        if (mismatched.length > 0) {
          lLogger.warn('Found ' + mismatched.length + ' mismatched column types in ' + self._dakota._keyspace + '.' + self._name);
          lLogger.warn(mismatched);
        }
        if (extra.length > 0) {
          lLogger.warn('Found ' + extra.length + ' extra columns in ' + self._dakota._keyspace + '.' + self._name);
          lLogger.warn(extra);
        }
        if (missing.length > 0) {
          lLogger.warn('Found ' + missing.length + ' missing columns in ' + self._dakota._keyspace + '.' + self._name);
          lLogger.warn(missing);
        }
        
        // fix
        if ((mismatched.length > 0 || extra.length > 0 || missing.length > 0) && options.recreate) {
          recreate.call(self, callback);
        }
        else {
          var promises = [];
          if (mismatched.length > 0 && options.fixMismatch === '$recreate') {
            promises = promises.concat(fixMismatched.call(self, mismatched));
          }
          if (extra.length > 0 && options.removeExtra) {
            promises = promises.concat(fixExtra.call(self, extra));
            
          }
          if (missing.length > 0 && options.addMissing) {
            promises = promises.concat(fixMissing.call(self, missing));
          }
          nmWhen.settle(promises).done(function(descriptors) {
            var success = true;
            nm_.each(descriptors, function(descriptor, index) {
              if (descriptor.state === 'rejected') {
                success = false;
                lLogger.error(descriptor.reason);
              }
            });
            if (success) {
              callback();
            }
            else {
              throw new lErrors.Table.FixError('Fixing table schema failed: rejected promises.');
            }
          });
        }
        
      }
    }
  });
};

// expects: (function(err{Error}))
// returns: undefined
function recreate(callback) {
  var self = this;
  
  this.drop(function(err, result) {
    if (err) {
      throw new lErrors.Table.FixError('Drop table failed: ' + err + '.');
    }
    else {
      self.create(function(err, result) {
        if (err) {
          throw new lErrors.Table.FixError('Create table failed: ' + err + '.');
        }
        else {
          callback(err);
        }
      }, { ifNotExists: true });
    }
  }, { ifExists: true });
}

function fixMismatched(mismatched) {
  var self = this;
  
  lLogger.warn('Recreating columns with mismatched types...');
  
  var promises = [];
  nm_.each(mismatched, function(row, index) {
    var name = row.column_name;
    promises.push(nmWhen.promise(function(resolve, reject) {
      self.dropColumn(name, function(err, result) {
        if (err) {
          reject(err);
        }
        else {
          self.addColumn(name, self._schema.columnType(name), function(err, result) {
            if (err) {
              reject(err);
            }
            else {
              resolve();
            }
          });
        }
      });
    }));
  });
  return promises;
}

function fixExtra(extra) {
  var self = this;
  
  lLogger.warn('Removing extra columns...');
  
  var promises = [];
  nm_.each(extra, function(row, index) {
    promises.push(nmWhen.promise(function(resolve, reject) {
      self.dropColumn(row.column_name, function(err, result) {
        if (err) {
          reject(err);
        }
        else {
          resolve();
        }
      });
    }));
  });
  return promises;
}

function fixMissing(missing) {
  var self = this;
  
  lLogger.warn('Adding missing columns...');
  
  var promises = [];
  nm_.each(missing, function(column, index) {
    promises.push(nmWhen.promise(function(resolve, reject) {
      self.addColumn(column, self._schema.columnType(column), function(err, result) {
        if (err) {
          reject(err);
        }
        else {
          resolve();
        }
      });
    }));
  });
  return promises;
}

// =================
// = Select Schema =
// =================

// expects: (function(err{Error}, result))
// returns: undefined
Table.prototype.selectSchema = function(callback) {
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Table.InvalidArgument('Argument should be a function.');
  }
  
  var query = {
    query: 'SELECT * FROM system.schema_columns WHERE columnfamily_name = ? AND keyspace_name = ? ALLOW FILTERING',
    params: [this._name, this._dakota._keyspace],
    prepare: true
  };
  
  this._dakota.execute(query.query, query.params, { prepare: query.prepare }, function(err, result) {
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
Table.prototype.create = function(callback, options) {
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Table.InvalidArgument('Argument should be a function.');
  }
  else if (options && !lHelpers.isHash(options)) {
    throw new lErrors.Table.InvalidArgument('Argument should be a {}.');
  }
  options = nm_.extend({ ifNotExists: false }, options);
  
  var query = {
    query: 'CREATE TABLE',
    params: [],
    prepare: true
  };
  
  if (options.ifNotExists) {
    query.query += ' IF NOT EXISTS';
  }
  
  concatBuilders.call(this, [buildTableName, buildColumns, buildWith], query);
  
  this._dakota.execute(query.query, query.params, { prepare: query.prepare }, function(err, result) {
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
Table.prototype.drop = function(callback, options) {
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Table.InvalidArgument('Argument should be a function.');
  }
  else if (options && !lHelpers.isHash(options)) {
    throw new lErrors.Table.InvalidArgument('Argument should be a {}.');
  }
  options = nm_.extend({ ifExists: false }, options);
  
  var query = {
    query: 'DROP TABLE',
    params: [],
    prepare: true
  };
  
  if (options.ifExists) {
    query.query += ' IF EXISTS';
  }
  
  concatBuilders.call(this, [buildTableName], query);
  
  this._dakota.execute(query.query, query.params, { prepare: query.prepare }, function(err, result) {
    callback(err, result);
  });
};

// =========
// = Alter =
// =========

// expects: (column{String}, type{String}, function(err{Error}, result))
// returns: undefined
Table.prototype.addColumn = function(column, type, callback) {
  if (!nm_.isString(column)) {
    throw new lErrors.Table.InvalidArgument('Argument should be a string.');
  }
  else if (!nm_.isString(type)) {
    throw new lErrors.Table.InvalidArgument('Argument should be a string.');
  }
  else if (!nm_.isFunction(callback)) {
    throw new lErrors.Table.InvalidArgument('Argument should be a function.');
  }
  
  var query = {
    query: 'ALTER TABLE',
    params: [],
    prepare: true
  };
  
  concatBuilders.call(this, [buildTableName], query);
  
  query.query += ' ADD "' + column + '" ' + type;
  
  this._dakota.execute(query.query, query.params, { prepare: query.prepare }, function(err, result) {
    callback(err, result);
  });
};

// expects: (column{String}, function(err{Error}, result))
// returns: undefined
Table.prototype.dropColumn = function(column, callback) {
  if (!nm_.isString(column)) {
    throw new lErrors.Table.InvalidArgument('Argument should be a string.');
  }
  else if (!nm_.isFunction(callback)) {
    throw new lErrors.Table.InvalidArgument('Argument should be a function.');
  }
  
  var query = {
    query: 'ALTER TABLE',
    params: [],
    prepare: true
  };
  
  concatBuilders.call(this, [buildTableName], query);
  
  query.query += ' DROP "' + column + '"';
  
  this._dakota.execute(query.query, query.params, { prepare: query.prepare }, function(err, result) {
    callback(err, result);
  });
};

// expects: (column{String}, newName{String}, function(err{Error}, result))
// returns: undefined
Table.prototype.renameColumn = function(column, newName, callback) {
  if (!nm_.isString(column)) {
    throw new lErrors.Table.InvalidArgument('Argument should be a string.');
  }
  else if (!nm_.isString(newName)) {
    throw new lErrors.Table.InvalidArgument('Argument should be a string.');
  }
  else if (!nm_.isFunction(callback)) {
    throw new lErrors.Table.InvalidArgument('Argument should be a function.');
  }
  
  var query = {
    query: 'ALTER TABLE',
    params: [],
    prepare: true
  };
  
  concatBuilders.call(this, [buildTableName], query);
  
  query.query += ' RENAME "' + column + '" TO "' + newName + '"';
  
  this._dakota.execute(query.query, query.params, { prepare: query.prepare }, function(err, result) {
    callback(err, result);
  });
};

// expects: (column{String}, newType{String}, function(err{Error}, result))
// returns: undefined
Table.prototype.alterType = function(column, newType, callback) {
  if (!nm_.isString(column)) {
    throw new lErrors.Table.InvalidArgument('Argument should be a string.');
  }
  else if (!nm_.isString(newType)) {
    throw new lErrors.Table.InvalidArgument('Argument should be a string.');
  }
  else if (!nm_.isFunction(callback)) {
    throw new lErrors.Table.InvalidArgument('Argument should be a function.');
  }
  
  var query = {
    query: 'ALTER TABLE',
    params: [],
    prepare: true
  };
  
  concatBuilders.call(this, [buildTableName], query);
  
  query.query += ' ALTER "' + column + '" TYPE ' + newType;
  
  this._dakota.execute(query.query, query.params, { prepare: query.prepare }, function(err, result) {
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

function buildTableName() {
  var clause = this._dakota._keyspace + '.' + this._name;
  var params = [];
  return { clause: clause, params: params };
}

function buildColumns() {
  var self = this;
  
  var clause = '(';
  var params = [];
  
  // columns
  nm_.each(this._schema.columns(), function(column, index) {
    if (index > 0) {
      clause += ', ';
    }
    clause += '"' + column + '" ' + self._schema.columnType(column);
  });
  
  // key
  clause += ', PRIMARY KEY (';
  var partitionKey = this._schema.partitionKey();
  if (nm_.isArray(partitionKey)) {
    clause += '(';
    nm_.each(partitionKey, function(column, index) {
      if (index > 0) {
        clause += ', ';
      }
      clause += '"' + column + '"';
    });
    clause += ')';
  }
  else {
    clause += '"' + partitionKey + '"';
  }
  var clusteringKey = this._schema.clusteringKey();
  if (clusteringKey) {
    clause += ', ';
    if (nm_.isArray(clusteringKey)) {
      nm_.each(clusteringKey, function(column, index) {
        if (index > 0) {
          clause += ', ';
        }
        clause += '"' + column + '"';
      });
    }
    else {
      clause += '"' + clusteringKey + '"';
    }
  }
  clause += ')';
  
  clause += ')';
  return { clause: clause, params: params };
}

function buildWith() {
  var clause = '';
  var params = [];
  var properties = this._schema.with();
  if (properties) {
    clause += ' WITH';
    var i = 0;
    nm_.each(properties, function(value, property) {
      if (i > 0) {
        clause += ' AND';
      }
      if (property === '$clustering_order_by') {
        clause += ' ' + lTableWithProperties.PROPERTIES[property] + '(';
        nm_.each(value, function(order, column) {
          clause += '"' + column + '" ' + lTableWithProperties.CLUSTERING_ORDER[order];
        });
        clause += ')';
      }
      else if (property === '$compact_storage') {
        clause += ' ' + lTableWithProperties.PROPERTIES[property];
      }
      else {
        throw new lErrors.Table.InvalidWith('Invalid with:' + property);
      }
      i++;
    });
  }
  return { clause: clause, params: params };
}

module.exports = Table;