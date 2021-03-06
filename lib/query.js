// node modules
var nmCassandra = require('cassandra-driver');
var nm_ = require('underscore');

// lib
var lErrors = require('./errors');
var lHelpers = require('./helpers');
var lTypes = require('./types');

var ACTIONS = {
  'select'   : 'SELECT',
  'update'   : 'UPDATE',
  'insert'   : 'INSERT',
  'delete'   : 'DELETE',
  'truncate' : 'TRUNCATE'
};

var WHERE_OPERATIONS = {
  '$eq' : '=',
  '$gt' : '=',
  '$gte': '>=',
  '$lt' : '<',
  '$lte': '<=',
  '$in' : 'IN'
};

var ORDERING = {
  '$asc' : 'ASC',
  '$desc' : 'DESC'
};

var USING = {
  '$ttl' : 'TTL',
  '$timestamp' : 'TIMESTAMP'
};

var UPDATE_OPERATIONS = {
  
  // all
  all: {
    '$set' : '%c = %v'
  },
  
  // sets
  set: {
    '$add'    : '%c = %c + %v',
    '$remove' : '%c = %c - %v'
  },
  
  // lists
  list: {
    '$prepend' : '%c = %v + %c',
    '$append' : '%c = %c + %v',
    '$remove' : '%c = %c - %v',
    '$inject' : '%c[%k] = %v'
  },
  
  map: {
    '$inject' : '%c[%k] = %v'
  },
  
  // counters
  counter: {
    '$incr' : '%c = %c + %v',
    '$decr' : '%c = %c - %v'
  }
  
};

var IF_OPERATIONS = WHERE_OPERATIONS;

// expects: model{ModelClass}, instance{Model}
// returns: undefined
function Query(model, instance) {
  if (!(nm_.isFunction(model))) {
    throw new lErrors.Query.InvalidArgument('Argument should be a Model class.');
  }
  else if (instance && !(instance instanceof model)) {
    throw new lErrors.Query.InvalidArgument('Argument should be an instance of Model.');
  }
  
  this._model = model;
  this._instance = instance;
  
  // fields
  this._action = null;
  this._count = null;
  this._select = null;
  this._where = null;
  this._orderBy = null;
  this._limit = null;
  this._allowFiltering = null;
  this._using = null;
  this._ifExists = null;
  this._ifNotExists = null;
  this._insert = null
  this._update = null;
  this._if = null;
}

// ====================
// = Find and FindOne =
// ====================

// expects: ({ column{String}: value, ... }, function(err{Error}, result{Array})) or ({ column{String}: { operation[$eq, $gt, $gte, ...]{String}: value }, ... }, function(err{Error}, result{Array}))
// returns: undefined
Query.prototype.find = function(conditions, callback) {
  if (!lHelpers.isPlainObject(conditions)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a {}.');
  }
  else if (!nm_.isFunction(callback)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a function.');
  }
  else {
    this.where(conditions);
    this.all();
  }
};

// expects: ({ column{String}: value, ... }, function(err{Error}, model{Model})) or ({ column{String}: { operation[$eq, $gt, $gte, ...]{String}: value }, ... }, function(err{Error}, model{Model}))
// returns: undefined
Query.prototype.findOne = function(conditions, callback) {
  if (!lHelpers.isPlainObject(conditions)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a {}.');
  }
  else if (!nm_.isFunction(callback)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a function.');
  }
  else {
    this.where(conditions);
    this.first();
  }
};

// ==========
// = Action =
// ==========

// expects: action[select, update, ...]{String}
// returns: this{Query}
Query.prototype.action = function(action) {
  if (!ACTIONS[action]) {
    throw new lErrors.Query.InvalidAction('Invalid action: ' + action + '.');
  }
  else {
    this._action = action;
  }
  return this;
};

// ==========
// = Select =
// ==========

// expects: ([column{String}, ...]) or (column{String}, ...)
// returns: this{Query}
Query.prototype.select = function(columns) {
  var self = this;
  
  nm_.each(arguments, function(column, index) {
    if (nm_.isArray(column)) {
      if (column.length > 0) {
        if (!self._select) {
          self._select = {};
        }
        nm_.each(column, function(c, index) {
          self._select[c] = true;
        });
      }
    }
    else if (nm_.isString(column)) {
      if (!self._select) {
        self._select = {};
      }
      self._select[column] = true;
    }
    else {
      throw new lErrors.Query.InvalidArgument('Argument should be an array or string.');
    }
  });
  return this;
};

// =========
// = Where =
// =========

// expects: (column{String}, value) or ({ column{String}: value }) or ({ column{String}: { operation[$eq, $gt, $gte, ...]{String}: value }})
// returns: this{Query}
Query.prototype.where = function(arg1, arg2) {
  var self = this;
  
  // normalize
  if (nm_.isString(arg1)) {
    var where = {};
    where[arg1] = { '$eq': arg2 };
    arg1 = where;
  }
  
  if (!lHelpers.isPlainObject(arg1)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a {}.');
  }
  else {
    nm_.each(arg1, function(conditions, column) {
      
      // normalize value
      if (!lHelpers.isPlainObject(conditions)) {
        conditions = { '$eq': conditions };
      }
      
      // validate operations
      nm_.each(conditions, function(value, operator) {
        if (!WHERE_OPERATIONS[operator]) {
          throw new lErrors.Query.InvalidWhereOperation('Invalid operation: ' + operator + '.');
        }
      });
      
      // add to conditions
      if (!self._where) {
        self._where = {};
      }
      if (!self._where[column]) {
        self._where[column] = {};
      }
      nm_.extend(self._where[column], conditions);
    });
  }
  return this;
};

// ============
// = Order By =
// ============

// expects: ({ partitionKey{String}: order[$asc, $desc]{String} }) or (partitionKey{String}, order[$asc, $desc]{String})
// returns: this{Query}
Query.prototype.orderBy = function(arg1, arg2) {
  
  // normalize
  if (nm_.isString(arg1) && nm_.isString(arg2)) {
    var order = {};
    order[arg1] = arg2;
    arg1 = order;
  }
  
  if (!lHelpers.isPlainObject(arg1)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a {}.');
  }
  else if (nm_.size(arg1) !== 1) {
    throw new lErrors.Query.InvalidArgument('{} should only contain 1 key.');
  }
  else {
    nm_.each(arg1, function(order, column) {
      if (!ORDERING[order]) {
        throw new lErrors.Query.InvalidOrdering('Invalid ordering: ' + order + '.');
      }
    });
    this._orderBy = arg1;
  }
  return this;
};

// =========
// = Limit =
// =========

// expects: (limit{Integer})
// returns: this{Query}
Query.prototype.limit = function(limit) {
  if (!lHelpers.isInteger(limit)) {
    throw new lErrors.Query.InvalidArgument('Argument should be an integer.');
  }
  else {
    this._limit = limit;
  }
  return this;
};

// ===================
// = Allow Filtering =
// ===================

// expects: (allow{Boolean})
// returns: this{Query}
Query.prototype.allowFiltering = function(allow) {
  if (!nm_.isBoolean(allow)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a boolean.');
  }
  else {
    this._allowFiltering = allow;
  }
  return this;
};

// =========
// = Using =
// =========

// expects: ({ using[$ttl, $timestamp]{String}: value{Integer} }) or (using[$ttl, $timestamp]{String}, value{Integer})
// returns: this{Query}
Query.prototype.using = function(arg1, arg2) {
  
  // normalize
  if (nm_.isString(arg1) && lHelpers.isInteger(arg2)) {
    var using = {};
    using[arg1] = arg2;
    arg1 = using;
  }
  
  if (!lHelpers.isPlainObject(arg1)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a {}.');
  }
  else {
    nm_.each(arg1, function(value, using) {
      if (!USING[using]) {
        throw new lErrors.Query.InvalidUsing('Invalid using: ' + using + '.');
      }
    });
    if (!this._using) {
      this._using = {};
    }
    nm_.extend(this._using, arg1);
  }
  return this;
};

Query.prototype.ttl = function(ttl) {
  return this.using('$ttl', ttl);
};

Query.prototype.timestamp = function(timestamp) {
  return this.using('$timestamp', timestamp);
};

// =============
// = If Exists =
// =============

// expects: (exists{Boolean})
// returns: this{Query}
Query.prototype.ifExists = function(exists) {
  if (!nm_.isBoolean(exists)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a boolean.');
  }
  else {
    this._ifExists = exists;
  }
  return this;
};

// =================
// = If Not Exists =
// =================

// expects: (notExists{Boolean})
// returns: this{Query}
Query.prototype.ifNotExists = function(notExists) {
  if (!nm_.isBoolean(notExists)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a boolean.');
  }
  else {
    this._ifNotExists = notExists;
  }
  return this;
};

// ==========
// = Update =
// ==========

// expects: (column{String}, value) or ({ column{String}: value }) or ({ column{String}: { operation[$set, $add, $remove, ...]{String}: value }})
// returns: this{Query}
Query.prototype.update = function(arg1, arg2) {
  var self = this;
  
  if (this._action && this._action !== 'update') {
    throw new lErrors.Query.ActionConflit('Conflicting action already set.');
  }
  else {
    this.action('update');
  }
  
  // normalize
  if (nm_.isString(arg1)) {
    var where = {};
    where[arg1] = { '$set': arg2 };
    arg1 = where;
  }
  
  if (!lHelpers.isPlainObject(arg1)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a {}.');
  }
  else {
    nm_.each(arg1, function(assignments, column) {
      var type = self._model._schema.baseColumnType(column);
      
      // normalize value
      if (nm_.isFunction(assignments)) {
        throw new lErrors.Query.InvalidType('Type should NOT be a function.');
      }
      else if (!lHelpers.isPlainObject(assignments)) {
        assignments = { '$set': assignments };
      }
      else if (type === 'map' || lTypes.isUserDefinedType(self._model._dakota, type)) {
        var containsOperators = false;
        nm_.each(assignments, function(value, key) {
          if (UPDATE_OPERATIONS.all[key] || UPDATE_OPERATIONS.map[key]) {
            containsOperators = true;
          }
        });
        if (!containsOperators) {
          assignments = { '$set': assignments };
        }
      }
      
      // validate conditions
      if (!lHelpers.isPlainObject(assignments)) {
        throw new lErrors.Query.InvalidType('Type should be a {}.');
      }
      else {
        
        // validate operations
        nm_.each(assignments, function(value, operator) {
          if (!UPDATE_OPERATIONS.all[operator]) {
            if (!UPDATE_OPERATIONS[type] || !UPDATE_OPERATIONS[type][operator]) {
              throw new lErrors.Query.InvalidUpdateOperation('Invalid operation: ' + operator + '.');
            }
          }
        });
        
        // add to conditions
        if (!self._update) {
          self._update = {};
        }
        
        // add injection assignments
        if (type === 'list' || type === 'map') {
          if (assignments['$inject']) {
            nm_.each(assignments['$inject'], function(v, key) {
              if (type === 'list') {
                self._update[column + '[' + key + ']'] = { '$set': v };
              }
              else {
                var mapKeyType = lTypes.mapKeyType(self._model._dakota, self._model._schema.columnType(column));
                if (lTypes.isStringType(self._model._dakota, mapKeyType)) {
                  self._update[column + '[\'' + key + '\']'] = { '$set': v };
                }
                else {
                  self._update[column + '[' + key + ']'] = { '$set': v };
                }
              }
            });
          }
          delete assignments['$inject'];
        }
        
        // add column level assignments
        if (!self._update[column]) {
          self._update[column] = {};
        }
        nm_.extend(self._update[column], assignments);
      }
    });
  }
  return this;
};

// ==========
// = Insert =
// ==========

// expects: ({ column{String}: value, ... }) or (column{String}, value)
// returns: this{Query}
Query.prototype.insert = function(arg1, arg2) {
  if (this._action && this._action !== 'insert') {
    throw new lErrors.Query.ActionConflit('Conflicting action already set.');
  }
  else {
    this.action('insert');
  }
  
  // normalize
  if (nm_.isString(arg1)) {
    var set = {};
    set[arg1] = arg2;
    arg1 = set;
  }
  
  if (!lHelpers.isPlainObject(arg1)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a {}.');
  }
  else {
    if (!this._insert) {
      this._insert = {};
    }
    nm_.extend(this._insert, arg1);
  }
  return this;
};

// ======
// = If =
// ======

// expects: ({ column{String}: value }) or ({ column{String}: { operation[$eq, $gt, $gte, ...]{String}: value }})
// returns: this{Query}
Query.prototype.if = function(obj) {
  var self = this;
  
  if (!lHelpers.isPlainObject(obj)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a {}.');
  }
  else {
    nm_.each(obj, function(conditions, column) {
      
      // normalize value
      if (nm_.isString(conditions)) {
        conditions = { '$eq': conditions };
      }
      
      // validate conditions
      if (!lHelpers.isPlainObject(conditions)) {
        throw new lErrors.Query.InvalidType('Type should be a {}.');
      }
      else {
        
        // validate operations
        nm_.each(conditions, function(value, operator) {
          if (!IF_OPERATIONS[operator]) {
            throw new lErrors.Query.InvalidIfOperation('Invalid operation: ' + operator + '.');
          }
        });
        
        // add to conditions
        if (!self._if) {
          self._if = {};
        }
        if (!self._if[column]) {
          self._if[column] = {};
        }
        nm_.extend(self._if[column], conditions);
      }
    });
  }
  return this;
};

// =============
// = Execution =
// =============

// expects: (function(err{Error}, result{Result}))
// return: undefined
Query.prototype.execute = function(callback) {
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a function.');
  }
  else {
    var query = this.build();
    this._model._execute(query.query, query.params, { prepare: query.prepare }, callback);
  }
};

// expects: (function(err{Error}, model{Model}))
// return: undefined
Query.prototype.first = function(callback) {
  var self = this;
  
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a function.');
  }
  else if (this._action && this._action !== 'select') {
    throw new lErrors.Query.ActionConflit('Conflicting action already set.');
  }
  else {
    this.action('select');
    this.limit(1);
    this.execute(function(err, result) {
      if (err) {
        result = null;
      }
      else if (result) {
        if (!nm_.isObject(result)) {
          throw new lErrors.Query.UnexpectedType('Result type should be an object.');
        }
        else if (!nm_.isArray(result.rows)) {
          throw new lErrors.Query.UnexpectedType('Result.rows type should be an array.');
        }
        else if (result.rows.length > 0 ){
          result = self._model._newFromQueryRow(result.rows[0]);
        }
        else {
          result = null;
        }
      }
      callback(err, result);
    });
  }
};

// expects: (function(err{Error}, result{Array}))
// return: undefined
Query.prototype.all = function(callback) {
  var self = this;
  
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a function.');
  }
  else if (this._action && this._action !== 'select') {
    throw new lErrors.Query.ActionConflict('Conflicting action already set.');
  }
  else {
    this.action('select');
    this.execute(function(err, result) {
      if (err) {
        result = null;
      }
      else if (result) {
        if (!nm_.isObject(result)) {
          throw new lErrors.Query.UnexpectedType('Result type should be an object.');
        }
        else if (!nm_.isArray(result.rows)) {
          throw new lErrors.Query.UnexpectedType('Result.rows type should be an array.');
        }
        else {
          result = nm_.map(result.rows, function(row, index) {
            return self._model._newFromQueryRow(row);
          });
        }
      }
      callback(err, result);
    });
  }
};

// expects: (function(err{Error}, count{Integer}))
// return: undefined
Query.prototype.count = function(callback) {
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a function.');
  }
  else if (this._action && this._action !== 'select') {
    throw new lErrors.Query.ActionConflit('Conflicting action already set.');
  }
  else {
    this._count = true;
    this.action('select');
    this.execute(function(err, result) {
      if (err) {
        result = null;
      }
      else if (result) {
        if (!nm_.isObject(result)) {
          throw new lErrors.Query.UnexpectedType('Result type should be an object.');
        }
        else if (!nm_.isArray(result.rows)) {
          throw new lErrors.Query.UnexpectedType('Result.rows type should be an array.');
        }
        else {
          result = result.rows[0];
          if (!nm_.isObject(result)) {
            throw new lErrors.Query.UnexpectedType('Result type should be an object.');
          }
          else if (!nm_.isObject(result.count)) {
            throw new lErrors.Query.UnexpectedType('Result type should be an object.');
          }
          else {
            result = result.count.toNumber();
          }
        }
      }
      
      callback(err, result);
    });
  }
};

// ==============
// = Truncating =
// ==============

// expects: (function(err{Error}))
// return: undefined
Query.prototype.truncate = function(callback) {
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a function.');
  }
  else {
    this.action('truncate');
    this.execute(callback);
  }
};

// alias: truncate
Query.prototype.deleteAll = function(callback) {
  this.truncate(callback);
};

// ===========================
// = Model Instance Specific =
// ===========================

// expects: (function(err{Error}), options{Object}?)
// options:
//          validate{Object} : passed directly to validate method
//            validate.only{Array} : validate only these columns
//            validate.except{Array} : validate all columns except these
//          callbacks{Object} : modify callback behavior
//            callbacks.skipAll{Boolean} : skips all callbacks, takes precedence over subsequent settings
//            callbacks.skipBeforeValidate{Boolean}
//            callbacks.skipAfterValidate{Boolean}
//            callbacks.skipBeforeCreate{Boolean}
//            callbacks.skipAfterCreate{Boolean}
//            callbacks.skipBeforeSave{Boolean}
//            callbacks.skipAfterSave{Boolean}
// return: undefined
Query.prototype.save = function(callback, options) {
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a function.');
  }
  else if(this._where) {
    throw new lErrors.Query.WhereConflict('Where cannot already have been configured for save queries.');
  }
  else if (!this._instance) {
    throw new lErrors.Query.InstanceNotSet('Model instance must be set in Query constructor to call save.');
  }
  
  this._instance._save(callback, this, options);
};

// expects: (function(err{Error}), options{Object}?)
// options:
//          callbacks{Object} : modify callback behavior
//            callbacks.skipAll{Boolean} : skips all callbacks, takes precedence over subsequent settings
//            callbacks.skipBeforeDelete{Boolean}
//            callbacks.skipAfterDelete{Boolean}
// return: undefined
Query.prototype.delete = function(callback, options) {
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a function.');
  }
  if (this._instance) {
    if(this._where) {
      throw new lErrors.Query.WhereConflict('Where cannot already have been configured for delete queries.');
    }
    this._instance._delete(callback, this, options);
  }
  else {
    if(!this._where) {
      throw new lErrors.Query.WhereNotSet('Where must be set manually to call delete. Call truncate or deleteAll to remove all rows.');
    }
    this.action('delete').execute(callback);
  }
};

// ============
// = Each Row =
// ============

// expects: (function(n{Number}, model{Model}), function(err{Error}))
// return: undefined
Query.prototype.eachRow = function(modelCallback, completeCallback) {
  var self = this;
  
  if (!nm_.isFunction(modelCallback) || !nm_.isFunction(completeCallback)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a function.');
  }
  else {
    if (this._action && this._action !== 'select') {
      throw new lErrors.Query.ActionConflict('Conflicting action already set.');
    }
    else {
      this.action('select');
      
      var query = this.build();
      this._model._eachRow(query.query, query.params, { prepare: query.prepare }, function(n, row) {
        var result = self._model._newFromQueryRow(row);
        modelCallback(n, result);
      }, completeCallback);
    }
  }
};

// =============
// = Streaming =
// =============

// expects: ()
// return: stream{Stream}
Query.prototype.stream = function() {
  if (this._action && this._action !== 'select') {
    throw new lErrors.Query.ActionConflict('Conflicting action already set.');
  }
  else {
    this.action('select');
    
    var query = this.build();
    return this._model._stream(query.query, query.params, { prepare: query.prepare });
  }
};

// ============
// = Building =
// ============

// expects: nothing
// returns: { query: query{String}, params: params{Object}, prepare: prepare{Boolean} }
Query.prototype.build = function() {
  if (!this._action || !ACTIONS[this._action]) {
    throw new lErrors.Query.InvalidAction('Action not set or unknown: ' + this._action + '.');
  }
  else {
    var query = {
      query: '',
      params: [],
      prepare: true
    };
    
    if (this._action === 'select') {
      concatBuilders.call(this, [buildAction, buildSelectForSelectAction, buildFromForSelectAction, buildWhere, buildOrderBy, buildLimit, buildAllowFiltering], query);
    }
    else if (this._action === 'update') {
      concatBuilders.call(this, [buildAction, buildFromForUpdateAction, buildUsing, buildUpdate, buildWhere, buildIf, buildIfExists], query);
    }
    else if (this._action === 'insert') {
      concatBuilders.call(this, [buildAction, buildFromForInsertAction, buildInsert, buildIfNotExists, buildUsing], query);
    }
    else if (this._action === 'delete') {
      concatBuilders.call(this, [buildAction, buildSelectForDeleteAction, buildFromForDeleteAction, buildUsing, buildWhere, buildIf, buildIfExists], query);
    }
    else if (this._action === 'truncate') {
      concatBuilders.call(this, [buildAction, buildFromForTruncateAction], query);
    }
    
    return query;
  }
};

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

// action
function buildAction() {
  var clause = ACTIONS[this._action];
  var params = [];
  return { clause: clause, params: params };
}

// select
function buildSelectForSelectAction() {
  var clause = '';
  var params = [];
  if (this._select) {
    nm_.each(this._select, function(value, column) {
      if (clause.length > 0) {
        clause += ', ';
      }
      clause += '"' + column + '"';
    });
  }
  if (clause.length === 0) {
    clause = '*';
  }
  if (!nm_.isNull(this._count) && this._count) {
    clause = 'COUNT(' + clause + ')'
  }
  return { clause: clause, params: params };
}

function buildSelectForDeleteAction() {
  var self = this;
  
  var clause = '';
  var params = [];
  if (this._select) {
    nm_.each(this._select, function(value, column) {
      if (clause.length > 0) {
        clause += ', ';
      }
      if (!self._model._schema.isColumn(column)) { // map[key] columns will return false
        clause += column;
      }
      else {
        clause += '"' + column + '"';
      }
    });
  }
  return { clause: clause, params: params };
}

// from
function buildFromForSelectAction() {
  var parts = buildFromForUpdateAction.apply(this);
  var clause = parts.clause;
  var params = parts.params;
  clause = 'FROM ' + clause;
  return { clause: clause, params: params };
}

function buildFromForUpdateAction() {
  var clause = this._model._dakota._keyspace + '.' + this._model._table._name;
  var params = [];
  return { clause: clause, params: params };
}

function buildFromForInsertAction() {
  var parts = buildFromForUpdateAction.apply(this);
  var clause = parts.clause;
  var params = parts.params;
  clause = 'INTO ' + clause;
  return { clause: clause, params: params };
}

function buildFromForDeleteAction() {
  return buildFromForSelectAction.apply(this);
}

function buildFromForTruncateAction() {
  return buildFromForUpdateAction.apply(this);
}

// where
function buildWhere() {
  var clause = '';
  var params = [];
  if (this._where) {
    clause += 'WHERE';
    nm_.each(this._where, function(conditions, column) {
      nm_.each(conditions, function(value, operator) {
        if (params.length > 0) {
          clause += ' AND';
        }
        if (operator === '$in') {
          clause += ' "' + column + '" ' + WHERE_OPERATIONS[operator] + '(';
          nm_.each(value, function(v, index) {
            if (index > 0) {
              clause += ', ';
            }
            clause += '?';
            params.push(v);
          });
        }
        else {
          clause += ' "' + column + '" ' + WHERE_OPERATIONS[operator] + ' ?';
          params.push(value);
        }
      });
    });
  }
  return { clause: clause, params: params };
}

// order by
function buildOrderBy() {
  var clause = '';
  var params = [];
  if (this._orderBy) {
    clause += 'ORDER BY';
    nm_.each(this._orderBy, function(order, column) {
      clause += ' "' + column + '" ' + ORDERING[order];
    });
  }
  return { clause: clause, params: params };
}

// limit
function buildLimit() {
  var clause = '';
  var params = [];
  if (!nm_.isNull(this._limit)) {
    clause += 'LIMIT ' + this._limit;
  }
  return { clause: clause, params: params };
}

// allow filtering
function buildAllowFiltering() {
  var clause = '';
  var params = [];
  if (!nm_.isNull(this._allowFiltering) && this._allowFiltering) {
    clause += 'ALLOW FILTERING';
  }
  return { clause: clause, params: params };
}

// using
function buildUsing() {
  var clause = '';
  var params = [];
  if (this._using) {
    clause += 'USING';
    nm_.each(this._using, function(value, using) {
      if (params.length > 0) {
        clause += ' AND';
      }
      clause += ' ' + USING[using] + ' ?';
      params.push(value);
    });
  }
  return { clause: clause, params: params };
}

// if exists
function buildIfExists() {
  var clause = '';
  var params = [];
  if (!nm_.isNull(this._ifExists) && this._ifExists) {
    clause += 'IF EXISTS';
  }
  return { clause: clause, params: params };
}

// if not exists
function buildIfNotExists() {
  var clause = '';
  var params = [];
  if (!nm_.isNull(this._ifNotExists) && this._ifNotExists) {
    clause += 'IF NOT EXISTS';
  }
  return { clause: clause, params: params };
}

// update
function buildUpdate() {
  var self = this;
  
  var clause = '';
  var params = [];
  if (this._update) {
    clause += 'SET'
    nm_.each(this._update, function(assignments, column) {
      nm_.each(assignments, function(value, operator) {
        if (params.length > 0) {
          clause += ',';
        }
        
        var type = null;
        var match = column.match(/(\w+)\[.+\]/);
        if (match) {
          type = self._model._schema.baseColumnType(match[1]);
        }
        else {
          type = self._model._schema.baseColumnType(column);
        }
        
        var format = null;
        if (UPDATE_OPERATIONS.all[operator]) {
          format = UPDATE_OPERATIONS.all[operator];
        }
        else if (UPDATE_OPERATIONS[type] && UPDATE_OPERATIONS[type][operator]) {
          format = UPDATE_OPERATIONS[type][operator];
        }
        if (!format) {
          throw new lErrors.Query.InvalidUpdateOperation('Invalid operation: ' + operator + '.');
        }
        else {
          var assignment = '';
          for(var i = 0; i < format.length; i++) {
            if (format[i] === '%') {
              if (format[i + 1] === 'c') {
                
                if (match) {
                  assignment += column; // don't quote injection assignments
                }
                else {
                  assignment += '"' + column + '"';
                }
                
                i += 1;
                continue;
              }
              else if (format[i + 1] === 'v') {
                assignment += '?';
                params.push(value);
                i += 1;
                continue;
              }
            }
            assignment += format[i];
          }
          clause += ' ' + assignment;
        }
      });
    });
  }
  return { clause: clause, params: params };
}

// insert
function buildInsert() {
  var clause = '';
  var params = [];
  if (this._insert) {
    var columns = '(';
    var values = '(';
    nm_.each(this._insert, function(value, column) {
      if (columns.length > 1) {
        columns += ', ';
        values += ', ';
      }
      columns += '"' + column + '"';
      values += '?';
      params.push(value);
    });
    clause = columns + ') VALUES ' + values + ')';
  }
  return { clause: clause, params: params };
}

// if
function buildIf() {
  var clause = '';
  var params = [];
  if (this._if) {
    clause += 'IF';
    nm_.each(this._if, function(conditions, column) {
      nm_.each(conditions, function(value, operator) {
        if (params.length > 0) {
          clause += ' AND';
        }
        if (operator === '$in') {
          clause += ' "' + column + '" ' + IF_OPERATIONS[operator] + '(';
          nm_.each(value, function(v, index) {
            if (index > 0) {
              clause += ', ';
            }
            clause += '?';
            params.push(v);
          });
        }
        else {
          clause += ' "' + column + '" ' + IF_OPERATIONS[operator] + ' ?';
          params.push(value);
        }
      });
    });
  }
  return { clause: clause, params: params };
}

// ============
// = Batching =
// ============

// expects: (client{Cassandra.Client}, [query{Query}, ...], function(err{Error}))
// return: undefined
Query.batch = function(client, queries, callback) {
  if (!(client instanceof nmCassandra.Client)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a Cassandra.Client.');
  }
  else if (!nm_.isArray(queries)) {
    throw new lErrors.Query.InvalidArgument('Argument should be an array.');
  }
  else if (!nm_.isFunction(callback)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a function.');
  }
  else {
    
    // build queries
    for (var i = 0; i < queries.length; i++) {
      var query = queries[i];
      if (query instanceof Query) {
        queries[i] = query.build();
      }
    }
    
    client.batch(queries, { prepare: true }, function(err) {
      callback(err);
    });
  }
};

module.exports = Query;