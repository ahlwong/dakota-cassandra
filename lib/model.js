// node modules
var nm_ = require('underscore');
var nm_i = require('underscore.inflections');
var nm_s = require('underscore.string');

// lib
var lErrors = require('./errors');
var lHelpers = require('./helpers');
var lQuery = require('./query');
var lSchema = require('./schema');
var lTable = require('./table');
var lTypes = require('./types');
var lValidations = require('./validations');
var util = require('./util');

// expects: ({ column{String}: value, ... }?)
// returns: undefined
function Model(assignments) {
  this._exists = false;
  this._columns = {};
  this._changes = {};
  this._invalidColumns = null;
  
  // set assignments
  if (assignments) {
    if (!lHelpers.isHash(assignments)) {
      throw new lErrors.Model.InvalidArgument('Argument should be a {}.');
    }
    else {
      this.set(assignments);
    }
  }
}

var staticMethods = {};

// =============
// = Compiling =
// =============

// expects: (dakota{Dakota}, name{String}, schema{Schema}, validations{Validations}, options{Object}?) or (dakota{Dakota}, name{String}, schema{Schema}, null, options{Object}?)
// return: model{Model}
Model.compile = function(dakota, name, schema, validations, options) {
  if (!(dakota instanceof require('./index'))) { // need to require here
    throw new lErrors.Model.InvalidArgument('Argument should be a Dakota.');
  }
  else if (!nm_.isString(name)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a string.');
  }
  else if (!(schema instanceof lSchema)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a Schema.');
  }
  else if (validations && !(validations instanceof lValidations)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a {}.');
  }
  else if (options && !lHelpers.isHash(options)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a {}.');
  }
  
  // new class
  function model(assignments, options) {
    
    // super constructor
    Model.call(this, assignments);
    
    if (!options || !options.skipAfterNewCallback) {
      
      // afterNew callbacks
      runCallbacks.call(this, 'afterNew');
    }
  }
  
  // inheritance
  util.inherits(model, Model);
  
  // instance property to model class
  model.prototype._model = model;
  
  // static properties
  model._dakota = dakota;
  model._name = name;
  model._schema = schema;
  model._validations = validations;
  model._options = options;
  
  model._ready = false;
  model._queryQueue = {
    execute: [],
    eachRow: []
  };
  
  // static callbacks
  model._callbacks = {};
  nm_.each(lSchema._CALLBACK_KEYS, function(key, index) {
    model._callbacks[key] = [];
  });
  
  // create table
  ensureTable.call(model, function() {
    
    // table ready
    model._ready = true;
    processQueryQueue.call(model);
    
  }, options);
  
  // static methods
  nm_.each(staticMethods, function(func, name) {
    model[name] = function() { return func.apply(model, arguments) };
  });
  
  // schema mixins
  schema.mixin(model);
  
  // validations mixins
  if (validations) {
    validations.mixin(model);
  }
  
  return model;
}

function ensureTable(callback, options) {
  var name = options && !nm_.isUndefined(options.pluralize) && !options.pluralize ? nm_s.underscored(this._name) : nm_i.pluralize(nm_s.underscored(this._name));
  this._table = new lTable(this._dakota, name, this._schema);
  this._table.ensureExists(function(err) {
    if (err) {
      throw new lErrors.Model.EnsureTableExists('Error trying to ensure table exists: ' + err + '.');
    }
    else {
      callback();
    }
  }, this._dakota._options.table);
}

// =========================
// = Validate and Sanitize =
// =========================

Model.prototype.validate = function() {
  var self = this;
  
  // beforeValidate callbacks
  runCallbacks.call(this, 'beforeValidate');
  
  var invalidColumns = null;
  nm_.each(this._model._schema.columns(), function(column, index) {
    var messages = self._model.validate(column, self.get(column));
    if (messages) {
      if (!invalidColumns) {
        invalidColumns = {};
      }
      invalidColumns[column] = messages;
    }
  });
  this._invalidColumns = invalidColumns;
  
  // afterValidate callbacks
  runCallbacks.call(this, 'afterValidate');
  
  return invalidColumns;
};

Model.prototype.invalidColumns = function() {
  return this._invalidColumns;
}

// expects: (column{String}, value) or ({ column{String}: value, ... })
// returns: [message{String}, ...] or { column{String}: [message{String}, ...], ... }
staticMethods.validate = function(column, value) {
  var self = this;
  
  if (nm_.isString(column)) {
    return validate.call(this, column, value);
  }
  else if (lHelpers.isHash(column)) {
    var invalidColumns = null;
    nm_.each(column, function(value, c) {
      var messages = validate.call(self, c, value);
      if (messages) {
        if (!invalidColumns) {
          invalidColumns = {};
        }
        invalidColumns[c] = messages;
      }
    });
    return invalidColumns;
  }
  else {
    throw new lErrors.Model.InvalidArgument('Argument should be a string or a {}.');
  }
};

function validate(column, value) {
  if (this._validations) {
    var recipe = this._validations.recipe(column);
    var displayName = displayNameFromRecipe(recipe, column);
    return lValidations.validate(recipe, value, displayName);
  }
  else {
    return null;
  }
}

// expects: (column{String}, value) or ({ column{String}: value, ... })
// returns: value or { column{String}: value, ... }
staticMethods.sanitize = function(column, value) {
  var self = this;
  
  if (nm_.isString(column)) {
    return sanitize.call(this, column, value);
  }
  else if (lHelpers.isHash(column)) {
    var values = {};
    nm_.each(column, function(value, c) {
      values[c] = sanitize.call(self, c, value);
    });
    return values;
  }
  else {
    throw new lErrors.Model.InvalidArgument('Argument should be a string or a {}.');
  }
};

function sanitize(column, value) {
  if (this._validations) {
    var recipe = this._validations.recipe(column);
    return lValidations.sanitize(recipe, value);
  }
  else {
    return value;
  }
}

// expects: (column{String}, value) or ({ column{String}: value, ... })
// returns: value or { column{String}: value, ... }
staticMethods.validateSanitized = function(column, value) {
  var self = this;
  
  if (nm_.isString(column)) {
    return validateSanitized.call(this, column, value);
  }
  else if (lHelpers.isHash(column)) {
    var invalidColumns = null;
    nm_.each(column, function(value, c) {
      var messages = validateSanitized.call(self, c, value);
      if (messages) {
        if (!invalidColumns) {
          invalidColumns = {};
        }
        invalidColumns[c] = messages;
      }
    });
    return invalidColumns;
  }
  else {
    throw new lErrors.Model.InvalidArgument('Argument should be a string or a {}.');
  }
};

function validateSanitized(column, value) {
  var recipe = this._validations.recipe(column);
  var displayName = displayNameFromRecipe(recipe, column);
  return lValidations.validateSanitized(recipe, value, displayName);
}

function displayNameFromRecipe(recipe, column) {
  if (recipe.displayName) {
    return recipe.displayName;
  }
  else {
    return column;
  }
}

// =======================
// = Getters and Setters =
// =======================

// expects: (column{String}, value) or (assignments{Object})
// returns: undefined
Model.prototype.set = function(column, value) {
  var self = this;
  
  if (lHelpers.isHash(column)) {
    nm_.each(column, function(v, c) {
      set.call(self, c, v);
    });
  }
  else if (nm_.isString(column)) {
    set.call(this, column, value);
  }
  else {
    throw new lErrors.Model.InvalidArgument('Argument should be a string or a {}.');
  }
};

// expects: (column{String}, value)
// returns: undefined
function set(column, value) {
  if (!this._model._schema.isColumn(column)) {
    throw new lErrors.Model.InvalidColumn('Invalid column: ' + column + '.');
  }
  else {
    var prevValue = this._get(column);
    
    // sanitize
    value = this._model.sanitize(column, value);
    
    this._set(column, value);
    
    // mark column changed
    if (this._get(column) !== prevValue) {
      this._changes[column] = { prev: prevValue };
    }
  }
}

// expects: (column{String}, value) or (assignments{Object})
// returns: undefined
Model.prototype._set = function(column, value) {
  var self = this;
  
  if (lHelpers.isHash(column)) {
    nm_.each(column, function(v, c) {
      _set.call(self, c, v);
    });
  }
  else if (nm_.isString(column)) {
    _set.call(this, column, value);
  }
  else {
    throw new lErrors.Model.InvalidArgument('Argument should be a string or a {}.');
  }
};

function _set(column, value) {
  if (!this._model._schema.isColumn(column)) {
    throw new lErrors.Model.InvalidColumn('Invalid column: ' + column + '.');
  }
  else {
    
    if (!nm_.isNull(value)) {
      
      // validate type
      if (!this._model._schema.isValidValueTypeForColumn(column, value)) {
        throw new lErrors.Model.TypeMismatch('Value for ' + column + ' should be of type: ' + this._model._schema.columnType(column) + '.');
      }
      
      // cast all uuid and timeuuid to string
      var type = this._model._schema.columnType(column);
      if (type == 'uuid' || type == 'timeuuid') {
        value = value.toString();
      }
    }
    
    this._columns[column] = value;
  }
}

// expects: (column{String})
// returns: value
Model.prototype.get = function(column) {
  if (!nm_.isString(column)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a string.');
  }
  else if (!this._model._schema.isColumn(column)) {
    throw new lErrors.Model.InvalidColumn('Invalid column: ' + column + '.');
  }
  else {
    return this._get(column);
  }
};

// expects: (column{String})
// returns: value
Model.prototype._get = function(column) {
  if (!nm_.isString(column)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a string.');
  }
  else if (!this._model._schema.isColumn(column)) {
    throw new lErrors.Model.InvalidColumn('Invalid column: ' + column + '.');
  }
  else {
    return this._columns[column];
  }
};

// ===========
// = Changes =
// ===========

// expects: (column{String}?)
//          column: returns if changed for specified column; or if no column specified, returns if any column changed
// returns: changed{Boolean}
Model.prototype.changed = function(column) {
  if (column && !nm_.isString(column)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a string.');
  }
  else if (column && !this._model._schema.isColumn(column)) {
    throw new lErrors.Model.InvalidColumn('Invalid column: ' + column + '.');
  }
  else {
    if (column) {
      return !!this._changes[column];
    }
    else {
      return nm_.size(this._changes) > 0;
    }
  }
};

// expects: (column{String}?)
//          column: returns changes for specified column; or if no column specified, returns all changes
// returns: { column: { from: prevValue, to: currentValue }, ... } or { from: prevValue, to: currentValue }
Model.prototype.changes = function(column) {
  var self = this;
  
  if (column && !nm_.isString(column)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a string.');
  }
  else if (column && !this._model._schema.isColumn(column)) {
    throw new lErrors.Model.InvalidColumn('Invalid column: ' + column + '.');
  }
  else {
    if (column) {
      var value = this._changes[column];
      if (!nm_.isUndefined(value)) {
        return { from: value.prev, to: this._get(column) }
      }
      else {
        return value;
      }
    }
    else {
      var changes = {};
      nm_.each(this._changes, function(value, column) {
        changes[column] = { from: value.prev, to: self._get(column) }
      });
      return changes;
    }
  }
};

// ==========
// = Create =
// ==========

// expects: (assignments{Object})
// returns: model{Model}
staticMethods.new = function(assignments) {
  return new this(assignments);
};

// expects: (assignments{Object}, function(err{Error}))
// returns: model{Model}
staticMethods.create = function(assignments, callback) {
  if (!nm_.isFunction(assignments)) {
    if (!nm_.isFunction(callback)) {
      throw new lErrors.Model.InvalidArgument('Argument should be a function.');
    }
    else if (!lHelpers.isHash(assignments)) {
      throw new lErrors.Model.InvalidArgument('Argument should be a {}.');
    }
  }
  else {
    callback = assignments;
    assignments = {};
  }
  
  var model = new this(assignments);
  model.save(callback);
  return model;
};

// expects: (row{Row})
// returns: model{Model}
staticMethods._newFromQueryRow = function(row) {
  var model = new this({}, { skipAfterNewCallback: true });
  model._set(row);
  model._exists = true;
  return model
};

// ====================
// = Find and FindOne =
// ====================

// expects: ({ column{String}: value, ... }, function(err{Error}, result{Array})) or ({ column{String}: { operation[$eq, $gt, $gte, ...]{String}: value }, ... }, function(err{Error}, result{Array}))
// returns: undefined
staticMethods.find = function(conditions, callback) {
  if (!lHelpers.isHash(conditions)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a {}.');
  }
  else if (!nm_.isFunction(callback)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a function.');
  }
  else {
    this.where(conditions).all(callback);
  }
};

// expects: ({ column{String}: value, ... }, function(err{Error}, result{Model})) or ({ column{String}: { operation[$eq, $gt, $gte, ...]{String}: value }, ... }, function(err{Error}, result{Model}))
// returns: undefined
staticMethods.findOne = function(conditions, callback) {
  if (!lHelpers.isHash(conditions)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a {}.');
  }
  else if (!nm_.isFunction(callback)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a function.');
  }
  else {
    this.where(conditions).first(callback);
  }
};

// =========
// = Query =
// =========

// expects: ([column{String}, ...], options{Object}?) or (column{String}, ..., options{Object}?)
// returns: this{Query}
staticMethods.select = function(columns, options) {
  var cols = [];
  nm_.each(arguments, function(column, index) {
    if (nm_.isString(column)) {
      cols.push(column);
    }
    else if (lHelpers.isHash(column)) {
      options = column;
    }
    else {
      throw new lErrors.Model.InvalidArgument('Argument should be a string or {}.');
    }
  });
  var query = new lQuery(this, options);
  return query.select(cols);
};


// expects: (column{String}, value, options{Object}?) or ({ column{String}: value }, options{Object}?) or ({ column{String}: { operation[$eq, $gt, $gte, ...]{String}: value }}, options{Object}?)
// returns: query{Query}
staticMethods.where = function(arg1, arg2, options) {
  if (nm_.isString(arg1)) {
    if (options && !lHelpers.isHash(options)) {
      throw new lErrors.Model.InvalidArgument('Argument should be a {}.');
    }
  }
  else {
    if (!lHelpers.isHash(arg1)) {
      throw new lErrors.Model.InvalidArgument('Argument should be a {}.');
    }
    else if (arg2 && !lHelpers.isHash(arg2)) {
      throw new lErrors.Model.InvalidArgument('Argument should be a {}.');
    }
    options = arg2;
    arg2 = null;
  }
  var query = new lQuery(this, options);
  return query.where(arg1, arg2);
};

// expects: ({ partitionKey{String}: order[$asc, $desc]{String} }, options{Object}?) or (partitionKey{String}, order[$asc, $desc]{String}, options{Object}?)
// returns: query{Query}
staticMethods.orderBy = function(arg1, arg2, options) {
  if (nm_.isString(arg1)) {
    if (!nm_.isString(arg2)) {
      throw new lErrors.Model.InvalidArgument('Argument should be a string.');
    }
    else if (options && !lHelpers.isHash(options)) {
      throw new lErrors.Model.InvalidArgument('Argument should be a {}.');
    }
  }
  else {
    if (!lHelpers.isHash(arg1)) {
      throw new lErrors.Model.InvalidArgument('Argument should be a {}.');
    }
    else if (arg2 && !lHelpers.isHash(arg2)) {
      throw new lErrors.Model.InvalidArgument('Argument should be a {}.');
    }
    options = arg2;
    arg2 = null;
  }
  var query = new lQuery(this, options);
  return query.orderBy(arg1, arg2);
};

// expects: (limit{Integer}, options{Object}?)
// returns: query{Query}
staticMethods.limit = function(limit, options) {
  if (!lHelpers.isInteger(limit)) {
    throw new lErrors.Model.InvalidArgument('Argument should be an Integer.');
  }
  else if (options && !lHelpers.isHash(options)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a {}.');
  }
  var query = new lQuery(this, options);
  return query.limit(limit);
};

// expects: (allow{Boolean}, options{Object}?)
// returns: query{Query}
staticMethods.allowFiltering = function(allow, options) {
  if (!nm_.isBoolean(allow)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a boolean.');
  }
  else if (options && !lHelpers.isHash(options)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a {}.');
  }
  var query = new lQuery(this, options);
  return query.allowFiltering(allow);
};

// expects: (function(err{Error}, result{Row}), options{Object}?)
// return: undefined
staticMethods.first = function(callback, options) {
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a function.');
  }
  else if (options && !lHelpers.isHash(options)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a {}.');
  }
  var query = new lQuery(this, options);
  query.first(callback);
};

// expects: (function(err{Error}, result{Array}), options{Object}?)
// return: undefined
staticMethods.all = function(callback, options) {
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a function.');
  }
  else if (options && !lHelpers.isHash(options)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a {}.');
  }
  var query = new lQuery(this, options);
  query.all(callback);
};

// expects: (function(err{Error}, count{Number}), options{Object}?)
// return: undefined
staticMethods.count = function(callback, options) {
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a function.');
  }
  else if (options && !lHelpers.isHash(options)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a {}.');
  }
  var query = new lQuery(this, options);
  query.count(callback);
};

// expects: (function(n{Number}, row{Row}), function(err{Error}), options{Object}?)
// return: undefined
staticMethods.eachRow = function(rowCallback, completeCallback, options) {
  if (!nm_.isFunction(rowCallback) || !nm_.isFunction(completeCallback)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a function.');
  }
  else if (options && !lHelpers.isHash(options)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a {}.');
  }
  var query = new lQuery(this, options);
  query.eachRow(rowCallback, completeCallback);
};

// ==========
// = Update =
// ==========

// expects: (function(err{Error}), options{Object}?)
// returns: undefined
Model.prototype.save = function(callback, options) {
  var self = this;
  
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a function.');
  }
  else if (options && !lHelpers.isHash(options)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a {}.');
  }
  
  options = nm_.extend({}, options);
  
  // validate
  var invalidColumns = this.validate();
  if (invalidColumns) {
    callback(new lErrors.Model.ValidationFailedError(invalidColumns));
    return;
  }
  
  // only set columns that were changed
  var assignments = {};
  nm_.each(this._changes, function(value, column) {
    assignments[column] = self._get(column);
  });
  
  var query = new lQuery(this._model, options);
  if (this._exists) {
    
    // beforeSave callbacks
    runCallbacks.call(this, 'beforeSave');
    
    query.action('update').update(assignments).where(primaryKeyConditions.call(this)).execute(function(err, result) {
      if (err) {
        callback(err);
      }
      else {
        
        // clear changed fields
        self._changes = {};
        
        // afterSave callbacks
        runCallbacks.call(self, 'afterSave');
        
        callback();
      }
    });
  }
  else {
    
    // beforeCreate callbacks
    runCallbacks.call(this, 'beforeCreate');
    
    // beforeSave callbacks
    runCallbacks.call(this, 'beforeSave');
    
    query.action('insert').insert(assignments).execute(function(err, result) {
      if (err) {
        callback(err);
      }
      else {
        
        // clear changed fields
        self._changes = {};
        
        // afterCreate callbacks
        runCallbacks.call(self, 'afterCreate');
        
        // afterSave callbacks
        runCallbacks.call(self, 'afterSave');
        
        self._exists = true;
        callback();
      }
    });
  }
};

// ==========
// = Delete =
// ==========

// expects: (function(err{Error}), options{Object}?)
// returns: undefined
Model.prototype.delete = function(callback, options) {
  var self = this;
  
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a function.');
  }
  else if (options && !lHelpers.isHash(options)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a {}.');
  }
  
  // beforeDelete callbacks
  runCallbacks.call(this, 'beforeDelete');
  
  options = nm_.extend({}, options);
  var query = new lQuery(this._model, options);
  query.action('delete').where(primaryKeyConditions.call(this));
  
  if (options.using) {
    query.using(options.using);
  }
  if (options.if) {
    query.if(options.if);
  }
  if (options.ifExists) {
    query.ifExists(options.ifExists);
  }
  
  query.execute(function(err, result) {
    if (err) {
      callback(err);
    }
    else {
      
      // afterDelete callbacks
      runCallbacks.call(self, 'afterDelete');
      
      self._exists = false;
      callback();
    }
  });
};

// expects: (function(err{Error}), options{Object}?)
// return: undefined
staticMethods.truncate = function(callback, options) {
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a function.');
  }
  else if (options && !lHelpers.isHash(options)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a {}.');
  }
  var query = new lQuery(this, options);
  query.truncate(callback);
}

// alias truncate
staticMethods.deleteAll = function(callback, options) {
  this.truncate(callback, options);
};

// ===========
// = Execute =
// ===========

// expects: the same params as Cassandra.Client.execute(); (query{String}, [param, param, ...], options{Object}, function(err{Error}, result))
// returns: undefined
staticMethods._execute = function(query, params, options, callback) {
  if (!this._ready) {
    addToQueryQueue.call(this, 'execute', arguments);
  }
  else {
    this._dakota.execute(query, params, options, callback);
  }
};

// expects: the same params as Cassandra.Client.eachRow(); (query{String}, [param, param, ...], options{Object}, function(n{Number}, row{Row}), function(err{Error}))
// returns: undefined
staticMethods._eachRow = function(query, params, options, rowCallback, completeCallback) {
  if (!this._ready) {
    addToQueryQueue.call(this, 'eachRow', arguments);
  }
  else {
    this._dakota.eachRow(query, params, options, rowCallback, completeCallback);
  }
};

// expects: the same params as Cassandra.Client.eachRow(); (query{String}, [param, param, ...], options{Object})
// returns: undefined
staticMethods._stream = function(query, params, options) {
  if (!this._ready) {
    // need to buffer stream incase client is not yet set
    throw new Error('Not implemented yet.');
  }
  else {
    this._dakota.stream(query, params, options);
  }
};

// ===============
// = Query Queue =
// ===============

// expects: (action[execute, eachRow]{String}, [arguments])
// returns: undefined
function addToQueryQueue(action, arguments) {
  if (!this._queryQueue) {
    throw new lErrors.Model.QueryQueueAlreadyProcessed('Cannot enqueue query. Queue already processed.');
  }
  else if (action !== 'execute' && action !== 'eachRow') {
    throw new lErrors.Model.InvalidQueryQueueAction('Invalid action: ' + action + '.');
  }
  else {
    this._queryQueue[action].push(arguments);
  }
}

// expects: ()
// returns: undefined
function processQueryQueue() {
  var self = this;
  
  if (!this._queryQueue) {
    throw new lErrors.Model.QueryQueueAlreadyProcessed('Cannot process queue. Queue already processed.');
  }
  else {
    nm_.each(this._queryQueue, function(queue, action) {
      nm_.each(queue, function(query, index) {
        if (action === 'execute') {
          self._execute.apply(self, query);
        }
        else if (action === 'eachRow') {
          self._eachRow.apply(self, query);
        }
      });
    });
    this._queryQueue = null;
  }
}

// =============
// = Callbacks =
// =============

function runCallbacks(key) {
  var self = this;
  
  if (!this._model._callbacks[key]) {
    throw new lErrors.Model.InvalidCallbackKey('Invalid callback key: ' + key + '.');
  }
  else {
    nm_.each(this._model._callbacks[key], function(callback, index) {
      callback.call(self);
    });
  }
}

// ===========
// = Helpers =
// ===========

// expects: ()
// returns: primaryKeyConditions{Object}
function primaryKeyConditions() {
  var self = this;
  
  var conditions = {};
  
  var partitionKey = this._model._schema.partitionKey();
  if (nm_.isString(partitionKey)) {
    partitionKey = [partitionKey];
  }
  nm_.each(partitionKey, function(column, index) {
    conditions[column] = self.get(column);
  });
  
  var clusteringKey = this._model._schema.clusteringKey();
  if (clusteringKey) {
    if (nm_.isString(clusteringKey)) {
      clusteringKey = [clusteringKey];
    }
    nm_.each(clusteringKey, function(column, index) {
      conditions[column] = self.get(column);
    });
  }
  
  return conditions;
}

module.exports = Model;