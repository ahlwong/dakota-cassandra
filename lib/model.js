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
    
    if (!options || !options._skipAfterNewCallback) {
      
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
    
  });
  
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

function ensureTable(callback) {
  this._table = new lTable(this._dakota, this._options.tableName(this._name), this._schema, this._options.table);
  this._table.ensureExists(function(err) {
    if (err) {
      throw new lErrors.Model.EnsureTableExists('Error trying to ensure table exists: ' + err + '.');
    }
    else {
      callback();
    }
  });
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
  
  var prevValue = this._changes[column] ? this._changes[column].prev : this._get(column);
  
  // sanitize
  value = this._model.sanitize(column, value);
  
  // schema setter
  var setter = this._model._schema.columnSetter(column);
  if (setter) {
    value = setter.call(this, value);
  }
  
  this._set(column, value);
  
  // mark column changed
  value = this._get(column);
  if (value !== prevValue) {
    this._changes[column] = { prev: prevValue };
  }
  else {
    delete this._changes[column];
  }
}

// expects: (column{String}, value) or (assignments{Object})
// returns: undefined
Model.prototype._set = function(column, value) {
  var self = this;
  
  // disallow setting primary key columns
  if (this._exists && this._model._schema.isKeyColumn()) {
    throw new lErrors.Model.CannotSetKeyColumns('Columns in primary key cannot be modified once set: ' + column + '.');
  }
  
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
  
  // cassandra treats empty sets and lists as null values
  if (nm_.isArray(value) && value.length === 0) {
    value = null;
  }
  
  if (!nm_.isNull(value)) { // allow null values
    
    // cast value
    value = lTypes.castValue(this._model._dakota, value);
    
    // validate type
    if (!this._model._schema.isValidValueTypeForColumn(column, value)) {
      throw new lErrors.Model.TypeMismatch('Value for ' + column + ' should be of type: ' + this._model._schema.columnType(column) + '.');
    }
  }
  
  this._columns[column] = value;
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
  
  var value = this._get(column);
  
  // schema getter
  var getter = this._model._schema.columnGetter(column);
  if (getter) {
    value = getter.call(this, value);
  }
  
  return value
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
  
  var value = this._columns[column];
  
  // cast undefined
  if (nm_.isUndefined(value)) {
    
    // cast counters to 0
    if (this._model._schema.columnType(column) === 'counter') {
      value = 0;
    }
    
    // cast everything else to null
    else {
      value = null;
    }
  }
  
  return value;
};

// =========================
// = Type Specific Setters =
// =========================

// expects: (column{String}, value)
// returns: undefined
Model.prototype.add = function(column, value) {
  if (!this._model._schema.isColumn(column)) {
    throw new lErrors.Model.InvalidColumn('Invalid column: ' + column + '.');
  }
  else if (this._model._schema.baseColumnType(column) !== 'set') {
    throw new lErrors.Model.InvalidColumnType('Add can only be performed on columns of type set.');
  }
  
  append.call(this, '$add', column, value);
};

// expects: (column{String}, value)
// returns: undefined
Model.prototype.remove = function(column, value) {
  if (!this._model._schema.isColumn(column)) {
    throw new lErrors.Model.InvalidColumn('Invalid column: ' + column + '.');
  }
  else {
    var baseType = this._model._schema.baseColumnType(column)
    if (baseType !== 'set' && baseType !== 'list') {
      throw new lErrors.Model.InvalidColumnType('Add can only be performed on columns of type set or list.');
    }
  }
  
  var prevValue = this._changes[column] ? this._changes[column].prev : this._get(column);
  
  // normalize
  if (!nm_.isArray(value)) {
    value = [value];
  }
  
  // remove
  var newValue = this._get(column);
  if (newValue) {
    var arguments = [newValue].concat(value);
    newValue = nm_.without.apply(this, arguments);
  }
  else {
    newValue = null;
  }
  
  typeSpecificSet.call(this, column, newValue);
  
  // mark column changed
  newValue = this._get(column);
  if (newValue !== prevValue) {
    if (this._changes[column] && this._changes[column].op) {
      if (this._changes[column].op['$remove']) {
        this._changes[column].op['$remove'] = this._changes[column].op['$remove'].concat(value);
      }
      else {
        delete this._changes[column].op;
      }
    }
    else {
      this._changes[column] = { prev: prevValue, operation: { '$remove': value } };
    }
  }
  else {
    delete this._changes[column];
  }
};

// expects: (column{String}, value)
// returns: undefined
Model.prototype.prepend = function(column, value) {
  if (!this._model._schema.isColumn(column)) {
    throw new lErrors.Model.InvalidColumn('Invalid column: ' + column + '.');
  }
  else if (this._model._schema.baseColumnType(column) !== 'list') {
    throw new lErrors.Model.InvalidColumnType('Add can only be performed on columns of type list.');
  }
  
  var prevValue = this._changes[column] ? this._changes[column].prev : this._get(column);
  
  // normalize
  if (!nm_.isArray(value)) {
    value = [value];
  }
  
  // prepend
  var newValue = this._get(column);
  if (newValue) {
    newValue = value.concat(newValue);
  }
  else {
    newValue = value;
  }
  
  typeSpecificSet.call(this, column, newValue);
  
  // mark column changed
  newValue = this._get(column);
  if (newValue !== prevValue) {
    if (this._changes[column] && this._changes[column].op) {
      if (this._changes[column].op['$prepend']) {
        this._changes[column].op['$prepend'] = this._changes[column].op['$prepend'].concat(value);
      }
      else {
        delete this._changes[column].op;
      }
    }
    else {
      var op = {};
      op['$prepend'] = value;
      this._changes[column] = { prev: prevValue, op: op };
    }
  }
  else {
    delete this._changes[column];
  }
};

// expects: (column{String}, value)
// returns: undefined
Model.prototype.append = function(column, value) {
  if (!this._model._schema.isColumn(column)) {
    throw new lErrors.Model.InvalidColumn('Invalid column: ' + column + '.');
  }
  else if (this._model._schema.baseColumnType(column) !== 'list') {
    throw new lErrors.Model.InvalidColumnType('Add can only be performed on columns of type list.');
  }
  
  append.call(this, '$append', column, value);
};

// expects: (operation[$add, $append]{String}, column{String}, value)
// returns: undefined
function append(operation, column, value) {
  var prevValue = this._changes[column] ? this._changes[column].prev : this._get(column);
  
  // normalize
  if (!nm_.isArray(value)) {
    value = [value];
  }
  
  // append
  var newValue = this._get(column);
  if (newValue) {
    newValue = newValue.concat(value);
  }
  else {
    newValue = value;
  }
  
  typeSpecificSet.call(this, column, newValue);
  
  // mark column changed
  newValue = this._get(column);
  if (newValue !== prevValue) {
    if (this._changes[column] && this._changes[column].op) {
      if (this._changes[column].op[operation]) {
        this._changes[column].op[operation] = this._changes[column].op[operation].concat(value);
      }
      else {
        delete this._changes[column].op;
      }
    }
    else {
      var op = {};
      op[operation] = value;
      this._changes[column] = { prev: prevValue, op: op };
    }
  }
  else {
    delete this._changes[column];
  }
}

// expects: (column{String}, key, value)
// returns: undefined
Model.prototype.inject = function(column, key, value) {
  if (!this._model._schema.isColumn(column)) {
    throw new lErrors.Model.InvalidColumn('Invalid column: ' + column + '.');
  }
  else {
    var type = this._model._schema.baseColumnType(column);
    if (type !== 'list' && type !== 'map') {
      throw new lErrors.Model.InvalidColumnType('Inject can only be performed on columns of type list or map.');
    }
    else if (type === 'list' && !lHelpers.isInteger(key)) {
      throw new lErrors.Model.InvalidArgument('Key should be of type integer.');
    }
  }
  
  var prevValue = this._changes[column] ? this._changes[column].prev : this._get(column);
  
  // create empty object if null
  var newValue = this._get(column);
  if (!newValue) {
    if (type === 'list') {
      newValue = [];
    }
    else {
      newValue = {};
    }
  }
  
  // validate index
  if (type === 'list' && key > newValue.length) {
    throw new lErrors.Model.InvalidArgument('Key index is out of bounds. Length of list is: ' + newValue.length + '.');
  }
  
  // inject
  newValue[key] = value;
  
  typeSpecificSet.call(this, column, newValue);
  
  // mark column changed
  newValue = this._get(column);
  if (newValue !== prevValue) {
    if (this._changes[column] && this._changes[column].op) {
      if (this._changes[column].op['$inject']) {
        this._changes[column].op['$inject'][key] = value;
      }
      else {
        delete this._changes[column].op;
      }
    }
    else {
      var op = {};
      op['$inject'] = {};
      op['$inject'][key] = value;
      this._changes[column] = { prev: prevValue, op: op };
    }
  }
  else {
    delete this._changes[column];
  }
};

// expects: (column{String}, delta{Number})
// returns: undefined
Model.prototype.increment = function(column, delta) {
  if (!this._model._schema.isColumn(column)) {
    throw new lErrors.Model.InvalidColumn('Invalid column: ' + column + '.');
  }
  else if (this._model._schema.baseColumnType(column) !== 'counter') {
    throw new lErrors.Model.InvalidColumnType('Increment can only be performed on columns of type counter.');
  }
  
  increment.call(this, column, delta);
};

// expects: (column{String}, delta{Number})
// returns: undefined
Model.prototype.decrement = function(column, delta) {
  if (!this._model._schema.isColumn(column)) {
    throw new lErrors.Model.InvalidColumn('Invalid column: ' + column + '.');
  }
  else if (this._model._schema.baseColumnType(column) !== 'counter') {
    throw new lErrors.Model.InvalidColumnType('Decrement can only be performed on columns of type counter.');
  }
  
  increment.call(this, column, -delta);
};

// expects: (column{String}, delta{Number})
// returns: undefined
function increment(column, delta) {
  var prevValue = this._changes[column] ? this._changes[column].prev : this._get(column);
  
  // increment
  var newValue = this._get(column) + delta;
  
  typeSpecificSet.call(this, column, newValue);
  
  // mark column changed
  newValue = this._get(column);
  if (newValue !== prevValue) {
    delta = newValue - prevValue;
    var op = {};
    if (delta > 0) {
      op['$incr'] = delta;
    }
    else {
      op['$decr'] = -delta;
    }
    this._changes[column] = { prev: prevValue, op: op };
  }
  else {
    delete this._changes[column];
  }
}

function typeSpecificSet(column, value) {
  
  // sanitize
  value = this._model.sanitize(column, value);
  
  // schema setter
  var setter = this._model._schema.columnSetter(column);
  if (setter) {
    value = setter.call(this, value);
  }
  
  this._set(column, value);
}

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
        var newValue = this._get(column);
        return { from: value.prev, to: newValue, op: value.op ? value.op : { '$set' : newValue } }
      }
      else {
        return value;
      }
    }
    else {
      var changes = {};
      nm_.each(this._changes, function(value, column) {
        var newValue = self._get(column);
        changes[column] = { from: value.prev, to: newValue, op: value.op ? value.op : { '$set' : newValue } }
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
  var model = new this({}, { _skipAfterNewCallback: true });
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
  
  // decide if update or insert
  var update = this._exists || this._model._schema._isCounterColumnFamily;
  
  // only set columns that were changed
  var assignments = {};
  nm_.each(this._changes, function(value, column) {
    if (!(!self._exists && update && self._model._schema.isKeyColumn(column))) { // don't assign keys in SET clause for UPDATE insertions
      if (value.op) {
        var key = nm_.keys(value.op)[0];
        var assignment = {};
        assignment[key] = lTypes.formatValueType(self._model._dakota, self._model._schema.columnType(column), value.op[key]);
        assignments[column] = assignment;
      }
      else {
        assignments[column] = lTypes.formatValueType(self._model._dakota, self._model._schema.columnType(column), self._get(column));
      }
    }
  });
  
  // prevent empty SET clause in UPDATE insertions for counter rows by incrementing counters by 0
  if (!this._exists && this._model._schema._isCounterColumnFamily && nm_.isEmpty(assignments)) {
    nm_.each(this._model._schema.columns(), function(column, index) {
      if (self._model._schema.columnType(column) === 'counter') {
        assignments[column] = { '$incr': 0 }
      }
    });
  }
  
  // create query
  var query = new lQuery(this._model, options);
  
  // beforeCreate callbacks
  if (!this._exists) {
    runCallbacks.call(this, 'beforeCreate');
  }
  
  // beforeSave callbacks
  runCallbacks.call(this, 'beforeSave');
  
  // wrap callback
  function wrappedCallback(err, result) {
    if (err) {
      callback(err);
    }
    else {
      
      // clear changed fields
      self._changes = {};
      
      // afterCreate callbacks
      if (!self._exists) {
        runCallbacks.call(self, 'afterCreate');
      }
      
      // afterSave callbacks
      runCallbacks.call(self, 'afterSave');
      
      // mark as exists
      if (!self._exists) {
        self._exists = true;
      }
      
      callback();
    }
  }
  
  // execute query
  if (update) {
    query.action('update').update(assignments).where(primaryKeyConditions.call(this)).execute(wrappedCallback);
  }
  else {
    query.action('insert').insert(assignments).execute(wrappedCallback);
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
    var value = self.get(column);
    conditions[column] = nm_.isUndefined(value) ? null : value;
  });
  
  var clusteringKey = this._model._schema.clusteringKey();
  if (clusteringKey) {
    if (nm_.isString(clusteringKey)) {
      clusteringKey = [clusteringKey];
    }
    nm_.each(clusteringKey, function(column, index) {
      var value = self.get(column);
      conditions[column] = nm_.isUndefined(value) ? null : value;
    });
  }
  
  return conditions;
}

module.exports = Model;