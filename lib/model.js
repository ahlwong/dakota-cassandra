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
var lWrappedStream = require('./wrapped_stream');
var util = require('./util');

// expects: ({ column{String}: value, ... }?)
// returns: undefined
function Model(assignments) {
  this._exists = false;
  this._upsert = false;
  this._columns = {};
  this._changes = {};
  this._prevChanges = {};
  this._invalidColumns = null;
  
  // set assignments
  if (assignments) {
    if (!lHelpers.isPlainObject(assignments)) {
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
  else if (options && !lHelpers.isPlainObject(options)) {
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
    eachRow: [],
    stream: []
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

// expects: (function())
// returns: undefined
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

// expects: (options{Object}?)
// options:
//          only{Array} : validate only these columns
//          except{Array} : validate all columns except these
//          callbacks{Object} : modify callback behavior
//            callbacks.skipAll{Boolean} : skips all callbacks, takes precedence over subsequent settings
//            callbacks.skipBeforeValidate{Boolean}
//            callbacks.skipAfterValidate{Boolean}
// returns: { column{String}: [message{String}, ...], ... } or null
Model.prototype.validate = function(options) {
  var self = this;
  
  if (options) {
    if (!lHelpers.isPlainObject(options)) {
      throw new lErrors.Model.InvalidArgument('Argument should be a {}.');
    }
    else if (options.only && options.except) {
      throw new lErrors.Model.InvalidArgument('Only or except are mutually exclusive.');
    }
  }
  
  // default options
  options = nm_.extend({}, options);
  
  // beforeValidate callbacks
  if (!options.callbacks || !(options.callbacks.skipAll || options.callbacks.skipBeforeValidate)) {
    runCallbacks.call(this, 'beforeValidate');
  }
  
  var columns = null;
  
  // if update, only validate changed columns that aren't idempotent operations
  if (this._upsert) {
    columns = nm_.reduce(nm_.keys(this._changes), function(memo, column) {
      if (self._changes[column].op['$set']) {
        memo.push(column);
      }
      return memo;
    }, []);
  }
  
  // validate all columns
  else {
    columns = this._model._schema.columns();
  }
  
  var invalidColumns = null;
  nm_.each(columns, function(column, index) {
    if (!options || !(options.only || options.except) || (options.only && options.only.indexOf(column) > -1) || (options.except && options.except.indexOf(column) === -1)) {
      var messages = self._model.validate(column, self.get(column), self);
      if (messages) {
        if (!invalidColumns) {
          invalidColumns = {};
        }
        invalidColumns[column] = messages;
      }
    }
  });
  this._invalidColumns = invalidColumns;
  
  // afterValidate callbacks
  if (!options.callbacks || !(options.callbacks.skipAll || options.callbacks.skipAfterValidate)) {
    runCallbacks.call(this, 'afterValidate');
  }
  
  return invalidColumns;
};

Model.prototype.invalidColumns = function() {
  return this._invalidColumns;
}

// expects: (column{String}, value, instance{model}?) or ({ column{String}: value, ... }, instance{model}?)
// returns: [message{String}, ...] or { column{String}: [message{String}, ...], ... } or null
staticMethods.validate = function(column, value, instance) {
  var self = this;
  
  if (nm_.isString(column)) {
    return validate.call(this, column, value, instance);
  }
  else if (lHelpers.isPlainObject(column)) {
    instance = value;
    value = null;
    var invalidColumns = null;
    nm_.each(column, function(v, c) {
      var messages = validate.call(self, c, v, instance);
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

// expects: (column{String}, value, instance{model}?)
// returns: [message{String}, ... ]
function validate(column, value, instance) {
  if (this._validations) {
    var recipe = this._validations.recipe(column);
    var displayName = displayNameFromRecipe(recipe, column);
    return lValidations.validate(recipe, value, displayName, instance);
  }
  else {
    return null;
  }
}

// expects: (column{String}, value, instance{model}?) or ({ column{String}: value, ... }, instance{model}?)
// returns: value or { column{String}: value, ... }
staticMethods.sanitize = function(column, value, instance) {
  var self = this;
  
  if (nm_.isString(column)) {
    return sanitize.call(this, column, value);
  }
  else if (lHelpers.isPlainObject(column)) {
    instance = value;
    value = null;
    var values = {};
    nm_.each(column, function(v, c) {
      values[c] = sanitize.call(self, c, v, instance);
    });
    return values;
  }
  else {
    throw new lErrors.Model.InvalidArgument('Argument should be a string or a {}.');
  }
};

// expects: (column{String}, value, instance{model}?)
// returns: value
function sanitize(column, value, instance) {
  if (this._validations) {
    var recipe = this._validations.recipe(column);
    return lValidations.sanitize(recipe, value, instance);
  }
  else {
    return value;
  }
}

// expects: (column{String}, value, instance{model}?) or ({ column{String}: value, ... }, instance{model}?)
// returns: [message{String}, ... ] or { column{String}: [message{String}, ...], ... }
staticMethods.validateSanitized = function(column, value, instance) {
  var self = this;
  
  if (nm_.isString(column)) {
    return validateSanitized.call(this, column, value, instance);
  }
  else if (lHelpers.isPlainObject(column)) {
    instance = value;
    value = null;
    var invalidColumns = null;
    nm_.each(column, function(v, c) {
      var messages = validateSanitized.call(self, c, v, instance);
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

// exoects: (column{String}, value)
// return: [message{String}, ... ]
function validateSanitized(column, value, instance) {
  var recipe = this._validations.recipe(column);
  var displayName = displayNameFromRecipe(recipe, column);
  return lValidations.validateSanitized(recipe, value, displayName, instance);
}

// exoects: (validationRecipe{Object}, column{String})
// return: displayName{String}
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
  
  if (lHelpers.isPlainObject(column)) {
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
  if (!nm_.isString(column)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a string.');
  }
  
  // convert alias to column
  if (this._model._schema.isAlias(column)) {
    column = this._model._schema.columnFromAlias(column);
  }
  
  if (!this._model._schema.isColumn(column)) {
    throw new lErrors.Model.InvalidColumn('Invalid column: ' + column + '.');
  }
  else if (this._model._schema.baseColumnType(column) === 'counter') {
    throw new lErrors.Model.CannotSetCounterColumns('Counter column: ' + column + ' cannot be set directly. Increment or decrement instead.');
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
  
  // don't mark keys as changed for UPDATE operations
  if (this._upsert && this._model._schema.isKeyColumn(column)) {
    return;
  }
  
  // mark column changed
  else {
    value = this._get(column);
    if (!lHelpers.isEqual(value, prevValue)) {
      this._changes[column] = { prev: prevValue, op: { '$set': true } };
    }
    else {
      delete this._changes[column];
    }
  }
}

// expects: (column{String}, value) or (assignments{Object})
// returns: undefined
Model.prototype._set = function(column, value) {
  var self = this;
  
  // disallow setting primary key columns
  if (!this._upsert && this._exists && this._model._schema.isKeyColumn()) {
    throw new lErrors.Model.CannotSetKeyColumns('Columns in primary key cannot be modified once set: ' + column + '.');
  }
  
  if (nm_.isObject(column)) {
    nm_.each(column, function(v, c) {
      _set.call(self, c, v);
    });
  }
  else if (nm_.isString(column)) {
    _set.call(this, column, value);
  }
  else {
    throw new lErrors.Model.InvalidArgument('Argument should be a string or an object.');
  }
};

// expects: (column{String}, value)
// returns: undefined
function _set(column, value) {
  
  if (!this._model._schema.isColumn(column)) {
    throw new lErrors.Model.InvalidColumn('Invalid column: ' + column + '.');
  }
  
  // cassandra treats empty sets and lists as null values
  if (nm_.isArray(value) && value.length === 0) {
    value = null;
  }
  
  if (!nm_.isNull(value)) { // allow null values
    
    // cast string type to javascript types
    if (nm_.isString(value)) {
      var type = this._model._schema.baseColumnType(column);
      if (lTypes.isNumberType(this._model._dakota, type)) {
        value = parseFloat(value.replace(/[^\d\.\-]/g, ''));
        if (nm_.isNaN(value)) {
          value = null;
        }
      }
      else if (lTypes.isBooleanType(this._model._dakota, type)) {
        value = value !== '0' && value !== 'false' && value;
      }
    }
    
    // cast cassandra types to javascript types
    else {
      value = lTypes.castValue(this._model._dakota, value);
    }
    
    // validate type
    // recheck null, since casting can cast to null
    if (!nm_.isNull(value) && !this._model._schema.isValidValueTypeForColumn(column, value)) {
      throw new lErrors.Model.TypeMismatch('Value for ' + column + ' should be of type: ' + this._model._schema.columnType(column) + '.');
    }
    
    // make set array uniq
    if (this._model._schema.baseColumnType(column) === 'set') {
      value = lHelpers.uniq(value);
    }
  }
  
  this._columns[column] = value;
}

// expects: (column{String}) or ([column{String}, ... ])
// returns: value or { column{String}: value, ... }
Model.prototype.get = function(column) {
  var self = this;
  
  if (nm_.isArray(column)) {
    var columns = {};
    nm_.each(column, function(c, index) {
      columns[c] = get.call(self, c);
    });
    return columns;
  }
  else if (nm_.isString(column)) {
    return get.call(this, column);
  }
  else {
    throw new lErrors.Model.InvalidArgument('Argument should be a string or a {}.');
  }
};

// expects: (column{String})
// returns: value
function get(column) {
  if (!nm_.isString(column)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a string.');
  }
  else if (!this._model._schema.isColumn(column)) {
    throw new lErrors.Model.InvalidColumn('Invalid column: ' + column + '.');
  }
  
  if (this._upsert && !this._model._schema.isKeyColumn(column)) {
    if (!this._changes[column]) {
      throw new lErrors.Model.IndeterminateValue('Reading value not previously set for column: ' + column + '.');
    }
    else if (!this._changes[column].op['$set']) {
      throw new lErrors.Model.IndeterminateValue('Reading value modified by idempotent operations for column: ' + column + '.');
    }
  }
  
  var value = this._get(column);
  
  // schema getter
  var getter = this._model._schema.columnGetter(column);
  if (getter) {
    value = getter.call(this, value);
  }
  
  return value
}

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
  
  // don't allow adding duplicates
  var newValue = this._get(column);
  if (newValue && newValue.indexOf(value) > -1) {
    return;
  }
  
  append.call(this, '$add', column, value);
};

// expects: (column{String}, value) for sets and lists, and (column{String}, key) for maps
// returns: undefined
Model.prototype.remove = function(column, value) {
  if (!this._model._schema.isColumn(column)) {
    throw new lErrors.Model.InvalidColumn('Invalid column: ' + column + '.');
  }
  else {
    var baseType = this._model._schema.baseColumnType(column)
    if (baseType !== 'set' && baseType !== 'list' && baseType !== 'map') {
      throw new lErrors.Model.InvalidColumnType('Remove can only be performed on columns of type set, list, and map.');
    }
  }
  
  // if map, inject a null value
  if (baseType === 'map') {
    this.inject(column, value, null);
    return;
  }
  
  // if update, only record idempotent operations
  if (this._upsert) {
    
    if (this._changes[column]) {
      if (this._changes[column].op['$remove']) {
        this._changes[column].op['$remove'].push(value);
      }
      else {
        throw new lErrors.Model.OperationConflict('Multiple conflicting operations on column: ' + column + '.');
      }
    }
    else {
      this._changes[column] = { op: { '$remove': [value] } };
    }
    return;
  }
  
  // full set and change tracking
  var prevValue = this._changes[column] ? this._changes[column].prev : this._get(column);
  var newValue = null;
  
  // remove
  newValue = this._get(column);
  if (newValue) {
    newValue = lHelpers.without(newValue, value);
    if (newValue.length === 0) {
      newValue = null;
    }
  }
  else {
    newValue = null;
  }
  
  typeSpecificSet.call(this, column, newValue);
  
  // mark column changed
  newValue = this._get(column);
  if (!lHelpers.isEqual(newValue, prevValue)) {
    if (this._changes[column]) {
      if (this._changes[column].op['$remove']) {
        this._changes[column].op['$remove'].push(value);
      }
      else {
        this._changes[column].op = { '$set': true };
      }
    }
    else {
      this._changes[column] = { prev: prevValue, op: { '$remove': [value] } };
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
  
  // if update, only record idempotent operations
  if (this._upsert) {
    
    if (this._changes[column]) {
      if (this._changes[column].op['$prepend']) {
        this._changes[column].op['$prepend'].push(value);
      }
      else {
        throw new lErrors.Model.OperationConflict('Multiple conflicting operations on column: ' + column + '.');
      }
    }
    else {
      this._changes[column] = { op: { '$prepend' : [value] } };
    }
    return;
  }
  
  // full set and change tracking
  var prevValue = this._changes[column] ? this._changes[column].prev : this._get(column);
  
  // prepend
  var newValue = this._get(column);
  if (newValue) {
    newValue = [value].concat(newValue); // use concat to return a copy
  }
  else {
    newValue = [value];
  }
  
  typeSpecificSet.call(this, column, newValue);
  
  // mark column changed
  newValue = this._get(column);
  if (!lHelpers.isEqual(newValue, prevValue)) {
    if (this._changes[column]) {
      if (this._changes[column].op['$prepend']) {
        this._changes[column].op['$prepend'].push(value);
      }
      else {
        this._changes[column].op = { '$set': true };
      }
    }
    else {
      this._changes[column] = { prev: prevValue, op: { '$prepend' : [value] } };
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
  
  // if update, only record idempotent operations
  if (this._upsert) {
    
    if (this._changes[column]) {
      if (this._changes[column].op[operation]) {
        this._changes[column].op[operation].push(value);
      }
      else {
        throw new lErrors.Model.OperationConflict('Multiple conflicting operations on column: ' + column + '.');
      }
    }
    else {
      var op = {};
      op[operation] = [value];
      this._changes[column] = { op: op };
    }
    return;
  }
  
  // full set and change tracking
  var prevValue = this._changes[column] ? this._changes[column].prev : this._get(column);
  
  // append
  var newValue = this._get(column);
  if (newValue) {
    newValue = newValue.concat([value]); // use concat to return a copy
  }
  else {
    newValue = [value];
  }
  
  typeSpecificSet.call(this, column, newValue);
  
  // mark column changed
  newValue = this._get(column);
  if (!lHelpers.isEqual(newValue, prevValue)) {
    if (this._changes[column]) {
      if (this._changes[column].op[operation]) {
        this._changes[column].op[operation].push(value);
      }
      else {
        this._changes[column].op = { '$set': true };
      }
    }
    else {
      var op = {};
      op[operation] = [value];
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
  
  // if update, only record idempotent operations
  if (this._upsert) {
    if (this._changes[column]) {
      if (this._changes[column].op['$inject']) {
        this._changes[column].op['$inject'][key] = value;
      }
      else {
        throw new lErrors.Model.OperationConflict('Multiple conflicting operations on column: ' + column + '.');
      }
    }
    else {
      var op = {};
      op['$inject'] = {};
      op['$inject'][key] = value;
      this._changes[column] = { op: op };
    }
    return;
  }
  
  // full set and change tracking
  var prevValue = this._changes[column] ? this._changes[column].prev : this._get(column);
  
  // create empty object if null
  var newValue = this._get(column);
  if (!newValue) {
    if (!nm_.isNull(value)) {
      if (type === 'list') {
        newValue = [];
      }
      else {
        newValue = {};
      }
    }
  }
  
  // shallow copy
  else {
    var currentValue = newValue;
    if (nm_.isArray(newValue)) {
      newValue = [];
    }
    else {
      newValue = {};
    }
    nm_.each(currentValue, function(value, key) {
      newValue[key] = value;
    });
  }
  
  // validate index
  if (type === 'list' && (!newValue || key >= newValue.length)) {
    throw new lErrors.Model.InvalidArgument('Key index is out of bounds for column: ' + column + '.');
  }
  
  // inject
  if (nm_.isNull(value)) {
    if (newValue) {
      if (type === 'list') {
        newValue.splice(key, 1);
      }
      else {
        delete newValue[key];
      }
      if (nm_.size(newValue) === 0) {
        newValue = null;
      }
    }
  }
  else {
    newValue[key] = value;
  }
  
  typeSpecificSet.call(this, column, newValue);
  
  // mark column changed
  newValue = this._get(column);
  if (!lHelpers.isEqual(newValue, prevValue)) {
    if (this._changes[column]) {
      if (this._changes[column].op['$inject']) {
        this._changes[column].op['$inject'][key] = value;
      }
      else {
        this._changes[column].op = { '$set': true };
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
  
  // if update, only record idempotent operations
  if (this._upsert) {
    if (this._changes[column]) {
      delta += this._changes[column].op['$incr'] ? this._changes[column].op['$incr'] : this._changes[column].op['$decr'];
    }
    if (delta > 0) {
      this._changes[column] = { prev: prevValue, op: { '$incr': delta } };
    }
    else if (delta < 0) {
      this._changes[column] = { prev: prevValue, op: { '$decr': -delta } };
    }
    else {
      delete this._changes[column];
    }
    return;
  }
  
  // full set and change tracking
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

// expects: (column{String}, value)
// returns: undefined
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
  return changed.call(this, this._changes, column);
};

// expects: (column{String}?)
//          column: returns if changed for specified column; or if no column specified, returns if any column changed
// returns: changed{Boolean}
Model.prototype.prevChanged = function(column) {
  return changed.call(this, this._prevChanges, column);
};

// expects: (changes{Object}, column{String}?)
//          column: returns if changed for specified column; or if no column specified, returns if any column changed
// returns: changed{Boolean}
function changed(changes, column) {
  if (column && !nm_.isString(column)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a string.');
  }
  else if (column && !this._model._schema.isColumn(column)) {
    throw new lErrors.Model.InvalidColumn('Invalid column: ' + column + '.');
  }
  else {
    if (column) {
      return !!changes[column];
    }
    else {
      return nm_.size(changes) > 0;
    }
  }
}

// expects: (column{String}?)
//          column: returns changes for specified column; or if no column specified, returns all changes
// returns: { column: { from: prevValue, to: currentValue }, ... } or { from: prevValue, to: currentValue }
Model.prototype.changes = function(column) {
  return changes.call(this, this._changes, column);
};

// expects: (column{String}?)
//          column: returns changes for specified column; or if no column specified, returns all changes
// returns: { column: { from: prevValue, to: currentValue }, ... } or { from: prevValue, to: currentValue }
Model.prototype.prevChanges = function(column) {
  return changes.call(this, this._prevChanges, column);
};


// expects: (changes{Object}, column{String}?)
//          column: returns changes for specified column; or if no column specified, returns all changes
// returns: { column: { from: prevValue, to: currentValue }, ... } or { from: prevValue, to: currentValue }
function changes(changes, column) {
  var self = this;
  
  if (column && !nm_.isString(column)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a string.');
  }
  else if (column && !this._model._schema.isColumn(column)) {
    throw new lErrors.Model.InvalidColumn('Invalid column: ' + column + '.');
  }
  else {
    if (column) {
      var value = changes[column];
      if (!nm_.isUndefined(value)) {
        var newValue = this._get(column);
        return { from: value.prev, to: newValue, op: !value.op['$set'] ? value.op : { '$set' : newValue } }
      }
      else {
        return value;
      }
    }
    else {
      var c = {};
      nm_.each(changes, function(value, column) {
        var newValue = self._get(column);
        c[column] = { from: value.prev, to: newValue, op: !value.op['$set'] ? value.op : { '$set' : newValue } }
      });
      return c;
    }
  }
}

// ==========
// = Create =
// ==========

// expects: (assignments{Object})
// returns: model{Model}
staticMethods.new = function(assignments) {
  return new this(assignments);
};

// expects: (assignments{Object}, function(err{Error})) or (function(err{Error}))
// returns: model{Model}
staticMethods.create = function(assignments, callback) {
  if (!nm_.isFunction(assignments)) {
    if (!nm_.isFunction(callback)) {
      throw new lErrors.Model.InvalidArgument('Argument should be a function.');
    }
    else if (!lHelpers.isPlainObject(assignments)) {
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
  model._exists = true; // ordering matters for key setting
  return model
};

// =======================
// = Upsert and Deleting =
// =======================

// expects: (assignments{Object}, function(err{Error})) or (assignments{Object}) or ()
// returns: model{Model}
staticMethods.upsert = function(assignments, callback) {
  if (assignments && !lHelpers.isPlainObject(assignments)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a {}.');
  }
  else if (callback && !nm_.isFunction(callback)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a function.');
  }
  
  var model = new this({}, { _skipAfterNewCallback: true });
  model._upsert = true; // ordering matters for key setting
  model._exists = true; // ordering matters for key setting
  if (assignments) {
    model._set(assignments);
  }
  if (callback) {
    model.save(callback);
  }
  return model
};

// expects: (where{Object}, function(err{Error}))
// returns: model{Model}
staticMethods.delete = function(where, callback) {
  if (!lHelpers.isPlainObject(where)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a {}.');
  }
  else if (nm_.size(where) === 0) {
    throw new lErrors.Model.InvalidArgument('Delete where condition cannot be empty. Run deleteAll or truncate to remove all records.');
  }
  else if (!nm_.isFunction(callback)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a function.');
  }
  
  this.where(where).delete(callback);
};

// ====================
// = Find and FindOne =
// ====================

// expects: ({ column{String}: value, ... }, function(err{Error}, result{Array})) or ({ column{String}: { operation[$eq, $gt, $gte, ...]{String}: value }, ... }, function(err{Error}, result{Array}))
// returns: undefined
staticMethods.find = function(conditions, callback) {
  if (!lHelpers.isPlainObject(conditions)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a {}.');
  }
  else if (!nm_.isFunction(callback)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a function.');
  }
  else {
    this.where(conditions).all(callback);
  }
};

// expects: ({ column{String}: value, ... }, function(err{Error}, model{Model})) or ({ column{String}: { operation[$eq, $gt, $gte, ...]{String}: value }, ... }, function(err{Error}, model{Model}))
// returns: undefined
staticMethods.findOne = function(conditions, callback) {
  if (!lHelpers.isPlainObject(conditions)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a {}.');
  }
  else if (!nm_.isFunction(callback)) {
    throw new lErrors.Query.InvalidArgument('Argument should be a function.');
  }
  else {
    this.where(conditions).first(callback);
  }
};

// ==================
// = Static Queries =
// ==================

// expects: ()
// returns: query{Query}
staticMethods.query = function() {
  return new lQuery(this);
}

// define static query methods
nm_.each(['select', 'where', 'orderBy', 'limit', 'allowFiltering', 'first', 'all', 'count', 'eachRow', 'stream', 'truncate', 'deleteAll'], function(method, index) {
  staticMethods[method] = function() {
    var query = new lQuery(this);
    return query[method].apply(query, arguments);
  }
});

// ====================
// = Instance Queries =
// ====================

// expects: ()
// returns: query{Query}
Model.prototype.query = function() {
  return new lQuery(this._model, this);
}

// define instance query methods
nm_.each(['using', 'ttl', 'timestamp', 'if', 'ifExists', 'ifNotExists', 'save', 'delete'], function(method, index) {
  Model.prototype[method] = function() {
    var query = new lQuery(this._model, this);
    return query[method].apply(query, arguments);
  }
});

// expects: (function(err{Error}), query{Query}?, options{Object}?)
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
// returns: undefined
Model.prototype._save = function(callback, query, options) {
  var self = this;
  
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a function.');
  }
  
  if (query && !(query instanceof lQuery) && !options) {
    options = query;
    query = null;
  }
  
  if (query && !(query instanceof lQuery)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a Query.');
  }
  else if (options && !lHelpers.isPlainObject(options)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a {}.');
  }
  
  // default options
  options = nm_.extend({}, options);
  
  // validate
  var validateOptions = nm_.extend({}, options.validate);
  validateOptions.callbacks = options.callbacks;
  var invalidColumns = this.validate(validateOptions);
  if (invalidColumns) {
    callback(new lErrors.Model.ValidationFailedError(invalidColumns));
    return;
  }
  
  // create query
  if (!query) {
    query = new lQuery(this._model, this);
  }
  
  // don't save if no changes
  if (this._exists && nm_.size(this._changes) === 0) {
    callback();
    return;
  }
  
  // beforeCreate callbacks
  if (!this._exists) {
    if (!options.callbacks || !(options.callbacks.skipAll || options.callbacks.skipBeforeCreate)) {
      runCallbacks.call(this, 'beforeCreate');
    }
  }
  
  // beforeSave callbacks
  if (!options.callbacks || !(options.callbacks.skipAll || options.callbacks.skipBeforeSave)) {
    runCallbacks.call(this, 'beforeSave');
  }
  
  // wrap callback
  function wrappedCallback(err, result) {
    if (err) {
      callback(err);
    }
    else {
      
      // clear changed fields
      self._prevChanges = self._changes;
      self._changes = {};
      
      // afterCreate callbacks
      if (!self._exists) {
        if (!options.callbacks || !(options.callbacks.skipAll || options.callbacks.skipAfterCreate)) {
          runCallbacks.call(self, 'afterCreate');
        }
      }
      
      // afterSave callbacks
      if (!options.callbacks || !(options.callbacks.skipAll || options.callbacks.skipAfterSave)) {
        runCallbacks.call(self, 'afterSave');
      }
      
      // mark as exists
      if (!self._exists) {
        self._exists = true;
      }
      
      callback();
    }
  }
  
  // decide if update or insert
  var update = this._exists || this._model._schema._isCounterColumnFamily;
  
  // only set columns that were changed
  var assignments = {};
  nm_.each(this._changes, function(value, column) {
    if (!(!self._exists && update && self._model._schema.isKeyColumn(column))) { // don't assign keys in SET clause for UPDATE insertions
      if (!value.op['$set']) {
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
  
  // execute query
  if (update) {
    query.update(assignments).where(primaryKeyConditions.call(this)).execute(wrappedCallback);
  }
  else {
    query.insert(assignments).execute(wrappedCallback);
  }
};

// expects: (function(err{Error}), query{Query}?, options{Object}?)
// options:
//          callbacks{Object} : modify callback behavior
//            callbacks.skipAll{Boolean} : skips all callbacks, takes precedence over subsequent settings
//            callbacks.skipBeforeDelete{Boolean}
//            callbacks.skipAfterDelete{Boolean}
// return: undefined
Model.prototype._delete = function(callback, query, options) {
  var self = this;
  
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a function.');
  }
  
  if (query && !(query instanceof lQuery) && !options) {
    options = query;
    query = null;
  }
  
  if (query && !(query instanceof lQuery)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a Query.');
  }
  else if (options && !lHelpers.isPlainObject(options)) {
    throw new lErrors.Model.InvalidArgument('Argument should be a {}.');
  }
  
  // default options
  options = nm_.extend({}, options);
  
  // beforeDelete callbacks
  if (!options.callbacks || !(options.callbacks.skipAll || options.callbacks.skipBeforeDelete)) {
    runCallbacks.call(this, 'beforeDelete');
  }
  
  if (!query) {
    query = new lQuery(this._model, this);
  }
  query.action('delete').where(primaryKeyConditions.call(this)).execute(function(err, result) {
    if (err) {
      callback(err);
    }
    else {
      
      // afterDelete callbacks
      if (!options.callbacks || !(options.callbacks.skipAll || options.callbacks.skipAfterDelete)) {
        runCallbacks.call(self, 'afterDelete');
      }
      
      self._exists = false;
      callback();
    }
  });
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

// expects: the same params as Cassandra.Client.eachRow(); (query{String}, [param, param, ...], options{Object}, function(n{Number}, model{Model}), function(err{Error}))
// returns: undefined
staticMethods._eachRow = function(query, params, options, modelCallback, completeCallback) {
  if (!this._ready) {
    addToQueryQueue.call(this, 'eachRow', arguments);
  }
  else {
    this._dakota.eachRow(query, params, options, rowCallback, completeCallback);
  }
};

// expects: the same params as Cassandra.Client.stream(); (query{String}, [param, param, ...], options{Object})
// returns: undefined
staticMethods._stream = function(query, params, options) {
  if (!this._ready) {
    var stream = new lWrappedStream(this);
    addToQueryQueue.call(this, 'stream', { stream: stream, args: arguments });
    return stream;
  }
  else {
    return this._dakota.stream(this, query, params, options);
  }
};

// ===============
// = Query Queue =
// ===============

// expects: (action[execute, eachRow, stream]{String}, [arguments])
// returns: undefined
function addToQueryQueue(action, arguments) {
  if (!this._queryQueue) {
    throw new lErrors.Model.QueryQueueAlreadyProcessed('Cannot enqueue query. Queue already processed.');
  }
  else if (action !== 'execute' && action !== 'eachRow' && action !== 'stream') {
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
        else if (action === 'stream') {
          query.stream._setStream(self._stream.apply(self, query.args));
        }
      });
    });
    this._queryQueue = null;
  }
}

// =============
// = Callbacks =
// =============

// expects: (key{String})
// returns: undefined
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