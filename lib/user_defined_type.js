// node modules
var nmCassandra = require('cassandra-driver');
var nm_ = require('underscore');

// lib
var lErrors = require('./errors');
var lHelpers = require('./helpers');

// expects: (name{String}, { field{String}: type{String}, ... }, options{Object}?)
// returns: undefined
function UserDefinedType(name, definition, options) {
  if (!nm_.isString(name)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a string.');
  }
  else if (!lHelpers.isHash(definition)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a {}.');
  }
  else if (!nm_.isUndefined(options) && !lHelpers.isHash(options)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a {}.');
  }
  else {
    this._name = name;
    this._definition = definition;
    this._options = options;
    
    validateAndNormalizeDefinition.call(this, definition); // must be called after setting this._definition
  }
}

// ================================
// = Validation and Normalization =
// ================================

function validateAndNormalizeDefinition(definition) {
  if (!lHelpers.isHash(definition)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a {}.');
  }
  else {
    nm_.each(definition, function(type, field) {
      if (!nm_.isString(type) || !lTypes.isValidType(type)) {
        throw new lErrors.UserDefinedType.InvalidTypeDefinition('Invalid type: ' + type + '.');
      }
    });
  }
}

// ==========
// = Create =
// ==========

// expects: (function(err{Error}, result), options{Object}?)
// options:
//          ifNotExists{Boolean} : add IF NOT EXISTS property to query
// returns: undefined
UserDefinedType.prototype.create = function(callback, options) {
  if (!nm_.isFunction(callback)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a function.');
  }
  else if (!nm_.isUndefined(options) && !lHelpers.isHash(options)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a {}.');
  }
  options = nm_.extend({ ifNotExists: false }, options);
  
  var query = {
    query: 'CREATE TYPE',
    params: [],
    prepare: true
  };
  
  if (options.ifNotExists) {
    query.query += ' IF NOT EXISTS';
  }
  
  concatBuilders.call(this, [buildTypeName, buildFields], query);
  
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
UserDefinedType.prototype.drop = function(callback, options) {
  if (!nm_.isFunction(callback)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a function.');
  }
  else if (!nm_.isUndefined(options) && !lHelpers.isHash(options)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a {}.');
  }
  options = nm_.extend({ ifExists: false }, options);
  
  var query = {
    query: 'DROP TYPE',
    params: [],
    prepare: true
  };
  
  if (options.ifExists) {
    query.query += ' IF EXISTS';
  }
  
  concatBuilders.call(this, [buildTypeName], query);
  
  this._dakota.execute(query.query, query.params, { prepare: query.prepare }, function(err, result) {
    callback(err, result);
  });
};

// =========
// = Alter =
// =========

// expects: (field{String}, type{String}, function(err{Error}, result))
// returns: undefined
UserDefinedType.prototype.addField = function(field, type, callback) {
  if (!nm_.isString(field)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a string.');
  }
  else if (!nm_.isString(type)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a string.');
  }
  else if (!nm_.isFunction(callback)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a function.');
  }
  
  var query = {
    query: 'ALTER TYPE',
    params: [],
    prepare: true
  };
  
  concatBuilders.call(this, [buildTypeName], query);
  
  query.query += ' ADD "' + field + '" ' + type;
  
  this._dakota.execute(query.query, query.params, { prepare: query.prepare }, function(err, result) {
    callback(err, result);
  });
};

// expects: (field{String}, newName{String}, function(err{Error}, result)) or ({ field{String}: newName{String}, ... }, function(err{Error}, result))
// returns: undefined
UserDefinedType.prototype.renameField = function(field, newName, callback) {
  if (lHelper.isHash(field)) {
    if (!nm_.isFunction(newName)) {
      throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a function.');
    }
    
    callback = newName;
    newName = null;
  }
  else {
    if (!nm_.isString(field)) {
      throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a string.');
    }
    else if (!nm_.isString(newName)) {
      throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a string.');
    }
    else if (!nm_.isFunction(callback)) {
      throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a function.');
    }
    
    // normalize
    var rename = {};
    rename[field] = newName;
    field = rename;
    newName = null;
  }
  
  var query = {
    query: 'ALTER TYPE',
    params: [],
    prepare: true
  };
  
  concatBuilders.call(this, [buildTypeName], query);
  
  query.query += ' RENAME';
  var i = 0;
  nm_.each(field, function(newName, currentName) {
    if (i > 0) {
      query.query += ' AND';
    }
    query.query += ' "' + currentName + '" TO "' + newName + '"';
    i++;
  });
  
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

function buildTypeName() {
  var clause = this._dakota._keyspace + '.' + this._name;
  var params = [];
  return { clause: clause, params: params };
}

function buildFields() {
  var clause = '(';
  var params = [];
  var i = 0;
  nm_.each(this._fields.fields, function(definition, field) {
    if (i > 0) {
      clause += ', ';
    }
    clause += field + ' ';
    if (nm_.isString(definition.type)) {
      clause += definition.type;
    }
    else if (lHelpers.isHash(definition.type)) {
      clause += definition.type.collection + '<';
      clause += definition.type.type + '>';
    }
    else {
      throw new lErrors.UserDefinedType.InvalidFieldDefinition('Type should be a string or a {}.');
    }
    i++;
  });
  clause += ')';
  return { clause: clause, params: params };
}

module.exports = UserDefinedType;