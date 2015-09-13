// node modules
var nmWhen = require('when');
var nm_ = require('underscore');

// lib
var lErrors = require('./errors');
var lHelpers = require('./helpers');
var lLogger = require('./logger');
var lTypes = require('./types');

// expects: (dakota{Dakota}, name{String}, { field{String}: type{String}, ... }, options{Object}?)
// returns: undefined
function UserDefinedType(dakota, name, definition, options) {
  if (!(dakota instanceof require('./index'))) { // need to require here
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a Dakota.');
  }
  else if (!nm_.isString(name)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a string.');
  }
  else if (!lHelpers.isPlainObject(definition)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a {}.');
  }
  else if (options && !lHelpers.isPlainObject(options)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a {}.');
  }
  else {
    this._dakota = dakota;
    this._name = name;
    this._definition = definition;
    this._options = options;
    
    validateAndNormalizeDefinition.call(this, definition); // must be called after setting this._definition
  }
}

// ==========
// = Fields =
// ==========

UserDefinedType.prototype.fields = function() {
  return nm_.keys(this._definition);
};

UserDefinedType.prototype.isField = function(field) {
  if (!nm_.isString(field)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a string.');
  }
  
  return !!this._definition[field];
};

UserDefinedType.prototype.fieldType = function(field) {
  return this._definition[field];
};

UserDefinedType.prototype.isValidValueTypeForField = function(field, value) {
  if (!nm_.isString(field)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a string.');
  }
  
  return lTypes.isValidValueType(self._dakota, this.fieldType(field), value);
};

UserDefinedType.prototype.isValidValueTypeForSelf = function(value) {
  var self = this;
  
  if (!lHelpers.isPlainObject(value)) {
    return false;
  }
  else {
    for (var field in value) {
      var type = self.fieldType(field);
      if (!type || !lTypes.isValidValueType(self._dakota, type, value[field])) {
        return false;
      }
    }
    return true;
  }
};

UserDefinedType.prototype.formatValueTypeForSelf = function(value) {
  var self = this;
  
  nm_.each(value, function(v, field) {
    var type = self.fieldType(field);
    value[field] = lTypes.formatValueType(self._dakota, type, v);
  });
  return value;
};

UserDefinedType.prototype.dbValidator = function() {
  var self = this;
  
  var validator = this._dakota._keyspace;
  validator += ',' + hexEncode(this._name);
  nm_.each(this._definition, function(type, field) {
    validator += ',' + hexEncode(field) + ':' + lTypes.dbValidator(self._dakota, type, true);
  });
  validator = 'org.apache.cassandra.db.marshal.UserType(' + validator + ')';
  return validator;
};

function hexEncode(string) {
  var hex, i;
  var result = "";
  for (i=0; i<string.length; i++) {
      hex = string.charCodeAt(i).toString(16);
      result += hex;
  }
  return result
}

// ================================
// = Validation and Normalization =
// ================================

function validateAndNormalizeDefinition(definition) {
  var self = this;
  
  if (!lHelpers.isPlainObject(definition)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a {}.');
  }
  else {
    nm_.each(definition, function(type, field) {
      if (!nm_.isString(type)) {
        throw new lErrors.UserDefinedType.InvalidFieldDefinition('Invalid type: ' + type + ' for field: ' + self._dakota._keyspace + '.' + field +'.');
      }
      if (type.indexOf('<') > -1 && type.indexOf('frozen') !== 0) {
        throw new lErrors.UserDefinedType.InvalidFieldDefinition('Collection types must be frozen: ' + type + ' for field: ' + self._dakota._keyspace + '.' + field +'.');
      }
      else {
        type = lTypes.sanitize(self._dakota, type);
        definition[field] = type;
        if (!lTypes.isValidType(self._dakota, type)) {
          throw new lErrors.UserDefinedType.InvalidFieldDefinition('Invalid type: ' + type + ' for field: ' + self._dakota._keyspace + '.' + field +'.');
        }
      }
    });
  }
}

// =================
// = Ensure Exists =
// =================

// expects: (function(err{Error}), options{Object}?)
// options:
//          recreate{Boolean}   : drop and recreate type on schema mismatch, takes precedence over following options
//          changeType{Boolean} : change field types to match schema
//          addMissing{Boolean} : add fields in schema that aren't in type
// returns: undefined
UserDefinedType.prototype.ensureExists = function(callback, options) {
  var self = this;
  
  if (!nm_.isFunction(callback)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a function.');
  }
  else if (options && !lHelpers.isPlainObject(options)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a {}.');
  }
  
  // default options
  options = nm_.extend({ recreate: false, changeType: false, addMissing: false }, this._options.ensureExists, options);
  
  // skip running
  if (!nm_.isUndefined(options.run) && !options.run) {
    callback();
    lLogger.debug('Ensure user defined type skipped: ' + this._dakota._keyspace + '.' + this._name  + '.');
    return;
  }
  
  this.selectSchema(function(err, result) {
    if (err) {
      throw new lErrors.UserDefinedType.SelectSchemaError('Error occurred trying to select schema: ' + err + '.');
    }
    else if (!result || !result.rows) {
      throw new lErrors.UserDefinedType.SelectSchemaError('Select schema returned no result or no rows.');
    }
    else {
      
      // create type
      if (result.rows.length === 0) {
        lLogger.warn('Creating type: ' + self._dakota._keyspace + '.' + self._name + '.');
        self.create(function(err, result) {
          if (err) {
            throw new lErrors.UserDefinedType.CreateError('Create type failed: ' + err + '.');
          }
          else {
            callback(err);
          }
        }, { ifNotExists: true });
      }
      
      // compare schema to existing type
      else {
        
        // create hash for diff
        var fields = {};
        nm_.each(self._definition, function(type, field) {
          fields[field] = true;
        });
        
        // diff
        var mismatched = [];
        var extra = [];
        result = result.rows[0];
        nm_.each(result.field_names, function(field_name, index) {
          if (fields[field_name]) {
            var type = self.fieldType(field_name);
            var dbValidator = lTypes.dbValidator(self._dakota, type);
            if (dbValidator !== result.field_types[index]) {
              mismatched.push({
                field: field_name,
                expected: dbValidator,
                actual: result.field_types[index]
              });
            }
            delete fields[field_name];
          }
          else {
            extra.push(field_name);
          }
        });
        var missing = nm_.keys(fields);
        
        // log
        if (mismatched.length > 0) {
          lLogger.warn('Found ' + mismatched.length + ' mismatched field types in ' + self._dakota._keyspace + '.' + self._name);
          lLogger.warn(mismatched);
        }
        if (extra.length > 0) {
          lLogger.warn('Found ' + extra.length + ' extra fields in ' + self._dakota._keyspace + '.' + self._name);
          lLogger.warn(extra);
        }
        if (missing.length > 0) {
          lLogger.warn('Found ' + missing.length + ' missing fields in ' + self._dakota._keyspace + '.' + self._name);
          lLogger.warn(missing);
        }
        
        // fix
        if ((mismatched.length > 0 || extra.length > 0 || missing.length > 0) && options.recreate) {
          recreate.call(self, callback);
        }
        else {
          var promises = [];
          if (mismatched.length > 0 && options.changeType) {
            promises = promises.concat(fixMismatched.call(self, nm_.map(mismatched, function(mismatch, index) { return mismatch.field })));
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
              throw new lErrors.UserDefinedType.FixError('Fixing table schema failed: rejected promises.');
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
      throw new lErrors.UserDefinedType.FixError('Drop type failed: ' + err + '.');
    }
    else {
      self.create(function(err, result) {
        if (err) {
          throw new lErrors.UserDefinedType.FixError('Create type failed: ' + err + '.');
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
  
  lLogger.warn('Changing field types to match schema...');
  
  var promises = [];
  nm_.each(mismatched, function(field, index) {
    promises.push(nmWhen.promise(function(resolve, reject) {
      self.alterType(field, self.fieldType(field), function(err, result) {
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
  
  lLogger.warn('Adding missing fields...');
  
  var promises = [];
  nm_.each(missing, function(field, index) {
    promises.push(nmWhen.promise(function(resolve, reject) {
      self.addField(field, self.fieldType(field), function(err, result) {
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
UserDefinedType.prototype.selectSchema = function(callback) {
  if (!nm_.isFunction(callback)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a function.');
  }
  
  var query = {
    query: 'SELECT * FROM system.schema_usertypes WHERE type_name = ? AND keyspace_name = ? ALLOW FILTERING',
    params: [this._name, this._dakota._keyspace],
    prepare: true
  };
  
  this._dakota._system_execute(query.query, query.params, { prepare: query.prepare }, function(err, result) {
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
UserDefinedType.prototype.create = function(callback, options) {
  if (!nm_.isFunction(callback)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a function.');
  }
  else if (options && !lHelpers.isPlainObject(options)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a {}.');
  }
  
  // default options
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
  
  this._dakota._system_execute(query.query, query.params, { prepare: query.prepare }, callback);
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
  else if (options && !lHelpers.isPlainObject(options)) {
    throw new lErrors.UserDefinedType.InvalidArgument('Argument should be a {}.');
  }
  
  // default options
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
  
  this._dakota._system_execute(query.query, query.params, { prepare: query.prepare }, callback);
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
  
  this._dakota._system_execute(query.query, query.params, { prepare: query.prepare }, callback);
};

// expects: (field{String}, newName{String}, function(err{Error}, result)) or ({ field{String}: newName{String}, ... }, function(err{Error}, result))
// returns: undefined
UserDefinedType.prototype.renameField = function(field, newName, callback) {
  if (lHelpers.isPlainObject(field)) {
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
  
  this._dakota._system_execute(query.query, query.params, { prepare: query.prepare }, callback);
};

// expects: (column{String}, newType{String}, function(err{Error}, result))
// returns: undefined
UserDefinedType.prototype.alterType = function(field, type, callback) {
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
  
  query.query += ' ALTER "' + field + '" TYPE ' + type;
  
  this._dakota._system_execute(query.query, query.params, { prepare: query.prepare }, callback);
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
  nm_.each(this._definition, function(type, field) {
    if (i > 0) {
      clause += ', ';
    }
    clause += '"' + field + '" ' + type;
    i++;
  });
  clause += ')';
  return { clause: clause, params: params };
}

module.exports = UserDefinedType;