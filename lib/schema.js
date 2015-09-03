// node modules
var nm_ = require('underscore');
var nm_s = require('underscore.string');

// lib
var lErrors = require('./errors');
var lHelpers = require('./helpers');
var lLogger = require('./logger');
var lTypes = require('./types');
var lTableWithProperties = require('./table_with_properties');

// expects: (dakota{Dakota}, definition{Object})
// returns: undefined
function Schema(dakota, definition) {
  if (!(dakota instanceof require('./index'))) { // need to require here
    throw new lErrors.Schema.InvalidArgument('Argument should be a Dakota.');
  }
  else if (!lHelpers.isHash(definition)) {
    throw new lErrors.Schema.InvalidArgument('Argument should be a {}.');
  }
  
  this._dakota = dakota;
  this._definition = definition;
  this._isCounterColumnFamily = false;
  
  validateAndNormalizeDefinition.call(this, definition); // must be called after setting this._definition
}

Schema._CALLBACK_KEYS = ['afterNew', 'beforeCreate', 'afterCreate', 'beforeValidate', 'afterValidate', 'beforeSave', 'afterSave', 'beforeDelete', 'afterDelete'];

// ===========
// = Columns =
// ===========

Schema.prototype.columns = function() {
  return nm_.keys(this._definition.columns);
};

Schema.prototype.isColumn = function(column) {
  if (!nm_.isString(column)) {
    throw new lErrors.Schema.InvalidArgument('Argument should be a string.');
  }
  
  return !!this._definition.columns[column];
};

Schema.prototype.baseColumnType = function(column) {
  if (!nm_.isString(column)) {
    throw new lErrors.Schema.InvalidArgument('Argument should be a string.');
  }
  
  return lTypes.baseType(this._dakota, this._definition.columns[column].type);
};

Schema.prototype.columnType = function(column) {
  if (!nm_.isString(column)) {
    throw new lErrors.Schema.InvalidArgument('Argument should be a string.');
  }
  
  return this._definition.columns[column].type;
};

Schema.prototype.isValidValueTypeForColumn = function(column, value) {
  if (!nm_.isString(column)) {
    throw new lErrors.Schema.InvalidArgument('Argument should be a string.');
  }
  return lTypes.isValidValueType(this._dakota, this.columnType(column), value);
}

Schema.prototype.columnGetter = function(column) {
  return this._definition.columns[column].get;
};

Schema.prototype.columnSetter = function(column) {
  return this._definition.columns[column].set;
};

// ========
// = Key =
// ========

Schema.prototype.partitionKey = function() {
  return this._definition.key[0];
};

Schema.prototype.clusteringKey = function() {
  var key = nm_.rest(this._definition.key, 1);
  if (key.length > 1) {
    return key;
  }
  else if (key.length === 1) {
    return key[0];
  }
  else {
    return false;
  }
};

// ========
// = With =
// ========

Schema.prototype.with = function() {
  return this._definition.with;
};

// =============
// = Callbacks =
// =============

nm_.each(Schema._CALLBACK_KEYS, function(key, index) {
  var methodName = 'add' + nm_s.capitalize(key) + 'Callback';
  Schema.prototype[methodName] = function(callback) {
    addCallback.call(this, key, callback);
  };
});

function addCallback(key, callback) {
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Schema.InvalidArgument('Argument should be a function.');
  }
  
  if (!this._definition.callbacks) {
    this._definition.callbacks = {};
  }
  if (!this._definition.callbacks[key]) {
    this._definition.callbacks[key] = [];
  }
  this._definition.callbacks[key].push(callback);
}

// ================================
// = Validation and Normalization =
// ================================

function validateAndNormalizeDefinition(definition) {
  if (!lHelpers.isHash(definition)) {
    throw new lErrors.Schema.InvalidArgument('Argument should be a {}.');
  }
  else {
    nm_.each(definition, function(value, key) {
      if (key !== 'columns' && key !== 'key' && key !== 'with' && key !== 'callbacks' && key !== 'methods' && key !== 'staticMethods' && key !== 'options') {
        throw new lErrors.Schema.InvalidSchemaDefinitionKey('Unknown schema definition key: ' + key + '.');
      }
    });
    
    if (!definition.columns) {
      throw new lErrors.Schema.MissingDefinition('Schema must define columns.');
    }
    else {
      validateAndNormalizeColumns.call(this, definition.columns);
    }
    
    if (!definition.key) {
      throw new lErrors.Schema.MissingDefinition('Schema must define a key.');
    }
    else {
      validateAndNormalizeKey.call(this, definition.key); // must be called after validateAndNormalizeColumns
    }
    
    if (definition.with) {
      validateAndNormalizeWith.call(this, definition.with); // must be called after validateAndNormalizeKey
    }
    
    if (definition.callbacks) {
      validateAndNormalizeCallbacks.call(this, definition.callbacks);
    }
    
    if (definition.methods) {
      validateAndNormalizeMethods.call(this, definition.methods);
    }
    
    if (definition.staticMethods) {
      validateAndNormalizeStaticMethods.call(this, definition.staticMethods);
    }
  }
}

function validateAndNormalizeColumns(columns) {
  var self = this;
  
  if (!lHelpers.isHash(columns)) {
    throw new lErrors.Schema.InvalidArgument('Argument should be a {}.');
  }
  else {
    nm_.each(columns, function(definition, column) {
      
      // normalize
      if (nm_.isString(definition)) {
        definition = { type: definition };
        columns[column] = definition;
      }
      
      // validate definition
      if (!lHelpers.isHash(definition)) {
        throw new lErrors.Schema.InvalidType('Type should be a {}.');
      }
      else if (!definition.type || !nm_.isString(definition.type)) {
        throw new lErrors.Schema.InvalidTypeDefinition('Invalid type: ' + definition.type + '.');
      }
      else {
        definition.type = lTypes.sanitize(self._dakota, definition.type);
        if (!lTypes.isValidType(self._dakota, definition.type)) {
          throw new lErrors.Schema.InvalidTypeDefinition('Invalid type: ' + definition.type + '.');
        }
        
        // mark counter column family
        if (definition.type === 'counter') {
          self._isCounterColumnFamily = true;
        }
      }
    });
  }
}

function validateAndNormalizeKey(key) {
  var self = this;
  
  if (!nm_.isArray(key)) {
    throw new lErrors.Schema.InvalidArgument('Argument should be an array.');
  }
  else {
    nm_.each(key, function(column, index) {
      if (nm_.isArray(column)) {
        if (index != 0) {
          throw new lErrors.Schema.InvalidKeyDefinition('Composite key can only appear at beginning of key definition.');
        }
        else {
          nm_.each(column, function(c, i) {
            if (!self.isColumn(c)) {
              throw new lErrors.Schema.InvalidKeyDefinition('Key refers to invalid column.');
            }
          });
        }
      }
      else if (!nm_.isString(column)) {
        throw new lErrors.Schema.InvalidType('Type should be a string.');
      }
      else if (!self.isColumn(column)) {
        throw new lErrors.Schema.InvalidKeyDefinition('Key refers to invalid column.');
      }
    });
  }
}

function validateAndNormalizeWith(properties) {
  var self = this;
  
  nm_.each(properties, function(value, property) {
    if (!lTableWithProperties.PROPERTIES[property]) {
      throw new lErrors.Schema.InvalidWithDefinition('Invalid with property: ' + property + '.');
    }
    else if (property === '$clustering_order_by') {
      var clusteringKey = self.clusteringKey();
      nm_.each(value, function(order, column) {
        if (!lTableWithProperties.CLUSTERING_ORDER[order]) {
          throw new lErrors.Schema.InvalidWithDefinition('Invalid with clustering order: ' + order + '.');
        }
        else {
          if (!clusteringKey || (nm_.isArray(clusteringKey) && indexOf(clusteringKey, column) === -1) || clusteringKey !== column) {
            throw new lErrors.Schema.InvalidWithDefinition('Invalid with clustering column: ' + column + '.');
          }
        }
      });
    }
    i++;
  });
}

function validateAndNormalizeCallbacks(callbacks) {
  nm_.each(callbacks, function(c, key) {
    if (nm_.indexOf(Schema._CALLBACK_KEYS, key) < 0) {
      throw new lErrors.Schema.InvalidCallbackKey('Invalid callback key: ' + key + '.');
    }
    else {
      
      // normalize
      if (nm_.isFunction(c)) {
        c = [c];
        callbacks[key] = c;
      }
      
      if (!nm_.isArray(c)) {
        throw new lErrors.Schema.InvalidType('Type should be an array.');
      }
      else {
        nm_.each(c, function(func, index) {
          if (!nm_.isFunction(func)) {
            throw new lErrors.Schema.InvalidType('Type should be a function.');
          }
        });
      }
    }
  });
}

function validateAndNormalizeMethods(methods) {
  nm_.each(methods, function(method, key) {
    if (!nm_.isFunction(method)) {
      throw new lErrors.Schema.InvalidType('Type should be a function.');
    }
  });
}

function validateAndNormalizeStaticMethods(staticMethods) {
  nm_.each(staticMethods, function(static_method, key) {
    if (!nm_.isFunction(static_method)) {
      throw new lErrors.Schema.InvalidType('Type should be a function.');
    }
  });
}

// =========
// = Mixin =
// =========

Schema.prototype.mixin = function(model) {
  mixinGettersAndSetters.call(this, model);
  mixinCallbacks.call(this, model);
  mixinMethods.call(this, model);
  mixinStaticMethods.call(this, model);
};

function mixinGettersAndSetters(model) {
  var self = this;
  
  nm_.each(nm_.keys(this._definition.columns), function(column, index) {
    var name = column;
    if (!nm_.isUndefined(model.prototype[column]) && column !== 'name') { // explicitly allow overriding name property
      lLogger.warn('Column name conflicts with existing property name: ' + column + ' in ' + model._name + '.');
      name = 'property_' + column;
      lLogger.warn('Defining column property as ' + name + '.');
    }
    Object.defineProperty(model.prototype, name, {
      get: function() {
        return this.get(column);
      },
      set: function(value) {
        this.set(column, value);
      }
    });
  });
}

function mixinCallbacks(model) {
  if (this._definition.callbacks) {
    nm_.each(this._definition.callbacks, function(callbacks, key) {
      model._callbacks[key].push.apply(model._callbacks[key], callbacks);
    });
  }
}

function mixinMethods(model) {
  var self = this;
  
  if (this._definition.methods) {
    nm_.each(this._definition.methods, function(method, key) {
      if (!nm_.isUndefined(model.prototype[key]) && (key !== 'name' && !self.isColumn(key))) { // explicitly allow overriding name property
        lLogger.warn('Method name conflicts with existing property name: ' + key + ' in ' + model._name + '.');
        key = 'method_' + key;
        lLogger.warn('Defining method as ' + key + '.');
      }
      model.prototype[key] = method;
    });
  }
}

function mixinStaticMethods(model) {
  var self = this;
  
  if (this._definition.staticMethods) {
    nm_.each(this._definition.staticMethods, function(static_method, key) {
      if (!nm_.isUndefined(model[key])  && key !== 'name') { // explicitly allow overriding name property
        lLogger.warn('Static method name conflicts with existing property name: ' + key + ' in ' + model._name + '.');
        key = 'method_' + key;
        lLogger.warn('Defining method as ' + key + '.');
      }
      model[key] = static_method;
    });
  }
}

module.exports = Schema;