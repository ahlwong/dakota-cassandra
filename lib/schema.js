// node modules
var nm_ = require('underscore');
var nm_i = require('underscore.inflections');
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
  else if (!lHelpers.isPlainObject(definition)) {
    throw new lErrors.Schema.InvalidArgument('Argument should be a {}.');
  }
  
  this._dakota = dakota;
  this._aliases = {};
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

Schema.prototype.columnAlias = function(column) {
  return this._definition.columns[column].alias;
};

Schema.prototype.isAlias = function(alias) {
  return !!this._aliases[alias];
};

Schema.prototype.columnFromAlias = function(alias) {
  return this._aliases[alias];
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

Schema.prototype.isKeyColumn = function(column) {
  return nm_.flatten(this._definition.key).indexOf(column) > -1;
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
  if (!lHelpers.isPlainObject(definition)) {
    throw new lErrors.Schema.InvalidArgument('Argument should be a {}.');
  }
  else {
    nm_.each(definition, function(value, key) {
      if (key !== 'columns' && key !== 'key' && key !== 'with' && key !== 'callbacks' && key !== 'methods' && key !== 'staticMethods') {
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
  
  if (!lHelpers.isPlainObject(columns)) {
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
      if (!lHelpers.isPlainObject(definition)) {
        throw new lErrors.Schema.InvalidType('Type should be a {}.');
      }
      else if (!definition.type) {
        throw new lErrors.Schema.InvalidTypeDefinition('Type must be defined in column: ' + column + ' schema.');
      }
      
      nm_.each(definition, function(value, key) {
        
        // type
        if (key === 'type') {
          if (!value || !nm_.isString(value)) {
            throw new lErrors.Schema.InvalidTypeDefinition('Type: ' + value + ' should be a string in column: ' + column + ' schema.');
          }
          else {
            definition.type = value = lTypes.sanitize(self._dakota, value);
            if (!lTypes.isValidType(self._dakota, value)) {
              throw new lErrors.Schema.InvalidTypeDefinition('Invalid type: ' + value + ' in column: ' + column + ' schema.');
            }
            
            // mark counter column family
            if (value === 'counter') {
              self._isCounterColumnFamily = true;
            }
          }
        }
        else if (key === 'set' || key === 'get') {
          if (value && !nm_.isFunction(value)) {
            throw new lErrors.Schema.InvalidGetterSetterDefinition('Setter / getters should be functions in column: ' + column + ' schema.');
          }
        }
        else if (key === 'alias') {
          if (value && !nm_.isString(value)) {
            throw new lErrors.Schema.InvalidAliasDefinition('Alias should be a string in column: ' + column + ' schema.');
          }
          else if (self._aliases[value] || columns[value]) {
            throw new lErrors.Schema.InvalidAliasDefinition('Alias conflicts with another alias or column name in column: ' + column + ' schema.');
          }
          else {
            self._aliases[value] = column;
          }
        }
        else {
          throw new lErrors.Schema.InvalidColumnDefinitionKey('Invalid column definition key: ' + key + ' in column: ' + column + ' schema.');
        }
        
      });
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
    if (Schema._CALLBACK_KEYS.indexOf(key) < 0) {
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
  mixinTypeSpecificSetters.call(this, model);
  mixinCallbacks.call(this, model);
  mixinMethods.call(this, model);
  mixinStaticMethods.call(this, model);
};

function mixinGettersAndSetters(model) {
  var self = this;
  
  nm_.each(this.columns(), function(column, index) {
    
    // column
    var name = model._options.getterSetterName(column);
    if (!nm_.isUndefined(model.prototype[name]) && name !== 'name') { // explicitly allow overriding name property
      lLogger.warn('Getter, setter name conflicts with existing property name: ' + name + ' in ' + model._name + '.');
      name = 'get_set_' + name;
      lLogger.warn('Defining getter, setter as ' + name + '.');
    }
    defineGetterSetter.call(self, model, name, column);
    
    // alias
    var alias = self.columnAlias(column);
    if (alias) {
      var aliasName = model._options.getterSetterName(alias);
      if (!nm_.isUndefined(model.prototype[aliasName]) && aliasName !== 'name') { // explicitly allow overriding name property
        lLogger.warn('Alias getter, setter name conflicts with existing property name: ' + aliasName + ' in ' + model._name + '.');
        aliasName = 'get_set_' + aliasName;
        lLogger.warn('Defining alias getter, setter as ' + aliasName + '.');
      }
      defineGetterSetter.call(self, model, aliasName, column);
    }
    
  });
}

function defineGetterSetter(model, name, column) {
  Object.defineProperty(model.prototype, name, {
    get: function() {
      return this.get(column);
    },
    set: function(value) {
      this.set(column, value);
    }
  });
}

function mixinTypeSpecificSetters(model) {
  var self = this;
  
  nm_.each(this.columns(), function(column, index) {
    
    var operations = [];
    var type = self.baseColumnType(column);
    if (type === 'list') {
      operations = ['append', 'prepend', 'remove', 'inject'];
    }
    else if (type === 'set') {
      operations = ['add', 'remove'];
    }
    else if (type === 'map') {
      operations = ['inject', 'remove'];
    }
    else if (type === 'counter') {
      operations = ['increment', 'decrement'];
    }
    
    if (operations.length > 0) {
      nm_.each(operations, function(operation, index) {
        
        // column
        var name = model._options.typeSpecificSetterName(operation, column);
        if (!nm_.isUndefined(model.prototype[name])) {
          lLogger.warn('Type specific setter name conflicts with existing property name: ' + name + ' in ' + model._name + '.');
          name = 'specific_' + name;
          lLogger.warn('Defining setter as ' + name + '.');
        }
        
        // alias
        var alias = self.columnAlias(column);
        var aliasName = false;
        if (alias) {
          aliasName = model._options.typeSpecificSetterName(operation, alias);
          if (!nm_.isUndefined(model.prototype[aliasName])) {
            lLogger.warn('Type specific alias setter name conflicts with existing property name: ' + aliasName + ' in ' + model._name + '.');
            aliasName = 'specific_' + aliasName;
            lLogger.warn('Defining setter as ' + aliasName + '.');
          }
        }
        
        if (operation === 'inject') {
          model.prototype[name] = function(key, value) {
            this.inject(column, key, value);
          };
          
          if (alias) {
            model.prototype[aliasName] = function(key, value) {
              this.inject(column, key, value);
            };
          }
        }
        else {
          model.prototype[name] = function(value) {
            this[operation](column, value);
          };
          
          if (alias) {
            model.prototype[aliasName] = function(value) {
              this[operation](column, value);
            };
          }
        }
      });
    }
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