// node modules
var nm_ = require('underscore');
var nm_s = require('underscore.string');

// lib
var lCollections = require('./collections');
var lErrors = require('./errors');
var lHelpers = require('./helpers');
var lTypes = require('./types');
var lTableWithProperties = require('./table_with_properties');

function Schema(definition, options) {
  if (!lHelpers.isHash(definition)) {
    throw new lErrors.Schema.InvalidArgument('Argument should be a {}.');
  }
  else if (!nm_.isUndefined(options) && !lHelpers.isHash(options)) {
    throw new lErrors.Schema.InvalidArgument('Argument should be a {}.');
  }
  else {
    this._definition = definition;
    this._options = options;
    
    validateAndNormalizeDefinition.call(this, definition); // must be called after setting this._definition
  }
}

Schema._CALLBACK_KEYS = ['afterNew', 'beforeCreate', 'afterCreate', 'beforeValidate', 'afterValidate', 'beforeSave', 'afterSave', 'beforeDelete', 'afterDelete'];

// ===========
// = Columns =
// ===========

Schema.prototype.columns = function() {
  return nm_.keys(this._definition.columns);
};

Schema.prototype.isColumn = function(column) {
  return !nm_.isUndefined(this._definition.columns[column]);
};

Schema.prototype.columnType = function(column) {
  var type = this._definition.columns[column].type;
  if (lHelpers.isHash(type)) {
    return type.collection;
  }
  else {
    return type;
  }
};

Schema.prototype.isColumnCollection = function(column) {
  return lHelpers.isHash(this._definition.columns[column].type);
};

Schema.prototype.columnCollectionType = function(column) {
  var type = this._definition.columns[column].type;
  if (lHelpers.isHash(type)) {
    return type.type;
  }
  else {
    return false;
  }
};

Schema.prototype.fullColumnTypeString = function(column) {
  if (this.isColumnCollection(column)) {
    return lCollections.fullCollectionTypeString(this.columnType(column), this.columnCollectionType(column));
  }
  else {
    return this.columnType(column);
  }
};

Schema.prototype.validTypeForColumn = function(column, value) {
  
  // collection
  var type = this.columnType(column);
  if (this.isColumnCollection(column)) {
    if (!lCollections.COLLECTIONS[type].validator(value)) {
      return false;
    }
    else {
      
      // shallow type check
      var collectionType = this.columnCollectionType(column);
      if (nm_.isArray(collectionType)) {
        nm_.each(value, function(v, key) {
          if (!lTypes.TYPES[collectionType[0]].validator(key)) {
            return false;
          }
          else if (!lTypes.TYPES[collectionType[1]].validator(v)) {
            return false
          }
        });
      }
      else {
        nm_.each(value, function(v, index) {
          if (!lTypes.TYPES[collectionType].validator(v)) {
            return false;
          }
        });
      }
    }
  }
  
  // non-collection
  else {
    if (!lTypes.TYPES[type].validator(value)) {
      return false
    }
  }
  
  return true;
}

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
  else if (key.length === 0) {
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
  
  if (nm_.isUndefined(this._definition.callbacks)) {
    this._definition.callbacks = {};
  }
  if (nm_.isUndefined(this._definition.callbacks[key])) {
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
      if (key !== 'columns' && key !== 'key' && key !== 'with' && key !== 'callbacks') {
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
  }
}

function validateAndNormalizeColumns(columns) {
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
      else {
        
        // validate type
        if (nm_.isString(definition.type)) {
          if (!lTypes.TYPES[definition.type]) {
            throw new lErrors.Schema.InvalidTypeDefinition('Invalid type: ' + definition.type + '.');
          }
        }
        else if (lHelpers.isHash(definition.type)) {
          if (!lCollections.COLLECTIONS[definition.type.collection]) {
            throw new lErrors.Schema.InvalidCollectionDefinition('Invalid collection: ' + definition.type.collection + '.');
          }
          else {
            if (definition.type.collection === 'map') {
              if (!nm_.isArray(definition.type.type) || definition.type.type.length !== 2) {
                throw new lErrors.Schema.InvalidTypeDefinition('Invalid map key, value type:' + definition.type.type + '.');
              }
              else {
                nm_.each(definition.type.type, function(type, index) {
                  if (!lTypes.TYPES[type]) {
                    throw new lErrors.Schema.InvalidTypeDefinition('Invalid type: ' + type + '.');
                  }
                });
              }
            }
            else if (!lTypes.TYPES[definition.type.type]) {
              throw new lErrors.Schema.InvalidTypeDefinition('Invalid type: ' + definition.type.type + '.');
            }
          }
        }
        else {
          throw new lErrors.Schema.InvalidType('Type should be a {}.');
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
      
      nm_.each(c, function(func, index) {
        if (!nm_.isFunction(func)) {
          throw new lErrors.Schema.InvalidType('Type should be a function.');
        }
      });
    }
  });
}

// =========
// = Mixin =
// =========

Schema.prototype.mixin = function(model) {
  mixinGettersAndSetters.call(this, model);
  mixinCallbacks.call(this, model);
};

function mixinGettersAndSetters(model) {
  nm_.each(nm_.keys(this._definition.columns), function(column, index) {
    var name = column;
    if (!nm_.isUndefined(model[column]) && column !== 'name') { // explicitly allow overriding name property
      console.log('Column name conflicts with existing method name: ' + column + ' in ' + model._name + '.');
      name = 'property_' + column;
      console.log('Defining column property as ' + name + '.');
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
  if (!nm_.isUndefined(this._definition.callbacks)) {
    nm_.each(this._definition.callbacks, function(callbacks, key) {
      model._callbacks[key].push.apply(model._callbacks[key], callbacks);
    });
  }
}

module.exports = Schema;