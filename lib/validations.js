// node modules
var nm_ = require('underscore');

// lib
var lErrors = require('./errors');
var lHelpers = require('./helpers');
var lSchema = require('./schema');

function Validations(schema, definition) {
  if (!(schema instanceof lSchema)) {
    throw new lErrors.Validations.InvalidArgument('Argument should be a Schema');
  }
  else if (!lHelpers.isPlainObject(definition)) {
    throw new lErrors.Validations.InvalidArgument('Argument should be a {}.');
  }
  else {
    this._schema = schema;
    this._definition = definition;
    
    validateAndNormalizeDefinition.call(this, definition); // must be called after setting this._definition
  }
}

// =========================
// = Validate and Sanitize =
// =========================

Validations.validate = function(recipe, value, displayName, instance) {
  var self = this;
  
  var messages = null;
  if (!lHelpers.isPlainObject(recipe)) {
    throw new lErrors.Validations.InvalidType('Type should be a {}.');
  }
  else if (!nm_.isString(displayName)) {
    throw new lErrors.Validations.InvalidType('Type should be a string.');
  }
  else if (recipe.validator) {
    
    // normalize
    var validators = recipe.validator;
    if (lHelpers.isPlainObject(validators)) {
      validators = [validators];
    }
    
    if (nm_.isArray(validators)) {
      nm_.each(validators, function(validator) {
        if (!validator.validator(value, instance)) {
          if (!messages) {
            messages = [];
          }
          messages.push(validator.message(displayName));
        }
      });
    }
    else {
      throw new lErrors.Validations.InvalidType('Type should be an array.');
    }
  }
  return messages;
},

Validations.sanitize = function(recipe, value, instance) {
  var self = this;
  
  if (!lHelpers.isPlainObject(recipe)) {
    throw new lErrors.Validations.InvalidType('Type should be a {}.');
  }
  else if (recipe.sanitizer) {
    
    // normalize
    var sanitizers = recipe.sanitizer;
    if (nm_.isFunction(sanitizers)) {
      sanitizers = [sanitizers];
    }
    
    if (nm_.isArray(sanitizers)) {
      return nm_.reduce(sanitizers, function(value, sanitizer) { return sanitizer(value, instance); }, value);
    }
    else {
      throw new lErrors.Validations.InvalidType('Type should be an array.');
    }
  }
  return value;
},

Validations.validateSanitized = function(recipe, value, displayName, instance) {
  return Validations.validate(recipe, Validations.sanitize(recipe, value, instance), displayName, instance);
}

// ===========
// = Recipes =
// ===========

Validations.prototype.recipe = function(column) {
  if (this._definition[column]) {
    return this._definition[column];
  }
  else {
    return {};
  }
};

// ================================
// = Validation and Normalization =
// ================================

function validateAndNormalizeDefinition(definition) {
  var self = this;
  nm_.each(definition, function(recipe, column) {
    if (!self._schema.isColumn(column)) {
      throw new lErrors.Validations.InvalidColumn('Invalid column: ' + column + '.');
    }
    nm_.each(recipe, function(value, key) {
      if (key === 'displayName') {
        if (!nm_.isString(value)) {
          throw new lErrors.Validations.InvalidType('Type should be a string.');
        }
      }
      else if (key === 'validator') {
        
        // normalize
        if (lHelpers.isPlainObject(value)) {
          value = [value];
          recipe[key] = value;
        }
        
        // validate
        if (!nm_.isArray(value)) {
          throw new lErrors.Validations.InvalidType('Type should be an array.');
        }
        else {
          nm_.each(value, function(v, index) {
            if (!lHelpers.isPlainObject(v)) {
              throw new lErrors.Validations.InvalidType('Type should be a {}.');
            }
            else if (!v.validator || !nm_.isFunction(v.validator)) {
              throw new lErrors.Validations.InvalidType('Type should be a function.');
            }
            else if (!v.message || !nm_.isFunction(v.message)) {
              throw new lErrors.Validations.InvalidType('Type should be a function.');
            }
          });
        }
      }
      else if (key === 'sanitizer') {
        
        // normalize
        if (nm_.isFunction(value)) {
          value = [value];
          recipe[key] = value;
        }
        
        // validate
        else if (!nm_.isArray(value)) {
          throw new lErrors.Validations.InvalidType('Type should be an array.');
        }
        else {
          nm_.each(value, function(v, index) {
            if (!nm_.isFunction(v)) {
              throw new lErrors.Validations.InvalidType('Type should be a function.');
            }
          });
        }
      }
      else {
        throw new lErrors.Validations.InvalidValidationDefinitionKey('Unknown validation definition key: ' + key + '.');
      }
    });
  });
}

// =========
// = Mixin =
// =========

Validations.prototype.mixin = function(model) {
  
};

module.exports = Validations;