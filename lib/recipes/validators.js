// node modules
var nmValidator = require('validator');
var nm_ = require('underscore');

// ====================
// = Specific Recipes =
// ====================

exports.email = {
  validator: nmValidator.isEmail,
  message: function(displayName) { return displayName + ' must be a valid email address.'; }
};

exports.password = {
  validator: function(value, instance) {
    return nmValidator.matches(value, /^(?=.*[a-zA-Z])(?=.*[0-9]).{6,}$/);
  },
  message: function(displayName) { return displayName + ' must be at least 6 characters long and contain one number and one letter.'; }
};

// ===================
// = General Recipes =
// ===================

exports.validateIf = function(conditional, validator) {
  return {
    validator: function(value, instance) {
      if (conditional.call(this, value, instance)) {
        return validator.validator.call(this, value, instance);
      }
      else {
        return true;
      }
    },
    message: function(displayName) { return validator.message.call(this, displayName); }
  }
};

exports.required = {
  validator: function(value, instance) {
    return !nmValidator.isNull(value);
  },
  message: function(displayName) { return displayName + ' is required.'; }
};

// expects: ([value, ... ]) or ({ value: displayName })
exports.isIn = function(values) {
  return {
    validator: function(value, instance) {
      if (nm_.isArray(values)) {
        return values.indexOf(value) > -1;
      }
      else {
        return !!values[value];
      }
    },
    message: function(displayName) {
      var displayNames = nm_.isArray(values) ? values : nm_.values(values);
      return displayName + ' must have one of these values: ' + displayNames.join(', ') + '.';
    }
  }
};

// expects: ({ column: displayName, ... })
exports.requiresOneOf = function(columns) {
  return {
    validator: function(value, instance) {
      var valid = false;
      nm_.find(columns, function(displayName, column) {
        if (!nmValidator.isNull(instance.get(column))) {
          valid = true;
          return true;
        }
      });
      return valid;
    },
    message: function(displayName) {
      var displayNames = nm_.values(columns);
      displayNames = nm_.without(displayNames, displayName);
      return displayName + ' or ' + displayNames.join(', ') + ' is required.';
    }
  }
};

// expects: (minLength{Integer})
exports.minLength = function(length) {
  return {
    validator: function(value, instance) {
      return nmValidator.isLength(value, length);
    },
    message: function(displayName) { return displayName + ' is too short (minimum is ' + length.toString() + ' characters).'; }
  }
};

// expects: (maxLength{Integer})
exports.maxLength = function(length) {
  return {
    validator: function(value, instance) {
      return nmValidator.isLength(value, 0, length);
    },
    message: function(displayName) { return displayName + ' is too long (maximum is ' + length.toString() + ' characters).'; }
  }
};