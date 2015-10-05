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

// ensures value is not null
// expects: undefined
exports.required = {
  validator: function(value, instance) {
    return !nmValidator.isNull(value);
  },
  message: function(displayName) { return displayName + ' is required.'; }
};

// ensures value is included in values
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

// ensures at least one of the columns is not null
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

// ensures length of value is greater than or equal to length
// expects: (minLength{Integer})
exports.minLength = function(length) {
  return {
    validator: function(value, instance) {
      if (nm_.isArray(value)) {
        return value.length >= length;
      }
      else {
        return nmValidator.isLength(value, length);
      }
    },
    message: function(displayName) { return displayName + ' is too short (minimum is ' + length.toString() + ' characters).'; }
  }
};

// ensures length of value is less than or equal to length
// expects: (maxLength{Integer})
exports.maxLength = function(length) {
  return {
    validator: function(value, instance) {
      if (nm_.isArray(value)) {
        return value.length <= length;
      }
      else {
        return nmValidator.isLength(value, 0, length);
      }
    },
    message: function(displayName) { return displayName + ' is too long (maximum is ' + length.toString() + ' characters).'; }
  }
};

// ensures value is greater than or equal to number
// expects: (number{Number})
exports.greaterThanOrEqualTo = function(number) {
  return {
    validator: function(value, instance) {
      return value >= number;
    },
    message: function(displayName) { return displayName + ' is too small (minimum is ' + number.toString() + ').'; }
  }
};

// ensures value is greater than number
// expects: (number{Number})
exports.greaterThan = function(number) {
  return {
    validator: function(value, instance) {
      return value > number;
    },
    message: function(displayName) { return displayName + ' is too small (must be greater than ' + number.toString() + ').'; }
  }
};

// ensures value is less than or equal to number
// expects: (number{Number})
exports.lessThanOrEqualTo = function(number) {
  return {
    validator: function(value, instance) {
      return value <= number;
    },
    message: function(displayName) { return displayName + ' is too big (maximum is ' + number.toString() + ').'; }
  }
};

// ensures value is less than number
// expects: (number{Number})
exports.lessThan = function(number) {
  return {
    validator: function(value, instance) {
      return value < number;
    },
    message: function(displayName) { return displayName + ' is too big (must be less than ' + number.toString() + ').'; }
  }
};

// ===================
// = Control Recipes =
// ===================

// runs validator(s) if conditional function returns true
// expects: (function(value, this)::boolean, validator{Validator}) or (function(value, this)::boolean, [validator{Validator}, ...])
exports.validateIf = function(conditional, validators) {
  var validateMultiple = exports.validateMultiple(validators);
  return {
    validator: function(value, instance) {
      if (conditional.call(this, value, instance)) {
        return validateMultiple.validator.call(this, value, instance);
      }
      else {
        return true;
      }
    },
    message: function(displayName) { return validateMultiple.message(displayName); }
  }
};

// runs validator(s) on object fields
// expects: (field{String}, validator{Validator}) or (field{String}, [validator{Validator}, ...])
exports.validateObjectFields = function(field, validators) {
  var validateMultiple = exports.validateMultiple(validators);
  return {
    validator: function(value, instance) {
      if (!nm_.isNull(value)) {
        return validateMultiple.validator.call(this, value.field, instance);
      }
      else {
        return false;
      }
    },
    message: function(displayName) { return validateMultiple.message(displayName); }
  }
};

// runs validator(s) on value
// expects: (validator{Validator}) or ([validator{Validator}, ...])
exports.validateMultiple = function(validators) {
  var failedValidators = null;
  return {
    validator: function(value, instance) {
      var self = this;
      
      // normalize
      if (!nm_.isArray(validators)) {
        validators = [validators];
      }
      
      failedValidators = nm_.reduce(validators, function(memo, validator) {
        if (!validator.validator.call(self, value, instance)) {
          memo.push(validator);
        }
        return memo;
      }, []);
      
      return failedValidators.length === 0;
    },
    message: function(displayName) {
      var self = this;
      
      return nm_.map(failedValidators, function(validator) { return validator.message.call(self, displayName); });
    }
  }
};