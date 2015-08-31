// node modules
var nmValidator = require('validator');

var exports = {};

// ====================
// = Specific Recipes =
// ====================

exports.email = {
  validator: nmValidator.isEmail,
  message: function(displayName) { return displayName + ' must be a valid email address.'; }
};

exports.password = {
  validator: function(value) {
    return nmValidator.matches(value, /^(?=.*[a-zA-Z])(?=.*[0-9]).{6,}$/);
  },
  message: function(displayName) { return displayName + ' must be at least 6 characters long and contain one number and one letter.'; }
};

// ===================
// = General Recipes =
// ===================

exports.required = {
  validator: function(value) {
    return !nmValidator.isNull(value);
  },
  message: function(displayName) { return displayName + ' is required.'; }
};

exports.minLength = function(length) {
  return {
    validator: function(value) {
      return nmValidator.isLength(value, length);
    },
    message: function(displayName) { return displayName + ' is too short (minimum is ' + length.toString() + ' characters).'; }
  }
};

exports.maxLength = function(length) {
  return {
    validator: function(value) {
      return nmValidator.isLength(value, 0, length);
    },
    message: function(displayName) { return displayName + ' is too long (maximum is ' + length.toString() + ' characters).'; }
  }
};

module.exports = exports;