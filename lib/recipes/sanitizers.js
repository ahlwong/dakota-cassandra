// node modules
var nmValidator = require('validator');
var nm_ = require('underscore');

// ====================
// = Specific Recipes =
// ====================

exports.email = function(value, instance) {
  if (nmValidator.isEmail(value)) {
    return nmValidator.normalizeEmail(value);
  }
  else {
    return nmValidator.trim(value).toLowerCase();
  }
};

// ===================
// = General Recipes =
// ===================

exports.map = function(sanitizer) {
  return function(value, instance) {
    return nm_.map(value, function(v, index) {
      return sanitizer(v, instance);
    });
  }
};

exports.uppercase = function(value, instance) {
  return value.toUpperCase();
};

exports.lowercase = function(value, instance) {
  return value.toLowerCase();
};

exports.trim = function(value, instance) {
  return nmValidator.trim(value);
};