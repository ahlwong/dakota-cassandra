// node modules
var nmValidator = require('validator');

var exports = {};

// ====================
// = Specific Recipes =
// ====================

exports.email = function(value) {
  if (nmValidator.isEmail(value)) {
    return nmValidator.normalizeEmail(value);
  }
  else {
    return nmValidator.trim(value).toLowerCase();
  }
};

module.exports = exports;