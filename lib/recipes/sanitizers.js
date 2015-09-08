// node modules
var nmValidator = require('validator');

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