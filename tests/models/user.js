// node modules
var nmDakota = require('../../index');

module.exports = function(dakota) {
  
  var schema = new nmDakota.Schema(require('./user.schema'), {});
  var validations = new nmDakota.Validations(schema, require('./user.validations'), {});
  var model = dakota.addModel('User', schema, validations, {});
  
  return model;
};