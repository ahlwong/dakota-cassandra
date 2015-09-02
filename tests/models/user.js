// node modules
var nmDakota = require('../../index');

module.exports = function(dakota) {
  return dakota.addModel('User', require('./user.schema'), require('./user.validations'), {});
};