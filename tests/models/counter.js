// node modules
var nmDakota = require('../../index');

module.exports = function(dakota) {
  return dakota.addModel('Counter', require('./counter.schema'), null, {});
};