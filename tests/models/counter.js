module.exports = function(dakota) {
  return dakota.addModel('Counter', require('./counter.schema'));
};