// node modules
var nmCassandraTypes = require('cassandra-driver').types;
var nmValidator = require('validator');
var nm_ = require('underscore');

// ===================
// = Type Validators =
// ===================
exports.isHash = function(x) {
  return nm_.isObject(x) && !nm_.isArray(x) && !nm_.isFunction(x);
};

exports.isInteger = function(x) {
  return nmValidator.isInt(x);
};

exports.isDatetime = function(x) {
  return nmValidator.isDate(x);
};

exports.isAnything = function(x) {
  return true;
};

exports.isUUID = function(x) {
  //var pattern_uuid4 = /^[0-9A-F]{8}-[0-9A-F]{4}-4[0-9A-F]{3}-[89AB][0-9A-F]{3}-[0-9A-F]{12}$/i;
  var pattern_uuid1 = /^[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}$/i;
  return pattern_uuid1.test(x.toString());
};

exports.isInet = function(x) {
  if (!nm_.isString(x)) {
    return false;
  }
  else {
    //var pattern_uuid4 = /^[0-9A-F]{8}-[0-9A-F]{4}-4[0-9A-F]{3}-[89AB][0-9A-F]{3}-[0-9A-F]{12}$/i;
    var patt_ip4 = /^(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}$/i,
        patt_ip6_1 = /^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$/i,
        patt_ip6_2 = /^((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)::((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)$/i;
    return patt_ip4.test(x) || patt_ip6_1.test(x) || patt_ip6_2.test(x);
  }
};

exports.isTuple = function(x) {
  return nm_.isArray(x) || (x instanceof nmCassandraTypes.Tuple);
};
