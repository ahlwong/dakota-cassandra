// node modules
var nmCassandraTypes = require('cassandra-driver').types;
var nmIsPlainObject = require('is-plain-object');
var nmValidator = require('validator');
var nm_ = require('underscore');

// ===================
// = Type Validators =
// ===================
exports.isPlainObject = function(x) {
  return nmIsPlainObject(x);
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
  var pattern_uuid1 = /^[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}$/i;
  return pattern_uuid1.test(x.toString());
};

exports.isInet = function(x) {
  if (!nm_.isString(x)) {
    return false;
  }
  else {
    var patt_ip4 = /^(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}$/i,
        patt_ip6_1 = /^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$/i,
        patt_ip6_2 = /^((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)::((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)$/i,
        patt_ip6_3 = /^::ffff:(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}$/i,
        patt_ip6_4 = /^::ffff:(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}$/i;
return patt_ip4.test(x) || patt_ip6_1.test(x) || patt_ip6_2.test(x) || patt_ip6_3.test(x) || patt_ip6_4.test(x);
  }
};

exports.isTuple = function(x) {
  return nm_.isArray(x) || (x instanceof nmCassandraTypes.Tuple);
};

// ============
// = Equality =
// ============
exports.isEqual = function(x, y) {
  if (nm_.isArray(x) || exports.isPlainObject(x)) {
    return JSON.stringify(x) === JSON.stringify(y);
  }
  else {
    return x == y;
  }
};

exports.uniq = function(array) {
  var hash = {};
  nm_.each(array, function(value, index) {
    var key = JSON.stringify(value);
    if (!hash[key]) {
      hash[key] = value;
    }
  });
  return nm_.values(hash);
};

exports.without = function(array, value) {
  var newArray = [];
  nm_.each(array, function(v, index) {
    if (!exports.isEqual(v, value)) {
      newArray.push(v);
    }
  });
  return newArray;
};
