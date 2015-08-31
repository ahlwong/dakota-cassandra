// node modules
var nm_ = require('underscore');

// lib
var lHelpers = require('./helpers');

var exports = {};

exports.TYPES = {
  ascii     : { validator: nm_.isString,        dbValidator: 'org.apache.cassandra.db.marshal.AsciiType' },
  bigint    : { validator: lHelpers.isInteger,  dbValidator: 'org.apache.cassandra.db.marshal.LongType' },
  blob      : { validator: lHelpers.isAnything, dbValidator: 'org.apache.cassandra.db.marshal.BytesType' },
  boolean   : { validator: nm_.isBoolean,       dbValidator: 'org.apache.cassandra.db.marshal.BooleanType' },
  counter   : { validator: lHelpers.isInteger,  dbValidator: 'org.apache.cassandra.db.marshal.CounterColumnType' },
  decimal   : { validator: nm_.isNumber,        dbValidator: 'org.apache.cassandra.db.marshal.DecimalType' },
  double    : { validator: nm_.isNumber,        dbValidator: 'org.apache.cassandra.db.marshal.DoubleType' },
  float     : { validator: nm_.isNumber,        dbValidator: 'org.apache.cassandra.db.marshal.FloatType' },
  inet      : { validator: lHelpers.isInet,     dbValidator: 'org.apache.cassandra.db.marshal.InetAddressType' },
  int       : { validator: lHelpers.isInteger,  dbValidator: 'org.apache.cassandra.db.marshal.Int32Type' },
  text      : { validator: nm_.isString,        dbValidator: 'org.apache.cassandra.db.marshal.UTF8Type' },
  timestamp : { validator: lHelpers.isDatetime, dbValidator: 'org.apache.cassandra.db.marshal.TimestampType' },
  timeuuid  : { validator: lHelpers.isUUID,     dbValidator: 'org.apache.cassandra.db.marshal.TimeUUIDType' },
  uuid      : { validator: lHelpers.isUUID,     dbValidator: 'org.apache.cassandra.db.marshal.UUIDType' },
  varchar   : { validator: nm_.isString,        dbValidator: 'org.apache.cassandra.db.marshal.UTF8Type' },
  varint    : { validator: lHelpers.isInteger,  dbValidator: 'org.apache.cassandra.db.marshal.IntegerType' }
};

module.exports = exports;