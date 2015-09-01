// node modules
var nm_ = require('underscore');

// lib
var lHelpers = require('./helpers');

exports.PRIMITIVES = {
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

exports.COLLECTIONS = {
  list: { validator: nm_.isArray,     dbValidator: 'org.apache.cassandra.db.marshal.ListType' },
  set : { validator: nm_.isArray,     dbValidator: 'org.apache.cassandra.db.marshal.SetType' },
  map : { validator: lHelpers.isHash, dbValidator: 'org.apache.cassandra.db.marshal.MapType' }
};

// ===========
// = Methods =
// ===========

// expects: (type{String})
// returns: isPrimitiveType{boolean}
exports.isPrimitiveType = function(type) {
  return collectionTypeToComponentTypes(type).length === 1;
};

// expects: (type{String})
// returns: isCollectionType{boolean}
exports.isCollectionType = function(type) {
  return collectionTypeToComponentTypes(type).length > 1;
};

// expects: (type{String})
// returns: collectionType[list, set, map]{String}
exports.collectionType = function(type) {
  return collectionTypeToComponentTypes(type)[0];
};

// ===============
// = Validations =
// ===============

// expects: (type{String})
// returns: isValidType{boolean}
exports.isValidType = function(type) {
  return exports.isValidPrimitiveType(type) || exports.isValidCollectionType(type);
};

// expects: (type{String})
// returns: isValidPrimitiveType{boolean}
exports.isValidPrimitiveType = function(type) {
  if (!nm_.isString(type)) {
    return false;
  }
  else {
    return !!exports.PRIMITIVES[type];
  }
};

// expects: (type{String})
// returns: isValidCollectionType{boolean}
exports.isValidCollectionType = function(type) {
  if (!nm_.isString(type)) {
    return false;
  }
  else {
    var types = collectionTypeToComponentTypes(type);
    if (!exports.COLLECTIONS[types[0]]) {
      return false;
    }
    else if (types[0] === 'map') {
      if (types.length !== 3) {
        return false;
      }
    }
    else if (types.length !== 2) {
      return false;
    }
    
    nm_.each(nm_.rest(types, 1), function(t, index) {
      if (!exports.PRIMITIVES[t]) {
        return false;
      }
    });
    return true;
  }
};

// expects: (type{String}, value)
// returns: isValidValueType{boolean}
exports.isValidValueType = function(type, value) {
  var types = collectionTypeToComponentTypes(type);
  if (types.length == 1) {
    return exports.PRIMITIVES[type].validator(value);
  }
  else if (!exports.COLLECTIONS[types[0]].validator(value)) {
    return false;
  }
  else {
    nm_.each(nm_.rest(types, 1), function(t, index) {
      if (!exports.PRIMITIVES[t].validator(value)) {
        return false;
      }
    });
    return true;
  }
};

// expects: (type{String})
// returns: dbValidator(dbValidator) or dbValidator(dbValidator,dbValidator)
exports.dbValidator = function(type) {
  var types = collectionTypeToComponentTypes(type);
  if (types.length == 1) {
    return exports.PRIMITIVES[type].dbValidator;
  }
  else {
    var validator = '';
    nm_.each(nm_.rest(types, 1), function(t, index) {
      if (index > 0) {
        validator += ',';
      }
      validator += exports.PRIMITIVES[t].dbValidator;
    });
    return exports.COLLECTIONS[types[0]].dbValidator + '(' + validator + ')';
  }
};

// ===========
// = Helpers =
// ===========

function collectionTypeToComponentTypes(type) {
  return type.match(/(\w+)/g);
}
