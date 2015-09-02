// node modules
var nm_ = require('underscore');

// lib
var lHelpers = require('./helpers');

exports.KEYWORDS = {
  
  // primitives
  ascii     : { validator: nm_.isString,        dbValidator: 'org.apache.cassandra.db.marshal.AsciiType',         size: 0 },
  bigint    : { validator: lHelpers.isInteger,  dbValidator: 'org.apache.cassandra.db.marshal.LongType',          size: 0 },
  blob      : { validator: lHelpers.isAnything, dbValidator: 'org.apache.cassandra.db.marshal.BytesType',         size: 0 },
  boolean   : { validator: nm_.isBoolean,       dbValidator: 'org.apache.cassandra.db.marshal.BooleanType',       size: 0 },
  counter   : { validator: lHelpers.isInteger,  dbValidator: 'org.apache.cassandra.db.marshal.CounterColumnType', size: 0 },
  decimal   : { validator: nm_.isNumber,        dbValidator: 'org.apache.cassandra.db.marshal.DecimalType',       size: 0 },
  double    : { validator: nm_.isNumber,        dbValidator: 'org.apache.cassandra.db.marshal.DoubleType',        size: 0 },
  float     : { validator: nm_.isNumber,        dbValidator: 'org.apache.cassandra.db.marshal.FloatType',         size: 0 },
  inet      : { validator: lHelpers.isInet,     dbValidator: 'org.apache.cassandra.db.marshal.InetAddressType',   size: 0 },
  int       : { validator: lHelpers.isInteger,  dbValidator: 'org.apache.cassandra.db.marshal.Int32Type',         size: 0 },
  text      : { validator: nm_.isString,        dbValidator: 'org.apache.cassandra.db.marshal.UTF8Type',          size: 0 },
  timestamp : { validator: lHelpers.isDatetime, dbValidator: 'org.apache.cassandra.db.marshal.TimestampType',     size: 0 },
  timeuuid  : { validator: lHelpers.isUUID,     dbValidator: 'org.apache.cassandra.db.marshal.TimeUUIDType',      size: 0 },
  uuid      : { validator: lHelpers.isUUID,     dbValidator: 'org.apache.cassandra.db.marshal.UUIDType',          size: 0 },
  varchar   : { validator: nm_.isString,        dbValidator: 'org.apache.cassandra.db.marshal.UTF8Type',          size: 0 },
  varint    : { validator: lHelpers.isInteger,  dbValidator: 'org.apache.cassandra.db.marshal.IntegerType',       size: 0 },
  
  // collections
  list      : { validator: nm_.isArray,         dbValidator: 'org.apache.cassandra.db.marshal.ListType',          size: 1 },
  set       : { validator: nm_.isArray,         dbValidator: 'org.apache.cassandra.db.marshal.SetType',           size: 1 },
  map       : { validator: lHelpers.isHash,     dbValidator: 'org.apache.cassandra.db.marshal.MapType',           size: 2 },
  
  // tuple
  tuple     : { validator: nm_.isArray,         dbValidator: 'org.apache.cassandra.db.marshal.TupleType',         size: -1 },
  
  // frozen
  frozen    : {                                 dbValidator: 'org.apache.cassandra.db.marshal.FrozenType',        size: 1 }
  
};

// ===========
// = Methods =
// ===========

// expects: (dakota{Dakota}, type{String})
// returns: sanitizedType{String}
exports.sanitize = function(dakota, type) {
  return type.replace(/ /g, '');
};

// expects: (dakota{Dakota}, type{String})
// returns: isCollectionType{Boolean}
exports.isCollectionType = function(dakota, type) {
  var keyword = exports.KEYWORDS[type];
  return keyword && keyword.size > 0;
};

// expects: (dakota{Dakota}, type{String})
// returns: isUserDefinedType{Boolean}
exports.isUserDefinedType = function(dakota, type) {
  return !!dakota.getUserDefinedType(type);
};

// ===============
// = Validations =
// ===============

// expects: (dakota{Dakota}, type{String})
// returns: isValidType{Boolean}
exports.isValidType = function(dakota, type) {
  var e = extract(type);
  var keyword = exports.KEYWORDS[e.keyword];
  if (keyword) {
    if (e.contents) {
      var parts = split(e.contents);
      if (keyword.size !== -1 && keyword.size !== parts.length) {
        return false;
      }
      else {
        var length = parts.length;
        for (var i = 0; i < length; i++) {
          if (!exports.isValidType(dakota, parts[i])) {
            return false;
          }
        }
        return true;
      }
    }
    else {
      return keyword.size == 0;
    }
  }
  else {
    return exports.isUserDefinedType(dakota, type);
  }
};

// expects: (dakota{Dakota}, type{String}, value)
// returns: isValidValueType{Boolean}
exports.isValidValueType = function(dakota, type, value) {
  var e = extract(type);
  var keyword = exports.KEYWORDS[e.keyword];
  if (keyword) {
    if (e.contents) {
      var parts = split(e.contents);
      if (keyword === 'frozen') {
        return !exports.isValidValueType(dakota, parts[0], value);
      }
      else if (!keyword.validator(value)) {
        return false;
      }
      else if (keyword === 'map') {
        for (var key in value) {
          if (!exports.isValidValueType(dakota, parts[0], key) || !exports.isValidValueType(dakota, parts[1], value[key])) {
            return false;
          }
        }
        return true;
      }
      else if (keyword === 'tuple') {
        var length = parts.length;
        for (var i = 0; i < length; i++) {
          if (!exports.isValidValueType(dakota, parts[i], value[i])) {
            return false;
          }
        }
        return true;
      }
      else {
        var length = value.length;
        for (var i = 0; i < length; i++) {
          if (!exports.isValidValueType(dakota, parts[0], value[i])) {
            return false;
          }
        }
        return true;
      }
    }
    else {
      return keyword.validator(value);
    }
  }
  else if (exports.isUserDefinedType(dakota, type)) {
    return dakota.getUserDefinedType(type);
  }
  else {
    return false;
  }
};

// expects: (dakota{Dakota}, type{String})
// returns: dbValidator{String}
exports.dbValidator = function(dakota, type) {
  type = type.replace(/</g, '(');
  type = type.replace(/>/g, ')');
  nm_.each(exports.KEYWORDS, function(value, keyword) {
    type = type.replace(new RegExp(keyword, 'g'), value.dbValidator);
  });
  return type;
};

// ===========
// = Helpers =
// ===========

function extract(type) {
  var i = type.indexOf('<');
  if (i < 0) {
    return {
      keyword: type,
      contents: null
    }
  }
  else {
    return { 
      keyword: type.substring(0, i),
      contents: type.substring(i + 1, type.length - 1)
    }
  }
}

function split(type) {
  var level = 0;
  var part = '';
  var parts = [];
  
  var length = type.length;
  for(var i = 0; i < length; i++) {
    var c = type[i];
    if (c === '<') {
      level++;
    }
    else if (c === '>') {
      level--;
    }
    else if (c === ',' && level == 0) {
      parts.push(part);
      part = '';
      continue;
    }
    part += c;
  }
  parts.push(part);
  
  return parts;
}