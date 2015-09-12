// node modules
var nmCassandraTypes = require('cassandra-driver').types;
var nm_ = require('underscore');

// lib
var lErrors = require('./errors');
var lHelpers = require('./helpers');

exports.KEYWORDS = {
  
  // primitives
  ascii     : { validator: nm_.isString,           dbValidator: 'org.apache.cassandra.db.marshal.AsciiType',         size: 0 },
  bigint    : { validator: lHelpers.isInteger,     dbValidator: 'org.apache.cassandra.db.marshal.LongType',          size: 0 },
  blob      : { validator: lHelpers.isAnything,    dbValidator: 'org.apache.cassandra.db.marshal.BytesType',         size: 0 },
  boolean   : { validator: nm_.isBoolean,          dbValidator: 'org.apache.cassandra.db.marshal.BooleanType',       size: 0 },
  counter   : { validator: lHelpers.isInteger,     dbValidator: 'org.apache.cassandra.db.marshal.CounterColumnType', size: 0 },
  decimal   : { validator: nm_.isNumber,           dbValidator: 'org.apache.cassandra.db.marshal.DecimalType',       size: 0 },
  double    : { validator: nm_.isNumber,           dbValidator: 'org.apache.cassandra.db.marshal.DoubleType',        size: 0 },
  float     : { validator: nm_.isNumber,           dbValidator: 'org.apache.cassandra.db.marshal.FloatType',         size: 0 },
  inet      : { validator: lHelpers.isInet,        dbValidator: 'org.apache.cassandra.db.marshal.InetAddressType',   size: 0 },
  int       : { validator: lHelpers.isInteger,     dbValidator: 'org.apache.cassandra.db.marshal.Int32Type',         size: 0 },
  text      : { validator: nm_.isString,           dbValidator: 'org.apache.cassandra.db.marshal.UTF8Type',          size: 0 },
  timestamp : { validator: lHelpers.isDatetime,    dbValidator: 'org.apache.cassandra.db.marshal.TimestampType',     size: 0 },
  timeuuid  : { validator: lHelpers.isUUID,        dbValidator: 'org.apache.cassandra.db.marshal.TimeUUIDType',      size: 0 },
  uuid      : { validator: lHelpers.isUUID,        dbValidator: 'org.apache.cassandra.db.marshal.UUIDType',          size: 0 },
  varchar   : { validator: nm_.isString,           dbValidator: 'org.apache.cassandra.db.marshal.UTF8Type',          size: 0 },
  varint    : { validator: lHelpers.isInteger,     dbValidator: 'org.apache.cassandra.db.marshal.IntegerType',       size: 0 },
  
  // collections
  list      : { validator: nm_.isArray,            dbValidator: 'org.apache.cassandra.db.marshal.ListType',          size: 1 },
  set       : { validator: nm_.isArray,            dbValidator: 'org.apache.cassandra.db.marshal.SetType',           size: 1 },
  map       : { validator: lHelpers.isPlainObject, dbValidator: 'org.apache.cassandra.db.marshal.MapType',           size: 2 },
  
  // tuple
  tuple     : { validator: lHelpers.isTuple,       dbValidator: 'org.apache.cassandra.db.marshal.TupleType',         size: -1 },
  
  // frozen
  frozen    : {                                    dbValidator: 'org.apache.cassandra.db.marshal.FrozenType',        size: 1 }
  
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
// returns: isUserDefinedType{Boolean}
exports.isUserDefinedType = function(dakota, type) {
  return !!dakota.getUserDefinedType(type);
};

// expects: (dakota{Dakota}, type{String})
// returns: baseType{String}
exports.baseType = function(dakota, type) {
  while(true) {
    var e = extract(type);
    if (e.keyword === 'frozen') {
      type = e.contents;
    }
    else {
      return e.keyword;
    }
  }
};

// expects: (dakota{Dakota}, map<key,...>{String})
// returns: keyType{String}
exports.mapKeyType = function(dakota, mapType) {
  var e = extract(mapType);
  if (e.keyword !== 'map') {
    throw new lErrors.Types.InvalidArgument('Argument type should be a string with the format: map<key,...>.');
  }
  return exports.baseType(dakota, split(e.contents)[0]);
};

// expects: (dakota{Dakota}, type{String})
// returns: isStringType{Boolean}
exports.isStringType = function(dakota, type) {
  var definition = exports.KEYWORDS[type];
  return definition && (definition.dbValidator === 'org.apache.cassandra.db.marshal.AsciiType' || definition.dbValidator === 'org.apache.cassandra.db.marshal.UTF8Type');
};

// expects: (dakota{Dakota}, type{String})
// returns: isNumberType{Boolean}
exports.isNumberType = function(dakota, type) {
  var definition = exports.KEYWORDS[type];
  return definition && (definition.dbValidator === 'org.apache.cassandra.db.marshal.CounterColumnType' || definition.dbValidator === 'org.apache.cassandra.db.marshal.DecimalType' || definition.dbValidator === 'org.apache.cassandra.db.marshal.DoubleType' || definition.dbValidator === 'org.apache.cassandra.db.marshal.FloatType' || definition.dbValidator === 'org.apache.cassandra.db.marshal.Int32Type' || definition.dbValidator === 'org.apache.cassandra.db.marshal.IntegerType');
};

// expects: (dakota{Dakota}, type{String})
// returns: isBooleanType{Boolean}
exports.isBooleanType = function(dakota, type) {
  var definition = exports.KEYWORDS[type];
  return definition && (definition.dbValidator === 'org.apache.cassandra.db.marshal.BooleanType');
};

// ===============
// = Validations =
// ===============

// expects: (dakota{Dakota}, type{String})
// returns: isValidType{Boolean}
exports.isValidType = function(dakota, type) {
  var e = extract(type);
  var definition = exports.KEYWORDS[e.keyword];
  if (definition) {
    if (e.contents) {
      var parts = split(e.contents);
      if (definition.size !== -1 && definition.size !== parts.length) {
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
      return definition.size == 0;
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
  var definition = exports.KEYWORDS[e.keyword];
  if (definition) {
    if (e.contents) {
      var parts = split(e.contents);
      if (e.keyword === 'frozen') {
        return exports.isValidValueType(dakota, parts[0], value);
      }
      else if (!definition.validator(value)) {
        return false;
      }
      else if (e.keyword === 'map') {
        for (var key in value) {
          if (!exports.isValidValueType(dakota, parts[0], key) || !exports.isValidValueType(dakota, parts[1], value[key])) {
            return false;
          }
        }
        return true;
      }
      else if (e.keyword === 'tuple') {
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
      return definition.validator(value);
    }
  }
  else if (exports.isUserDefinedType(dakota, type)) {
    return dakota.getUserDefinedType(type).isValidValueTypeForSelf(value);
  }
  else {
    return false;
  }
};

// expects: (dakota{Dakota}, type{String}, includeFrozen{Boolean}?)
// returns: dbValidator{String}
exports.dbValidator = function(dakota, type, includeFrozen) {
  var validator = '';
  var keyword = '';
  var frozen = false;
  var level = 0;
  nm_.each(type, function(c, index) {
    if (c === '<' || c === '>' || c === ',') {
      
      if (frozen) {
        if (c === '<') {
          level++;
        }
        else if (c === '>') {
          level--;
          
          if (level < 0) {
            frozen = false;
            c = '';
          }
        }
      }
      
      if (!includeFrozen && keyword === 'frozen') {
        frozen = true;
        level = 0;
        keyword = '';
      }
      else {
        if (keyword.length > 0) {
          if (exports.isUserDefinedType(dakota, keyword)) {
            validator += dakota.getUserDefinedType(keyword).dbValidator();
          }
          else {
            validator += exports.KEYWORDS[keyword].dbValidator;
          }
          keyword = '';
        }
        validator += c;
      }
    }
    else {
      keyword += c;
    }
  });
  if (keyword.length > 0) {
    if (exports.isUserDefinedType(dakota, keyword)) {
      validator += dakota.getUserDefinedType(keyword).dbValidator();
    }
    else {
      validator += exports.KEYWORDS[keyword].dbValidator;
    }
  }
  validator = validator.replace(/</g, '(');
  validator = validator.replace(/>/g, ')');
  return validator;
};

// =================
// = Format Values =
// =================

exports.formatValueType = function(dakota, type, value) {
  var e = extract(type);
  var definition = exports.KEYWORDS[e.keyword];
  if (definition) {
    if (e.contents) {
      var parts = split(e.contents);
      if (e.keyword === 'frozen') {
        return exports.formatValueType(dakota, parts[0], value);
      }
      else if (e.keyword === 'map') {
        nm_.each(value, function(v, key) {
          key = exports.formatValueType(dakota, parts[0], key);
          value[key] = exports.formatValueType(dakota, parts[1], v);
        });
        return value;
      }
      else if (e.keyword === 'tuple') {
        value = nm_.map(parts, function(part, index) {
          return exports.formatValueType(dakota, part, value[index]);
        });
        return nmCassandraTypes.Tuple.fromArray(value);
      }
      else {
        value = nm_.map(value, function(v, index) {
          return exports.formatValueType(dakota, parts[0], v);
        });
        return value;
      }
    }
    else {
      return value;
    }
  }
  else if (exports.isUserDefinedType(dakota, type)) {
    return dakota.getUserDefinedType(type).formatValueTypeForSelf(value);
  }
  else {
    return false;
  }
};

// ===============
// = Cast Values =
// ===============

exports.castValue = function(dakota, value) {
  
  // cassandra tuple
  if (value instanceof nmCassandraTypes.Tuple) {
    value = value.values();
  }
  
  // cassandra types
  if (value instanceof nmCassandraTypes.BigDecimal) {
    return value.toNumber();
  }
  else if (value instanceof nmCassandraTypes.InetAddress) {
    return value.toString();
  }
  else if (value instanceof nmCassandraTypes.Integer) {
    return value.toNumber();
  }
  else if (value instanceof nmCassandraTypes.LocalDate) {
    return value.toString();
  }
  else if (value instanceof nmCassandraTypes.LocalTime) {
    return value.toString();
  }
  else if (value instanceof nmCassandraTypes.TimeUuid) {
    return value.toString();
  }
  else if (value instanceof nmCassandraTypes.Uuid) {
    return value.toString();
  }
  
  // array
  if (nm_.isArray(value)) {
    nm_.each(value, function(v, index) {
      value[index] = exports.castValue(dakota, v);
    });
    return value;
  }
  
  // hash
  else if (lHelpers.isPlainObject(value)) {
    nm_.each(value, function(v, key) {
      value[key] = exports.castValue(dakota, v);
    });
    return value;
  }
  
  return value;
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