// node modules
var nm_ = require('underscore');

// lib
var lHelpers = require('./helpers');
var lTypes = require('./types');

exports.COLLECTIONS = {
  list: { validator: nm_.isArray,     dbValidator: 'org.apache.cassandra.db.marshal.ListType' },
  set : { validator: nm_.isArray,     dbValidator: 'org.apache.cassandra.db.marshal.SetType' },
  map : { validator: lHelpers.isHash, dbValidator: 'org.apache.cassandra.db.marshal.MapType' }
};

// ===========
// = Helpers =
// ===========

// expects: (collection{String}, subType{String, [type{String}, type{String}]})
// returns: collection<subType> or map<subType[0], subType[1]>
exports.fullCollectionTypeString = function(collection, subType) {
  var typeString = '';
  if (nm_.isArray(subType)) {
    nm_.each(subType, function(type, index) {
      if (index > 0) {
        typeString += ', ';
      }
      typeString += type;
    });
  }
  else {
    typeString += subType;
  }
  return collection + '<' + typeString + '>';
};

// expects: (collection{String}, subType{String, [type{String}, type{String}]})
// returns: dbValidator(dbValidator) or dbValidator(dbValidator,dbValidator)
exports.fullDBValidatorString = function(collection, subType) {
  var validatorString = '';
  if (nm_.isArray(subType)) {
    nm_.each(subType, function(type, index) {
      if (index > 0) {
        validatorString += ',';
      }
      validatorString += lTypes.TYPES[type].dbValidator;
    });
  }
  else {
    validatorString += lTypes.TYPES[subType].dbValidator;
  }
  return exports.COLLECTIONS[collection].dbValidator + '(' + validatorString + ')';
};