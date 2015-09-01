'use strict';

// node modules
var nmUtils = require('util');
var nm_ = require('underscore');

var prefix = 'dakota.error.';

function compileErrors(prefix, namespace, errors) {
  nm_.each(errors, function(error, index) {
    namespace[error] = function(message) {
      Error.captureStackTrace(this, this.constructor);
      this.name = prefix + this.constructor.name;
      this.message = message;
    }
    nmUtils.inherits(namespace[error], Error);
  });
}

// ==========
// = Dakota =
// ==========
var Dakota = exports.Dakota = {};
compileErrors(prefix + 'dakota.', Dakota, [
  'DuplicateModel',
  'InvalidArgument',
  'EnsureKeyspaceExists',
  'QueryQueueAlreadyProcessed',
  'InvalidQueryQueueAction'
]);

// ============
// = Keyspace =
// ============
var Keyspace = exports.Keyspace = {};
compileErrors(prefix + 'keyspace.', Keyspace, [
  'InvalidArgument',
  'SelectSchemaError',
  'CreateError',
  'FixError'
]);

// ==========
// = Logger =
// ==========
var Logger = exports.Logger = {};
compileErrors(prefix + 'logger.', Logger, [
  'InvalidArgument'
]);

// =========
// = Model =
// =========
var Model = exports.Model = {};
compileErrors(prefix + 'model.', Model, [
  'InvalidArgument',
  'EnsureTableExists',
  'InvalidColumn',
  'TypeMismatch',
  'InvalidCallbackKey',
  'ValidationFailedError',
  'QueryQueueAlreadyProcessed',
  'InvalidQueryQueueAction'
]);

// =========
// = Query =
// =========
var Query = exports.Query = {};
compileErrors(prefix + 'query.', Query, [
  'InvalidArgument',
  'InvalidType',
  'UnexpectedType',
  'ActionConflict',
  'InvalidAction',
  'InvalidWhereOperation',
  'InvalidOrdering',
  'InvalidUsing',
  'InvalidUsing',
  'InvalidIfOperation',
  'InvalidUpdateOperation'
]);

// ==========
// = Schema =
// ==========
var Schema = exports.Schema = {};
compileErrors(prefix + 'schema.', Schema, [
  'InvalidArgument',
  'InvalidType',
  'InvalidTypeDefinition',
  'InvalidCollectionDefinition',
  'InvalidSchemaDefinitionKey',
  'InvalidKeyDefinition',
  'InvalidWithDefinition',
  'MissingDefinition',
  'InvalidCallbackKey'
]);

// =========
// = Table =
// =========
var Table = exports.Table = {};
compileErrors(prefix + 'table.', Table, [
  'InvalidArgument',
  'InvalidColumnDefinition',
  'InvalidWith',
  'SelectSchemaError',
  'CreateError',
  'FixError'
]);

// =====================
// = User Defined Type =
// =====================
var UserDefinedType = exports.UserDefinedType = {};
compileErrors(prefix + 'userDefinedType.', UserDefinedType, [
  'InvalidArgument',
  'InvalidType',
  'InvalidFieldDefinition'
]);

// ===============
// = Validations =
// ===============
var Validations = exports.Validations = {};
compileErrors(prefix + 'validations.', Validations, [
  'InvalidArgument',
  'InvalidType',
  'InvalidColumn',
  'InvalidValidationDefinitionKey'
]);