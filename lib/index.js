// node modules
var nmCassandra = require('cassandra-driver');
var nm_ = require('underscore');

// lib
var lErrors = require('./errors');
var lHelpers = require('./helpers');
var lKeyspace = require('./keyspace');
var lLogger = require('./logger');
var lModel = require('./model');
var lQuery = require('./query');
var lSchema = require('./schema');
var lValidations = require('./validations');

var lCallbackRecipes = require('./recipes/callbacks');
var lSanitizerRecipes = require('./recipes/sanitizers');
var lValidatorRecipes = require('./recipes/validators');

// expects: (options{Object})
// options:
//          connection{Object} : is passed directly to cassandra-driver's Client constructor
//          example options.connection : { contactPoints: ['127.0.0.1', ...], keyspace: 'keyspace' }
//          keyspace{Object} : is passed as default options to Keyspace
//          model{Object} : is passed as default options to Model
//          table{Object} : is passed as default options to Table
// returns: undefined
function Dakota(options) {
  if (!lHelpers.isHash(options)) {
    throw new lErrors.Dakota.InvalidArgument('Argument should be a {}.');
  }
  else if (!lHelpers.isHash(options.connection)) {
    throw new lErrors.Dakota.InvalidArgument('Argument should be a {}.');
  }
  else if (!options.connection.contactPoints || !options.connection.keyspace) {
    throw new lErrors.Dakota.InvalidArgument('Connection contactPoints and keyspace must be defined.');
  }
  else {
    this._client = null;
    this._queryQueue = {
      execute: [],
      eachRow: []
    };
    this._userDefinedTypes = {};
    this._models = {};
    
    this._keyspace = options.connection.keyspace;
    this._options = options;
    
    init.call(this);
  }
}

function init() {
  var self = this;
  
  // configure logger
  if (this._options.logging) {
    lLogger.configure(this._options.logging);
  }
  
  // ensure keyspace exists
  ensureKeyspace.call(this, function(err) {
    if (err) {
      throw new lErrors.Dakota.EnsureKeyspaceExists('Error trying to ensure keyspace exists: ' + err + '.');
    }
    else {
      self._client = new nmCassandra.Client(self._options.connection);
      processQueryQueue.call(self);
    }
  }, this._options.keyspace);
}

// =================
// = Namespace Lib =
// =================
Dakota.Errors = lErrors;
Dakota.Keyspace = lKeyspace;
Dakota.Model = lModel;
Dakota.Query = lQuery;
Dakota.Schema = lSchema;
Dakota.Validations = lValidations;

Dakota.Recipes = {
  Callbacks: lCallbackRecipes,
  Sanitizers: lSanitizerRecipes,
  Validators: lValidatorRecipes
};

// ============
// = Keyspace =
// ============

// expects: (options{Object}?)
// options:
//          example options : { replication: replication{Object}, durableWrites: durableWrites{Boolean} }
// returns: undefined
function ensureKeyspace(callback, options) {
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Dakota.InvalidArgument('Argument should be a function.');
  }
  else if (!nm_.isUndefined(options) && !lHelpers.isHash(options)) {
    throw new lErrors.Dakota.InvalidArgument('Argument should be a {}.');
  }
  options = nm_.extend({ replication: { 'class': 'SimpleStrategy', 'replication_factor': 1 }, durableWrites: true }, options);
  
  // copy connection contactPoints, ignore keyspace
  var connection = {
    contactPoints: this._options.connection.contactPoints
  };
  
  // create temporary client
  // keyspace needs a client without a keyspace defined
  var client = new nmCassandra.Client(connection);
  
  // ensure keyspace exists
  var keyspace = new lKeyspace(this, client, this._keyspace, options.replication, options.durableWrites, options);
  keyspace.ensureExists(function(err) {
    if (err) {
      callback(err);
    }
    
    // shutdown client since no longer needed
    client.shutdown(function(err) {
      if (err) {
        lLogger.error(err);
      }
      callback();
    });
  }, options);
}

// ======================
// = User Defined Types =
// ======================
Dakota.prototype.addUserDefinedType = function(name, definition, options) {
  
};

// ==========
// = Models =
// ==========

// expects: (name{String}, schema{Schema}, validations{Validations}, options{Object}?) or (name{String}, schema{Schema}, null, options{Object}?) or (name{String}, schema{Schema}, options{Object}?)
// return: model{Model}
Dakota.prototype.addModel = function(name, schema, validations, options) {
  if (!nm_.isString(name)) {
    throw new lErrors.Dakota.InvalidArgument('Argument should be a string.');
  }
  else if (!(schema instanceof lSchema)) {
    throw new lErrors.Dakota.InvalidArgument('Argument should be a Schema.');
  }
  
  if (!nm_.isUndefined(validations)) {
    if (!nm_.isNull(validations) || (validations instanceof lValidations)) {
      // do nothing
    }
    else if (lHelpers.isHash(validations)) {
      options = validations;
      validations = null;
    }
    else {
      throw new lErrors.Dakota.InvalidArgument('Argument should be a Validations or null.');
    }
  }
  
  if (!nm_.isUndefined(options) && !lHelpers.isHash(options)) {
    throw new lErrors.Dakota.InvalidArgument('Argument should be a {}.');
  }
  
  if (!nm_.isUndefined(this._models[name])) {
    throw new lErrors.Dakota.DuplicateModel('Model with same name already added.');
  }
  else {
    options = nm_.extend(this._options.model ? this._options.model : {}, options);
    var model = lModel.compile(this, name, schema, validations, options);
    this._models[name] = model;
    return model;
  }
};

// expects: (name{String})
// returns: model{Model}
Dakota.prototype.getModel = function(name) {
  if (!nm_.isString(name)) {
    throw new lErrors.Dakota.InvalidArgument('Argument should be a string.');
  }
  else {
   return this._models[name]; 
  }
};

// ===========
// = Execute =
// ===========

// expects: the same params as Cassandra.Client.execute(); (query{String}, [param, param, ...], options{Object}, function(err{Error}, result))
// returns: undefined
Dakota.prototype.execute = function(query, params, options, callback) {
  if (nm_.isNull(this._client)) {
    addToQueryQueue.call(this, 'execute', arguments);
  }
  else {
    lLogger.query(query, params, options);
    this._client.execute(query, params, options, callback);
  }
};

// expects: the same params as Cassandra.Client.eachRow(); (query{String}, [param, param, ...], options{Object}, function(n{Number}, row{Row}), function(err{Error}))
// returns: undefined
Dakota.prototype.eachRow = function(query, params, options, rowCallback, completeCallback) {
  if (nm_.isNull(this._client)) {
    addToQueryQueue.call(this, 'eachRow', arguments);
  }
  else {
    this._client.eachRow(query, params, options, rowCallback, completeCallback);
  }
};

// expects: the same params as Cassandra.Client.eachRow(); (query{String}, [param, param, ...], options{Object})
// returns: undefined
Dakota.prototype.stream = function(query, params, options) {
  if (nm_.isNull(this._client)) {
    // need to buffer stream incase client is not yet set
    throw new Error('Not implemented yet.');
  }
  else {
    this._client.stream(query, params, options);
  }
};

// ===============
// = Query Queue =
// ===============

// expects: (action[execute, eachRow]{String}, [arguments])
// returns: undefined
function addToQueryQueue(action, arguments) {
  if (nm_.isNull(this._queryQueue)) {
    throw new lErrors.Dakota.QueryQueueAlreadyProcessed('Cannot enqueue query. Queue already processed.');
  }
  else if (action !== 'execute' && action !== 'eachRow') {
    throw new lErrors.Dakota.InvalidQueryQueueAction('Invalid action: ' + action + '.');
  }
  else {
    this._queryQueue[action].push(arguments);
  }
}

// expects: ()
// returns: undefined
function processQueryQueue() {
  var self = this;
  
  if (nm_.isNull(this._queryQueue)) {
    throw new lErrors.Dakota.QueryQueueAlreadyProcessed('Cannot process queue. Queue already processed.');
  }
  else {
    nm_.each(this._queryQueue, function(queue, action) {
      nm_.each(queue, function(query, index) {
        if (action === 'execute') {
          self.execute.apply(self, query);
        }
        else if (action === 'eachRow') {
          self.eachRow.apply(self, query);
        }
      });
    });
    this._queryQueue = null;
  }
}

// =========
// = Batch =
// =========

// expects: ([query{Query}, ...], function(err{Error}))
// returns: undefined
Dakota.prototype.batch = function(queries, callback) {
  lQuery.batch(this._client, queries, callback);
};

// ================
// = UUID Helpers =
// ================

Dakota.generateUUID = function() {
  return nmCassandra.types.Uuid.random().toString();
};

Dakota.generateTimeUUID = function() {
  return nmCassandra.types.TimeUuid.now().toString();
};

// ====================
// = Datetime Helpers =
// ====================

Dakota.nowToTimestamp = function() {
  return Dakota.dateToTimestamp(new Date());
};

Dakota.dateToTimestamp = function(date) {
  return date.getUTCFullYear().toString() + '-' + (date.getUTCMonth() + 1).toString() + '-' + date.getUTCDate().toString() + ' ' + date.getUTCHours().toString() + ':' + date.getUTCMinutes().toString() + ':' + date.getUTCSeconds().toString();
};

module.exports = Dakota;