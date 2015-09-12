// node modules
var nmCassandra = require('cassandra-driver');
var nmWhen = require('when');
var nm_ = require('underscore');
var nm_i = require('underscore.inflections');
var nm_s = require('underscore.string');

// lib
var lErrors = require('./errors');
var lHelpers = require('./helpers');
var lKeyspace = require('./keyspace');
var lLogger = require('./logger');
var lModel = require('./model');
var lQuery = require('./query');
var lSchema = require('./schema');
var lUserDefinedType = require('./user_defined_type');
var lValidations = require('./validations');

var lCallbackRecipes = require('./recipes/callbacks');
var lSanitizerRecipes = require('./recipes/sanitizers');
var lValidatorRecipes = require('./recipes/validators');

// expects: (options{Object}, { type: definition{Object}, ... }?)
// options:
//          connection{Object} : is passed directly to cassandra-driver's Client constructor
//          example options.connection : { contactPoints: ['127.0.0.1', ...], keyspace: 'keyspace' }
//          keyspace{Object} : is passed as default options to Keyspace
//          logger{Object} : is passed as default options to Model
//          model{Object} : is passed as default options to Model
//          model.table{Object} : is passed as default options to Table
//          userDefinedType{Object} : is passed as default options to Table
// returns: undefined
function Dakota(options, userDefinedTypes) {
  if (!lHelpers.isPlainObject(options)) {
    throw new lErrors.Dakota.InvalidArgument('Argument should be a {}.');
  }
  else if (!lHelpers.isPlainObject(options.connection)) {
    throw new lErrors.Dakota.InvalidArgument('Argument should be a {}.');
  }
  else if (!options.connection.contactPoints || !options.connection.keyspace) {
    throw new lErrors.Dakota.InvalidArgument('Connection contactPoints and keyspace must be defined.');
  }
  else if (!userDefinedTypes || !lHelpers.isPlainObject(userDefinedTypes)) {
    throw new lErrors.Dakota.InvalidArgument('Argument should be a {}.');
  }
  
  this._ready = false;
  this._client = null;
  this._queryQueue = {
    system: [],
    execute: [],
    eachRow: []
  };
  
  this._userDefinedTypes = {};
  this._models = {};
  
  this._keyspace = options.connection.keyspace;
  this._options = defaultOptions(options);
  
  init.call(this, userDefinedTypes, this._options);
}

function defaultOptions(options) {
  
  var defaults = {
    
    // keyspace
    keyspace: {
      replication: { 'class': 'SimpleStrategy', 'replication_factor': 1 },
      durableWrites: true,
      ensureExists: {
        run: true, // check if keyspace exists and automaticcaly create it if it doesn't
        alter: false // alter existing keyspace to match replication or durableWrites
      }
    },
    
    // logger
    logger: {
      level: 'debug', // log this level and higher [debug < info < warn < error]
      queries: true // log queries
    },
    
    // model
    model: {
      tableName: function(modelName) {
        return nm_i.pluralize(nm_s.underscored(modelName));
      },
      getterSetterName: function(columnName) {
        return columnName.trim().replace(/\s/g, '_');
      },
      typeSpecificSetterName: function(operation, columnName) {
        var name = nm_s.capitalize(columnName.trim().replace(/\s/g, '_'));
        if (operation == 'increment' || operation == 'decrement') {
          return operation + name;
        }
        else {
          return operation + nm_i.singularize(name);
        }
      },
      table: {
        ensureExists: {
          run: true, // check if keyspace exists and automaticcaly create it if it doesn't
          recreate: false, // drop and recreate table on schema mismatch, takes precedence over following options
          recreateColumn: false,  // recreate columns where types don't match schema
          removeExtra: false,  // remove extra columns not in schema
          addMissing: false // add columns in schema that aren't in table
        }
      }
    },
    
    // user defined type
    userDefinedType: {
      ensureExists: {
        run: true,
        recreate: false, // drop and recreate type on schema mismatch, takes precedence over following options
        changeType: false, // change field types to match schema
        addMissing: false // add fields in schema that aren't in type
      }
    }
    
  };
  
  var mergedOptions = nm_.extend({}, nm_.omit(options, 'keyspace', 'logger', 'model', 'userDefinedType'));
  
  if (options.keyspace) {
    mergedOptions.keyspace = nm_.extend(defaults.keyspace, nm_.omit(options.keyspace, 'ensureExists'));
    mergedOptions.keyspace.ensureExists = nm_.extend(defaults.keyspace.ensureExists, options.keyspace.ensureExists);
  }
  else {
    mergedOptions.keyspace = defaults.keyspace;
  }
  
  if (options.logger) {
    mergedOptions.logger = nm_.extend(defaults.logger, options.logger);
  }
  else {
    mergedOptions.logger = defaults.logger;
  }
  
  if (options.model) {
    mergedOptions.model = nm_.extend(nm_.omit(defaults.model, 'table'), nm_.omit(options.model, 'table'));
    if (options.model.table) {
      mergedOptions.model.table = {};
      mergedOptions.model.table.ensureExists = nm_.extend(defaults.model.table.ensureExists, options.model.table.ensureExists);
    }
    else {
      mergedOptions.model.table = defaults.model.table;
    }
  }
  else {
    mergedOptions.model = defaults.model;
  }
  
  if (options.userDefinedType) {
    mergedOptions.userDefinedType = nm_.extend(defaults.userDefinedType, nm_.omit(options.userDefinedType, 'userDefinedType'));
    mergedOptions.userDefinedType.ensureExists = nm_.extend(defaults.userDefinedType.ensureExists, options.userDefinedType.ensureExists);
  }
  else {
    mergedOptions.userDefinedType = defaults.userDefinedType;
  }
  
  return mergedOptions;
}

function init(userDefinedTypes, options) {
  var self = this;
  
  // configure logger
  configureLogger.call(this, options.logger);
  
  // proccess user defined types
  processUserDefinedTypes.call(this, userDefinedTypes, options.userDefinedType);
  
  // ensure keyspace exists
  ensureKeyspace.call(this, function() {
    
    // set client
    self._client = new nmCassandra.Client(options.connection);
    processQueryQueue.call(self, true);
    
    // ensure user defined types
    ensureUserDefinedTypes.call(self, userDefinedTypes, function() {
      
      // dakota ready
      self._ready = true;
      processQueryQueue.call(self, false);
      
    }, options.userDefinedTypes);
  }, options.keyspace);
}

// =================
// = Namespace Lib =
// =================
Dakota.Errors = lErrors;
Dakota.Keyspace = lKeyspace;
Dakota.Model = lModel;
Dakota.Query = lQuery;
Dakota.Schema = lSchema;
Dakota.UserDefinedType = lUserDefinedType;
Dakota.Validations = lValidations;

Dakota.Recipes = {
  Callbacks: lCallbackRecipes,
  Sanitizers: lSanitizerRecipes,
  Validators: lValidatorRecipes
};

// ==========
// = Logger =
// ==========

// expects: (options{Object}?)
// options:
//          example options : { replication: replication{Object}, durableWrites: durableWrites{Boolean} }
// returns: undefined
function configureLogger(options) {
  if (options) {
    lLogger.configure(options);
  }
}

// ======================
// = User Defined Types =
// ======================

// expects: ({ type: fields{Object}, ... }, function(), options{Object}?)
// returns: undefined
function processUserDefinedTypes(userDefinedTypes, options) {
  var self = this;
  
  nm_.each(userDefinedTypes, function(definition, name) {
    if (self._userDefinedTypes[name]) {
      throw new lErrors.Dakota.DuplicateUserDefinedType('User defined type with same name already added: ' + name + '.');
    }
    else {
      self._userDefinedTypes[name] = new lUserDefinedType(self, name, definition, options);
    }
  });
}

// expects: ({ type: fields{Object}, ... }, function(), options{Object}?)
// returns: undefined
function ensureUserDefinedTypes(userDefinedTypes, callback, options) {
  var self = this;
  
  // ensure
  var promises = [];
  nm_.each(self._userDefinedTypes, function(userDefinedType, index) {
    promises.push(nmWhen.promise(function(resolve, reject) {
      userDefinedType.ensureExists(function(err) {
        if (err) {
          reject(err);
        }
        else {
          resolve();
        }
      });
    }));
  });
  
  nmWhen.settle(promises).done(function(descriptors) {
    var success = true;
    nm_.each(descriptors, function(descriptor, index) {
      if (descriptor.state === 'rejected') {
        success = false;
        lLogger.error(descriptor.reason);
      }
    });
    if (success) {
      callback();
    }
    else {
      throw new lErrors.Dakota.EnsureUserDefinedTypeExists('Ensuring user defined types exist failed: rejected promises.');
    }
  });
}

// expects: (name{String})
// returns: userDefinedType{UserDefinedType}
Dakota.prototype.getUserDefinedType = function(name) {
  if (!nm_.isString(name)) {
    throw new lErrors.Dakota.InvalidArgument('Argument should be a string.');
  }
  else {
   return this._userDefinedTypes[name];
  }
};

// ============
// = Keyspace =
// ============

// expects: (function(), options{Object}?)
// options:
//          example options : { replication: replication{Object}, durableWrites: durableWrites{Boolean} }
// returns: undefined
function ensureKeyspace(callback, options) {
  if (!nm_.isFunction(callback)) {
    throw new lErrors.Dakota.InvalidArgument('Argument should be a function.');
  }
  else if (options && !lHelpers.isPlainObject(options)) {
    throw new lErrors.Dakota.InvalidArgument('Argument should be a {}.');
  }
  
  // copy connection contactPoints, ignore keyspace
  var connection = {
    contactPoints: this._options.connection.contactPoints
  };
  
  // create temporary client
  // keyspace needs a client without a keyspace defined
  var client = new nmCassandra.Client(connection);
  
  // ensure keyspace exists
  var keyspace = new lKeyspace(client, this._keyspace, options.replication, options.durableWrites, options);
  keyspace.ensureExists(function(err) {
    if (err) {
      throw new lErrors.Dakota.EnsureKeyspaceExists('Error trying to ensure keyspace exists: ' + err + '.');
    }
    else {
      
      // shutdown client since no longer needed
      client.shutdown(function(err) {
        if (err) {
          lLogger.error(err);
        }
        callback();
      });
    }
  });
}

// ==========
// = Models =
// ==========

// expects: (name{String}, schema{Object}, validations{Object}, options{Object}?)
// return: model{Model}
Dakota.prototype.addModel = function(name, schema, validations, options) {
  if (!nm_.isString(name)) {
    throw new lErrors.Dakota.InvalidArgument('Argument should be a string.');
  }
  else if (!lHelpers.isPlainObject(schema)) {
    throw new lErrors.Dakota.InvalidArgument('Argument should be a Schema.');
  }
  else if (validations && !lHelpers.isPlainObject(validations)) {
    throw new lErrors.Dakota.InvalidArgument('Argument should be a {}.');
  }
  else if (options && !lHelpers.isPlainObject(options)) {
    throw new lErrors.Dakota.InvalidArgument('Argument should be a {}.');
  }
  
  if (this._models[name]) {
    throw new lErrors.Dakota.DuplicateModel('Model with same name already added.');
  }
  else {
    
    // default options
    options = nm_.extend({}, this._options.model, options);
    
    var schema = new lSchema(this, schema, options.schema);
    var validations = validations ? new lValidations(schema, validations, options.validations) : null;
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
  if (!this._ready) {
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
  if (!this._ready) {
    addToQueryQueue.call(this, 'eachRow', arguments);
  }
  else {
    this._client.eachRow(query, params, options, rowCallback, completeCallback);
  }
};

// expects: the same params as Cassandra.Client.eachRow(); (query{String}, [param, param, ...], options{Object})
// returns: undefined
Dakota.prototype.stream = function(query, params, options) {
  if (!this._ready) {
    // need to buffer stream incase client is not yet set
    throw new Error('Not implemented yet.');
  }
  else {
    this._client.stream(query, params, options);
  }
};

// expects: the same params as Cassandra.Client.execute(); (query{String}, [param, param, ...], options{Object}, function(err{Error}, result))
// returns: undefined
Dakota.prototype._system_execute = function(query, params, options, callback) {
  if (!this._client) {
    addToQueryQueue.call(this, 'system', arguments);
  }
  else {
    lLogger.query(query, params, options);
    this._client.execute(query, params, options, callback);
  }
};

// ===============
// = Query Queue =
// ===============

// expects: (action[execute, eachRow]{String}, [arguments])
// returns: undefined
function addToQueryQueue(action, arguments) {
  if (!this._queryQueue) {
    throw new lErrors.Dakota.QueryQueueAlreadyProcessed('Cannot enqueue query. Queue already processed.');
  }
  else if (action !== 'system' && action !== 'execute' && action !== 'eachRow') {
    throw new lErrors.Dakota.InvalidQueryQueueAction('Invalid action: ' + action + '.');
  }
  else {
    this._queryQueue[action].push(arguments);
  }
}

// expects: (systemOnly{Boolean})
// returns: undefined
function processQueryQueue(systemOnly) {
  var self = this;
  
  if (!this._queryQueue) {
    throw new lErrors.Dakota.QueryQueueAlreadyProcessed('Cannot process queue. Queue already processed.');
  }
  else {
    nm_.each(this._queryQueue, function(queue, action) {
      nm_.each(queue, function(query, index) {
        if (action === 'system') {
          self._system_execute.apply(self, query);
        }
        if (!systemOnly) {
          if (action === 'execute') {
            self.execute.apply(self, query);
          }
          else if (action === 'eachRow') {
            self.eachRow.apply(self, query);
          }
        }
      });
    });
    
    // unset
    if (systemOnly) {
      this._queryQueue.system = [];
    }
    else {
      this._queryQueue = null;
    }
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

Dakota.getDateFromTimeUUID = function(timeUUID) {
  if (nm_.isString(timeUUID)) {
    timeUUID = nmCassandra.types.TimeUuid.fromString(timeUUID);
  }
  return timeUUID.getDate();
};

Dakota.getTimeFromTimeUUID = function(timeUUID) {
  if (nm_.isString(timeUUID)) {
    timeUUID = nmCassandra.types.TimeUuid.fromString(timeUUID);
  }
  return timeUUID.getTime();
};

// ====================
// = Timestamp Helpers =
// ====================

Dakota.nowToTimestamp = function() {
  return Dakota.dateToTimestamp(new Date());
};

Dakota.dateToTimestamp = function(date) {
  return date;
};

module.exports = Dakota;