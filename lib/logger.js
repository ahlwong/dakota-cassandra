// node modules
var nm_ = require('underscore');

// lib
var lErrors = require('./errors');
var lHelpers = require('./helpers');

var LEVELS = {
  debug: 1, // start at 0 to avoid falsy value
  info:  2,
  warn:  3,
  error: 4
};

function Logger() {
  this._options = {
    level: 'debug',
    queries: true
  };
}

// =============
// = Configure =
// =============
Logger.prototype.configure = function(options) {
  if (!lHelpers.isHash(options)) {
    throw new lErrors.Logger.InvalidArgument('Argument should be a {}.');
  }
  else if (!nm_.isUndefined(options.level) && !LEVELS[options.level]) {
    throw new lErrors.Logger.InvalidArgument('Argument should be \'debug\', \'info\', \'warn\', or \'error\'.');
  }
  else if (!nm_.isUndefined(options.queries) && !nm_.isBoolean(options.queries)) {
    throw new lErrors.Logger.InvalidArgument('Argument should be a boolean.');
  }
  
  this._options = nm_.extend(this._options, options);
}

// ==================
// = Logging Levels =
// ==================
Logger.prototype.debug = function() {
  if (LEVELS['debug'] < LEVELS[this._options.level]) {
    return;
  }
  
  log('log', 90, arguments);
},

Logger.prototype.info = function() {
  if (LEVELS['info'] < LEVELS[this._options.level]) {
    return;
  }
  
  log('info', 34, arguments);
},

Logger.prototype.warn = function() {
  if (LEVELS['warn'] < LEVELS[this._options.level]) {
    return;
  }
  
  log('warn', 33, arguments);
},

Logger.prototype.error = function() {
  if (LEVELS['error'] < LEVELS[this._options.level]) {
    return;
  }
  
  log('error', 31, arguments);
};

// ===========
// = Queries =
// ===========
Logger.prototype.query = function() {
  if (!this._options.queries) {
    return;
  }
  log('info', 35, nm_.initial(arguments));
};

// ===========
// = Helpers =
// ===========
function log(level, color, args) {
  nm_.each(args, function(arg) {
    console[level]('\x1b[' + color + 'm', arg ,'\x1b[0m');
  });
}

module.exports = new Logger();