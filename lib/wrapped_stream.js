// node modules
var nm_ = require('underscore');

// lib
var lErrors = require('./errors');

// expects: model{ModelClass}
// returns: undefined
function WrappedStream(model) {
  this._model = model;
  this._stream = null;
  this._on = {};
}

// expects: (event{String}, function(...))
// returns: this{lWrappedStream}
WrappedStream.prototype.on = function(key, func) {
  var self = this;
  
  this._on[key] = function() {
    func.apply(self, arguments);
  };
  
  if (this._stream) {
    return this._stream.on(key, this._on[key]);
  }
  else {
    return this;
  }
};

// expects: ()
// returns: model{Model} or null
WrappedStream.prototype.read = function() {
  if (this._stream) {
    var result = this._stream.read();
    if (result) {
      if (!this._model || result instanceof this._model) {
        return result;
      }
      else {
        return this._model._newFromQueryRow(result);
      }
    }
  }
  
  return null;
};

// expects: (stream{Cassandra.Stream})
// returns: undefined
WrappedStream.prototype._setStream = function(stream) {
  var self = this;
  
  this._stream = stream;
  
  nm_.each(this._on, function(func, key) {
    stream.on(key, self._on[key]);
  });
};

module.exports = WrappedStream;