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
  this._pipe = null;
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
    return this._stream.read();
  }
  return null;
};

WrappedStream.prototype.pipe = function(dest, options) {
  if (this._stream) {
    this._stream.pipe(dest, options);
  }
  else {
    this._pipe = { dest: dest, options: options };
  }
  return dest;
};

// expects: (stream{Cassandra.Stream}) or (stream{WrappedStream})
// returns: undefined
WrappedStream.prototype._setStream = function(stream) {
  var self = this;
  
  // set stream
  this._stream = stream;
  
  // intercept chunk by overridding .add
  if (!(stream instanceof WrappedStream)) {
    var add = stream.add;
    stream.add = function(chunk) {
      if (chunk && self._model && !(chunk instanceof self._model)) {
        chunk = self._model._newFromQueryRow(chunk);
      }
      add.call(stream, chunk);
    };
  }
  
  // apply on
  nm_.each(this._on, function(func, key) {
    stream.on(key, self._on[key]);
  });
  
  // apply pipe
  if (this._pipe) {
    stream.pipe(this._pipe.dest, this._pipe.options);
  }
};

module.exports = WrappedStream;