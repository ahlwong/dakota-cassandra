// lib
var lLogger = require('../logger');

module.exports = {
  
  setUUID: function(column) {
    return (function(column) {
      return function() {
        var lDakota = require('../index'); // needs to be required here, not set otherwise
        this.set(column, lDakota.generateUUID());
      }
    })(column);
  },
  
  setTimeUUID: function(column) {
    return (function(column) {
      return function() {
        var lDakota = require('../index'); // needs to be required here, not set otherwise
        this.set(column, lDakota.generateTimeUUID());
      }
    })(column);
  },
  
  setTimestampToNow: function(column) {
    return (function(column) {
      return function() {
        var lDakota = require('../index'); // needs to be required here, not set otherwise
        this.set(column, lDakota.nowToTimestamp());
      }
    })(column);
  },
  
  // ==============
  // = Deprecated =
  // ==============
  setUuid: function(column) {
    lLogger.warn('Recipes.Callbacks.setUuid deprecated. Use Recipes.Callbacks.setUUID instead.');
    return module.exports.setUUID(column);
  },
  
  setTimeuuid: function(column) {
    lLogger.warn('Recipes.Callbacks.setTimeuuid deprecated. Use Recipes.Callbacks.setTimeUUID instead.');
    return module.exports.setTimeUUID(column);
  }
}