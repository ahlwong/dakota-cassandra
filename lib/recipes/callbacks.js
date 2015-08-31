// lib
var lDakota = require('../index');

var exports = {
  
  setUuid: function(column_name) {
    return (function(column_name) {
      return function() {
        this.set(column_name, lDakota.generateUUID());
      }
    })(column_name);
  },
  
  setTimeuuid: function(column_name) {
    return (function(column_name) {
      return function() {
        this.set(column_name, lDakota.generateTimeUUID());
      }
    })(column_name);
  },
  
  setTimestampToNow: function(column_name) {
    return (function(column_name) {
      return function() {
        this.set(column_name, lDakota.nowToTimestamp());
      }
    })(column_name);
  }
}

module.exports = exports;