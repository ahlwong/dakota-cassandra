module.exports = {
  
  setUuid: function(column) {
    return (function(column) {
      return function() {
        var lDakota = require('../index'); // needs to be required here, not set otherwise
        this.set(column, lDakota.generateUUID());
      }
    })(column);
  },
  
  setTimeuuid: function(column) {
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
  }
}