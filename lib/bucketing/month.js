module.exports = {
  
  fromDate: function(date) {
    return date.getUTCFullYear().toString() + '/' + (date.getUTCMonth() + 1).toString();
  },
  
  fromTimeUUID: function(timeUUID) {
    return module.exports.fromDate(require('../index').getDateFromTimeUUID(timeUUID));
  },
  
  now: function() {
    return module.exports.fromDate(new Date());
  },
  
  nowOffset: function(offset) {
    return module.exports.offset(module.exports.now(), offset);
  },
  
  offset: function(bucket, offset) {
    var parts = bucket.split('/');
    parts[0] = parseInt(parts[0]);
    parts[1] = parseInt(parts[1]);
    if (offset > 0) {
      var offset = offset + parts[1] - 1;
      var years = Math.floor(offset / 12);
      parts[0] += years;
      parts[1] = offset % 12 + 1;
    }
    else if (offset < 0) {
      var offset = offset + parts[1] - 1;
      var years = Math.ceil(offset / -12);
      parts[0] -= years;
      offset = offset % 12;
      parts[1] = offset < 0 ? offset + 13 : offset + 1;
    }
    return parts.join('/');
  }
};