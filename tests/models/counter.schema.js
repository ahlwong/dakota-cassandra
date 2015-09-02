// node modules
var nmDakota = require('../../index');
var nmLogger = require('../../lib/logger');

module.exports = {
  
  // columns
  columns: {
    
    // data
    email: 'text',
    loc: 'text',
    name: 'text',
    num: 'counter'
    
  },
  
  // key
  key: [['email', 'name'], 'loc']
  
};