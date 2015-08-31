// node modules
var nmDakota = require('../index');

// connection
var dakota = require('./connection');

// ========================
// = Create, Select, Drop =
// ========================
var User = require('./models/user');

var results = [];

// User._table.create(function(err, result) {
//   console.log({
//     action: 'CREATE',
//     err: err,
//     result: result
//   });
//   
//   User._table.selectSchema(function(err, result) {
//     console.log({
//       action: 'SELECT SCHEMA',
//       err: err,
//       result: result
//     });
//     
//     User._table.drop(function(err, result) {
//       console.log({
//         action: 'DROP',
//         err: err,
//         result: result
//       });
//     });
//     
//   });
//   
// });

// =================
// = Ensure Exists =
// =================

User._table.create(function(err, result) {
  console.log({
    action: 'CREATE',
    err: err,
    result: result
  });
  
  User._table.ensureExists(function(err, result) {
    console.log({
      action: 'ENSURE EXISTS',
      err: err,
      result: result
    });
  }, { fixMismatch: '$recreate', removeExtra: true, addMissing: true });
});

module.exports = results;