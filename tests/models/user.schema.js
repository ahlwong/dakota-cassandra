// node modules
var nmDakota = require('../../index');
var nmLogger = require('../../lib/logger');

module.exports = {
  
  // columns
  columns: {
    
    // timestamps
    ctime: 'timestamp',
    utime: 'timestamp',
    
    // data
    id: 'uuid',
    bio: 'text',
    email: 'text',
    loc: 'text',
    name: 'text',
    
    exclamation: {
      type: 'text',
      set: function(value) { return value + '!'; },
      get: function(value) { return value.toUpperCase() }
    },
    
    address: 'address',
    addresses: 'list<frozen address>',
    
    // types
    desc: 'ascii',
    cnt: 'bigint',
    bits: 'blob',
    sub: 'boolean',
    // num: 'counter',
    wht: 'decimal',
    prc: 'double',
    qty: 'float',
    ip: 'inet',
    age: 'int',
    slug: 'text',
    sgn: 'timestamp',
    tid: 'timeuuid',
    aid: 'uuid',
    url: 'varchar',
    del: 'varint',
    
    // collections
    projs: { type: 'set<timeuuid>' },
    hash: { type: 'map<text,inet>' },
    thngs: { type: 'list<text>' }
  
  },
  
  // key
  key: [['id', 'name'], 'loc'],
  
  // callbacks
  callbacks: {
    
    // new
    afterNew: [
      function(){ nmLogger.debug('afterNew callback'); },
      nmDakota.Recipes.Callbacks.setUuid('id'),
      nmDakota.Recipes.Callbacks.setTimestampToNow('ctime')
    ],
    
    // create
    beforeCreate: [
      function(){ nmLogger.debug('beforeCreate callback'); }
    ],
    afterCreate: [
      function(){ nmLogger.debug('afterCreate callback'); }
    ],
    
    // validate
    beforeValidate: [
      function(){ nmLogger.debug('beforeValidate callback'); },
      nmDakota.Recipes.Callbacks.setTimestampToNow('utime')
    ],
    afterValidate: [
      function(){ nmLogger.debug('afterValidate callback'); }
    ],
    
    // save
    beforeSave: [
      function(){ nmLogger.debug('beforeSave callback'); }
    ],
    afterSave: [
      function(){ nmLogger.debug('afterSave callback'); }
    ],
    
    // delete
    beforeDelete: [
      function(){ nmLogger.debug('beforeDelete callback'); }
    ]
  },
  
  // methods
  methods: {
    greet: function() { console.log('Hello, my name is ' + this.name + '.'); }
  },
  
  // static methods
  static_methods: {
    greet: function() {
      this.eachRow(function(n, user) {
        user.greet()
      }, function(err) {
        if (err) {
          throw err;
        }
      });
    }
  },
  
  // options
  options: {
    
  }
  
};