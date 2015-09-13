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
      get: function(value) { return value ? value.toUpperCase() : value; }
    },
    
    // types
    desc: 'ascii',
    cnt: 'bigint',
    bits: 'blob',
    sub: 'boolean',
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
    projs: 'set<timeuuid>',
    hash: 'map<text,inet>',
    thngs: 'list<text>',
    listOfLists: 'list<frozen<list<text>>>',
    setOfSets: 'set<frozen<set<text>>>',
    mapOfMaps: 'map<text,frozen<map<text,int>>>',
    setOfMaps: 'set<frozen<map<text,int>>>',
    
    // user defined types
    address: 'frozen <address>',
    addresses: 'list<frozen <address>>',
    
    // tuples
    tuples: 'tuple<text, int, text>',
    nestedTuple: 'list<frozen <tuple<text, int, text>>>',
    
    // aliases
    wht: { type: 'decimal', alias: 'weight' },
    prc: { type: 'double', alias: 'price' },
    fuuids: { type: 'set<uuid>', alias: 'friendUUIDs' }
    
  },
  
  // key
  key: [['id', 'name'], 'loc'],
  
  // callbacks
  callbacks: {
    
    // new
    afterNew: [
      function(){ nmLogger.debug('afterNew callback'); },
      nmDakota.Recipes.Callbacks.setUuid('id')
    ],
    
    // create
    beforeCreate: [
      function(){ nmLogger.debug('beforeCreate callback'); },
      nmDakota.Recipes.Callbacks.setTimestampToNow('ctime')
    ],
    afterCreate: [
      function(){ nmLogger.debug('afterCreate callback'); }
    ],
    
    // validate
    beforeValidate: [
      function(){ nmLogger.debug('beforeValidate callback'); }
    ],
    afterValidate: [
      function(){ nmLogger.debug('afterValidate callback'); }
    ],
    
    // save
    beforeSave: [
      function(){ nmLogger.debug('beforeSave callback'); },
      nmDakota.Recipes.Callbacks.setTimestampToNow('utime')
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
  staticMethods: {
    greet: function() {
      this.eachRow(function(n, user) {
        user.greet()
      }, function(err) {
        if (err) {
          throw err;
        }
      });
    }
  }
  
};