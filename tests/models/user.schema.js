// node modules
var nmDakota = require('../../index');

// ==========
// = Schema =
// ==========
var schemaDefinition = {
  
  // columns
  columns: {
    
    // timestamps
    ctime: 'timestamp',
    utime: 'timestamp',
    
    // data
    id: 'uuid' ,
    bio: 'text',
    email: 'text',
    loc: 'text',
    name: 'text',
    
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
    projs: { type: { collection: 'set', type: 'timeuuid' } },
    hash: { type: { collection: 'map', type: ['text', 'inet'] } },
    thngs: { type: { collection: 'list', type: 'text' } }
  
  },
  
  // key
  key: [['id', 'name'], 'loc'],
  
  // callbacks
  callbacks: {
    
    // new
    afterNew: [
      function(){ console.log('afterNew callback'); },
      nmDakota.Recipes.Callbacks.setUuid('id'),
      nmDakota.Recipes.Callbacks.setTimestampToNow('ctime')
    ],
    
    // create
    beforeCreate: [
      function(){ console.log('beforeCreate callback'); }
    ],
    afterCreate: [
      function(){ console.log('afterCreate callback'); }
    ],
    
    // validate
    beforeValidate: [
      function(){ console.log('beforeValidate callback'); },
      nmDakota.Recipes.Callbacks.setTimestampToNow('utime')
    ],
    afterValidate: [
      function(){ console.log('afterValidate callback'); }
    ],
    
    // save
    beforeSave: [
      function(){ console.log('beforeSave callback'); }
    ],
    afterSave: [
      function(){ console.log('afterSave callback'); }
    ],
    
    // delete
    beforeDelete: [
      function(){ console.log('beforeDelete callback'); }
    ]
  }
};

module.exports = schemaDefinition;