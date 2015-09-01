# dakota-cassandra
A full feature Apache Cassandra ORM built on top of datastax/nodejs-driver

## Installation

```bash
$ npm install dakota-cassandra
```

## Stability
Dakota was written over a weekend (8/28/2015 - 8/31/2015) by Alexander Wong out of Boost VC in San Mateo, CA, USA to address the lack of a full featured NodeJS compatible Cassandra ORM. It is currently still a work in progress and will be refined in the coming weeks. Please note that this is still a very young library and may have some growing pains to come. Please check back often.

## Features
  - Written on top of datastax/nodejs-driver (official Cassandra JavaScript driver)
  - Based off of Mongoid and Mongoose design and usability patterns
  - Schema backed models
  - Utilizes prepared queries
  - Support for all data types and collection types
  - Support for forEach and stream to process large queries
  - All queries are buffered until a successful connection is established
  - Chainable query interface
  - Changed column tracking
  - Only updates or inserts changed fields
  - Automatic table schema rectification (configurable)
  - Validation and sanitization support with custom messages via Validator
  - Full Keyspace, Table, Type query support for CREATE, DROP, ALTER
  - Callback / Filter support for `afterNew`, `beforeCreate`, `afterCreate`, `beforeValidate`, `afterValidate`, `beforeSave`, `afterSave`, `beforeDelete`

## Missing But Coming
  - Indexes on tables
  - Stream does not buffer queries until a successful connection
  - User Defined Types

## Basic Usage Example

```javascript
var Dakota = require('dakota-cassandra');

var options = {
  connection: {
    contactPoints: [
      '127.0.0.1'
    ],
    keyspace: 'dakota_test'
  },
  keyspace: {
    replication: { 'class': 'SimpleStrategy', 'replication_factor': 1 },
    durableWrites: true
  }
}
var dakota = new Dakota(options);

var schema = new Dakota.Schema(require('./user.schema'));
var validations = new Dakota.Validations(schema, require('./user.validations'));
var User = dakota.addModel('User', schema, validations);

var user = new User({ name: 'Dakota' });
user.species = 'Canine';
user.name = 'Dakota Wong';
user.changes(); // returns { name: { from: 'Dakota', to: 'Dakota Wong' }, species: { from: undefined, to: 'Canine' }}
user.save(function(err) {
  if (err) {
    console.log('Saving failed.');
  }
  else {
    console.log('Saved successfully.');
  }
});

User.where({ name: 'Dakota Wong' }).first(function(err, user) {
  return user;
});

User.forEach(function(n, user) {
  console.log('Non-buffered row result');
}, function(err) {
  console.log('Complete');
});
```

## Connection

```javascript
var options = {
  
  // connection - required
  connection: {
    contactPoints: [
      '127.0.0.1'
    ],
    keyspace: 'dakota_test'
  },
  
  // keyspace - required
  keyspace: {
    replication: { 'class': 'SimpleStrategy', 'replication_factor': 1 },
    durableWrites: true
  },
  
  // model - optional
  model: {
    
  },
  
  // table - optional
  table: {
    
  }
  
};

var dakota = new nmDakota(options);
```
Instantiate a new Dakota object to create a connection to your database.
  - The `connection` object is passed directly to the `datastax/nodejs-driver` `Client` object.
  - `model` and `table` objects are optional. If set, they will be passed as default options to `Model` and `Table` instances.
  - All queries are buffered until a successful connection is established.

## Schema

```javascript
var schemaDefinition = {
  
  // columns
  columns: {
    
    // timestamps
    ctime: 'timestamp',
    utime: 'timestamp',
    
    // data
    id: 'uuid',
    name: 'text',
    email: 'text',
    ip: 'inet',
    age: 'int',
    
    // collections
    fields: { type: { collection: 'set', type: 'uuid' } }
  
  },
  
  // key
  key: [['id', 'name'], 'age'],
  
  // callbacks
  callbacks: {
    
    // new
    afterNew: [
      function(){ console.log('afterNew callback'); },
      
      Dakota.Recipes.Callbacks.setUuid('id'),
      Dakota.Recipes.Callbacks.setTimestampToNow('ctime')
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
      
      Dakota.Recipes.Callbacks.setTimestampToNow('utime')
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
var schema = new Dakota.Schema(schemaDefinition);
```

## Models

## Querying

```javascript
User.select('email', 'name').select(['ctime', 'utime']).where('species', 'dog').where({ name: 'dakota', age: { '$gte' : 5 } }).orderBy('age', '$asc').orderBy({ 'age' : '$desc' }).limit(99).allowFiltering(true).all(function(err, results) {
  if (err) {
    console.log('Query failed.');
  }
  else {
    console.log('Found ' + results.length + ' results');
  }
});
```

## Change Tracking

```javascript
var user = User.first();
user.changed(); // returns false, check if any changes to any columns
user.changes(); // returns {}
user.name = 'Dakota';
user.changed('name'); // return true
user.changes(); // returns { name: { from: 'prev name', to: 'Dakota }}
user.changes('name'); // returns { from: 'prev name', to: 'Dakota }
```

## Validations

```javascript
var validationsDefinition = {
  
  // timestamps
  ctime: {
    validator: Dakota.Recipes.Validators.required
  },
  utime: {
    validator: Dakota.Recipes.Validators.required
  },
  
  // data
  id: {
    validator: Dakota.Recipes.Validators.required
  },
  email: {
    displayName: 'Email',
    validator: Dakota.Recipes.Validators.email,
    sanitizer: Dakota.Recipes.Sanitizers.email
  },
  name: {
    displayName: 'Name',
    validator: [Dakota.Recipes.Validators.required, Dakota.Recipes.Validators.minLength(1)]
  }
};
var validations = new Dakota.Validations(schema, validationsDefinition, {});

var schema = new Dakota.Schema(schemaDefinition);
var User = dakota.addModel('User', schema, validations, {});

var user = new User();
user.email = 'dAkOtA@gmail.com'; // automatically sanitizes input
user.email; // returns 'dakota@gmail.com'
user.validate(); // returns { column: [errorMessage{String}, ...], ... } if validation errors
user.save(function(err) {
  if (err) {
    if (err instanceof Dakota.Model.ValidationFailedError) { // Dakota.Model.ValidationFailedError if validation errors
      var invalidColumns = err.message;
    }
  }
  ...
});
```

## Keyspaces

## Tables

## Collections
Dakota supports all collection types: lists, maps, and sets.

## EachRow and Stream Support
Dakota supports both `.eachRow` and `.stream` options for processing rows from Cassandra.

## Examples
For an in-depth look at using Dakota, take a look inside the `/tests` folder.