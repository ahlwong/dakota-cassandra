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
  - Support for eachRow and stream to process large queries
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
  - Counter support

## Basic Usage Example

```javascript
var Dakota = require('dakota-cassandra');

// create connection

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

// add model

var schema = {
  columns: {
    ctime: 'timestamp',
    utime: 'timestamp',
    name: 'text',
    email: 'text',
    friends: 'list<text>'
  },
  key: ['name'],
  callbacks: {
    beforeSave: [
      Dakota.Recipes.Callbacks.setTimestampToNow('utime')
    ]
  }
};
var validations = {
  ctime: {
    validator: Dakota.Recipes.Validators.required
  },
  utime: {
    validator: Dakota.Recipes.Validators.required
  },
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
var User = dakota.addModel('User', schema, validations);

// create a user

var user = new User({ name: 'Dakota' });
user.name = 'Dakota Cassandra';
user.changes(); // returns { name: { from: 'Dakota', to: 'Dakota Cassandra' } }
user.save(function(err) {
  ...
});

// querying

User.all(function(err, users) {
  
});

User.where({ name: 'Dakota Cassandra' }).first(function(err, user) {
  ...
});

User.eachRow(function(n, user) {
  ...
}, function(err) {
  ...
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

var dakota = new Dakota(options);
```
Instantiate a new Dakota object to create a connection to your database.
  - The `connection` object is passed directly to the `datastax/nodejs-driver` `Client` object.
  - `model` and `table` objects are optional. If set, they will be passed as default options to `Model` and `Table` instances.
  - All queries are buffered until a successful connection is established.

## Schema

```javascript
var schema = {
  
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
    fields: 'set<uuid>',
    map: 'map<text,timeuuid>'
    
  },
  
  // key
  key: ['name', 'age'],
  
  // callbacks
  callbacks: {
    
    // new
    afterNew: [
      function() { console.log('after new callback'); }
    ],
    
    // create
    beforeCreate: [
      ...
    ],
    afterCreate: [
      ...
    ],
    
    // validate
    beforeValidate: [
      ...
    ],
    afterValidate: [
      ...
    ],
    
    // save
    beforeSave: [
      ...
    ],
    afterSave: [
      ...
    ],
    
    // delete
    beforeDelete: [
      ...
    ]
  }
};
```

## Callbacks

```javascript
var schema = {
  
  ...
  
  // callbacks
  callbacks: {
    
    // new
    afterNew: [
      function() { console.log('after new callback'); }
    ],
    
    // create
    beforeCreate: [
      ...
    ],
    afterCreate: [
      ...
    ],
    
    // validate
    beforeValidate: [
      ...
    ],
    afterValidate: [
      ...
    ],
    
    // save
    beforeSave: [
      ...
    ],
    afterSave: [
      ...
    ],
    
    // delete
    beforeDelete: [
      ...
    ]
  }
};
```

## Models

## Querying

```javascript
User.select(['email', 'utime']).where({ name: 'Dakota Cassandra', age: { '$gte' : 5 } }).orderBy('age', '$asc').limit(1).allowFiltering(true).all(function(err, results) {
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
var validations = {
  
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

var User = dakota.addModel('User', schema, validations);

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

## User Defined Types
Dakota supports user defined types.

## Examples
For an in-depth look at using Dakota, take a look inside the `/tests` folder.