# dakota-cassandra

A full feature Apache Cassandra ORM built on top of datastax/nodejs-driver

## Installation

```bash
$ npm install dakota-cassandra
```

## Stability

Dakota was written over a weekend (8/28/2015 - 8/31/2015) by Alexander Wong out of Boost VC in San Mateo, CA, USA to address the lack of a full featured NodeJS compatible Cassandra ORM. It is currently still a work in progress and will be refined in the coming weeks. Please check back often for updates and bug fixes.

## Basic Usage Example

```javascript
var Dakota = require('dakota-cassandra');
var dakota = new Dakota(options);
var User = dakota.addModel('User', schema, validations);
var user = new User({ name: 'Alex' });
user.save(function(err) { ... });
User.where({ name: 'Alex' }).first(function(err, user) { ... });
```

## Features

  - Solid foundation
    - Written on top of datastax/nodejs-driver (official Cassandra JavaScript driver)
    - Based off of Mongoid and Mongoose design and usability patterns
  - Chainable query building interface and full support for CQL
    - Build queries by chaining methods like `.select`, `.where`, `.limit`, `.all`, and `.first`
    - Complete access to CQL queries through query builder
        - compiles `SELECT` queries with selective columns, `FROM`, `WHERE`, `ORDER BY`, `LIMIT`, and `ALLOW FILTERING` support
        - compiles `UPDATE` queries with `USING`, `SET`, `WHERE`, `IF`, and `IF EXISTS` support
        - compiles `INSERT` queries with `INTO`, `IF NOT EXISTS`, and `USING` support
        - compiles `DELETE` queries with `FROM`, `USING`, `WHERE`, `IF`, and `IF EXISTS` support
        - compiles `TRUNCATE` queries
    - compiles `prepared statements` in all cases
    - support for `eachRow` and `stream` to process large data sets
    - all queries are buffered until a successful connection is established
  - Full support for Cassandra types
    - All basic types (`ascii`, `bigint`, `blob`, `boolean`, `decimal`, `double`, `float`, `inet`, `int`, `text`, `timestamp`, `timeuuid`, `uuid`, `varchar`, `varint`)
    - All collection types (`list`, `set`, `map`)
    - Support for `user defined types`, `counters`, `tuples`, and `frozen` fields
  - Schema validation, and sanitization backed models
    - Define custom setters, getters, instance, and static methods
    - Callback / filter chains on `afterNew`, `beforeCreate`, `afterCreate`, `beforeValidate`, `afterValidate`, `beforeSave`, `afterSave`, `beforeDelete`
    - Define custom sanitizers and validators for fields
        - 'Recipes' for common and chainable validation and sanitization tasks
        - ... examples include: `minLength`, `maxLength`, `required`, `email`, and more ...
        - User definable validation messages that can be output to user
    - Set column values with pre-generated setters and getters
    - `Append`, `prepend`, `add`, `remove`, `increment`, `decrement`, and `inject` convenience methods for working with collection types
  - Changed column tracking
    - Only updates or inserts changed fields
    - Automatically combines multiple `append`, `prepend`, `add`, `remove`, `increment`, `decrement`, `inject` actions if they are additive or composes a single `set` action
  - Automatic `keyspace`, `table`, and `user defined type` schema rectification (configurable)
    - Detects and alerts on differences between schemas and structures
    - Automatically creates structures, adds columns, removes columns, or changes types and replication settings (configurable)
        - Keyspaces have options for `ensure exists`, and `alter` (to alter `replication` and `durableWrites`)
        - Tables have options for `ensure exists`, `recreate`, `recreateColumn`, `removeExtra`, `addMissing`
        - User defined types have options for `ensure exists`, `recreate`, `changeType`, `addMissing`

## Missing But Coming

  - Indexes on tables
  - Stream does not buffer queries until a successful connection

## Connection and Options

#### Minimal Options

```javascript
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
};
var dakota = new Dakota(options);
```
  - `options.connection` is passed directly to the datastax/nodejs-driver `Client` object; you can specify additional fields here as necessary
  - `options.keyspace` is used to configure your app's keyspace
    - If a keyspace with the name in `options.connection.keyspace` doesn't exist, it is automatically created. If it does exist, its schema is compared against the options here (see below for automatic discrepancy resolution).

#### Full Options (Library Defaults)

```javascript
var nm_ = require('underscore');
var nm_i = require('underscore.inflections');
var nm_s = require('underscore.string');

var defaultOptions = {
  
  // connection
  connection: {
    contactPoints: [
      '127.0.0.1'
    ],
    keyspace: 'dakota_test'
  },
  
  // keyspace
  keyspace: {
    replication: { 'class': 'SimpleStrategy', 'replication_factor': 1 },
    durableWrites: true,
    ensureExists: {
      run: true, // check if keyspace exists and automaticcaly create it if it doesn't
      alter: false // alter existing keyspace to match replication or durableWrites
    }
  },
  
  // logger
  logger: {
    level: 'debug', // log this level and higher [debug < info < warn < error]
    queries: true // log queries
  },
  
  // model
  model: {
    tableName: function(modelName) {
      return nm_i.pluralize(nm_s.underscored(modelName));
    },
    getterSetterName: function(columnName) {
      return columnName.trim().replace(/\s/g, '_');
    },
    typeSpecificSetterName: function(operation, columnName) {
      var name = nm_s.capitalize(columnName.trim().replace(/\s/g, '_'));
      if (operation == 'increment' || operation == 'decrement') {
        return operation + name;
      }
      else {
        return operation + nm_i.singularize(name);
      }
    },
    table: {
      ensureExists: {
        run: true, // check if keyspace exists and automaticcaly create it if it doesn't
        recreate: false, // drop and recreate table on schema mismatch, takes precedence over following options
        recreateColumn: false,  // recreate columns where types don't match schema
        removeExtra: false,  // remove extra columns not in schema
        addMissing: false // add columns in schema that aren't in table
      }
    }
  },
  
  // user defined type
  userDefinedType: {
    ensureExists: {
      run: true,
      recreate: false, // drop and recreate type on schema mismatch, takes precedence over following options
      changeType: false, // change field types to match schema
      addMissing: false // add fields in schema that aren't in type
    }
  }
  
};
```
  - `keyspace.ensureExists` dictates the keyspace discrepancy rectification policy
    - `run` - check existence of keyspace, create if missing, and compare schema?
    - `alter` - alter keyspace to match `replication` and `durableWrites` options
  - `logger` determines behavior of built in logger
    - `level` - can be set to 'debug', 'info', 'warn', or 'error' to only display log messages above the specified level
    - `queries` - log compiled query statements and params?
  - `model` dictates table discrepancy handling and general setup
    - `tableName` - function used to convert from model name to table name
      - by default, a model named 'UserByEmail' will create a table named 'user_by_emails'
    - `getterSetterName` - function used to name getters and setters for columns
      - by default, a column names 'email_addresses' will create `.email_addresses` and `.email_addresses =` methods
    - `typeSpecificSetterName` - function used to name getters and setters specific to certain types
      - by default, a column named 'friend_uuids' of type list will create `.appendFriend_uuid`, `.prependFriend_uuid`, `.removeFriend_uuid`, and `.injectFriend_uuid` methods
      - the `operation` argument in this function is passed strings like 'append', 'prepend', etc...
    - `table.ensureExists` specifies the table discrepancy rectification policy
      - `run` - check existence of table, create if missing, and compare schema?
      - `recreate` - drop and recreate table on discrepancy
      - `recreateColumn` - drop and recreate column on type mismatch
      - `removeExtra` - drop columns not in schema
      - `addExtra` - add columns that are defined in the schema but don't exist in the table
  - `userDefinedType.ensureExists` ditactes the UDT discrepancy rectification policy
    - `run` - check existence of UDT, create if missing, and compare schema?
    - `recreate` - drop and recreate UDT on discrepancy
    - `changeType` - attempt to change type on field if type mismatches schema
    - `addMissing` - add fields that are defined in the schema but don't exist in UDT

## Models

```javascript
var User = dakota.addModel('User', require('./user.schema'), require('./user.validations'), options);
```
  - Models are created via the `.addModel` method on `Dakota` instances. When they're added, they're immediately validated and compared against existing tables (see options on configuring `ensureExists` above).
  - The first argument specifies the name of the model; the second is an `Object` containing the model's schema; the third is an `Object` containing sanitizations and validations (`null` can be passed if no validations are necessary); the last is an options `Object` which can be used to override `options.model` passed in to the `new Dakota(options)` constructor.
## Schema
```javascript
var Dakota = require('dakota-cassandra');
var schema = {
  
  // columns
  columns: {
    
    // timestamps
    ctime: 'timestamp',
    utime: 'timestamp',
    
    // data
    id: 'uuid',
    name: 'text',
    email: {
      alias: 'emailAddress',
      type: 'text',
      set: function(value) { return value.toLowerCase(); },
      get: function(value) { return value.toUpperCase(); }
    },
    ip: 'inet',
    age: 'int',
    
    // collections
    friends: 'set<uuid>',
    tags: 'list<text>',
    browsers: 'map<text,inet>',
    craziness: 'list<frozen <tuple<text, int, text>>>'
    
  },
  
  // key
  key: [['email', 'name'], 'id'], // excuse the contrived example
  
  // callbacks
  callbacks: {
    
    // new
    afterNew: [
      function() { console.log('after new callback'); }
    ],
    
    // create
    beforeCreate: [
      Dakota.Recipes.Callbacks.setTimestampToNow('ctime')
    ],
    afterCreate: [],
    
    // validate
    beforeValidate: [
      Dakota.Recipes.Callbacks.setTimestampToNow('utime')
    ],
    afterValidate: [],
    
    // save
    beforeSave: [],
    afterSave: [],
    
    // delete
    beforeDelete: []
  },
  
  // methods
  method: {
    greet: function () { console.log('Hello, my name is ' + this.name + '.'); };
  },
  
  // static methods: {
    plusPlusAge: function () {
      User.eachRow(function(n, user) {
        user.age += 1;
        user.save(function(err) { ... });
      }, function(err) {
        console.log('All users .age++ complete!');
      });
    }
  }
};
```
 - `schema.columns` defines your model's fields and corresponding types
   - an `Object` can be set per field for additional configuration (see `email` above)
     - `alias` specifies the name to use for auto generated methods and arguments to those methods
       - because column names are stored in each record in Cassandra, it is sometimes desirable to have a more user friendly name
       - ... for instance: `fids: { alias: 'FriendIDs', type: set<uuid> }` will create `.friendIDs`, `.friendIDs =`, `.addFriendID`, ... methods
       - aliases are also support mass assignment, for instance: `new User({ FriendIDs: [...], ... })` and `user.set({ FriendIDs: [...], ... })`
     - `type` specifies the type of the field
     - `set` and `get` will be invoked when setting or getting the column value
       - *NOTICE* they both `return` the value
  - `schema.key` defines the model's primary key
    - composite keys should be grouped in a nested array
  - `schema.callbacks` defines chainable callbacks that are run in definition order for particular events
    - `Recipes` for common callbacks are provided in the `/lib/recipes` directory and are loaded under `Dakota.Rescipes.Callbacks`
  - `schema.methods` defines instance methods available on each model instance
  - `schema.staticMethods` defines static methods on the model

## Validations

### Usage

```javascript
var user = new User();

user.email = 'dAkOtA@dAKOta.DAkota'; // automatically sanitizes input
user.email; // returns 'dakota@dakota.dakota'

user.password = 'dak';
user.validate(); // returns { password: ['Password must contain at least one character and one number.', 'Password must be more than 6 characters long', ... ], ... } if validation errors

user.save(function(err) {
  if (err) {
    if (err instanceof Dakota.Model.ValidationFailedError) { // Dakota.Model.ValidationFailedError if validation errors
      var invalidColumns = err.message;
    }
  }
  ...
});

user.validate({ only: ['password'] });
user.validate({ except: [email] });
user.save(..., { validate: { only: [...] } });
user.save(..., { validate: { except: [...] } });
```
 - `sanitizers` are run when a column's value is set
   - in the example above, our sanitizer downcases the user's email address
 - `validators` are run when the `.validate()` methods is explicitly called and on model `.save(...)`
   - if validation errors exist, an `Object` will be produced where the keys correspond to column names and the values are arrays of validation error messages
   - `.validate(...)` returns a validation `Object` immediately on validation fail, and `false` on validation pass
   - `.save(...)` is interrupted on validation errors and a `Dakota.Model.ValidationFailedError` is passed as the `err` argument to the callback
   - both `.validate` and `.save` can take an options object that specify validations to `only` run on some columns or run on all columns `except` some

### Definition

```javascript
var Dakota = require('dakota-cassandra');
var validations = {
  ctime: {
    validator: {
        validator: function(value) {
            return !nmValidator.isNull(value);
        },
        message: function(displayName) { return displayName + ' is required.'; }
    }
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
```
  - validation keys correspond to the column names in the schema definition
  - `.displayName` specifies the name of the column to be displayed to users in validation messages
  - `.validator` is an array of validator definitions or a single definition
    - `.validator.validator` is a a function that must return true or false based on its input value
    - `.validator.message` returns a custom message based on a passed in `displayName`
  - `.sanitizer` is an array of sanitizer functions or a single sanitization function

## Creating and Deleting

```javascript
var User = dakota.addModel('User', schema, validations);
var user = new User({ name: 'Dakota' });
user.save(function(err) { ... });
user = User.new({ name: 'Alex' });
user.save(function(err) { ... });
user = User.create({ name: 'Cindy' }, function(err) { ... });

user.if({ name: 'Dakota' }).save(function(err) { ... });
user.ifNotExists(true).ttl(1000).save(function(err) { ... });
user.using({ '$ttl' : 1000, '$timestamp' : 123456789 }).save(function(err) { ... });
user.timestamp(123456789).save(function(err) { ... });

user.delete(function(err) { ... });
user.ifExists(true).delete(function(err) { ... });

User.deleteAll(function(err) { ... });
User.truncate(function(err) { ... });
```
  - Instances of models can be created 3 different ways
    - `new User([assignments])` and `User.new([assignments])` are functionally identical and create an instance without immediately persisting it to the database
    - `User.create([assignments], callback)` immediately but asynchronously persists the object to the database
  - `.delete(callback)` deletes the model instance's corresponding row in the database
  - `.deleteAll(callback)` and `.truncate(callback)` are identical and remove all rows from a table
  - `.ttl(...)`, `.timestamp(...)`, `.using(...)`, `.ifExists(...)`, `.ifNotExists(...)`, and `.if()` query chains can modify query parameters before `.delete(...)` or `.save(...)` compile and run the query on the database

## Querying

```javascript
User.all(function(err, users) { ... });
User.where({ name: 'Dakota' }).first(function(err, user) { ... });
User.where({ name: 'Dakota' }).limit(1).execute(function(err, user) { ... });
User.count(function(err, count) { ... });
User.select(['email', 'utime']).where({ name: 'Dakota', age: { '$gte' : 5 } }).orderBy('age', '$asc').limit(1).allowFiltering(true).all(function(err, users) { ... });

User.find({ name: 'Dakota', email: 'dakota@dakota.com' }, function(err, users) { ... });
User.findOne({ name: 'Dakota' }, function(err, user) { ... });

User.eachRow(function(n, user) { ... }, function(err) { ... });
User.stream();
```
  - There are a multitude of ways to dynamically query your data using the query builder
  - `.all(callback)`, `.first(callback)`, `.execute(callback)`, and `.count(callback)` methods terminate query building, compile your query, and submit it to the database; they should be used at the end of a query chain to execute the query
  - `.select([column{String}, ...])` and `.select(column{String}, ...)` specifies which columns to return in your results
    - `.select` is additive, meaning `.select('name').select('email')` will return both name and email in resulting rows
  - `.where(column{String}, value)`, `.where({ column{String}: value })`, and `.where({ column{String}: { operation[$eq, $gt, $gte, ...]{String}: value }})` specifies the WHERE conditions in your query
    - `.where` is additive but overrides conditions on the same column, meaning `.where('name', 'Dakota').where({ age: 5}).where('name', 'Dak')` will compiles to `WHERE "name" = 'Dak' AND "age" = 5`
  - `.orderBy({ partitionKey{String}: order[$asc, $desc]{String} })` and `.orderBy(partitionKey{String}, order[$asc, $desc]{String})` order your results by a particular partition key in either `$asc` or `$desc` order
  - `.limit(limit{Integer})` limits the number of rows returned
  - `.allowFiltering(allow{Boolean})` adds the `ALLOW FILTERING` clause to the compiled `SELECT` query
  - `.find([conditions], callback)` and `.findOne([conditions], callback)` are short hand methods for `.where(...).all(...)` and `.where(...).first(...)`
  - `.eachRow(...)` and `.stream()` methods invoke the corresponding Cassandra non-buffering row processing methods

## Setters and Getters

```javascript
User.first(function(err, user) {

user.email = 'dakota@dakota.dakota';
user.email; // returns 'dakota@dakota.dakota'

user.set('name', 'Dakota');
user.get('name'); // returns 'Dakota'
user.get(['name', 'email', ... ]); // returns { name: 'Dakota', email: 'dakota@dakota.dakota' }

// assuming schema items: { type: list<text> }
user.appendItem('item 1');
user.appendItem('item 1');
user.prependItem('item 0');
user.appendItem('item 2');
user.items; // returns ['item 0', 'item 1', 'item 1', 'item 2']
user.removeItem('item 1');
user.items; // returns ['item 0', 'item 2']
user.inject(1, 'item 0');
user.items; // returns ['item 0', 'item 0'];
user.removeItem('item 0');
user.items; // returns null
user.items = ['item 0', 'item 1', 'item 2'];
user.items; // returns ['item 0', 'item 1', 'item 2]

// assuming schema friends: { type: set<text> }
user.addFriend('Bob');
user.addFriend('Bob');
user.addFriend('Joe');
user.friends; // returns ['Bob', 'Joe']
user.removeFriend('Joe');
user.friends; // returns ['Bob']
user.friends = ['Jenny', 'Alex', 'Cathy', 'Cathy'];
user.friends; // returns ['Jenny', 'Alex', 'Cathy']

// assuming schema hosts: { type: map<text,inet> }
user.hosts = { localhost: '127.0.0.1', mask: '255.255.255.255' };
user.injectHost('home', '123.456.789.123');
user.hosts; // returns { home: '123.456.789.123', localhost: '127.0.0.1', mask: '255.255.255.255' }
user.removeHost('mask');
user.hosts; // returns { home: '123.456.789.123', localhost: '127.0.0.1' }

});

UserCounter.first(function(err, userCounter) {

// assuming schema cnt: { type: counter }
userCounter.incrementCnt(5);
userCounter.incrementCnt(3);
userCounter.decrementCnt(7);
userCounter.cnt; // returns 1

});

```
  - Single and multiple compatible calls to collection specific setters will modify collections without setting the whole column value
    - ... for example, `.addFriend('Bob')` will compile into `friends = friends + {'Bob'}`
    - ... likewise, `.addFriend('Bob')` followed by `.addFriend('Joe')` will compile into `friends = friends + {'Bob', 'Joe'}`
    - ... however, `.addFriend('Bob')` followed by `.removeFriend('...')` will compile into `friends = {'Bob', ... }` since `add` and `remove` calls cannot be combined
  - Single and multiple compatible calls to `.remove` on `map` typed columns will generate a `DELETE map1[key1], map2[key3] FROM...` query if performed in isolation
    - ... for example, `.removeHost('mask')` will compile into `DELETE hosts['mask'] FROM users WHERE...`
    - ... likewise, `.removeHost('mask')` followed by `.removeHost('home')` will compile into `DELETE hosts['mask'], hosts['home'] FROM users WHERE...`
    - ... however, `.removeHost('mask')` followed by `.addFriend('Bob')` or `.injectHost('home', '123.456.789.123')` will compile into `hosts = { 'home' : '123.456.789.123' }` since `add` breaks isolation and `inject` cannot be combined
## Change Tracking

```javascript
User.first(function(err, user) {

user.changed(); // returns false, check if any changes to any columns
user.changes(); // returns {}
user.name = 'Dakota';
user.changed('name'); // return true
user.changes(); // returns { name: { from: 'prev name', to: 'Dakota } }
user.changes('name'); // returns { from: 'prev name', to: 'Dakota }

});
```

## User Defined Types

```javascript
var Dakota = require('dakota-cassandra');

var address = {
  street: 'text',
  city: 'text',
  state: 'text',
  zip: 'int',
  phones: 'frozen<set<text>>',
  tenants: 'frozen<map<int,text>>'
};

var userDefinedTypes = {
  address: address
};
var dakota = new Dakota(options, userDefinedTypes);
```
  - User defined types must be passed into the `new Dakota(options, [userDefinedTypes])` constructor because model schemas may depend on their existence
  - The format of the `userDefinedTypes` argument should be an `Object` where each `key` is the `name` of the user defined type you'd like to define
  - The definition of each user defined type should be an `Object` that maps field names to types

## Examples

For an in-depth look at using Dakota, take a look inside the `/tests` folder.