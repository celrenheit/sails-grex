/**
 * Module Dependencies
 */
// ...
// e.g.
// var _ = require('lodash');
// var mysql = require('node-mysql');
// ...


var grex = require('grex');
var _ = require('lodash');


/**
 * waterline-sails-grex
 *
 * Most of the methods below are optional.
 *
 * If you don't need / can't get to every method, just implement
 * what you have time for.  The other methods will only fail if
 * you try to call them!
 *
 * For many adapters, this file is all you need.  For very complex adapters, you may need more flexiblity.
 * In any case, it's probably a good idea to start with one file and refactor only if necessary.
 * If you do go that route, it's conventional in Node to create a `./lib` directory for your private submodules
 * and load them at the top of the file with other dependencies.  e.g. var update = `require('./lib/update')`;
 */
module.exports = (function () {


  // You'll want to maintain a reference to each connection
  // that gets registered with this adapter.
  var connections = {};



  // You may also want to store additional, private data
  // per-connection (esp. if your data store uses persistent
  // connections).
  //
  // Keep in mind that models can be configured to use different databases
  // within the same app, at the same time.
  //
  // i.e. if you're writing a MariaDB adapter, you should be aware that one
  // model might be configured as `host="localhost"` and another might be using
  // `host="foo.com"` at the same time.  Same thing goes for user, database,
  // password, or any other config.
  //
  // You don't have to support this feature right off the bat in your
  // adapter, but it ought to get done eventually.
  //

  var adapter = {

    identity: 'sails-grex',
    // Set to true if this adapter supports (or requires) things like data types, validations, keys, etc.
    // If true, the schema for models using this adapter will be automatically synced when the server starts.
    // Not terribly relevant if your data store is not SQL/schemaful.
    //
    // If setting syncable, you should consider the migrate option,
    // which allows you to set how the sync will be performed.
    // It can be overridden globally in an app (config/adapters.js)
    // and on a per-model basis.
    //
    // IMPORTANT:
    // `migrate` is not a production data migration solution!
    // In production, always use `migrate: safe`
    //
    // drop   => Drop schema and data, then recreate it
    // alter  => Drop/add columns as necessary.
    // safe   => Don't change anything (good for production DBs)
    //
    syncable: false,
    pkFormat: 'integer',
    idAttribute: 'id',
    foreignKey: '_id',
    primaryKey: '_id',

    // Default configuration for connections
    defaults: {
			// For example, MySQLAdapter might set its default port and host.
      // port: 3306,
      // host: 'localhost',
      // schema: true,
      // ssl: false,
      // customThings: ['eh']
      autoPK: false,
      attributes: {
        _id: {
          unique:true,
          primaryKey: true
        }
      }
    },



    /**
     *
     * This method runs when a model is initially registered
     * at server-start-time.  This is the only required method.
     *
     * @param  {[type]}   connection [description]
     * @param  {[type]}   collection [description]
     * @param  {Function} cb         [description]
     * @return {[type]}              [description]
     */
    registerConnection: function(connection, collections, cb) {

      if(!connection.identity) return cb(new Error('Connection is missing an identity.'));
      if(connections[connection.identity]) return cb(new Error('Connection is already registered.'));

      // Add in logic here to initialize connection
      // e.g. connections[connection.identity] = new Database(connection, collections);

      grex.connect({
        'host': 'localhost',
        'port': 8182,
        'graph': 'tinkergraph'
      }, function(err, client) {
        //if(err) return cb(Errors.CollectionNotRegistered);
        connections[connection.identity] = client;
        cb();
      });
    },


    /**
     * Fired when a model is unregistered, typically when the server
     * is killed. Useful for tearing-down remaining open connections,
     * etc.
     *
     * @param  {Function} cb [description]
     * @return {[type]}      [description]
     */
    // Teardown a Connection
    teardown: function (conn, cb) {

      if (typeof conn == 'function') {
        cb = conn;
        conn = null;
      }
      if (!conn) {
        connections = {};
        return cb();
      }
      if(!connections[conn]) return cb();
      delete connections[conn];
      cb();
    },


    // Return attributes
    describe: function (connection, collection, cb) {
			// Add in logic here to describe a collection (e.g. DESCRIBE TABLE logic)
      return cb();
    },

    /**
     *
     * REQUIRED method if integrating with a schemaful
     * (SQL-ish) database.
     *
     */
    define: function (connection, collection, definition, cb) {
			// Add in logic here to create a collection (e.g. CREATE TABLE logic)
      return cb();
    },

    /**
     *
     * REQUIRED method if integrating with a schemaful
     * (SQL-ish) database.
     *
     */
    drop: function (connection, collection, relations, cb) {
			// Add in logic here to delete a collection (e.g. DROP TABLE logic)
			return cb();
    },

    /**
     *
     * REQUIRED method if users expect to call Model.find(), Model.findOne(),
     * or related.
     *
     * You should implement this method to respond with an array of instances.
     * Waterline core will take care of supporting all the other different
     * find methods/usages.
     *
     */
    find: function (connection, collection, options, cb) {
      var g = this.getGraph(connection);
      var gremlin = this.getGremlin(connection);
      
      var pipe = this.generatePipeline(connection, collection, options);

      var query = gremlin(pipe);
      query.exec(function(err, response){
        if(err) return cb(err);

        var results = normalizeIds(response.results);
        cb(null, results);

      });
    },

    create: function (connection, collection, values, cb) {
      var g = this.getGraph(connection);
      var gremlin = this.getGremlin(connection);

      var properties = _.merge({
        __type: collection 
      }, values);

      var query = gremlin(g.addVertex(properties));

      query.exec(function(err, response) {
        if(err) return cb(err);
        cb(null, normalizeId(response.results[0]));
      });
    },

    update: function (connection, collection, options, values, cb) {
      var g = this.getGraph(connection);
      var gremlin = this.getGremlin(connection);

      var pipe = this.generatePipeline(connection, collection, options);


      pipe.sideEffect(buildSideEffectProperties(values))
      console.log(pipe);

      var query = gremlin(pipe);
      console.log(pipe.toGroovy());
      query.exec(function(err, response) {
        if(err) return cb(err);
        cb(null, normalizeId(response.results[0]));
      });
    },

    destroy: function (connection, collection, options, cb) {
      var gremlin = this.getGremlin(connection);

      var pipe = this.generatePipeline(connection, collection, options);
      pipe.remove();

      var query = gremlin(pipe);

      query.exec(function(err, response) {
        if(err) return cb(err);
        var results = normalizeIds(response.results);
        cb(null, results);
      });
    },

    getClient: function(client) {
      return grabConnection(client);
    },
    getGraph: function(client) {
      return this.getClient(client).g;
    },
    getGremlin: function(client) {
      return this.getClient(client).gremlin;
    },
    getPipeline: function(client) {
      return this.getClient(client)._;
    },

    generatePipeline: function(connection, collection, options) {
      var g = this.getGraph(connection);
      var pipe;

      if("where" in options && options.where !== null &&  this.idAttribute in options.where) {
        pipe = g.v(options.where[this.idAttribute]);
        delete options.where[this.idAttribute];
      } else {
        pipe = g.V();
      }
      pipe = pipe.has('__type', collection);
      if(options.where !== null) {
        var whereKeys = Object.keys(options.where);
        for (var i = 0; i < whereKeys.length; i++) {
          var whereKey = whereKeys[i];
          var whereValue = options.where[whereKeys[i]];
          pipe.has(whereKey, whereValue);
        }
      }

      return pipe;
    }

    /*

    // Custom methods defined here will be available on all models
    // which are hooked up to this adapter:
    //
    // e.g.:
    //
    foo: function (collectionName, options, cb) {
      return cb(null,"ok");
    },
    bar: function (collectionName, options, cb) {
      if (!options.jello) return cb("Failure!");
      else return cb();
      destroy: function (connection, collection, options, values, cb) {
       return cb();
     }

    // So if you have three models:
    // Tiger, Sparrow, and User
    // 2 of which (Tiger and Sparrow) implement this custom adapter,
    // then you'll be able to access:
    //
    // Tiger.foo(...)
    // Tiger.bar(...)
    // Sparrow.foo(...)
    // Sparrow.bar(...)


    // Example success usage:
    //
    // (notice how the first argument goes away:)
    Tiger.foo({}, function (err, result) {
      if (err) return console.error(err);
      else console.log(result);

      // outputs: ok
    });

    // Example error usage:
    //
    // (notice how the first argument goes away:)
    Sparrow.bar({test: 'yes'}, function (err, result){
      if (err) console.error(err);
      else console.log(result);

      // outputs: Failure!
    })




    */




  };

  /**
   * Grab the connection object for a connection name
   *
   * @param {String} connectionName
   * @return {Object}
   * @api private
   */

  function grabConnection(connectionName) {
    return connections[connectionName];
  }
  
  function normalizeIds(arrayOfValues) {
    if(!arrayOfValues.length) return [];
    for (var i = 0; i < arrayOfValues.length; i++) {
      arrayOfValues[i] = normalizeId(arrayOfValues[i]);
    }
    return arrayOfValues;
  };

  function normalizeId(values) {
    if(!values || !values._id) return {};

    values.id = _.cloneDeep(values._id);
    delete values._id;

      // console.log("beforeval", values);
    // Convert string to Date Objects
    _.forEach(values, function(value, key) {
      if(_.isBoolean(value)) {
        values[key] = !!value;
      } else if(isInt(value)) {
        values[key] = parseInt(value);
      } else if(getType(value) === "float") {
        values[key] = parseFloat(value);
      } else if(!_.isNumber(value) && isDate(value)) {
        values[key] = new Date(Date.parse(value));
      }
    });
      // console.log("afterval", values);
    return values;
  };
  function isInt(x) {
    var y = parseInt(x, 10);
    return !isNaN(y) && x == y && x.toString() == y.toString();
  }
  function isFloat(n) {
    return n === +n && n !== (n|0);
  }
  function getType(input) {
    var m = (/[\d]+(\.[\d]+)?/).exec(input);
    if (m) {
       // Check if there is a decimal place
       if (m[1]) { return 'float'; }
       else { return 'int'; }          
    }
    return 'string';
  };
  function isDate(sDate) {
    var scratch = new Date(sDate);
    if (scratch.toString() == "NaN" || scratch.toString() == "Invalid Date") {
      return false;
    } else {
      return true;
    }
  };
  function buildSideEffectProperties(values) {
    var sideEffect = "";
    _.forEach(values, function(value, key) {
      sideEffect += "it."+key+" = '"+ value +"';";
    });
    sideEffect = "{" + sideEffect + "}";
    return sideEffect;
  }

  // Expose adapter definition
  return adapter;

})();

