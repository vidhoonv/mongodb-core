"use strict";

var f = require('util').format;

exports['Should iterate cursor'] = {
  metadata: {
    requires: {
      topology: ["single", "replicaset", "sharded"]
    }
  },

  test: function(configuration, test) {
    configuration.newTopology(function(err, server) {
      var ns = f("%s.cursor1", configuration.db);
      // Add event listeners
      server.on('connect', function(_server) {
        // Execute the write
        _server.insert(ns, [{a:1}, {a:2}, {a:3}], {
          writeConcern: {w:1}, ordered:true
        }, function(err, results) {
          test.equal(null, err);
          test.equal(3, results.result.n);

          // Execute find
          var cursor = _server.cursor(ns, {
              find: ns
            , query: {}
            , batchSize: 2
          });

          // Execute next
          cursor.next(function(err, d) {
            test.equal(null, err);
            test.equal(1, d.a);
            test.equal(1, cursor.bufferedCount());

            // Kill the cursor
            cursor.next(function(err, d) {
              test.equal(null, err);
              test.equal(2, d.a);
              test.equal(0, cursor.bufferedCount());
              // Destroy the server connection        
              _server.destroy();
              // Finish the test
              test.done();
            });
          });
        });
      })

      // Start connection
      server.connect();
    });
  }
}

exports['Should iterate cursor but readBuffered'] = {
  metadata: {
    requires: {
      topology: ["single"]
    }
  },

  test: function(configuration, test) {
    configuration.newTopology(function(err, server) {
      var ns = f("%s.cursor2", configuration.db);
      // Add event listeners
      server.on('connect', function(_server) {
        // Execute the write
        _server.insert(ns, [{a:1}, {a:2}, {a:3}, {a:4}, {a:5}], {
          writeConcern: {w:1}, ordered:true
        }, function(err, results) {
          test.equal(null, err);
          test.equal(5, results.result.n);

          // Execute find
          var cursor = _server.cursor(ns, {
              find: ns
            , query: {}
            , batchSize: 5
          });

          // Execute next
          cursor.next(function(err, d) {
            test.equal(null, err);
            test.equal(1, d.a);
            test.equal(4, cursor.bufferedCount());

            // Read the buffered Count
            var items = cursor.readBufferedDocuments(cursor.bufferedCount());

            // Get the next item
            cursor.next(function(err, d) {
              test.equal(null, err);
              test.equal(null, d);

              // Destroy the server connection        
              _server.destroy();
              // Finish the test
              test.done();
            });
          });
        });
      })

      // Start connection
      server.connect();
    });
  }
}

exports['Should callback exhausted cursor with error'] = {
  metadata: {
    requires: {
      topology: ["single"]
    }
  },

  test: function(configuration, test) {
    configuration.newTopology(function(err, server) {
      var ns = f("%s.cursor3", configuration.db);
      // Add event listeners
      server.on('connect', function(_server) {
        // Execute the write
        _server.insert(ns, [{a:1}], {
          writeConcern: {w:1}, ordered:true
        }, function(err, results) {
          test.equal(null, err);
          test.equal(1, results.result.n);

          // Execute find
          var cursor =
            _server.cursor(ns, { find: ns, query: {}, batchSize: 5 });

          // Execute next
          cursor.next(function(err, d) {
            test.equal(null, err);
            test.equal(1, d.a);

            // Get the next item
            cursor.next(function(err, d) {
              test.equal(null, err);
              test.equal(null, d);

              cursor.next(function(err, d) {
                test.ok(err);
                test.equal(null, d);
                // Destroy the server connection        
                _server.destroy();
                // Finish the test
                test.done();
              });
            });
          });
        });
      })

      // Start connection
      server.connect();
    });
  }
};

exports['Should correctly iterate using secondary'] = {
  metadata: {
    requires: {
      topology: ["single", "replicaset"]
    }
  },

  test: function(configuration, test) {
    var ReadPreference = configuration.require.ReadPreference;

    configuration.newTopology(function(err, server) {
      var ns = f("%s.cursor10", configuration.db);
      // Add event listeners
      server.on('fullsetup', function(_server) {
        // Execute the write
        _server.insert(ns, [{a:1}, {a:2}, {a:3}, {a:4}, {a:5}], {
          writeConcern: {w:'majority'}, ordered:true
        }, function(err, results) {
          test.equal(null, err);
          test.equal(5, results.result.n);
          // Execute find
          var cursor =
            _server.cursor(ns, 
              { find: ns, query: {}, batchSize: 2 }, 
              { readPreference: ReadPreference.secondary });

          // Execute next
          cursor.next(function(err, d) {
            test.equal(null, err);
            test.equal(1, d.a);

            // Get the next item
            cursor.next(function(err, d) {
              test.equal(null, err);
              test.equal(2, d.a);

              cursor.next(function(err, d) {
                test.equal(null, err);
                test.equal(3, d.a);
                // Destroy the server connection        
                _server.destroy();
                // Finish the test
                test.done();
              });
            });
          });
        });
      })

      // Start connection
      server.connect();
    });
  }
};

exports['Should correctly iterate using secondary with sharding'] = {
  metadata: {
    requires: {
      topology: ["sharded"]
    }
  },

  test: function(configuration, test) {
          console.log("-------------------------------------------- -1")
    var ReadPreference = configuration.require.ReadPreference;

    configuration.newTopology(function(err, server) {
      var ns = f("%s.cursor10", configuration.db);
      // Add event listeners
      server.on('connect', function(_server) {
        // Execute the write
        _server.insert(ns, [{a:1}, {a:2}, {a:3}, {a:4}, {a:5}], {
          writeConcern: {w:'majority'}, ordered:true
        }, function(err, results) {
          test.equal(null, err);
          test.equal(5, results.result.n);
          console.log("-------------------------------------------- 0")
          // Execute find
          var cursor =
            _server.cursor(ns, 
              { find: ns, query: {}, batchSize: 2 }, 
              { readPreference: ReadPreference.secondary });

          // Execute next
          cursor.next(function(err, d) {
          console.log("-------------------------------------------- 1")
          console.dir(err)
          console.dir(d)
            test.equal(null, err);
            test.equal(1, d.a);

            // Get the next item
            cursor.next(function(err, d) {
          console.log("-------------------------------------------- 2")
          console.dir(err)
          console.dir(d)
              test.equal(null, err);
              test.equal(2, d.a);

              cursor.next(function(err, d) {
          console.log("-------------------------------------------- 3")
          console.dir(err)
          console.dir(d)
                test.equal(null, err);
                test.equal(3, d.a);
                // Destroy the server connection        
                _server.destroy();
                // Finish the test
                test.done();
              });
            });
          });
        });
      })

      // Start connection
      server.connect();
    });
  }
};
