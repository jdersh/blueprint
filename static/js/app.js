angular.module('blueprint', ['ngResource', 'ngRoute'])
  .factory('Event', function($resource) {
    return $resource(
      '/event/:scope', null,
      {all: {url: '/events/all', method: 'GET', isArray: true},
       published: {url: '/events/published', method: 'GET'},
       history: {url: '/events/:scope/history', method: 'GET'}}
    );
  })
  .factory('Schema', function($resource) {
    return $resource(
      '/schemas', null,
      {all: {method: 'GET', isArray: true},
       get: {url: '/schema/:scope', method:'GET', isArray: true},
       create: {url: '/schema/:event', method: 'POST', isArray: true},
       update: {url: '/schema/:event', method: 'POST', isArray: true}
      }
    );
  })
  .factory('Types', function($resource) {
    return $resource(
      '/types', null, null
    );
  })
  .factory('Suggestions', function($resource) {
    return $resource(
      '/suggestions', null,
      {all: {method: 'GET', isArray: true},
       get: {url: '/suggestion/:scope.json', method:'GET',
             interceptor: {responseError: function(response) {return false;}}}
       }
    );
  })
  .factory('ColumnMaker', function() {
    return {
      make: function() {
      return {
        InboundName: '',
        OutboundName: '',
        Transformer: 'varchar',
        size: 255,
        ColumnCreationOptions: ''
        };
      },
      validate: function(column) {
        if (!column.InboundName || !column.OutboundName || !column.Transformer) {
          return false;
        } else if (column.Transformer == 'varchar' && !(column.size > 0 && column.size <= 65535)) {
          return false;
        }
        return true;
      }
    }
  })
  .config(function($routeProvider) {
    $routeProvider
      .when('/events/all', {
        controller: 'EventListCtrl',
        templateUrl: 'template/event/list.html'
      })
      .when('/event/:scope', {
        controller: 'EventCtrl',
        templateUrl: 'template/event/show.html'
      })
      .when('/schemas', {
        controller: 'SchemaListCtrl',
        templateUrl: 'template/schema/list.html'
      })
      .when('/schema', {
        controller: 'SchemaCreateCtrl',
        templateUrl: 'template/schema/create.html'
      })
      .when('/schema/:scope', {
        controller: 'SchemaShowCtrl',
        templateUrl: 'template/schema/show.html'
      })
      .otherwise({
        redirectTo: '/schemas'
      });
  })
  .controller('HeaderCtrl', function($scope, store) {
    $scope.getError = store.getError;
    $scope.clearError = store.clearError;
    $scope.getMessage = store.getMessage;
    $scope.clearMessage = store.clearMessage;
  })
  .controller('EventListCtrl', function($scope, Event) {
    Event.all(function(data) {
      $scope.events = data;
    });
  })
  .controller('EventCtrl', function($scope, $routeParams, $location, $q, store, Schema, Event) {
    var event, schema, types;
    var eventData = Event.get($routeParams, function(data) {
      if (data[0]) {
        event = data[0];
        angular.forEach(event.properties, function(prop) {
          if (prop.freq > 60 && prop.name !== 'token') {
            prop.publish = true;
          }
        });
      }
    }).$promise;

    var schemaData = Schema.get($routeParams, function(data) {
      if (data[0]) {
        schema = data[0];
      }
    }).$promise;

    $q.all([eventData, schemaData]).then(function() {
      // ordering is important here; we want to send you to the
      // schema page if it exists, otherwise to the event page to
      // begin creating the schema
      // TODO: angular tests so that things like this don't need comments!
      if (schema) {
        $location.path('/schema/' + schema.eventname);
      } else if (event) {
        $scope.event = event;
        $scope.showCreateSchema = function() {
          store.setEvent($scope.event);
          $location.path('/schema/create');
        };
      } else {
        store.setError('No event or schema by this name', '/');
      }
    });
  })
  .controller('SchemaShowCtrl', function ($scope, $location, $route, $routeParams, $q, store, Schema, Types, ColumnMaker) {
    var types, schema;
    var typeRequest = Types.get(function(data) {
      if (data) {
        types = data.result;
      } else {
        store.setError('Failed to fetch type information', undefined)
        types = [];
      }
    }).$promise;

    var schemaRequest = Schema.get($routeParams, function(data) {
      if (data) {
        schema = data[0];
      }
    }, function(err) {
      var msg;
      if (err.data) {
        msg = 'API Error: ' + err.data;
      } else {
        msg = 'Schema not found or threw an error';
      }
      store.setError(msg, '/schemas');
    }).$promise;

    $q.all([typeRequest, schemaRequest]).then(function() {
      if (!schema || !types) {
        store.setError('API Error', '/schemas');
      }
      $scope.schema = schema;
      $scope.additions = {Columns: [], EventName: schema.EventName}; // Used to hold new columns
      $scope.types = types;
      $scope.newCol = ColumnMaker.make();
      $scope.addColumnToSchema = function(column) {
        if (!ColumnMaker.validate(column)) {
          store.setError("New column is invalid", undefined);
          return false
        }
        store.clearError();
        if (column.Transformer === 'varchar') {
          if (parseInt(column.size)) {
            column.ColumnCreationOptions = '(' + parseInt(column.size) + ')';
          } else {
            store.setError("New column is invalid (needs nonempty value)", undefined);
            return false;
          }
        }
        if (column.Transformer === 'int') {
            column.Transformer = 'bigint';
        }

        // Update the view, but we only submit $scope.additions
        $scope.additions.Columns.push(column);
        $scope.newCol = ColumnMaker.make();
        document.getElementById('newInboundName').focus()
      };
      $scope.dropColumnFromAdditions = function(colInd) {
        $scope.additions.Columns.splice(colInd, 1);
      };
      $scope.updateSchema = function() {
        var additions = $scope.additions;
        if (additions.Columns.length < 1) {
          store.setError("No new columns, so no action taken.", undefined);
          return false;
        }
        var migration = {
          TableOperation:   'update',
          Name:             additions.EventName,
          ColumnOperations: [],
          TableOption: $scope.schema.TableOption
        }

        angular.forEach(additions.Columns, function(item) {
          var columnOperation = {
            Operation:           'add',
            InboundName:         item.InboundName,
            OutboundName:        item.OutboundName,
            NewColumnDefinition: item
          }
          migration.ColumnOperations.push(columnOperation);
        })

        Schema.update({event: additions.EventName, version: $scope.schema.Version}, migration, function() {
          $route.reload();
          store.setMessage("Succesfully updated schema: " +  additions.EventName);
        }, function(err) {
          store.setError(err, undefined);
        });
      };
    });
  })
  .controller('SchemaListCtrl', function($scope, $location, $http, Schema, Suggestions, store) {
    $scope.ingestTable = function(schema){
      schema.IngestStatus = 'flushing';
    $http.post("/ingest", {Table:schema.EventName}, {timeout: 7000}).success(function(data, status){
      schema.IngestStatus = 'flushed';
    }).error(function(data,status){
      schema.IngestStatus = 'failed';
    });
    }
    Schema.all(function(data) {
      $scope.schemas = data;
      var existingSchemas = {};
      angular.forEach($scope.schemas, function(s) {
        existingSchemas[s.EventName] = true;
        s.IngestStatus = 'default';
      });

      Suggestions.all(function(data) {
        $scope.suggestions = [];
        angular.forEach(data, function(s) {
          if (!existingSchemas[s.EventName]) {
            $scope.suggestions.push(s);
          }
        });
      });
    });
  })
  .controller('SchemaCreateCtrl', function($scope, $location, $q, $routeParams, store, Schema, Types, Suggestions, ColumnMaker) {
    var types, suggestions, suggestionData;
    var typeData = Types.get(function(data) {
      if (data) {
        types = data.result;
      } else {
        store.setError('Failed to fetch type information', undefined)
        types = [];
      }
    }).$promise;

    if ($routeParams['scope']) {
      suggestionData = Suggestions.get($routeParams, function(data) {
        if (data) {
          suggestions = data;
        }
      }).$promise;
    } else {
      var deferScratch = $q.defer();
      deferScratch.resolve();
      suggestionData = deferScratch.promise;
    }

    var rewriteColumns = function(cols) {
      var rewrites = [
        {"Name": "channel", "Change": [["size", 25]]  },
        {"Name": "device_id", "Change": [["size", 32]]  },
        {"Name": "url", "Change": [["size", 255]]},
        {"Name": "referrer_url", "Change": [["size", 255]]},
        {"Name": "domain", "Change": [["size", 255]]},
        {"Name": "host", "Change": [["size", 127]]},
        {"Name": "referrer_domain", "Change": [["size", 255]]},
        {"Name": "referrer_host", "Change": [["size", 127]]},
        {"Name": "received_language", "Change": [["size", 8]]},
        {"Name": "preferred_language", "Change": [["size", 8]]},
      ];

      var deletes = [
        "token",
      ];

      angular.forEach(rewrites, function (rule) {
        angular.forEach(cols, function(col) {
          if (col.InboundName == rule.Name) {
            angular.forEach(rule.Change, function(change) {
              col[change[0]] = change[1];
            })
          }
        });
      });

      angular.forEach(deletes, function (d) {
        for (i=0; i<cols.length; i++) {
          if (cols[i].InboundName == d) {
            cols.splice(i, 1);
            break;
          }
        }
      });
    };

    $q.all([typeData, suggestionData]).then(function() {
      var event = {distkey:''};
      var defaultColumns = [{
          InboundName: 'time',
          OutboundName: 'time',
          Transformer: 'f@timestamp@unix',
          ColumnCreationOptions: ' sortkey'
        },{
          InboundName: 'ip',
          OutboundName: 'ip',
          Transformer: 'varchar',
          size: 15,
          ColumnCreationOptions: ''
        },{
          InboundName: 'ip',
          OutboundName: 'city',
          Transformer: 'ipCity',
          ColumnCreationOptions: ''
        },{
          InboundName: 'ip',
          OutboundName: 'country',
          Transformer: 'ipCountry',
          ColumnCreationOptions: ''
        },{
          InboundName: 'ip',
          OutboundName: 'region',
          Transformer: 'ipRegion',
          ColumnCreationOptions: ''
        },{
          InboundName: 'ip',
          OutboundName: 'asn_id',
          Transformer: 'ipAsnInteger',
          ColumnCreationOptions: ''
        }];
      // this is icky, it is tightly coupled to what spade is
      // looking for. It would be good to have an intermediate
      // representation which BluePrint converts to what spade cares
      // about but for the timebeing this is the quickest solution
      if (!suggestions) {
        event.Columns = defaultColumns;
      } else {
        event = suggestions;
        event.Columns.sort(function(a, b) {return b.OccurrenceProbability - a.OccurrenceProbability});

        for (i=0; i<event.Columns.length; i++) {
          if (event.Columns[i].InboundName == 'time') {
            event.Columns.splice(i, 1);
            break;
          }
        }

        var re = /\((\d+)\)/
        angular.forEach(event.Columns, function(col) {
          if (col.Transformer == 'varchar') {
            var match = re.exec(col.ColumnCreationOptions);
            if (match) {
              col.size = parseInt(match[1]);
            }
          }
          if (col.InboundName == 'device_id') {
            event.distkey = 'device_id';
          }
        });

        event.Columns = defaultColumns.concat(event.Columns);
        rewriteColumns(event.Columns);
      }

      $scope.event = event;
      $scope.types = types;
      $scope.newCol = ColumnMaker.make();
      $scope.addColumnToSchema = function(column) {
        if (!ColumnMaker.validate(column)) {
          store.setError("New column is invalid", undefined);
          return false;
        }
        store.clearError();
        $scope.event.Columns.push(column);
        $scope.newCol = ColumnMaker.make();
        document.getElementById('newInboundName').focus();
      };
      $scope.dropColumnFromSchema = function(columnInd) {
        $scope.event.Columns.splice(columnInd, 1);
      }
      var makeTableCreateMigration = function(event) {
        store.clearError();
        var migration = {
          TableOperation:   'add',
          Name:             event.EventName,
          ColumnOperations: [],
          TableOption: {
            DistKey: [event.distkey],
            SortKey: [],
          }
        }

        angular.forEach(event.Columns, function(item) {
          if (!ColumnMaker.validate(item)) {
            store.setError("At least one column is invalid; look at '" + item.InboundName + "'", undefined);
            return false;
          }
          var columnDefinition = angular.copy(item);
          
          columnDefinition.ColumnCreationOptions = '';
          if (item.Transformer === 'varchar') {
            columnDefinition.ColumnCreationOptions += '(' + item.size + ')';
          }
          if (item.Transformer === 'int') {
            columnDefinition.Transformer = 'bigint';
          }
          if (item.OutboundName === 'time') {
            migration.TableOption.SortKey.push(item.OutboundName)
          }

          var columnOperation = {
            Operation:           'add',
            InboundName:         columnDefinition.InboundName,
            OutboundName:        columnDefinition.OutboundName,
            NewColumnDefinition: columnDefinition
          }

          migration.ColumnOperations.push(columnOperation)
        });

        if (store.getError()) {
          return;
        }
        return migration;
      }

      $scope.createSchema = function() {
        
        var migration = makeTableCreateMigration($scope.event);
        if (migration == null) return;
        Schema.create({event: $scope.event.EventName, version: 0}, 
                    migration, 
                    function() {
                      store.setMessage("Succesfully created schema: " + $scope.event.EventName)
                      $location.path('/schema/' + $scope.event.EventName);
                    }, 
                    function(err) {
                      var msg;
                      if (err.data) {
                        msg = err.data;
                      } else {
                        msg = 'Error creating schema:' + err;
                      }
                      store.setError(msg, '/schemas');
                      return;
                    });
      };
    });
  })
  .service('store', function($location) {
    var data = {
      event: undefined,
      message: undefined,
      error: undefined
    };

    return {
      setEvent: function(ev) {
        data.event = ev;
      },

      getEvent: function() {
        return data.event;
      },

      setError: function(err, path) {
        data.error = err;
        if (path) {
          $location.path(path);
        }
      },

      getError: function() {
        return data.error;
      },

      clearError: function() {
        data.error = undefined;
      },

      setMessage: function(msg) {
        data.message = msg;
      },

      getMessage: function() {
        return data.message;
      },

      clearMessage: function() {
        data.message = undefined;
      }
    };
  });
