/*jslint
  node: true
  indent: 2
  regexp: true
  stupid: true
*/

"use strict";

// Since this module is loaded as a symlink, it can be 
// best to use the parent's require path
module.paths = module.parent.paths;

var mysql = require('mysql'),
  config = require('config'),
  fs = require('fs'),
  is = require('is'),
  pool;


function noop() {}

function db(sql, param, callback) {
  var statistics;

  if (!pool) {
    pool = mysql.createPool(config.db);
  }

  if (arguments.length === 2) {
    if (is.array(param)) {
      callback = noop;
    }

    if (
      is.func(param) ||
        is.baseObject(param)
    ) {
      callback = param;
      param = [];
    }
  }

  if (this) {
    statistics = {
      "started": Date.now()
    };

    this.statistics[sql].invocations.push(statistics);
  }

  pool.getConnection(function (err, connection) {
    var concurrency,
      max;

    if (err) {
      throw err;
    }

    if (statistics) {
      statistics.gotConnection = Date.now() - statistics.started;
    }

    if (is.baseObject(callback)) {
      callback.row = callback.row || callback.result || noop;
      max = callback.concurrency || Infinity;
      concurrency = 0;

      connection.query(sql, param)
        .on('error', function (e) {
          throw e;
        })
        .on('result', function (row) {
          concurrency += 1;

          callback.row(row, function () {
            concurrency -= 1;

            if (concurrency < max) {
              connection.resume();
            }
          });

          if (concurrency >= max) {
            connection.pause();
          }
        })

        .on('end', function () {
          connection.release();

          if (statistics) {
            statistics.releasedConnection = Date.now() - statistics.started;
            statistics.queryTime = statistics.releasedConnection - statistics.gotConnection;
          }

          if (is.func(callback.end)) {
            callback.end();
          }
        });

      return;
    }

    connection.query(sql, param, function (err, data) {
      connection.release();

      if (statistics) {
        statistics.releasedConnection = Date.now() - statistics.started;
        statistics.queryTime = statistics.releasedConnection - statistics.gotConnection;
      }

      if (err) {
        throw err;
      }

      callback(data);
    });
  });
}

db.load = function (file) {
  var sqlRe = /\/\*([a-zA-Z]+)\*\/([^\/]+)/g,
    match,
    contents,
    queryObject = {
      "query": db,
      "statistics": {}
    };

  function makeQuery(name, sql) {
    queryObject.statistics[sql] = {
      "name": name,
      "invocations": []
    };

    queryObject[name] = db.bind(queryObject, sql);
  }

  contents = fs.readFileSync(file, 'ascii');
  match = sqlRe.exec(contents);

  while (match) {
    makeQuery(match[1], match[2].trim());
    match = sqlRe.exec(contents);
  }

  process.on("exit", function () {
    var fileContents = file + ' Statistics\n\n';

    console.log("printing statistics");

    function avg(arr, key) {
      var sum = 0,
        totalEntires = 0,
        i = arr.length;

      while (i !== 0) {
        i -= 1;

        if (arr[i].hasOwnProperty(key)) {
          totalEntires += 1;
          sum += arr[i][key];
        }
      }

      return sum / totalEntires;
    }

    Object.keys(queryObject.statistics)
      .forEach(function (sql) {
        var statistics = queryObject.statistics[sql];

        fileContents += ' \n' + statistics.name + '\n';

        if (statistics.invocations.length === 0) {
          fileContents += '(N/A)\n';
          return;
        }

        fileContents += 'Invocations: ' + statistics.invocations.length + '\n';
        fileContents += 'Average getConnection: ' + avg(statistics.invocations, 'gotConnection') + '\n';
        fileContents += 'Average releaseConnection: ' + avg(statistics.invocations, 'releasedConnection') + '\n';
        fileContents += 'Average queryTime: ' + avg(statistics.invocations, 'queryTime') + '\n';
      });

    console.log("saving statistics");
    fs.writeFileSync(file + '.stats', fileContents);
  });

  return queryObject;
};

process.on('SIGINT', process.exit);

module.exports = db;