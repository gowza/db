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
  pool = mysql.createPool(config.db),
  path = require('path'),
  fs = require('fs'),
  is = require('is'),
  statistics = [];

function noop() {}

function escape(query, inserts) {
  return query.replace(/\?/g, function (match, i) {
    if (inserts.length === 0) {
      return match;
    }

    // If the ? is following a WHERE and the param is an object
    // there are some special use cases
    if (
      /WHERE[\s]*$/.test(query.slice(0, i)) &&
        is.baseObject(inserts[0])
    ) {
      return escape.WHERE(inserts.shift());
    }

    return mysql.escape(inserts.shift());
  });
}

escape.WHERE = function WHERE(paramObj) {
  var sql = '';

  function b(string) {
    return '(' + string + ')';
  }

  function handle(key, value) {
    var isMultiple = is.array(value),
      isNot = (/([!<>])$/).exec(key);

    // Password Syntax:
    // "password!": "Pass1234"
    // "password": ["Pass1234", "Pass12343"]
    // "password": ["Pass1234", "Pass12343"]
    if (/password!?$/.test(key)) {
      sql += 'password ';

      if (isMultiple) {
        sql += isNot ? 'NOT IN' : 'IN';

        sql += b(value
          .map(function (individualValue) {
            return 'SHA1' + b(mysql.escape(individualValue));
          })
          .join(', '));
      } else {
        sql += (isNot ? '!' : '');

        sql += '= SHA1' + b(mysql.escape(value));
      }
    }

    // LIKE Syntax:
    // "col LIKE": "val%"
    // "col LIKE": ["val%", "%ue", "%alu%"]
    // "col NOT LIKE": ["val%", "%ue", "%alu%"]
    if (/ LIKE( |$)/.test(key)) {
      if (isMultiple) {
        sql += b(value.map(function (value) {
          return key + ' ' + mysql.escape(value);
        }).join(' || '));
      } else {
        sql += key + ' ' + mysql.escape(value);
      }
    }

    // || Syntax:
    // "||": {
    //   "key": "val",
    //   "key2": "val2"
    // },
    // "||": [{
    //   "key": "val",
    //   "key2": "val2"
    // }, ...]
    if (key === "||") {
      throw new Error("|| syntax not ready yet");
    }

    if (isMultiple) {
      sql += (isNot ? key.slice(0, -1) : key) + ' ' + (isNot ? 'NOT ' : '') + 'IN' + b(mysql.escape(value));
    }

    sql += (isNot ? key.slice(0, -1) : key) + ' ' + (isNot ? isNot[1] : '') + '= ' + mysql.escape(value);

    sql += ' && ';
  }

  Object.keys(paramObj)
    .forEach(function (key) {
      handle(key, paramObj[key]);
    });

  return sql.slice(0, -4);
};

pool.config.connectionConfig.queryFormat = escape;

function manageQueryStatistic(statObject) {
  var fileContents = statObject.file + ' Statistics\n\n';

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

  Object.keys(statObject)
    .forEach(function (sql) {
      var statistics = statObject[sql];

      if (sql === 'file') {
        return;
      }

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

  fs.writeFileSync(config.dir + '/var/db/' + statObject.file.replace(/\//g, '-') + '.stats', fileContents);
}

function db(sql, param, callback) {
  var statistics;

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

  if (
    this &&
      config.mode === "debug"
  ) {
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
        console.log(sql, param);
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
      "query": db
    };

  function makeQuery(name, sql) {
    if (config.mode === "debug") {
      if (!queryObject.hasOwnProperty('statistics')) {
        queryObject.statistics = {
          "file": file
        };

        statistics.push(queryObject.statistics);
      }

      queryObject.statistics[sql] = {
        "name": name,
        "invocations": []
      };
    }

    queryObject[name] = db.bind(queryObject, sql);
    queryObject[name].sql = sql.trim().replace(/;$/, '');
  }

  contents = fs.readFileSync(file, 'ascii');
  match = sqlRe.exec(contents);

  while (match) {
    makeQuery(match[1], match[2].trim());
    match = sqlRe.exec(contents);
  }

  return queryObject;
};

if (config.mode === "debug") {
  process.on("exit", function () {
    statistics.forEach(manageQueryStatistic);
  });
}

process.on('SIGINT', process.exit);

module.exports = db;