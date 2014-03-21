/*jslint
  node: true
  indent: 2
  regexp: true
  stupid: true
*/

"use strict";

var mysql = require('mysql'),
  config = require('config'),
  fs = require('fs'),
  is = require('is'),
  i = 0,
  pool;

function noop() {}

function db(sql, param, callback) {
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

  pool.getConnection(function (err, connection) {
    var concurrency,
      max;

    if (err) {
      throw err;
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

          if (is.func(callback.end)) {
            callback.end();
          }
        });

      return;
    }

    connection.query(sql, param, function (err, data) {
      connection.release();

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
    queryObject = {
      "query": db
    };

  file = fs.readFileSync(file, 'ascii');
  match = sqlRe.exec(file);

  while (match) {
    queryObject[match[1]] = db.bind(null, match[2].trim());
    match = sqlRe.exec(file);
  }

  return queryObject;
};

module.exports = db;