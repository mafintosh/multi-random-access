var multiRandom = require('./')
var fs = require('fs')
var randomFile = require('random-access-file')
var randomTest = require('random-access-test')

var opts = {
  dir : '/tmp/multi-random',
  store_size : 1024 * 1024,
  limit : 32
}

var topts = {
  writable : true,
  reopen : true,
  del : true,
  truncate : false,
  size : false,
  content : false
}

randomTest(function (filename, sopts, callback) {
  var length = randomFile(opts.dir + '/' + filename + '.len')
  var open = function (offset, cb) {
    var index = Math.floor(offset / opts.store_size)
    var start = index * opts.store_size
    var end = start + opts.store_size
    cb(null, { start : start, end : end, storage : randomFile(opts.dir + '/' + filename + '-part-' + index) })
  }
  callback(multiRandom(length, open, opts))
}, topts)
