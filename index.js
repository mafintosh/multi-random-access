var AbstractRandomAccess = require('abstract-random-access')
var inherits = require('inherits')

module.exports = Storage

function Storage (open) {
  if (!(this instanceof Storage)) return new Storage(open)
  AbstractRandomAccess.call(this)
  this._openStorage = open
  this._maxOpen = 128
  this._stores = []
}

inherits(Storage, AbstractRandomAccess)

Storage.prototype._write = function (offset, buf, cb) {
  var match = this._get(offset)
  if (!match) return this._openAndWrite(offset, buf, cb)

  var start = offset - match.start
  var max = match.end - match.start

  if (buf.length > max - start) this._writeMulti(offset, buf, max - start, cb)
  else match.storage.write(start, buf, cb)
}

Storage.prototype._writeMulti = function (offset, buf, next, cb) {
  var self = this

  this.write(offset, buf.slice(0, next), function (err) {
    if (err) return cb(err)
    self.write(offset + next, buf.slice(next), cb)
  })
}

Storage.prototype._read = function (offset, length, cb) {
  var match = this._get(offset)
  if (!match) return this._openAndRead(offset, length, cb)

  var start = offset - match.start
  var max = match.end - match.start

  if (length > max - start) this._readMulti(offset, length, max - start, cb)
  else match.storage.read(start, length, cb)
}

Storage.prototype._end = function (opts, cb) {
  var self = this

  function next (i) {
    if (!self._stores[i]) return cb()
    self._stores[i].end(opts, function (err) {
      if (err) return cb(err)
      next(i + 1)
    })
  }

  next(0)
}

Storage.prototype._readMulti = function (offset, length, next, cb) {
  var self = this

  this.read(offset, next, function (err, head) {
    if (err) return cb(err)
    self.read(offset + next, length - next, function (err, tail) {
      if (err) return cb(err)
      cb(null, Buffer.concat([head, tail]))
    })
  })
}

Storage.prototype._get = function (offset) {
  var len = this._stores.length
  var top = len - 1
  var btm = 0

  while (top >= btm && btm >= 0 && top < len) {
    var mid = Math.floor((top + btm) / 2)
    var next = this._stores[mid]

    if (offset < next.start) {
      top = mid - 1
      continue
    }

    if (offset >= next.end) {
      btm = mid + 1
      continue
    }

    return next
  }
}

Storage.prototype._add = function (match) {
  var prev = this._get(match.start)

  if (prev) {
    match.storage.close()
    return
  }

  this._stores.push(match)
  this._stores.sort(compare)
}

Storage.prototype._openAndWrite = function (offset, buf, cb) {
  var self = this
  this._openStorage(offset, function (err, match) {
    if (err) return cb(err)
    self._add(match)
    self.write(offset, buf, cb)
  })
}

Storage.prototype._openAndRead = function (offset, length, cb) {
  var self = this
  this._openStorage(offset, function (err, match) {
    if (err) return cb(err)
    self._add(match)
    self.read(offset, length, cb)
  })
}

function compare (a, b) {
  return a.start - b.start
}
