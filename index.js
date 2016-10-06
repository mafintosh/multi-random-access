var AbstractRandomAccess = require('abstract-random-access')
var inherits = require('inherits')

module.exports = Storage

function Storage (opts, open) {
  if (!(this instanceof Storage)) return new Storage(opts, open)

  if (typeof opts === 'function') {
    open = opts
    opts = null
  }
  if (!opts) opts = {}

  AbstractRandomAccess.call(this)

  this.stores = []
  this.limit = opts.limit || 16

  this._openStorage = open
  this._last = null
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

Storage.prototype._end = function (opts, cb) {
  var self = this

  next(0)

  function next (i) {
    if (!self.stores[i]) return cb()
    self.stores[i].storage.end(opts, function (err) {
      if (err) return cb(err)
      next(i + 1)
    })
  }
}

Storage.prototype._close = function (cb) {
  var self = this

  next(0)

  function next (i) {
    if (!self.stores[i]) return cb()
    self.stores[i].storage.close(function (err) {
      if (err) return cb(err)
      next(i + 1)
    })
  }
}

Storage.prototype._get = function (offset) {
  if (this._last) { // high chance that we'll hit the same at least twice
    if (this._last.start <= offset && this._last.end < offset) return this._last
  }

  var len = this.stores.length
  var top = len - 1
  var btm = 0

  while (top >= btm && btm >= 0 && top < len) {
    var mid = Math.floor((top + btm) / 2)
    var next = this.stores[mid]

    if (offset < next.start) {
      top = mid - 1
      continue
    }

    if (offset >= next.end) {
      btm = mid + 1
      continue
    }

    this._last = next
    return next
  }
}

Storage.prototype.add = function (match, cb) {
  if (!cb) cb = noop

  var self = this
  var prev = this._get(match.start)

  if (prev) {
    match.storage.close()
    return cb()
  }

  done(null)

  function done (err) {
    if (err) return cb(err)
    if (self.stores.length >= self.limit) {
      removeSorted(self.stores, Math.floor(Math.random() * self.stores.length)).storage.close(done)
    } else {
      insertSorted(self.stores, match)
      cb()
    }
  }
}

Storage.prototype._openAndWrite = function (offset, buf, cb) {
  var self = this
  this._openStorage(offset, function (err, match) {
    if (err) return cb(err)
    self.add(match, function (err) {
      if (err) return cb(err)
      self.write(offset, buf, cb)
    })
  })
}

Storage.prototype._openAndRead = function (offset, length, cb) {
  var self = this
  this._openStorage(offset, function (err, match) {
    if (err) return cb(err)
    self.add(match, function (err) {
      if (err) return cb(err)
      self.read(offset, length, cb)
    })
  })
}

function removeSorted (list, i) {
  for (; i < list.length - 1; i++) list[i] = list[i + 1]
  return list.pop()
}

function insertSorted (list, item) {
  var top = list.push(item) - 1
  while (top) {
    if (list[top - 1].start > item.start) {
      list[top] = list[top - 1]
      list[top - 1] = item
      top--
    } else {
      break
    }
  }
}

function noop () {}
