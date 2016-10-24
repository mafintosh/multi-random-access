var AbstractRandomAccess = require('abstract-random-access')
var inherits = require('inherits')
var sorted = require('sorted-array-functions')

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
    if (this._last.start <= offset && this._last.end > offset) return this._last
  }

  var i = sorted.lte(this.stores, {start: offset}, cmp)
  if (i === -1) return null

  var next = this.stores[i]
  if (next.start <= offset && next.end > offset) {
    this._last = next
    return next
  }

  return null
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
      var removed = self.stores.splice(Math.floor(Math.random() * self.stores.length), 1)[0]
      if (removed === this._last) this._last = null
      removed.storage.close(done)
    } else {
      sorted.add(self.stores, match, cmp)
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

function cmp (a, b) {
  return a.start - b.start
}

function noop () {}
