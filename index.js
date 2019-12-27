var Buffer = require('buffer').Buffer
var inherits = require('inherits')
var sorted = require('sorted-array-functions')
var RandomAccess = require('random-access-storage')

module.exports = Storage

function callback (req, err, data) {
  process.nextTick(callbackNT, req, err, data)
}

function callbackNT (req, err, data) {
  req.callback(err, data)
}

function Storage (length, open, opts) {
  if (!(this instanceof Storage)) return new Storage(length, open, opts)
  if (!opts) opts = {}

  RandomAccess.call(this)

  this.stores = []
  this.limit = opts.limit || 16
  this._reads = []
  this._writes = []
  this._deletes = []
  this._closing = []
  this._length = length
  this.length = -1
  this._openStorage = open
  this._last = null
}

inherits(Storage, RandomAccess)

Storage.prototype._writeLength = function (length, cb) {
  var buf = Buffer.from(length.toString(), 'utf8')
  this.length = length
  this._length.write(0, buf, cb)
}

Storage.prototype._write = function (req) {
  var self = this
  var offset = req.offset
  var buf = req.data
  var nextlen = offset + buf.length
  var cb = function (err) {
    if (err) {
      callback(req, err)
    } else if (nextlen > self.length) {
      self._writeLength(nextlen, function (err) {
        callback(req, err)
      })
    } else {
      callback(req, null)
    }
  }

  self._write2(offset, buf, cb)
}

Storage.prototype._write2 = function (offset, buf, cb) {
  var self = this
  self._getOrCreate(offset, function (err, match) {
    if (err) return cb(err)

    var start = offset - match.start
    var max = match.end - match.start

    if (buf.length > max - start) {
      self._writeMulti(offset, buf, max - start, cb)
    } else {
      var write = { start : offset, end : offset + buf.length }
      self._writes.push(write)
      match.storage.write(start, buf, function (err) {
        self._writes.splice(self._writes.indexOf(write), 1)
        cb(err)
      })
    }
  })
}

Storage.prototype._writeMulti = function (offset, buf, next, cb) {
  var self = this
  self._write2(offset, buf.slice(0, next), function (err) {
    if (err) return cb(err)
    self._write2(offset + next, buf.slice(next), cb)
  })
}

Storage.prototype._stat = function (req) {
  if (this.length < 0) {
    req.callback(new Error('not open'))
  } else {
    req.callback(null, { size : this.length })
  }
}

Storage.prototype._del = function (req) {
  var self = this
  var offset = req.offset
  var length = req.size
  var cb = function (err) {
    if (err) {
      callback(req, err)
    } else if (offset + length < self.length) { // copied behavior of random-access-file
      callback(req, null)
    } else {
      self._writeLength(offset, function (err) {
        callback(req, err)
      })
    }
  }

  self._del2(offset, length, cb)
}

Storage.prototype._del2 = function (offset, length, cb) {
  var self = this
  self._getOrCreate(offset, function (err, match) {
    if (err) return cb(err)

    var start = offset - match.start
    var max = match.end - match.start
    var del = { start : offset, end : offset + length }

    if (length > max - start) {
      self._delMulti(offset, length, max - start, cb)
    } else if (start === 0 && length === max) {
      self._deletes.push(del)
      self._destroyMatch(match, function (err) {
        self._deletes.splice(self._deletes.indexOf(del), 1)
        cb(err)
      })
    } else {
      self._deletes.push(del)
      match.storage.del(start, length, function (err) {
        self._deletes.splice(self._deletes.indexOf(del), 1)
        cb(err)
      })
    }
  })
}

Storage.prototype._delMulti = function (offset, length, next, cb) {
  var self = this
  self._del2(offset, length - next, function (err) {
    if (err) return cb(err)
    self._del2(offset + next, length - next, cb)
  })
}

Storage.prototype._destroy = function (req) {
  var self = this
  var cb = function (err) { callback(req, err) }

  loop(null)

  function loop (err) {
    if (err) return cb(err)
    if (!self._closing.length) return self._length.destroy(cb)
    var match = self._closing.shift()
    if (match === self._last) self._last = null
    match.storage.destroy(loop)
  }
}

Storage.prototype._destroyMatch = function (match, cb) {
  var i = this.stores.indexOf(match)
  if (i === -1) return process.nextTick(cb)

  this.stores.splice(i, 1)
  if (match === this._last) this._last = null
  if (match.storage.destroy) return match.storage.destroy(cb)

  match.storage.del(0, match.end - match.start, function (err) {
    if (err) return cb(err)
    match.storage.close(cb)
  })
}

Storage.prototype._read = function (req) {
  var offset = req.offset
  var length = req.size
  var cb = function (err, data) { callback(req, err, data) }
  if (offset + length > this.length) return cb(new Error('read outside of boundary'))
  if (length === 0) return cb(null, Buffer.alloc(0))

  this._read2(offset, length, cb)
}

Storage.prototype._read2 = function (offset, length, cb) {
  var self = this
  self._getOrCreate(offset, function (err, match) {
    if (err) return cb(err)

    var start = offset - match.start
    var max = match.end - match.start

    if (length > max - start) {
      self._readMulti(offset, length, max - start, cb)
    } else {
      var read = { start : offset, end : offset + length }
      self._reads.push(read)
      match.storage.stat(function (err, stat) {
        if (err && err.code !== 'ENOENT') {
          cb(err)
        } else if (err || stat.size <= start) { // not found or not yet written
          self._reads.splice(self._reads.indexOf(read), 1)
          cb(null, Buffer.alloc(length))
        } else if (stat.size < start + length) { // found partially written
          var zeros = Buffer.alloc((start + length) - stat.size)
          match.storage.read(start, stat.size, function (err, buf) {
            self._reads.splice(self._reads.indexOf(read), 1)
            if (err) return cb(err)
            cb(null, Buffer.concat([buf, zeros]))
          })
        } else { // found fully written
          match.storage.read(start, length, function (err, buf) {
            self._reads.splice(self._reads.indexOf(read), 1)
            cb(err, buf)
          })
        }
      })
    }
  })
}

Storage.prototype._readMulti = function (offset, length, next, cb) {
  var self = this
  self._read2(offset, next, function (err, head) {
    if (err) return cb(err)
    self._read2(offset + next, length - next, function (err, tail) {
      if (err) return cb(err)
      cb(null, Buffer.concat([head, tail]))
    })
  })
}

Storage.prototype._close = function (req) {
  var self = this
  var cb = function (err) {
    self.stores = []
    self._reads = []
    self._writes = []
    self._deletes = []
    self.length = -1
    self._last = null

    callback(req, err)
  }

  loop(null)

  function loop (err) {
    if (err) return cb(err)
    if (!self.stores.length) return self._length.close(cb)
    var match = self.stores.shift()
    self._closing.push(match)
    match.storage.close(loop)
  }
}

Storage.prototype._open = function (req) {
  var self = this
  if (self.length > -1) return req.callback(null)

  self._length.stat(function (err, stat) {
    if (err && err.code !== 'ENOENT') {
      req.callback(err)
    } else if (err || stat.size <= 0) {
      self.length = 0
      req.callback(null)
    } else {
      self._length.read(0, stat.size, function (err, buf) {
        if (err) return req.callback(err)
        self.length = parseInt(buf.toString('utf8'))
        req.callback(null)
      })
    }
  })
}

Storage.prototype._getOrCreate = function (offset, cb) {
  var self = this
  if (self._last && !self._last.storage.closed) { // high chance that we'll hit the same at least twice
    if (self._last.start <= offset && self._last.end > offset) return cb(null, self._last)
  }

  var i = sorted.lte(self.stores, {start: offset}, cmp)
  if (i === -1) {
    return self._openStorage(offset, function (err, match) {
      if (err) return cb(err)
      self.add(match, function (err) {
        cb(err, match)
      })
    })
  }

  var next = self.stores[i]
  if (next.start <= offset && next.end > offset && !next.storage.closed) {
    self._last = next
    cb(null, next)
  } else {
    self._openStorage(offset, function (err, match) {
      if (err) return cb(err)
      self.add(match, function (err) {
        cb(err, match)
      })
    })
  }
}

Storage.prototype.add = function (match, cb) {
  if (!cb) cb = noop

  var self = this
  var prev = self._get(match.start)

  if (prev && !prev.storage.closed) {
    return cb(new Error('duplicate add'))
  }

  done(null)

  function within (store) {
    return function (work) {
      return store.start <= work.start && store.end >= work.end
    }
  }

  function done (err) {
    var closable = self.stores.findIndex(function (store) {
      var reading = self._reads.some(within(store))
      var writing = self._writes.some(within(store))
      var deleting = self._deletes.some(within(store))
      return !(reading || writing || deleting)
    })

    if (err) return cb(err)
    if (self.stores.length >= self.limit && closable <= 0) {
      return cb(new Error('limit overflow: ' + self.stores.length))
    } else if (self.stores.length >= self.limit) {
      closable = self.stores.splice(closable, 1)[0]
      if (closable === self._last) self._last = null
      closable.storage.close(cb)
    } else {
      sorted.add(self.stores, match, cmp)
      cb(null)
    }
  }
}

Storage.prototype._get = function (offset) {
  if (this._last && !this._last.storage.closed) { // high chance that we'll hit the same at least twice
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

function cmp (a, b) {
  return a.start - b.start
}

function noop () {}
