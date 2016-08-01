module.exports = Storage

function Storage (open) {
  if (!(this instanceof Storage)) return new Storage(open)
  this._openStorage = open
  this._maxOpen = 32
  this._stores = []
}

Storage.prototype.write = function (offset, buf, cb) {
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

Storage.prototype.read = function (offset, length, cb) {
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

Storage.prototype._get = function (offset) {
  for (var i = 0; i < this._stores.length; i++) {
    var next = this._stores[i]
    if (next.start <= offset && offset < next.end) return next
  }

  return null
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
