var test = require('tape')
var Storage = require('.')
var ram = require('random-access-memory')

test('Storage(open)', function (t) {
  t.plan(5)

  var i = 0
  var storage = Storage(function (offset, cb) {
    if (i++ === 0) {
      t.equal(offset, 0)
      cb(Error('nooo'))
    } else {
      t.equal(offset, 10)
      var index = Math.floor(offset / 10)
      cb(null, {
        start: index * 10,
        end: index * 10 + 10,
        storage: ram(Buffer('yuss'))
      })
    }
  })
  storage.read(0, 10, function (err, buf) {
    t.ok(err)
    storage.read(10, 4, function (err, buf) {
      t.error(err)
      t.deepEqual(buf, Buffer('yuss'))
    })
  })
})

test('read write', function (t) {
  t.plan(2)

  var storage = Storage(function (offset, cb) {
    var index = Math.floor(offset / 10)
    cb(null, {
      start: index * 10,
      end: index * 10 + 10,
      storage: ram(Buffer(10))
    })
  })
  storage.write(0, Buffer('hello world'), function (err) {
    t.error(err)
    storage.read(0, 11, function (err, buf) {
      if (err) throw err
      t.deepEqual(buf, Buffer('hello world'))
    })
  })
})

