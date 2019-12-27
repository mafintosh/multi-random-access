var multi = require('./')
var Buffer = require('buffer').Buffer
var randomFile = require('random-access-file')

var list = []
var n = 0

for (var i = 0; i < 1024; i++) {
  n += (Math.random() * 5000) | 0
  list.push(n)
}

var length = randomFile('/tmp/multi.len')
var storage = multi(length, function (offset, cb) {
  console.log('opening', offset)
  for (var i = 0; i < list.length; i++) {
    if (list[i] > offset) {
      var start = list[i - 1] || 0
      return cb(null, {
        start,
        end: list[i],
        storage: randomFile('/tmp/multi-' + start)
      })
    }
  }
})

storage.write(0, Buffer.from('hello world 1'))
storage.write(4000, Buffer.from('hello world 2'))
storage.write(10000, Buffer.from('hello world 34'), function (err) {
  storage.read(10000, 11, function (err, data) {
    console.log(err, data.toString())
  })
})

storage.write(50000, Buffer.from('hi'))
