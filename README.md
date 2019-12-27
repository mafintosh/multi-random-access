# multi-random-access

An [random-access-storage](https://github.com/random-access-storage/random-access-storage) compliant instance (API similar to [random-access-file](https://github.com/random-access-storage/random-access-file)) that combines multiple other random-access-storage instances into a single one.

```
npm install multi-random-access
```

[![build status](http://img.shields.io/travis/mafintosh/multi-random-access.svg?style=flat)](http://travis-ci.org/mafintosh/multi-random-access)

## Usage

In the below example we'll create a multi-random-access instance that writes to different instances of [random-access-file](https://github.com/random-access-storage/random-access-file), each containing 10 bytes of data.

``` js
var multi = require('multi-random-access')
var file = require('random-access-memory')

var length = file('/tmp/multi-length-file')
var storage = multi(length, function (offset, cb) {
  var index = Math.floor(offset / 10)

  console.log('Creating new underlying storage')

  cb(null, {
    start: index * 10,
    end: index * 10 + 10,
    storage: file('/tmp/multi-part-' + index)
  })
})

storage.write(0, Buffer('hello world'), function (err) {
  if (err) throw err
  storage.read(0, 11, function (err, buf) {
    if (err) throw err
    console.log(buf.toString())
  })
})
```

## API

#### `var storage = multi(length, open, [options])`

Create a new instance. `length` is a random-access-storage that is used to keep track of total store length. `open` is a function that is called when a new storage instance is needed. A new instance is needed when a read or write happens in a byte range that has not been opened yet.

The signature for open is `(offset, cb)`. You should call the callback with an object containing the following properties:

``` js
function open (offset, cb) {
  cb(null, {
    start: startByteOffset,
    end: endByteOffset,
    storage: randomAccessStorageInstance
  })
}
```

Options include:

``` js
{
  limit: 16 // start closes old stores after this many was opened
}
```

## License

MIT
