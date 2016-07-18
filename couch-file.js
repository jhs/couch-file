exports.open = open

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

var C = require('constants')
var fs = require('fs')
var debug = require('debug')('couch-file:couch-file')
var erlang = require('erlang')

var couch_compress = require('./couch-compress.js')


class File {
  constructor(fd, options) {
    debug('New File: %j %j', fd, options)
    this.fd = fd
    this.pos = 0
  }

  bytes(callback) {
    var self = this
    debug('bytes in fd', self.fd)
    fs.fstat(self.fd, function(er, stat) {
      if (er)
        return callback(er)

      debug('fstat for fd %j: %j', self.fd, stat)
      callback(null, stat.size)
    })
  }

  append_term(term, callback) {
    debug('append_term to %j', this.fd, term)
    var comp = couch_compress.compress(term, 'none')
    this.append_binary(comp, callback)
  }

  append_binary(bin, callback) {
    var self = this
    debug('append_binary to %j', self.fd, bin)
    var offset = 0
    var length = bin.length

    fs.write(self.fd, bin, offset, length, function(er, written, buf) {
      if (er)
        return callback(er)

      if (written != length)
        return callback(new Error(`Append binary to fd ${fd}: Only wrote ${written}/${length} bytes`))

      var old_pos = self.pos
      self.pos += length
      debug('Append binary moved offset from %s to %s', old_pos, self.pos)
      callback(null, old_pos)
    })
  }
}


function open(filename, options, callback) {
  if (typeof options == 'function')
    return open(filename, {}, options)

  var mode = opts_to_mode(options)
  var is_writable = !!(mode & C.O_RDWR)
  debug('Open %j with %j, mode=%j (can write: %j)', filename, options, mode, is_writable)

  fs.open(filename, mode, function(er, fd) {
    var file = new File(fd, options)
    callback(er, file)
  })
}

function opts_to_mode(options) {
  var mode = 0
  if (options.create)
    mode |= C.O_RDWR | C.O_CREAT

  if (options.overwrite)
    mode |= C.O_TRUNC

  if (options.create || options.overwrite)
    mode |= (C.O_RDWR | C.O_APPEND)
  else
    mode |= C.O_RDONLY

  return mode
}
