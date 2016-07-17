exports.open = open
exports.bytes = bytes

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
var debug = require('debug')('couch-file:couch_file')


function open(filename, options, callback) {
  if (typeof options == 'function')
    return open(filename, {}, options)

  var mode = 0
  if (options.create)
    mode |= C.O_RDWR | C.O_CREAT
  if (options.overwrite)
    mode |= C.O_TRUNC

  if (options.create || options.overwrite)
    mode |= C.O_RDWR | C.O_APPEND
  else
    mode |= C.O_RDONLY

  debug('Open %j with %j, mode=%j (can write: %j)', filename, options, mode, !!(mode & C.O_RDWR))
  fs.open(filename, mode, function(er, fd) {
    callback(er, fd)
  })
}

function bytes(fd, callback) {
  debug('bytes in fd', fd)
  fs.fstat(fd, function(er, stat) {
    if (er)
      callback(er)
    else
      callback(null, stat.size)
  })
}
