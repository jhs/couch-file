exports.compress = compress
exports.decompress = decompress

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

var debug = require('debug')('couch-file:couch-compress')
var erlang = require('erlang')
var snappy = require('snappy')


const TERM_PREFIX = 131
const SNAPPY_PREFIX = 1
const SNAPPY_PREFIX_BUF = new Buffer([SNAPPY_PREFIX])


function compress(term, type) {
  var buf = erlang.term_to_binary(term)
  if (type != 'snappy')
    return buf

  var compressed = snappy.compressSync(buf)
  if (compressed.length < buf.length) {
    debug('Snappy compression %s vs %s: ok', compressed.length, buf.length)
    return Buffer.concat([SNAPPY_PREFIX_BUF, compressed])
  } else {
    debug('Snappy compression %s vs %s: discard', compressed.length, buf.length)
    return buf
  }
}


function decompress(buf) {
  var type = buf[0]
  if (type == SNAPPY_PREFIX) {
    debug('decompress: snappy buffer; length: %s', buf.length)
    return snappy.decompressSync(buf.slice(1))
  } else if (type == TERM_PREFIX) {
    debug('decompress: Erlang term; length: %s', buf.length)
    return erlang.binary_to_term(buf)
  } else
    throw new Error(`Unknown buffer type to decompress: ${type}`)
}
