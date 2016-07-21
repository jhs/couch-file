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

//var fs = require('fs')
var debug = require('debug')('couch-file:couch-btree')
//var erlang = require('erlang')
//var crypto = require('crypto')

//var couch_compress = require('./couch-compress.js')

const DEFAULT_COMPRESSION = 'snappy' // TODO, maybe move this to a common file. It comes from couch_db.hrl originally.


// pass in null for state if a new Btree.
function open(state, file, options, callback) {
  if (typeof options == 'function')
    return open(state, file, {}, options)

  debug('Open %j with %j: state = %j', file.filename, options, state)
  var btree = new Btree(state, file, options)
  callback(null, btree)
}


class Btree {
  constructor(state, file, options) {
    this.root = state
    this.file = file
    this.extract_kv  = options.split       || function(kv) { return kv }
    this.assemble_kv = options.join        || function(key, value) { return {t:[key,value]} }
    this.less        = options.less        || function(a, b) { return a < b }
    this.reduce      = options.reduce      || null
    this.compression = options.compression || DEFAULT_COMPRESSION
  }
}
