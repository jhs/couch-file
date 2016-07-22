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

  size(callback) {
    var self = this
    if (self.root === null)
      return callback(null, 0)

    callback(new Error('Size not implemented'))
    //size(#btree{root = {_P, _Red}}) ->
    //    % pre 1.2 format
    //    nil;
    //size(#btree{root = {_P, _Red, Size}}) ->
    //    Size.
  }

  foldl(fun, acc, options, callback) {
    if (!callback)
      return this.foldl(fun, acc, {}, options)

    debug('foldl')
    this.fold(fun, acc, options, callback)
  }

  fold(fun, acc, options, callback) {
    var self = this
    debug('fold')

    if (self.root === null)
      return callback(null, {t:[ [],[] ]}, acc)

    var dir = options.dir || 'fwd'
    var in_range = self.make_key_in_end_range_function(dir, options)

    var start_key = options.start_key
    if (start_key === undefined)
      self.stream_node([], self.root, in_range, dir, fun, acc, got_stream)
    else
      self.stream_node([], self.root, start_key, in_range, dir, fun, acc, got_stream)

    function got_stream(er, result) {
      if (er)
        return callback(er)

      if (result.ok) {
        var full_reduction = erlang.element(2, self.root)
        callback(null, {t:[ [], [full_reduction] ]}, result.acc)
      } else if (result.stop) {
        callback(null, result.last_reduction, result.acc)
      } else
        return callback(new Error(`Unknown stream result: ${JSON.stringify(result)}`))
    }
  }

  make_key_in_end_range_function(dir, options) {
    var self = this
    var is_forward = (dir == 'fwd')
    var end_key_gt = options.end_key_gt
    var end_key    = options.end_key

    if (typeof end_key_gt !== undefined)
      return is_forward
        ? function(key) { return self.less(key, end_key_gt) } // fwd
        : function(key) { return self.less(end_key_gt, key) } // rev

    if (typeof end_key !== undefined)
      return is_forward
        ? function(key) { return !self.less(end_key, key) } // fwd
        : function(key) { return !self.less(key, end_key) } // rev

    return function(_key) { return true }
  }
}

exports.Btree = Btree
