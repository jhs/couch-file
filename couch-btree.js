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
    if (state instanceof Btree)
      this.clone(state)
    else
      this.init(state, file, options)
  }

  init(root, file, options) {
    debug('Init btree')
    this.root = root
    this.file = file
    this.extract_kv  = options.split       || function(kv) { return kv }
    this.assemble_kv = options.join        || function(key, value) { return {t:[key,value]} }
    this.less        = options.less        || function(a, b) { return a < b }
    this.reduce      = options.reduce      || null
    this.compression = options.compression || DEFAULT_COMPRESSION
  }

  clone(bt) {
    debug('Clone btree')
    var options = {split:bt.extract_kv, join:bt.assemle_kv, less:bt.less, reduce:bt.reduce, compression:bt.compression}
    this.init(bt.root, bt.file, options)
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

  add(insert_keyvalues, callback) {
    debug('add')
    this.add_remove(insert_keyvalues, [], callback)
  }

  add_remove(insert_keyvalues, remove_keys, callback) {
    debug('add_remove')
    this.query_modify([], insert_keyvalues, remove_keys, (er, _, bt2) => {
      debug('add_remove complete')
      callback(er, bt2)
    })
  }

  query_modify(lookup_keys, insert_values, remove_keys, callback) {
    var self = this
    debug('query_modify')

    var insert_actions = insert_values.map(keyvalue => {
      var kv = self.extract_kv(keyvalue)
      return {op:'insert', key:kv.key, value:kv.value}
    })
    var remove_actions = remove_keys.map(key => ({op:'remove', key:key, value:null}))
    var fetch_actions = lookup_keys.map(key => ({op:'fetch', key:key, value:null}))

    // For ordering different operations with the same key. fetch < remove < insert
    var op_order = {fetch:1, remove:2, insert:3}
    function sortfun(A, B) {
      if (A.key == B.key) {
        // A and B are equal, sort by op.
        return op_order[A.op] - op_order[B.op]
      } else
        return self.less(A.key, B.key)
    }

    var actions = [].concat(insert_actions, remove_actions, fetch_actions)
    actions.sort(sortfun)
    debug('query_modify actions: %j', actions)

    self.modify_node(self.root, actions, [], (er, key_pointers, query_results) => {
      if (er)
        return callback(er)

      debug('query_modify complete: %s key pointers', key_pointers.length)
      self.complete_root(key_pointers, (er, new_root) => {
        if (er)
          return callback(er)

        var result = new Btree(self)
        result.
        // XXX HERE
      })
      callback(null)
    })
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
