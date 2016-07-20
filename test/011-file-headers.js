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

var fs = require('fs')
var tap = require('tap')
var test = tap.test
var erlang = require('erlang')

var couch_file = require('../couch-file.js')
var couch_compress = require('../couch-compress.js')

var DB = __dirname + '/db/'
var FILENAME = __filename + '.temp'
const SIZEBLOCK = 4096 // Need to keep this in sync with couch-file.js


test('CouchDB 011-file-headers.t', function(t) {
  couch_file.open(FILENAME+'.011', {create:true, overwrite:true}, (er, file) => {
    if (er) throw er
  file.bytes((er, size) => {
    if (er) throw er
    t.equal(size, 0, 'File should be initialized to contain zero bytes')

  file.write_header({t: [{b:'some_data'}, 32]}, (er) => {
    if (er) throw er
    t.ok(!er, 'Writing a header succeeds')

  file.bytes((er, size1) => {
    if (er) throw er
    t.ok(size1 > size, 'Writing a header allocates space in the file')

  file.read_header((er, header) => {
    if (er) throw er
    t.same(header, {t:[new Buffer('some_data'), 32]}, 'Reading the header returns what we wrote')

  file.write_header([{a:'foo'}, new Buffer('more')], (er) => {
    if (er) throw er
    t.ok(!er, 'Writing a second header succeeds')

  file.bytes((er, size2) => {
    if (er) throw er
    t.ok(size2 > size1, 'Writing a second header allocates more space')

  file.read_header((er, header2) => {
    if (er) throw er
    t.same(header2, [{a:'foo'}, new Buffer('more')], 'Reading the second header does not return the first header')

  // Delete the second header.
  file.truncate(size1, (er) => {
    if (er) throw er
  file.read_header((er, header3) => {
    if (er) throw er
    t.same(header3, {t: [new Buffer('some_data'), 32]}, 'Reading the header after a truncation returns a previous header')

  file.write_header([{a:'foo'}, new Buffer('more')], (er) => {
    if (er) throw er
  file.bytes((er, rewritten_size) => {
    if (er) throw er
    t.equal(rewritten_size, size2, 'Rewriting the same second header returns the same second size')

  // Make a big tuple.
  var big_term = []
  for (var i = 0; i < 5000; i++)
    big_term.push(new Buffer('CouchDB'))
  big_term = {t: big_term}

  file.write_header(big_term, (er) => {
    if (er) throw er
  file.read_header((er, big_header) => {
    if (er) throw er
    //console.log('big header: %j', big_header)
    t.same(big_header, big_term, 'Headers larger than the block size can be saved (COUCHDB-1319)')

  file.close((er) => {
    if (er) throw er

  // Now for the fun stuff. Try corrupting the second header and see if we recover properly.

  // Destroy the 0x1 byte that marks a header.
  check_header_recovery(function(er, file, expect_header, header_pos) {
    if (er) throw er

    file.read_header((er, last_header) => {
      if (er) throw er
      t.notSame(last_header, expect_header, 'Should return a different header before corruption')
    fs.write(file.fd, new Buffer([0x00]), 0, 1, header_pos, (er) => {
      if (er) throw er
    file.read_header((er, header) => {
      if (er) throw er
      t.same(header, expect_header, 'Corrupting the byte marker should read the previous header')
    file.close((er) => {
      if (er) throw er

  t.end()
  })
  })
  })
  })
  })
  })
  })
  })
  })
  })
  })
  })
  })
  })
  })
  })
  })
  })
  })
  })
})

function check_header_recovery(callback) {
  couch_file.open(FILENAME+'.011', {create:true, overwrite:true}, (er, file) => {
    if (er)
      return callback(er)

    write_random_data(file, (er, _size) => {
      if (er)
        return callback(er)

      var expect_header = {t:[ {a:'some_atom'}, new Buffer('a binary'), 756]}
      file.write_header(expect_header, (er) => {
        if (er)
          return callback(er)

        write_random_data(file, (er, header_pos) => {
          if (er)
            return callback(er)

          file.write_header({t:[ 2342, {b:'corruption! greed!'} ]}, (er) => {
            if (er)
              return callback(er)

            callback(null, file, expect_header, header_pos)
          })
        })
      })
    })
  })
}

function random_uniform(size) {
  return Math.floor(Math.random() * size)
}

function write_random_data(file, count, callback) {
  if (!callback) {
    callback = count
    count = 100 + random_uniform(10000)
  }

  write_datum()
  function write_datum() {
    count -= 1
    if (count == 0)
      return done()

    var choices = [{a:'foo'}, {a:'bar'}, {b:'bizzingle'}, 'bank', ['rough', {a:'stuff'}]]
    var term = choices[random_uniform(5)]
    file.append_term(term, (er, _pos) => {
      if (er)
        callback(er)
      else
        write_datum()
    })
  }

  function done() {
    file.bytes((er, bytes) => {
      if (er)
        return callback(er)
      var result = (1 + Math.floor(bytes / SIZEBLOCK)) * SIZEBLOCK
      callback(null, result)
    })
  }
}

/*

    % Corrupt the size.
    check_header_recovery(fun(CouchFd, RawFd, Expect, HeaderPos) ->
        etap:isnt(Expect, couch_file:read_header(CouchFd),
            "Should return a different header before corruption."),
        % +1 for 0x1 byte marker
        file:pwrite(RawFd, HeaderPos+1, <<10/integer>>),
        etap:is(Expect, couch_file:read_header(CouchFd),
            "Corrupting the size should read the previous header.")
    end),

    % Corrupt the MD5 signature
    check_header_recovery(fun(CouchFd, RawFd, Expect, HeaderPos) ->
        etap:isnt(Expect, couch_file:read_header(CouchFd),
            "Should return a different header before corruption."),
        % +5 = +1 for 0x1 byte and +4 for term size.
        file:pwrite(RawFd, HeaderPos+5, <<"F01034F88D320B22">>),
        etap:is(Expect, couch_file:read_header(CouchFd),
            "Corrupting the MD5 signature should read the previous header.")
    end),

    % Corrupt the data
    check_header_recovery(fun(CouchFd, RawFd, Expect, HeaderPos) ->
        etap:isnt(Expect, couch_file:read_header(CouchFd),
            "Should return a different header before corruption."),
        % +21 = +1 for 0x1 byte, +4 for term size and +16 for MD5 sig
        file:pwrite(RawFd, HeaderPos+21, <<"some data goes here!">>),
        etap:is(Expect, couch_file:read_header(CouchFd),
            "Corrupting the header data should read the previous header.")
    end),

    ok.
*/
