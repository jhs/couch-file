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

var tap = require('tap')
var test = tap.test
var erlang = require('erlang')

var couch_file = require('../couch-file.js')

var DB = __dirname + '/db/'
var FILENAME = __filename + '.temp'


test('CouchDB 010-file-basics.t', function(t) {
  couch_file.open('not a real file', function(er) {
    t.equal(er && er.code, 'ENOENT', "Opening a non-existant file should return an enoent error.")

  couch_file.open(FILENAME+'.0', {create:true, invalid_option:true}, function(er, file) {
    t.ok(!er, 'Invalid flags to open are ignored')

  couch_file.open(FILENAME+'.0', {create:true, overwrite:true}, function(er, file) {
    t.type(file.fd, 'number', 'Returned file has a file descriptor')

  file.bytes(function(er, size) {
    t.equal(size, 0, 'Newly created files have 0 bytes')

  file.append_term({a:'foo'}, function(er, old_pos) {
    if (er) throw er
    t.equal(old_pos, 0, 'Appending a term returns the previous end of file position')

  file.bytes((er, size) => {
    if (er) throw er
    t.ok(size > 0, `Writing a term increased the file size (${size})`)

  var bin = erlang.term_to_binary({b:'fancy!'})
  file.append_binary(bin, (er, old_pos) => {
    if (er) throw er
    t.equal(size, old_pos, 'Appending a binary returns the current file size')

  file.pread_term(0, (er, term) => {
    if (er) throw er
    t.same(term, {a:'foo'}, 'Reading the first term returns what we wrote: foo')

  file.pread_binary(size, (er, bin) => {
    if (er) throw er
    t.same(bin, erlang.term_to_binary({b:'fancy!'}), 'Reading back the binary returns what we wrote: <<"fancy!">>')

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
