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


const COMPRESSOR = 'snappy'
const SIZE_BLOCK = 4096
const MASK_31    = new Buffer([0b01111111, 0b11111111, 0b11111111, 0b11111111]).readUInt32BE()


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
    var comp = couch_compress.compress(term, COMPRESSOR)
    this.append_binary(comp, callback)
  }

  append_binary(bin, callback) {
    var self = this
    debug('append_binary to %j', self.fd, bin)

    var chunk = assemble_file_chunk(bin)
    var blocks = make_blocks(self.pos % SIZE_BLOCK, chunk)
    blocks = erlang.iolist_to_buffer(blocks)

    var offset = 0
    debug('Append to %j: %j', self.fd, blocks)
    fs.write(self.fd, blocks, offset, blocks.length, function(er, written, buf) {
      if (er)
        return callback(er)

      if (written != blocks.length)
        return callback(new Error(`Append binary to fd ${self.fd}: Only wrote ${written}/${blocks.length} bytes`))

      var old_pos = self.pos
      self.pos += blocks.length
      debug('Append binary moved offset from %s to %s', old_pos, self.pos)
      callback(null, old_pos)
    })
  }

  pread_term(pos, callback) {
    debug('pread_term from %j at position: %j', this.fd, pos)
    this.pread_binary(pos, (er, bin) => {
      if (er)
        return callback(er)

      var term = couch_compress.decompress(bin, COMPRESSOR)
      callback(null, term)
    })
  }

  pread_binary(pos, callback) {
    debug('pread_binary from %j at position: %j', this.fd, pos)
    this.pread_iolist(pos, (er, iolist) => {
      if (er)
        return callback(er)

      var bin = erlang.iolist_to_binary(iolist)
      callback(null, bin)
    })
  }

  pread_iolist(pos, callback) {
    var self = this
    debug('pread_iolist from %j at position: %j', this.fd, pos)

    var remaining = pos % SIZE_BLOCK
    // Up to 8KB of read ahead
    read_raw_iolist_int(this.fd, pos, 2 * SIZE_BLOCK - remaining, (er, raw_data, next_pos) => {
      if (! er)
        return got_data(raw_data, next_pos)

      if (!er.under_read)
        return callback(er)

      // Retry just reading the minimum.
      debug('Under-read fd %s; retry with minimum length', self.fd)
      read_raw_iolist_int(self.fd, pos, 4, (er, raw_data, next_pos) => {
        if (er)
          return callback(er)

        got_data(raw_data, next_pos)
      })
    })

    function got_data(raw_data, next_pos) {
      var buf = erlang.iolist_to_binary(raw_data)
      var {prefix, length, data} = get_block_prefix(buf)
      debug('pread_iolist next_pos=%j Prefix=%j Len=%j RestRawData=%j', next_pos, prefix, length, data)

      if (prefix == 1)
        return callback(new Error(`Not implemented: md5 support`))

      maybe_read_more_iolist(data, length, next_pos, self.fd, (er, all_data) => {
        if (er)
          return callback(er)

        debug('  pread_iolist all data', all_data)
        callback(null, all_data)
      })
    }
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


function make_blocks(block_offset, iolist) {
  debug('make_blocks(%j, %j)', block_offset, iolist)
  if (iolist.length == 0)
    return []
  if (block_offset == 0)
    return [new Buffer([0x00]), make_blocks(1, iolist)]

  var iolist_size = erlang.iolist_size(iolist)
  var bytes_remaining = SIZE_BLOCK - block_offset
  debug('  Block can hold %s and iolist size = %j', bytes_remaining, iolist_size)
  if (bytes_remaining >= iolist_size)
    return iolist

  // Not enough room; split out what will fit add another block.
  var all_data = erlang.iolist_to_buffer(iolist)
  var in_this_block = all_data.slice(0, bytes_remaining)
  var in_next_block = all_data.slice(bytes_remaining)

  return [in_this_block, make_blocks(0, in_next_block)]
}

function assemble_file_chunk(iolist) {
  var head = new Buffer(4)
  var size = erlang.iolist_size(iolist)

  // The most significant bit is not part of the size; it must be zeroed out, meaning no MD5.
  size &= MASK_31
  head.writeUInt32BE(size)
  debug('assemble_file_chunk(%j) -> %j', iolist, [head, iolist])
  return [head, iolist]
}

function read_raw_iolist_int(fd, pos, len, callback) {
  debug('read_raw_iolist_int(%j, pos=%j, len=%j)', fd, pos, len)
  var block_offset = pos % SIZE_BLOCK
  var total_bytes = calculate_total_read_len(block_offset, len)
  debug('  calculate_total_read_len(%j, %j) -> %j', block_offset, len, total_bytes)

  var buf = new Buffer(total_bytes)
  var buf_offset = 0
  fs.read(fd, buf, buf_offset, total_bytes, pos, (er, bytes_read, raw_bin) => {
    if (er)
      return callback(er)

    if (bytes_read != total_bytes) {
      er = new Error(`Only read ${bytes_read}/${total_bytes} from fd ${fd}`)
      er.under_read = true
      return callback(er)
    }

    debug('RawBin length should equal TotalBytes: %j == %j', buf.length, total_bytes)
    var raw_data = remove_block_prefixes(block_offset, raw_bin)
    var new_pos = pos + total_bytes

    debug('read_raw_iolist_int(%j, %j, %j) -> %s bytes; new position: %j', fd, pos, len, bytes_read, new_pos)
    debug('  raw_data = %j', raw_data)
    callback(null, raw_data, new_pos)
  })
}

function maybe_read_more_iolist(buf, data_size, next_pos, fd, callback) {
  debug('maybe_read_more_iolist(%j, data_size=%j, next_pos=%j, fd=%j)', buf, data_size, next_pos, fd)
  if (data_size <= buf.length)
    return callback(null, buf.slice(0, data_size))

  var bytes_needed = data_size - buf.length
  debug('  maybe_read_more_iolist must read %s more bytes from position %s', bytes_needed, next_pos)
  read_raw_iolist_int(fd, next_pos, bytes_needed, (er, rest) => {
    if (er)
      return callback(er)

    callback(null, [buf, rest])
  })
}

function calculate_total_read_len(block_offset, final_len) {
  debug('calculate_total_read_len(%j, %j)', block_offset, final_len)
  if (block_offset == 0)
    return 1 + calculate_total_read_len(1, final_len)

  var block_left = SIZE_BLOCK - block_offset
  if (block_left >= final_len)
    return final_len


  var result = final_len + div(final_len - block_left, SIZE_BLOCK - 1)
  if ((final_len - block_left) % (SIZE_BLOCK -1) != 0)
    result += 1

  return result
}

function remove_block_prefixes(block_offset, buf) {
  debug('remove_block_prefixes(%j, %j)', block_offset, buf)
  if (buf.length == 0)
    return []

  if (block_offset == 0)
    return remove_block_prefixes(1, buf.slice(1))

  var block_bytes_available = SIZE_BLOCK - block_offset
  if (buf.length <= block_bytes_available)
    return [buf]

  var data_block = buf.slice(0, block_bytes_available)
  var rest = buf.slice(block_bytes_available)
  return [data_block, remove_block_prefixes(0, rest)]
}


function get_block_prefix(buf) {
  var prefix = (buf.readUInt8(0) >> 7  )      // First bit
  var length = (buf.readUInt32BE() & MASK_31) // Last 31 bits
  var data   = buf.slice(4)
  return {prefix, length, data}
}

function div(a, b) {
  return Math.floor(a / b)
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
