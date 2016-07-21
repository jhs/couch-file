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
var crypto = require('crypto')

var couch_compress = require('./couch-compress.js')


const COMPRESSOR = 'snappy'
const SIZE_BLOCK = 4096
const MASK_31    = new Buffer([0b01111111, 0b11111111, 0b11111111, 0b11111111]).readUInt32BE()


function open(filename, options, callback) {
  if (typeof options == 'function')
    return open(filename, {}, options)

  var mode = opts_to_mode(options)
  var is_writable = !!(mode & C.O_RDWR)
  debug('Open %j with %j, mode=%j (can write: %j)', filename, options, mode, is_writable)

  fs.open(filename, mode, function(er, fd) {
    if (er)
      return callback(er)

    var file = new File(fd, filename, options)
    callback(null, file)
  })
}


class File {
  constructor(fd, filename, options) {
    debug('New File: %j %j', fd, options)
    this.fd = fd
    this.pos = 0
    this.filename = filename
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

  write_header(term, callback) {
    debug('write_header')
    var self = this
    var bin = erlang.term_to_binary(term)
    var md5 = hash(bin, 'md5')

    // Now we assemble the final header binary and write to disk.
    var data_bin = Buffer.concat([md5, bin])

    var block_offset = self.pos % SIZE_BLOCK
    if (block_offset == 0)
      var padding = new Buffer([])
    else
      var padding = Buffer.alloc(SIZE_BLOCK - block_offset, 0x00)

    debug('Padding length for header: %s', padding.length)
    var size_buf = new Buffer(4)
    size_buf.writeUInt32BE(data_bin.length)
    var blocks = make_blocks(5, [data_bin])
    var final_iolist = [padding, new Buffer([0x01]), size_buf, blocks]
    var final_bin = erlang.iolist_to_buffer(final_iolist)

    debug('Append header to %j: %s bytes', self.fd, final_bin.length)
    var offset = 0
    fs.write(self.fd, final_bin, offset, final_bin.length, (er, written, buf) => {
      if (er)
        return callback(er)
      if (written != final_bin.length)
        return callback(new Error(`Append binary to fd ${self.fd}: Only wrote ${written}/${final_bin.length} bytes`))

      var old_pos = self.pos
      self.pos += written
      debug('Append header moved offset: %s -> %s', old_pos, self.pos)
      callback(null)
    })
  }

  append_term(term, callback) {
    debug('append_term to %j', this.fd, term)
    var comp = couch_compress.compress(term, COMPRESSOR)
    this.append_binary(comp, callback)
  }

  append_binary(bin, callback) {
    var self = this
    debug('append_binary to %j', self.fd, bin.length)

    var chunk = assemble_file_chunk(bin)
    var blocks = make_blocks(self.pos % SIZE_BLOCK, chunk)
    blocks = erlang.iolist_to_buffer(blocks)

    var offset = 0
    debug('Append to %j: %j bytes', self.fd, blocks.length)
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

  read_header(callback) {
    debug('read_header')
    var self = this
    var block_number = div(self.pos, SIZE_BLOCK)
    find_header(self.fd, block_number, (er, header_bin) => {
      if (er)
        return callback(er)

      var header = erlang.binary_to_term(header_bin)
      debug('read_header found: %j', header)
      callback(null, header)
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
      debug('pread_iolist next_pos=%j Prefix=%j Len=%j RestRawData=%j', next_pos, prefix, length, data.length)

      if (prefix == 1)
        return callback(new Error(`Not implemented: md5 support`))

      maybe_read_more_iolist(data, length, next_pos, self.fd, (er, all_data) => {
        if (er)
          return callback(er)

        //debug('  pread_iolist all data', all_data)
        callback(null, all_data)
      })
    }
  }

  fsync(callback) {
    debug('fsync: %s', this.fd)
    fs.fsync(this.fd, callback)
  }

  truncate(new_pos, callback) {
    var self = this
    debug('truncate %s to size: %s', self.fd, new_pos)
    fs.ftruncate(self.fd, new_pos, (er) => {
      if (er)
        return callback(er)

      self.pos = new_pos
      callback(null)
    })
  }

  close(callback) {
    debug('close: %s', this.fd)
    fs.close(this.fd, callback)
  }
}


function make_blocks(block_offset, iolist) {
  debug('make_blocks(%j, %j)', block_offset, iolist.length)
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
  //debug('assemble_file_chunk(%j) -> %j', iolist, [head, iolist])
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
    //debug('  raw_data = %j', raw_data)
    callback(null, raw_data, new_pos)
  })
}

function maybe_read_more_iolist(buf, data_size, next_pos, fd, callback) {
  debug('maybe_read_more_iolist(%j, data_size=%j, next_pos=%j, fd=%j)', buf.length, data_size, next_pos, fd)
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
  debug('remove_block_prefixes(%j, %j)', block_offset, buf.length)
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

function find_header(fd, block, callback) {
  if (block == -1)
    return callback(new Error(`no_valid_header`))

  load_header(fd, block, (er, bin) => {
    if (er) {
      debug('Retry on load_header error at block %s: %s', block, er.message)
      return find_header(fd, block - 1, callback)
    }

    callback(null, bin)
  })
}

function load_header(fd, block, callback) {
  debug('load_header %s block=%j', fd, block)
  var block_bin = new Buffer(SIZE_BLOCK)
  var buf_offset = 0
  var block_pos = block * SIZE_BLOCK
  fs.read(fd, block_bin, buf_offset, SIZE_BLOCK, block_pos, (er, bytes_read) => {
    if (er)
      return callback(er)
    if (block_bin[0] != 1)
      return callback(new Error(`Block ${block} first byte is not 1: ${block_bin[0]}`))

    block_bin = block_bin.slice(0, bytes_read)
    var header_len = block_bin.readUInt32BE(1)
    var rest_block = block_bin.slice(5)
    var total_bytes = calculate_total_read_len(5, header_len)
    if (total_bytes <= rest_block.length)
      got_all_bytes(rest_block.slice(0, total_bytes))
    else {
      var remainder_pos = block * SIZE_BLOCK + 5 + rest_block.length
      var remainder_size = total_bytes - rest_block.length
      var remainder_bin = new Buffer(remainder_size)
      debug('Follow-up read %s bytes at %s for header remainder', remainder_size, remainder_pos)
      fs.read(fd, remainder_bin, buf_offset, remainder_size, remainder_pos, (er, bytes_read) => {
        if (er)
          return callback(er)
        if (bytes_read != remainder_size)
          return callback(new Error(`Read block remainder from ${fd} only read ${bytes_read}/${remainder_size} bytes`))

        var raw_bin = Buffer.concat([rest_block, remainder_bin])
        got_all_bytes(raw_bin)
      })
    }
  })

  function got_all_bytes(raw_bin) {
    debug('Got header binary: %s bytes', raw_bin.length)
    raw_bin = remove_block_prefixes(5, raw_bin)
    raw_bin = erlang.iolist_to_binary(raw_bin)

    var md5_bin    = raw_bin.slice(0, 16)
    var header_bin = raw_bin.slice(16)

    var md5 = hash(header_bin, 'md5')
    if (! md5.equals(md5_bin))
      return callback(new Error(`Bad header checksum: ${md5_bin.toString('hex')} does not match computed hash ${md5.toString('hex')}`))

    debug('load_header found %s at block %s: %s bytes', fd, block, header_bin.length)
    callback(null, header_bin)
  }
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

function hash(buf, algorithm) {
  var h = crypto.createHash(algorithm)
  h.update(buf)
  return h.digest()
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
