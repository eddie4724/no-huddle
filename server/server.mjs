import { createRequire } from 'module'; const require = createRequire(import.meta.url);
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __require = /* @__PURE__ */ ((x) => typeof require !== "undefined" ? require : typeof Proxy !== "undefined" ? new Proxy(x, {
  get: (a, b) => (typeof require !== "undefined" ? require : a)[b]
}) : x)(function(x) {
  if (typeof require !== "undefined") return require.apply(this, arguments);
  throw Error('Dynamic require of "' + x + '" is not supported');
});
var __commonJS = (cb, mod) => function __require2() {
  try {
    return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
  } catch (e) {
    throw mod = 0, e;
  }
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));

// node_modules/ws/lib/constants.js
var require_constants = __commonJS({
  "node_modules/ws/lib/constants.js"(exports, module) {
    "use strict";
    var BINARY_TYPES = ["nodebuffer", "arraybuffer", "fragments"];
    var hasBlob = typeof Blob !== "undefined";
    if (hasBlob) BINARY_TYPES.push("blob");
    module.exports = {
      BINARY_TYPES,
      CLOSE_TIMEOUT: 3e4,
      EMPTY_BUFFER: Buffer.alloc(0),
      GUID: "258EAFA5-E914-47DA-95CA-C5AB0DC85B11",
      hasBlob,
      kForOnEventAttribute: /* @__PURE__ */ Symbol("kIsForOnEventAttribute"),
      kListener: /* @__PURE__ */ Symbol("kListener"),
      kStatusCode: /* @__PURE__ */ Symbol("status-code"),
      kWebSocket: /* @__PURE__ */ Symbol("websocket"),
      NOOP: () => {
      }
    };
  }
});

// node_modules/ws/lib/buffer-util.js
var require_buffer_util = __commonJS({
  "node_modules/ws/lib/buffer-util.js"(exports, module) {
    "use strict";
    var { EMPTY_BUFFER } = require_constants();
    var FastBuffer = Buffer[Symbol.species];
    function concat(list, totalLength) {
      if (list.length === 0) return EMPTY_BUFFER;
      if (list.length === 1) return list[0];
      const target = Buffer.allocUnsafe(totalLength);
      let offset = 0;
      for (let i = 0; i < list.length; i++) {
        const buf = list[i];
        target.set(buf, offset);
        offset += buf.length;
      }
      if (offset < totalLength) {
        return new FastBuffer(target.buffer, target.byteOffset, offset);
      }
      return target;
    }
    function _mask(source, mask, output, offset, length) {
      for (let i = 0; i < length; i++) {
        output[offset + i] = source[i] ^ mask[i & 3];
      }
    }
    function _unmask(buffer, mask) {
      for (let i = 0; i < buffer.length; i++) {
        buffer[i] ^= mask[i & 3];
      }
    }
    function toArrayBuffer(buf) {
      if (buf.length === buf.buffer.byteLength) {
        return buf.buffer;
      }
      return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.length);
    }
    function toBuffer(data) {
      toBuffer.readOnly = true;
      if (Buffer.isBuffer(data)) return data;
      let buf;
      if (data instanceof ArrayBuffer) {
        buf = new FastBuffer(data);
      } else if (ArrayBuffer.isView(data)) {
        buf = new FastBuffer(data.buffer, data.byteOffset, data.byteLength);
      } else {
        buf = Buffer.from(data);
        toBuffer.readOnly = false;
      }
      return buf;
    }
    module.exports = {
      concat,
      mask: _mask,
      toArrayBuffer,
      toBuffer,
      unmask: _unmask
    };
    if (!process.env.WS_NO_BUFFER_UTIL) {
      try {
        const bufferUtil = __require("bufferutil");
        module.exports.mask = function(source, mask, output, offset, length) {
          if (length < 48) _mask(source, mask, output, offset, length);
          else bufferUtil.mask(source, mask, output, offset, length);
        };
        module.exports.unmask = function(buffer, mask) {
          if (buffer.length < 32) _unmask(buffer, mask);
          else bufferUtil.unmask(buffer, mask);
        };
      } catch (e) {
      }
    }
  }
});

// node_modules/ws/lib/limiter.js
var require_limiter = __commonJS({
  "node_modules/ws/lib/limiter.js"(exports, module) {
    "use strict";
    var kDone = /* @__PURE__ */ Symbol("kDone");
    var kRun = /* @__PURE__ */ Symbol("kRun");
    var Limiter = class {
      /**
       * Creates a new `Limiter`.
       *
       * @param {Number} [concurrency=Infinity] The maximum number of jobs allowed
       *     to run concurrently
       */
      constructor(concurrency) {
        this[kDone] = () => {
          this.pending--;
          this[kRun]();
        };
        this.concurrency = concurrency || Infinity;
        this.jobs = [];
        this.pending = 0;
      }
      /**
       * Adds a job to the queue.
       *
       * @param {Function} job The job to run
       * @public
       */
      add(job) {
        this.jobs.push(job);
        this[kRun]();
      }
      /**
       * Removes a job from the queue and runs it if possible.
       *
       * @private
       */
      [kRun]() {
        if (this.pending === this.concurrency) return;
        if (this.jobs.length) {
          const job = this.jobs.shift();
          this.pending++;
          job(this[kDone]);
        }
      }
    };
    module.exports = Limiter;
  }
});

// node_modules/ws/lib/permessage-deflate.js
var require_permessage_deflate = __commonJS({
  "node_modules/ws/lib/permessage-deflate.js"(exports, module) {
    "use strict";
    var zlib = __require("zlib");
    var bufferUtil = require_buffer_util();
    var Limiter = require_limiter();
    var { kStatusCode } = require_constants();
    var FastBuffer = Buffer[Symbol.species];
    var TRAILER = Buffer.from([0, 0, 255, 255]);
    var kPerMessageDeflate = /* @__PURE__ */ Symbol("permessage-deflate");
    var kTotalLength = /* @__PURE__ */ Symbol("total-length");
    var kCallback = /* @__PURE__ */ Symbol("callback");
    var kBuffers = /* @__PURE__ */ Symbol("buffers");
    var kError = /* @__PURE__ */ Symbol("error");
    var zlibLimiter;
    var PerMessageDeflate2 = class {
      /**
       * Creates a PerMessageDeflate instance.
       *
       * @param {Object} [options] Configuration options
       * @param {(Boolean|Number)} [options.clientMaxWindowBits] Advertise support
       *     for, or request, a custom client window size
       * @param {Boolean} [options.clientNoContextTakeover=false] Advertise/
       *     acknowledge disabling of client context takeover
       * @param {Number} [options.concurrencyLimit=10] The number of concurrent
       *     calls to zlib
       * @param {Boolean} [options.isServer=false] Create the instance in either
       *     server or client mode
       * @param {Number} [options.maxPayload=0] The maximum allowed message length
       * @param {(Boolean|Number)} [options.serverMaxWindowBits] Request/confirm the
       *     use of a custom server window size
       * @param {Boolean} [options.serverNoContextTakeover=false] Request/accept
       *     disabling of server context takeover
       * @param {Number} [options.threshold=1024] Size (in bytes) below which
       *     messages should not be compressed if context takeover is disabled
       * @param {Object} [options.zlibDeflateOptions] Options to pass to zlib on
       *     deflate
       * @param {Object} [options.zlibInflateOptions] Options to pass to zlib on
       *     inflate
       */
      constructor(options) {
        this._options = options || {};
        this._threshold = this._options.threshold !== void 0 ? this._options.threshold : 1024;
        this._maxPayload = this._options.maxPayload | 0;
        this._isServer = !!this._options.isServer;
        this._deflate = null;
        this._inflate = null;
        this.params = null;
        if (!zlibLimiter) {
          const concurrency = this._options.concurrencyLimit !== void 0 ? this._options.concurrencyLimit : 10;
          zlibLimiter = new Limiter(concurrency);
        }
      }
      /**
       * @type {String}
       */
      static get extensionName() {
        return "permessage-deflate";
      }
      /**
       * Create an extension negotiation offer.
       *
       * @return {Object} Extension parameters
       * @public
       */
      offer() {
        const params = {};
        if (this._options.serverNoContextTakeover) {
          params.server_no_context_takeover = true;
        }
        if (this._options.clientNoContextTakeover) {
          params.client_no_context_takeover = true;
        }
        if (this._options.serverMaxWindowBits) {
          params.server_max_window_bits = this._options.serverMaxWindowBits;
        }
        if (this._options.clientMaxWindowBits) {
          params.client_max_window_bits = this._options.clientMaxWindowBits;
        } else if (this._options.clientMaxWindowBits == null) {
          params.client_max_window_bits = true;
        }
        return params;
      }
      /**
       * Accept an extension negotiation offer/response.
       *
       * @param {Array} configurations The extension negotiation offers/reponse
       * @return {Object} Accepted configuration
       * @public
       */
      accept(configurations) {
        configurations = this.normalizeParams(configurations);
        this.params = this._isServer ? this.acceptAsServer(configurations) : this.acceptAsClient(configurations);
        return this.params;
      }
      /**
       * Releases all resources used by the extension.
       *
       * @public
       */
      cleanup() {
        if (this._inflate) {
          this._inflate.close();
          this._inflate = null;
        }
        if (this._deflate) {
          const callback = this._deflate[kCallback];
          this._deflate.close();
          this._deflate = null;
          if (callback) {
            callback(
              new Error(
                "The deflate stream was closed while data was being processed"
              )
            );
          }
        }
      }
      /**
       *  Accept an extension negotiation offer.
       *
       * @param {Array} offers The extension negotiation offers
       * @return {Object} Accepted configuration
       * @private
       */
      acceptAsServer(offers) {
        const opts = this._options;
        const accepted = offers.find((params) => {
          if (opts.serverNoContextTakeover === false && params.server_no_context_takeover || params.server_max_window_bits && (opts.serverMaxWindowBits === false || typeof opts.serverMaxWindowBits === "number" && opts.serverMaxWindowBits > params.server_max_window_bits) || typeof opts.clientMaxWindowBits === "number" && (typeof params.client_max_window_bits === "number" ? opts.clientMaxWindowBits > params.client_max_window_bits : !params.client_max_window_bits)) {
            return false;
          }
          return true;
        });
        if (!accepted) {
          throw new Error("None of the extension offers can be accepted");
        }
        if (opts.serverNoContextTakeover) {
          accepted.server_no_context_takeover = true;
        }
        if (opts.clientNoContextTakeover) {
          accepted.client_no_context_takeover = true;
        }
        if (typeof opts.serverMaxWindowBits === "number") {
          accepted.server_max_window_bits = opts.serverMaxWindowBits;
        }
        if (typeof opts.clientMaxWindowBits === "number") {
          accepted.client_max_window_bits = opts.clientMaxWindowBits;
        } else if (accepted.client_max_window_bits === true || opts.clientMaxWindowBits === false) {
          delete accepted.client_max_window_bits;
        }
        return accepted;
      }
      /**
       * Accept the extension negotiation response.
       *
       * @param {Array} response The extension negotiation response
       * @return {Object} Accepted configuration
       * @private
       */
      acceptAsClient(response) {
        const params = response[0];
        if (this._options.clientNoContextTakeover === false && params.client_no_context_takeover) {
          throw new Error('Unexpected parameter "client_no_context_takeover"');
        }
        if (!params.client_max_window_bits) {
          if (typeof this._options.clientMaxWindowBits === "number") {
            params.client_max_window_bits = this._options.clientMaxWindowBits;
          }
        } else if (this._options.clientMaxWindowBits === false || typeof this._options.clientMaxWindowBits === "number" && params.client_max_window_bits > this._options.clientMaxWindowBits) {
          throw new Error(
            'Unexpected or invalid parameter "client_max_window_bits"'
          );
        }
        return params;
      }
      /**
       * Normalize parameters.
       *
       * @param {Array} configurations The extension negotiation offers/reponse
       * @return {Array} The offers/response with normalized parameters
       * @private
       */
      normalizeParams(configurations) {
        configurations.forEach((params) => {
          Object.keys(params).forEach((key) => {
            let value = params[key];
            if (value.length > 1) {
              throw new Error(`Parameter "${key}" must have only a single value`);
            }
            value = value[0];
            if (key === "client_max_window_bits") {
              if (value !== true) {
                const num2 = +value;
                if (!Number.isInteger(num2) || num2 < 8 || num2 > 15) {
                  throw new TypeError(
                    `Invalid value for parameter "${key}": ${value}`
                  );
                }
                value = num2;
              } else if (!this._isServer) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
            } else if (key === "server_max_window_bits") {
              const num2 = +value;
              if (!Number.isInteger(num2) || num2 < 8 || num2 > 15) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
              value = num2;
            } else if (key === "client_no_context_takeover" || key === "server_no_context_takeover") {
              if (value !== true) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
            } else {
              throw new Error(`Unknown parameter "${key}"`);
            }
            params[key] = value;
          });
        });
        return configurations;
      }
      /**
       * Decompress data. Concurrency limited.
       *
       * @param {Buffer} data Compressed data
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @public
       */
      decompress(data, fin, callback) {
        zlibLimiter.add((done) => {
          this._decompress(data, fin, (err, result2) => {
            done();
            callback(err, result2);
          });
        });
      }
      /**
       * Compress data. Concurrency limited.
       *
       * @param {(Buffer|String)} data Data to compress
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @public
       */
      compress(data, fin, callback) {
        zlibLimiter.add((done) => {
          this._compress(data, fin, (err, result2) => {
            done();
            callback(err, result2);
          });
        });
      }
      /**
       * Decompress data.
       *
       * @param {Buffer} data Compressed data
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @private
       */
      _decompress(data, fin, callback) {
        const endpoint = this._isServer ? "client" : "server";
        if (!this._inflate) {
          const key = `${endpoint}_max_window_bits`;
          const windowBits = typeof this.params[key] !== "number" ? zlib.Z_DEFAULT_WINDOWBITS : this.params[key];
          this._inflate = zlib.createInflateRaw({
            ...this._options.zlibInflateOptions,
            windowBits
          });
          this._inflate[kPerMessageDeflate] = this;
          this._inflate[kTotalLength] = 0;
          this._inflate[kBuffers] = [];
          this._inflate.on("error", inflateOnError);
          this._inflate.on("data", inflateOnData);
        }
        this._inflate[kCallback] = callback;
        this._inflate.write(data);
        if (fin) this._inflate.write(TRAILER);
        this._inflate.flush(() => {
          const err = this._inflate[kError];
          if (err) {
            this._inflate.close();
            this._inflate = null;
            callback(err);
            return;
          }
          const data2 = bufferUtil.concat(
            this._inflate[kBuffers],
            this._inflate[kTotalLength]
          );
          if (this._inflate._readableState.endEmitted) {
            this._inflate.close();
            this._inflate = null;
          } else {
            this._inflate[kTotalLength] = 0;
            this._inflate[kBuffers] = [];
            if (fin && this.params[`${endpoint}_no_context_takeover`]) {
              this._inflate.reset();
            }
          }
          callback(null, data2);
        });
      }
      /**
       * Compress data.
       *
       * @param {(Buffer|String)} data Data to compress
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @private
       */
      _compress(data, fin, callback) {
        const endpoint = this._isServer ? "server" : "client";
        if (!this._deflate) {
          const key = `${endpoint}_max_window_bits`;
          const windowBits = typeof this.params[key] !== "number" ? zlib.Z_DEFAULT_WINDOWBITS : this.params[key];
          this._deflate = zlib.createDeflateRaw({
            ...this._options.zlibDeflateOptions,
            windowBits
          });
          this._deflate[kTotalLength] = 0;
          this._deflate[kBuffers] = [];
          this._deflate.on("data", deflateOnData);
        }
        this._deflate[kCallback] = callback;
        this._deflate.write(data);
        this._deflate.flush(zlib.Z_SYNC_FLUSH, () => {
          if (!this._deflate) {
            return;
          }
          let data2 = bufferUtil.concat(
            this._deflate[kBuffers],
            this._deflate[kTotalLength]
          );
          if (fin) {
            data2 = new FastBuffer(data2.buffer, data2.byteOffset, data2.length - 4);
          }
          this._deflate[kCallback] = null;
          this._deflate[kTotalLength] = 0;
          this._deflate[kBuffers] = [];
          if (fin && this.params[`${endpoint}_no_context_takeover`]) {
            this._deflate.reset();
          }
          callback(null, data2);
        });
      }
    };
    module.exports = PerMessageDeflate2;
    function deflateOnData(chunk) {
      this[kBuffers].push(chunk);
      this[kTotalLength] += chunk.length;
    }
    function inflateOnData(chunk) {
      this[kTotalLength] += chunk.length;
      if (this[kPerMessageDeflate]._maxPayload < 1 || this[kTotalLength] <= this[kPerMessageDeflate]._maxPayload) {
        this[kBuffers].push(chunk);
        return;
      }
      this[kError] = new RangeError("Max payload size exceeded");
      this[kError].code = "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH";
      this[kError][kStatusCode] = 1009;
      this.removeListener("data", inflateOnData);
      this.reset();
    }
    function inflateOnError(err) {
      this[kPerMessageDeflate]._inflate = null;
      if (this[kError]) {
        this[kCallback](this[kError]);
        return;
      }
      err[kStatusCode] = 1007;
      this[kCallback](err);
    }
  }
});

// node_modules/ws/lib/validation.js
var require_validation = __commonJS({
  "node_modules/ws/lib/validation.js"(exports, module) {
    "use strict";
    var { isUtf8 } = __require("buffer");
    var { hasBlob } = require_constants();
    var tokenChars = [
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // 0 - 15
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // 16 - 31
      0,
      1,
      0,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      1,
      1,
      0,
      1,
      1,
      0,
      // 32 - 47
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      0,
      0,
      0,
      0,
      // 48 - 63
      0,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      // 64 - 79
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      0,
      1,
      1,
      // 80 - 95
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      // 96 - 111
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      1,
      0,
      1,
      0
      // 112 - 127
    ];
    function isValidStatusCode(code) {
      return code >= 1e3 && code <= 1014 && code !== 1004 && code !== 1005 && code !== 1006 || code >= 3e3 && code <= 4999;
    }
    function _isValidUTF8(buf) {
      const len = buf.length;
      let i = 0;
      while (i < len) {
        if ((buf[i] & 128) === 0) {
          i++;
        } else if ((buf[i] & 224) === 192) {
          if (i + 1 === len || (buf[i + 1] & 192) !== 128 || (buf[i] & 254) === 192) {
            return false;
          }
          i += 2;
        } else if ((buf[i] & 240) === 224) {
          if (i + 2 >= len || (buf[i + 1] & 192) !== 128 || (buf[i + 2] & 192) !== 128 || buf[i] === 224 && (buf[i + 1] & 224) === 128 || // Overlong
          buf[i] === 237 && (buf[i + 1] & 224) === 160) {
            return false;
          }
          i += 3;
        } else if ((buf[i] & 248) === 240) {
          if (i + 3 >= len || (buf[i + 1] & 192) !== 128 || (buf[i + 2] & 192) !== 128 || (buf[i + 3] & 192) !== 128 || buf[i] === 240 && (buf[i + 1] & 240) === 128 || // Overlong
          buf[i] === 244 && buf[i + 1] > 143 || buf[i] > 244) {
            return false;
          }
          i += 4;
        } else {
          return false;
        }
      }
      return true;
    }
    function isBlob(value) {
      return hasBlob && typeof value === "object" && typeof value.arrayBuffer === "function" && typeof value.type === "string" && typeof value.stream === "function" && (value[Symbol.toStringTag] === "Blob" || value[Symbol.toStringTag] === "File");
    }
    module.exports = {
      isBlob,
      isValidStatusCode,
      isValidUTF8: _isValidUTF8,
      tokenChars
    };
    if (isUtf8) {
      module.exports.isValidUTF8 = function(buf) {
        return buf.length < 24 ? _isValidUTF8(buf) : isUtf8(buf);
      };
    } else if (!process.env.WS_NO_UTF_8_VALIDATE) {
      try {
        const isValidUTF8 = __require("utf-8-validate");
        module.exports.isValidUTF8 = function(buf) {
          return buf.length < 32 ? _isValidUTF8(buf) : isValidUTF8(buf);
        };
      } catch (e) {
      }
    }
  }
});

// node_modules/ws/lib/receiver.js
var require_receiver = __commonJS({
  "node_modules/ws/lib/receiver.js"(exports, module) {
    "use strict";
    var { Writable } = __require("stream");
    var PerMessageDeflate2 = require_permessage_deflate();
    var {
      BINARY_TYPES,
      EMPTY_BUFFER,
      kStatusCode,
      kWebSocket
    } = require_constants();
    var { concat, toArrayBuffer, unmask } = require_buffer_util();
    var { isValidStatusCode, isValidUTF8 } = require_validation();
    var FastBuffer = Buffer[Symbol.species];
    var GET_INFO = 0;
    var GET_PAYLOAD_LENGTH_16 = 1;
    var GET_PAYLOAD_LENGTH_64 = 2;
    var GET_MASK = 3;
    var GET_DATA = 4;
    var INFLATING = 5;
    var DEFER_EVENT = 6;
    var Receiver2 = class extends Writable {
      /**
       * Creates a Receiver instance.
       *
       * @param {Object} [options] Options object
       * @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {String} [options.binaryType=nodebuffer] The type for binary data
       * @param {Object} [options.extensions] An object containing the negotiated
       *     extensions
       * @param {Boolean} [options.isServer=false] Specifies whether to operate in
       *     client or server mode
       * @param {Number} [options.maxBufferedChunks=0] The maximum number of
       *     buffered data chunks
       * @param {Number} [options.maxFragments=0] The maximum number of message
       *     fragments
       * @param {Number} [options.maxPayload=0] The maximum allowed message length
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       */
      constructor(options = {}) {
        super();
        this._allowSynchronousEvents = options.allowSynchronousEvents !== void 0 ? options.allowSynchronousEvents : true;
        this._binaryType = options.binaryType || BINARY_TYPES[0];
        this._extensions = options.extensions || {};
        this._isServer = !!options.isServer;
        this._maxBufferedChunks = options.maxBufferedChunks | 0;
        this._maxFragments = options.maxFragments | 0;
        this._maxPayload = options.maxPayload | 0;
        this._skipUTF8Validation = !!options.skipUTF8Validation;
        this[kWebSocket] = void 0;
        this._bufferedBytes = 0;
        this._buffers = [];
        this._compressed = false;
        this._payloadLength = 0;
        this._mask = void 0;
        this._fragmented = 0;
        this._masked = false;
        this._fin = false;
        this._opcode = 0;
        this._totalPayloadLength = 0;
        this._messageLength = 0;
        this._numFragments = 0;
        this._fragments = [];
        this._errored = false;
        this._loop = false;
        this._state = GET_INFO;
      }
      /**
       * Implements `Writable.prototype._write()`.
       *
       * @param {Buffer} chunk The chunk of data to write
       * @param {String} encoding The character encoding of `chunk`
       * @param {Function} cb Callback
       * @private
       */
      _write(chunk, encoding, cb) {
        if (this._opcode === 8 && this._state == GET_INFO) return cb();
        if (this._maxBufferedChunks > 0 && this._buffers.length >= this._maxBufferedChunks) {
          cb(
            this.createError(
              RangeError,
              "Too many buffered chunks",
              false,
              1008,
              "WS_ERR_TOO_MANY_BUFFERED_PARTS"
            )
          );
          return;
        }
        this._bufferedBytes += chunk.length;
        this._buffers.push(chunk);
        this.startLoop(cb);
      }
      /**
       * Consumes `n` bytes from the buffered data.
       *
       * @param {Number} n The number of bytes to consume
       * @return {Buffer} The consumed bytes
       * @private
       */
      consume(n) {
        this._bufferedBytes -= n;
        if (n === this._buffers[0].length) return this._buffers.shift();
        if (n < this._buffers[0].length) {
          const buf = this._buffers[0];
          this._buffers[0] = new FastBuffer(
            buf.buffer,
            buf.byteOffset + n,
            buf.length - n
          );
          return new FastBuffer(buf.buffer, buf.byteOffset, n);
        }
        const dst = Buffer.allocUnsafe(n);
        do {
          const buf = this._buffers[0];
          const offset = dst.length - n;
          if (n >= buf.length) {
            dst.set(this._buffers.shift(), offset);
          } else {
            dst.set(new Uint8Array(buf.buffer, buf.byteOffset, n), offset);
            this._buffers[0] = new FastBuffer(
              buf.buffer,
              buf.byteOffset + n,
              buf.length - n
            );
          }
          n -= buf.length;
        } while (n > 0);
        return dst;
      }
      /**
       * Starts the parsing loop.
       *
       * @param {Function} cb Callback
       * @private
       */
      startLoop(cb) {
        this._loop = true;
        do {
          switch (this._state) {
            case GET_INFO:
              this.getInfo(cb);
              break;
            case GET_PAYLOAD_LENGTH_16:
              this.getPayloadLength16(cb);
              break;
            case GET_PAYLOAD_LENGTH_64:
              this.getPayloadLength64(cb);
              break;
            case GET_MASK:
              this.getMask();
              break;
            case GET_DATA:
              this.getData(cb);
              break;
            case INFLATING:
            case DEFER_EVENT:
              this._loop = false;
              return;
          }
        } while (this._loop);
        if (!this._errored) cb();
      }
      /**
       * Reads the first two bytes of a frame.
       *
       * @param {Function} cb Callback
       * @private
       */
      getInfo(cb) {
        if (this._bufferedBytes < 2) {
          this._loop = false;
          return;
        }
        const buf = this.consume(2);
        if ((buf[0] & 48) !== 0) {
          const error = this.createError(
            RangeError,
            "RSV2 and RSV3 must be clear",
            true,
            1002,
            "WS_ERR_UNEXPECTED_RSV_2_3"
          );
          cb(error);
          return;
        }
        const compressed = (buf[0] & 64) === 64;
        if (compressed && !this._extensions[PerMessageDeflate2.extensionName]) {
          const error = this.createError(
            RangeError,
            "RSV1 must be clear",
            true,
            1002,
            "WS_ERR_UNEXPECTED_RSV_1"
          );
          cb(error);
          return;
        }
        this._fin = (buf[0] & 128) === 128;
        this._opcode = buf[0] & 15;
        this._payloadLength = buf[1] & 127;
        if (this._opcode === 0) {
          if (compressed) {
            const error = this.createError(
              RangeError,
              "RSV1 must be clear",
              true,
              1002,
              "WS_ERR_UNEXPECTED_RSV_1"
            );
            cb(error);
            return;
          }
          if (!this._fragmented) {
            const error = this.createError(
              RangeError,
              "invalid opcode 0",
              true,
              1002,
              "WS_ERR_INVALID_OPCODE"
            );
            cb(error);
            return;
          }
          this._opcode = this._fragmented;
        } else if (this._opcode === 1 || this._opcode === 2) {
          if (this._fragmented) {
            const error = this.createError(
              RangeError,
              `invalid opcode ${this._opcode}`,
              true,
              1002,
              "WS_ERR_INVALID_OPCODE"
            );
            cb(error);
            return;
          }
          this._compressed = compressed;
        } else if (this._opcode > 7 && this._opcode < 11) {
          if (!this._fin) {
            const error = this.createError(
              RangeError,
              "FIN must be set",
              true,
              1002,
              "WS_ERR_EXPECTED_FIN"
            );
            cb(error);
            return;
          }
          if (compressed) {
            const error = this.createError(
              RangeError,
              "RSV1 must be clear",
              true,
              1002,
              "WS_ERR_UNEXPECTED_RSV_1"
            );
            cb(error);
            return;
          }
          if (this._payloadLength > 125 || this._opcode === 8 && this._payloadLength === 1) {
            const error = this.createError(
              RangeError,
              `invalid payload length ${this._payloadLength}`,
              true,
              1002,
              "WS_ERR_INVALID_CONTROL_PAYLOAD_LENGTH"
            );
            cb(error);
            return;
          }
        } else {
          const error = this.createError(
            RangeError,
            `invalid opcode ${this._opcode}`,
            true,
            1002,
            "WS_ERR_INVALID_OPCODE"
          );
          cb(error);
          return;
        }
        if (!this._fin && !this._fragmented) this._fragmented = this._opcode;
        this._masked = (buf[1] & 128) === 128;
        if (this._isServer) {
          if (!this._masked) {
            const error = this.createError(
              RangeError,
              "MASK must be set",
              true,
              1002,
              "WS_ERR_EXPECTED_MASK"
            );
            cb(error);
            return;
          }
        } else if (this._masked) {
          const error = this.createError(
            RangeError,
            "MASK must be clear",
            true,
            1002,
            "WS_ERR_UNEXPECTED_MASK"
          );
          cb(error);
          return;
        }
        if (this._payloadLength === 126) this._state = GET_PAYLOAD_LENGTH_16;
        else if (this._payloadLength === 127) this._state = GET_PAYLOAD_LENGTH_64;
        else this.haveLength(cb);
      }
      /**
       * Gets extended payload length (7+16).
       *
       * @param {Function} cb Callback
       * @private
       */
      getPayloadLength16(cb) {
        if (this._bufferedBytes < 2) {
          this._loop = false;
          return;
        }
        this._payloadLength = this.consume(2).readUInt16BE(0);
        this.haveLength(cb);
      }
      /**
       * Gets extended payload length (7+64).
       *
       * @param {Function} cb Callback
       * @private
       */
      getPayloadLength64(cb) {
        if (this._bufferedBytes < 8) {
          this._loop = false;
          return;
        }
        const buf = this.consume(8);
        const num2 = buf.readUInt32BE(0);
        if (num2 > Math.pow(2, 53 - 32) - 1) {
          const error = this.createError(
            RangeError,
            "Unsupported WebSocket frame: payload length > 2^53 - 1",
            false,
            1009,
            "WS_ERR_UNSUPPORTED_DATA_PAYLOAD_LENGTH"
          );
          cb(error);
          return;
        }
        this._payloadLength = num2 * Math.pow(2, 32) + buf.readUInt32BE(4);
        this.haveLength(cb);
      }
      /**
       * Payload length has been read.
       *
       * @param {Function} cb Callback
       * @private
       */
      haveLength(cb) {
        if (this._payloadLength && this._opcode < 8) {
          this._totalPayloadLength += this._payloadLength;
          if (this._totalPayloadLength > this._maxPayload && this._maxPayload > 0) {
            const error = this.createError(
              RangeError,
              "Max payload size exceeded",
              false,
              1009,
              "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH"
            );
            cb(error);
            return;
          }
        }
        if (this._masked) this._state = GET_MASK;
        else this._state = GET_DATA;
      }
      /**
       * Reads mask bytes.
       *
       * @private
       */
      getMask() {
        if (this._bufferedBytes < 4) {
          this._loop = false;
          return;
        }
        this._mask = this.consume(4);
        this._state = GET_DATA;
      }
      /**
       * Reads data bytes.
       *
       * @param {Function} cb Callback
       * @private
       */
      getData(cb) {
        let data = EMPTY_BUFFER;
        if (this._payloadLength) {
          if (this._bufferedBytes < this._payloadLength) {
            this._loop = false;
            return;
          }
          data = this.consume(this._payloadLength);
          if (this._masked && (this._mask[0] | this._mask[1] | this._mask[2] | this._mask[3]) !== 0) {
            unmask(data, this._mask);
          }
        }
        if (this._opcode > 7) {
          this.controlMessage(data, cb);
          return;
        }
        if (this._maxFragments > 0 && ++this._numFragments > this._maxFragments) {
          const error = this.createError(
            RangeError,
            "Too many message fragments",
            false,
            1008,
            "WS_ERR_TOO_MANY_BUFFERED_PARTS"
          );
          cb(error);
          return;
        }
        if (this._compressed) {
          this._state = INFLATING;
          this.decompress(data, cb);
          return;
        }
        if (data.length) {
          this._messageLength = this._totalPayloadLength;
          this._fragments.push(data);
        }
        this.dataMessage(cb);
      }
      /**
       * Decompresses data.
       *
       * @param {Buffer} data Compressed data
       * @param {Function} cb Callback
       * @private
       */
      decompress(data, cb) {
        const perMessageDeflate = this._extensions[PerMessageDeflate2.extensionName];
        perMessageDeflate.decompress(data, this._fin, (err, buf) => {
          if (err) return cb(err);
          if (buf.length) {
            this._messageLength += buf.length;
            if (this._messageLength > this._maxPayload && this._maxPayload > 0) {
              const error = this.createError(
                RangeError,
                "Max payload size exceeded",
                false,
                1009,
                "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH"
              );
              cb(error);
              return;
            }
            this._fragments.push(buf);
          }
          this.dataMessage(cb);
          if (this._state === GET_INFO) this.startLoop(cb);
        });
      }
      /**
       * Handles a data message.
       *
       * @param {Function} cb Callback
       * @private
       */
      dataMessage(cb) {
        if (!this._fin) {
          this._state = GET_INFO;
          return;
        }
        const messageLength = this._messageLength;
        const fragments = this._fragments;
        this._totalPayloadLength = 0;
        this._messageLength = 0;
        this._fragmented = 0;
        this._numFragments = 0;
        this._fragments = [];
        if (this._opcode === 2) {
          let data;
          if (this._binaryType === "nodebuffer") {
            data = concat(fragments, messageLength);
          } else if (this._binaryType === "arraybuffer") {
            data = toArrayBuffer(concat(fragments, messageLength));
          } else if (this._binaryType === "blob") {
            data = new Blob(fragments);
          } else {
            data = fragments;
          }
          if (this._allowSynchronousEvents) {
            this.emit("message", data, true);
            this._state = GET_INFO;
          } else {
            this._state = DEFER_EVENT;
            setImmediate(() => {
              this.emit("message", data, true);
              this._state = GET_INFO;
              this.startLoop(cb);
            });
          }
        } else {
          const buf = concat(fragments, messageLength);
          if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
            const error = this.createError(
              Error,
              "invalid UTF-8 sequence",
              true,
              1007,
              "WS_ERR_INVALID_UTF8"
            );
            cb(error);
            return;
          }
          if (this._state === INFLATING || this._allowSynchronousEvents) {
            this.emit("message", buf, false);
            this._state = GET_INFO;
          } else {
            this._state = DEFER_EVENT;
            setImmediate(() => {
              this.emit("message", buf, false);
              this._state = GET_INFO;
              this.startLoop(cb);
            });
          }
        }
      }
      /**
       * Handles a control message.
       *
       * @param {Buffer} data Data to handle
       * @return {(Error|RangeError|undefined)} A possible error
       * @private
       */
      controlMessage(data, cb) {
        if (this._opcode === 8) {
          if (data.length === 0) {
            this._loop = false;
            this.emit("conclude", 1005, EMPTY_BUFFER);
            this.end();
          } else {
            const code = data.readUInt16BE(0);
            if (!isValidStatusCode(code)) {
              const error = this.createError(
                RangeError,
                `invalid status code ${code}`,
                true,
                1002,
                "WS_ERR_INVALID_CLOSE_CODE"
              );
              cb(error);
              return;
            }
            const buf = new FastBuffer(
              data.buffer,
              data.byteOffset + 2,
              data.length - 2
            );
            if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
              const error = this.createError(
                Error,
                "invalid UTF-8 sequence",
                true,
                1007,
                "WS_ERR_INVALID_UTF8"
              );
              cb(error);
              return;
            }
            this._loop = false;
            this.emit("conclude", code, buf);
            this.end();
          }
          this._state = GET_INFO;
          return;
        }
        if (this._allowSynchronousEvents) {
          this.emit(this._opcode === 9 ? "ping" : "pong", data);
          this._state = GET_INFO;
        } else {
          this._state = DEFER_EVENT;
          setImmediate(() => {
            this.emit(this._opcode === 9 ? "ping" : "pong", data);
            this._state = GET_INFO;
            this.startLoop(cb);
          });
        }
      }
      /**
       * Builds an error object.
       *
       * @param {function(new:Error|RangeError)} ErrorCtor The error constructor
       * @param {String} message The error message
       * @param {Boolean} prefix Specifies whether or not to add a default prefix to
       *     `message`
       * @param {Number} statusCode The status code
       * @param {String} errorCode The exposed error code
       * @return {(Error|RangeError)} The error
       * @private
       */
      createError(ErrorCtor, message, prefix, statusCode, errorCode) {
        this._loop = false;
        this._errored = true;
        const err = new ErrorCtor(
          prefix ? `Invalid WebSocket frame: ${message}` : message
        );
        Error.captureStackTrace(err, this.createError);
        err.code = errorCode;
        err[kStatusCode] = statusCode;
        return err;
      }
    };
    module.exports = Receiver2;
  }
});

// node_modules/ws/lib/sender.js
var require_sender = __commonJS({
  "node_modules/ws/lib/sender.js"(exports, module) {
    "use strict";
    var { Duplex } = __require("stream");
    var { randomFillSync } = __require("crypto");
    var {
      types: { isUint8Array }
    } = __require("util");
    var PerMessageDeflate2 = require_permessage_deflate();
    var { EMPTY_BUFFER, kWebSocket, NOOP } = require_constants();
    var { isBlob, isValidStatusCode } = require_validation();
    var { mask: applyMask, toBuffer } = require_buffer_util();
    var kByteLength = /* @__PURE__ */ Symbol("kByteLength");
    var maskBuffer = Buffer.alloc(4);
    var RANDOM_POOL_SIZE = 8 * 1024;
    var randomPool;
    var randomPoolPointer = RANDOM_POOL_SIZE;
    var DEFAULT = 0;
    var DEFLATING = 1;
    var GET_BLOB_DATA = 2;
    var Sender2 = class _Sender {
      /**
       * Creates a Sender instance.
       *
       * @param {Duplex} socket The connection socket
       * @param {Object} [extensions] An object containing the negotiated extensions
       * @param {Function} [generateMask] The function used to generate the masking
       *     key
       */
      constructor(socket, extensions, generateMask) {
        this._extensions = extensions || {};
        if (generateMask) {
          this._generateMask = generateMask;
          this._maskBuffer = Buffer.alloc(4);
        }
        this._socket = socket;
        this._firstFragment = true;
        this._compress = false;
        this._bufferedBytes = 0;
        this._queue = [];
        this._state = DEFAULT;
        this.onerror = NOOP;
        this[kWebSocket] = void 0;
      }
      /**
       * Frames a piece of data according to the HyBi WebSocket protocol.
       *
       * @param {(Buffer|String)} data The data to frame
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @return {(Buffer|String)[]} The framed data
       * @public
       */
      static frame(data, options) {
        let mask;
        let merge = false;
        let offset = 2;
        let skipMasking = false;
        if (options.mask) {
          mask = options.maskBuffer || maskBuffer;
          if (options.generateMask) {
            options.generateMask(mask);
          } else {
            if (randomPoolPointer === RANDOM_POOL_SIZE) {
              if (randomPool === void 0) {
                randomPool = Buffer.alloc(RANDOM_POOL_SIZE);
              }
              randomFillSync(randomPool, 0, RANDOM_POOL_SIZE);
              randomPoolPointer = 0;
            }
            mask[0] = randomPool[randomPoolPointer++];
            mask[1] = randomPool[randomPoolPointer++];
            mask[2] = randomPool[randomPoolPointer++];
            mask[3] = randomPool[randomPoolPointer++];
          }
          skipMasking = (mask[0] | mask[1] | mask[2] | mask[3]) === 0;
          offset = 6;
        }
        let dataLength;
        if (typeof data === "string") {
          if ((!options.mask || skipMasking) && options[kByteLength] !== void 0) {
            dataLength = options[kByteLength];
          } else {
            data = Buffer.from(data);
            dataLength = data.length;
          }
        } else {
          dataLength = data.length;
          merge = options.mask && options.readOnly && !skipMasking;
        }
        let payloadLength = dataLength;
        if (dataLength >= 65536) {
          offset += 8;
          payloadLength = 127;
        } else if (dataLength > 125) {
          offset += 2;
          payloadLength = 126;
        }
        const target = Buffer.allocUnsafe(merge ? dataLength + offset : offset);
        target[0] = options.fin ? options.opcode | 128 : options.opcode;
        if (options.rsv1) target[0] |= 64;
        target[1] = payloadLength;
        if (payloadLength === 126) {
          target.writeUInt16BE(dataLength, 2);
        } else if (payloadLength === 127) {
          target[2] = target[3] = 0;
          target.writeUIntBE(dataLength, 4, 6);
        }
        if (!options.mask) return [target, data];
        target[1] |= 128;
        target[offset - 4] = mask[0];
        target[offset - 3] = mask[1];
        target[offset - 2] = mask[2];
        target[offset - 1] = mask[3];
        if (skipMasking) return [target, data];
        if (merge) {
          applyMask(data, mask, target, offset, dataLength);
          return [target];
        }
        applyMask(data, mask, data, 0, dataLength);
        return [target, data];
      }
      /**
       * Sends a close message to the other peer.
       *
       * @param {Number} [code] The status code component of the body
       * @param {(String|Buffer)} [data] The message component of the body
       * @param {Boolean} [mask=false] Specifies whether or not to mask the message
       * @param {Function} [cb] Callback
       * @public
       */
      close(code, data, mask, cb) {
        let buf;
        if (code === void 0) {
          buf = EMPTY_BUFFER;
        } else if (typeof code !== "number" || !isValidStatusCode(code)) {
          throw new TypeError("First argument must be a valid error code number");
        } else if (data === void 0 || !data.length) {
          buf = Buffer.allocUnsafe(2);
          buf.writeUInt16BE(code, 0);
        } else {
          const length = Buffer.byteLength(data);
          if (length > 123) {
            throw new RangeError("The message must not be greater than 123 bytes");
          }
          buf = Buffer.allocUnsafe(2 + length);
          buf.writeUInt16BE(code, 0);
          if (typeof data === "string") {
            buf.write(data, 2);
          } else if (isUint8Array(data)) {
            buf.set(data, 2);
          } else {
            throw new TypeError("Second argument must be a string or a Uint8Array");
          }
        }
        const options = {
          [kByteLength]: buf.length,
          fin: true,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 8,
          readOnly: false,
          rsv1: false
        };
        if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, buf, false, options, cb]);
        } else {
          this.sendFrame(_Sender.frame(buf, options), cb);
        }
      }
      /**
       * Sends a ping message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Boolean} [mask=false] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback
       * @public
       */
      ping(data, mask, cb) {
        let byteLength;
        let readOnly;
        if (typeof data === "string") {
          byteLength = Buffer.byteLength(data);
          readOnly = false;
        } else if (isBlob(data)) {
          byteLength = data.size;
          readOnly = false;
        } else {
          data = toBuffer(data);
          byteLength = data.length;
          readOnly = toBuffer.readOnly;
        }
        if (byteLength > 125) {
          throw new RangeError("The data size must not be greater than 125 bytes");
        }
        const options = {
          [kByteLength]: byteLength,
          fin: true,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 9,
          readOnly,
          rsv1: false
        };
        if (isBlob(data)) {
          if (this._state !== DEFAULT) {
            this.enqueue([this.getBlobData, data, false, options, cb]);
          } else {
            this.getBlobData(data, false, options, cb);
          }
        } else if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, data, false, options, cb]);
        } else {
          this.sendFrame(_Sender.frame(data, options), cb);
        }
      }
      /**
       * Sends a pong message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Boolean} [mask=false] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback
       * @public
       */
      pong(data, mask, cb) {
        let byteLength;
        let readOnly;
        if (typeof data === "string") {
          byteLength = Buffer.byteLength(data);
          readOnly = false;
        } else if (isBlob(data)) {
          byteLength = data.size;
          readOnly = false;
        } else {
          data = toBuffer(data);
          byteLength = data.length;
          readOnly = toBuffer.readOnly;
        }
        if (byteLength > 125) {
          throw new RangeError("The data size must not be greater than 125 bytes");
        }
        const options = {
          [kByteLength]: byteLength,
          fin: true,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 10,
          readOnly,
          rsv1: false
        };
        if (isBlob(data)) {
          if (this._state !== DEFAULT) {
            this.enqueue([this.getBlobData, data, false, options, cb]);
          } else {
            this.getBlobData(data, false, options, cb);
          }
        } else if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, data, false, options, cb]);
        } else {
          this.sendFrame(_Sender.frame(data, options), cb);
        }
      }
      /**
       * Sends a data message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Object} options Options object
       * @param {Boolean} [options.binary=false] Specifies whether `data` is binary
       *     or text
       * @param {Boolean} [options.compress=false] Specifies whether or not to
       *     compress `data`
       * @param {Boolean} [options.fin=false] Specifies whether the fragment is the
       *     last one
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Function} [cb] Callback
       * @public
       */
      send(data, options, cb) {
        const perMessageDeflate = this._extensions[PerMessageDeflate2.extensionName];
        let opcode = options.binary ? 2 : 1;
        let rsv1 = options.compress;
        let byteLength;
        let readOnly;
        if (typeof data === "string") {
          byteLength = Buffer.byteLength(data);
          readOnly = false;
        } else if (isBlob(data)) {
          byteLength = data.size;
          readOnly = false;
        } else {
          data = toBuffer(data);
          byteLength = data.length;
          readOnly = toBuffer.readOnly;
        }
        if (this._firstFragment) {
          this._firstFragment = false;
          if (rsv1 && perMessageDeflate && perMessageDeflate.params[perMessageDeflate._isServer ? "server_no_context_takeover" : "client_no_context_takeover"]) {
            rsv1 = byteLength >= perMessageDeflate._threshold;
          }
          this._compress = rsv1;
        } else {
          rsv1 = false;
          opcode = 0;
        }
        if (options.fin) this._firstFragment = true;
        const opts = {
          [kByteLength]: byteLength,
          fin: options.fin,
          generateMask: this._generateMask,
          mask: options.mask,
          maskBuffer: this._maskBuffer,
          opcode,
          readOnly,
          rsv1
        };
        if (isBlob(data)) {
          if (this._state !== DEFAULT) {
            this.enqueue([this.getBlobData, data, this._compress, opts, cb]);
          } else {
            this.getBlobData(data, this._compress, opts, cb);
          }
        } else if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, data, this._compress, opts, cb]);
        } else {
          this.dispatch(data, this._compress, opts, cb);
        }
      }
      /**
       * Gets the contents of a blob as binary data.
       *
       * @param {Blob} blob The blob
       * @param {Boolean} [compress=false] Specifies whether or not to compress
       *     the data
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @param {Function} [cb] Callback
       * @private
       */
      getBlobData(blob, compress, options, cb) {
        this._bufferedBytes += options[kByteLength];
        this._state = GET_BLOB_DATA;
        blob.arrayBuffer().then((arrayBuffer) => {
          if (this._socket.destroyed) {
            const err = new Error(
              "The socket was closed while the blob was being read"
            );
            process.nextTick(callCallbacks, this, err, cb);
            return;
          }
          this._bufferedBytes -= options[kByteLength];
          const data = toBuffer(arrayBuffer);
          if (!compress) {
            this._state = DEFAULT;
            this.sendFrame(_Sender.frame(data, options), cb);
            this.dequeue();
          } else {
            this.dispatch(data, compress, options, cb);
          }
        }).catch((err) => {
          process.nextTick(onError, this, err, cb);
        });
      }
      /**
       * Dispatches a message.
       *
       * @param {(Buffer|String)} data The message to send
       * @param {Boolean} [compress=false] Specifies whether or not to compress
       *     `data`
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @param {Function} [cb] Callback
       * @private
       */
      dispatch(data, compress, options, cb) {
        if (!compress) {
          this.sendFrame(_Sender.frame(data, options), cb);
          return;
        }
        const perMessageDeflate = this._extensions[PerMessageDeflate2.extensionName];
        this._bufferedBytes += options[kByteLength];
        this._state = DEFLATING;
        perMessageDeflate.compress(data, options.fin, (_, buf) => {
          if (this._socket.destroyed) {
            const err = new Error(
              "The socket was closed while data was being compressed"
            );
            callCallbacks(this, err, cb);
            return;
          }
          this._bufferedBytes -= options[kByteLength];
          this._state = DEFAULT;
          options.readOnly = false;
          this.sendFrame(_Sender.frame(buf, options), cb);
          this.dequeue();
        });
      }
      /**
       * Executes queued send operations.
       *
       * @private
       */
      dequeue() {
        while (this._state === DEFAULT && this._queue.length) {
          const params = this._queue.shift();
          this._bufferedBytes -= params[3][kByteLength];
          Reflect.apply(params[0], this, params.slice(1));
        }
      }
      /**
       * Enqueues a send operation.
       *
       * @param {Array} params Send operation parameters.
       * @private
       */
      enqueue(params) {
        this._bufferedBytes += params[3][kByteLength];
        this._queue.push(params);
      }
      /**
       * Sends a frame.
       *
       * @param {(Buffer | String)[]} list The frame to send
       * @param {Function} [cb] Callback
       * @private
       */
      sendFrame(list, cb) {
        if (list.length === 2) {
          this._socket.cork();
          this._socket.write(list[0]);
          this._socket.write(list[1], cb);
          this._socket.uncork();
        } else {
          this._socket.write(list[0], cb);
        }
      }
    };
    module.exports = Sender2;
    function callCallbacks(sender, err, cb) {
      if (typeof cb === "function") cb(err);
      for (let i = 0; i < sender._queue.length; i++) {
        const params = sender._queue[i];
        const callback = params[params.length - 1];
        if (typeof callback === "function") callback(err);
      }
    }
    function onError(sender, err, cb) {
      callCallbacks(sender, err, cb);
      sender.onerror(err);
    }
  }
});

// node_modules/ws/lib/event-target.js
var require_event_target = __commonJS({
  "node_modules/ws/lib/event-target.js"(exports, module) {
    "use strict";
    var { kForOnEventAttribute, kListener } = require_constants();
    var kCode = /* @__PURE__ */ Symbol("kCode");
    var kData = /* @__PURE__ */ Symbol("kData");
    var kError = /* @__PURE__ */ Symbol("kError");
    var kMessage = /* @__PURE__ */ Symbol("kMessage");
    var kReason = /* @__PURE__ */ Symbol("kReason");
    var kTarget = /* @__PURE__ */ Symbol("kTarget");
    var kType = /* @__PURE__ */ Symbol("kType");
    var kWasClean = /* @__PURE__ */ Symbol("kWasClean");
    var Event = class {
      /**
       * Create a new `Event`.
       *
       * @param {String} type The name of the event
       * @throws {TypeError} If the `type` argument is not specified
       */
      constructor(type) {
        this[kTarget] = null;
        this[kType] = type;
      }
      /**
       * @type {*}
       */
      get target() {
        return this[kTarget];
      }
      /**
       * @type {String}
       */
      get type() {
        return this[kType];
      }
    };
    Object.defineProperty(Event.prototype, "target", { enumerable: true });
    Object.defineProperty(Event.prototype, "type", { enumerable: true });
    var CloseEvent = class extends Event {
      /**
       * Create a new `CloseEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {Number} [options.code=0] The status code explaining why the
       *     connection was closed
       * @param {String} [options.reason=''] A human-readable string explaining why
       *     the connection was closed
       * @param {Boolean} [options.wasClean=false] Indicates whether or not the
       *     connection was cleanly closed
       */
      constructor(type, options = {}) {
        super(type);
        this[kCode] = options.code === void 0 ? 0 : options.code;
        this[kReason] = options.reason === void 0 ? "" : options.reason;
        this[kWasClean] = options.wasClean === void 0 ? false : options.wasClean;
      }
      /**
       * @type {Number}
       */
      get code() {
        return this[kCode];
      }
      /**
       * @type {String}
       */
      get reason() {
        return this[kReason];
      }
      /**
       * @type {Boolean}
       */
      get wasClean() {
        return this[kWasClean];
      }
    };
    Object.defineProperty(CloseEvent.prototype, "code", { enumerable: true });
    Object.defineProperty(CloseEvent.prototype, "reason", { enumerable: true });
    Object.defineProperty(CloseEvent.prototype, "wasClean", { enumerable: true });
    var ErrorEvent = class extends Event {
      /**
       * Create a new `ErrorEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {*} [options.error=null] The error that generated this event
       * @param {String} [options.message=''] The error message
       */
      constructor(type, options = {}) {
        super(type);
        this[kError] = options.error === void 0 ? null : options.error;
        this[kMessage] = options.message === void 0 ? "" : options.message;
      }
      /**
       * @type {*}
       */
      get error() {
        return this[kError];
      }
      /**
       * @type {String}
       */
      get message() {
        return this[kMessage];
      }
    };
    Object.defineProperty(ErrorEvent.prototype, "error", { enumerable: true });
    Object.defineProperty(ErrorEvent.prototype, "message", { enumerable: true });
    var MessageEvent = class extends Event {
      /**
       * Create a new `MessageEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {*} [options.data=null] The message content
       */
      constructor(type, options = {}) {
        super(type);
        this[kData] = options.data === void 0 ? null : options.data;
      }
      /**
       * @type {*}
       */
      get data() {
        return this[kData];
      }
    };
    Object.defineProperty(MessageEvent.prototype, "data", { enumerable: true });
    var EventTarget = {
      /**
       * Register an event listener.
       *
       * @param {String} type A string representing the event type to listen for
       * @param {(Function|Object)} handler The listener to add
       * @param {Object} [options] An options object specifies characteristics about
       *     the event listener
       * @param {Boolean} [options.once=false] A `Boolean` indicating that the
       *     listener should be invoked at most once after being added. If `true`,
       *     the listener would be automatically removed when invoked.
       * @public
       */
      addEventListener(type, handler, options = {}) {
        for (const listener of this.listeners(type)) {
          if (!options[kForOnEventAttribute] && listener[kListener] === handler && !listener[kForOnEventAttribute]) {
            return;
          }
        }
        let wrapper;
        if (type === "message") {
          wrapper = function onMessage(data, isBinary) {
            const event = new MessageEvent("message", {
              data: isBinary ? data : data.toString()
            });
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else if (type === "close") {
          wrapper = function onClose(code, message) {
            const event = new CloseEvent("close", {
              code,
              reason: message.toString(),
              wasClean: this._closeFrameReceived && this._closeFrameSent
            });
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else if (type === "error") {
          wrapper = function onError(error) {
            const event = new ErrorEvent("error", {
              error,
              message: error.message
            });
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else if (type === "open") {
          wrapper = function onOpen() {
            const event = new Event("open");
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else {
          return;
        }
        wrapper[kForOnEventAttribute] = !!options[kForOnEventAttribute];
        wrapper[kListener] = handler;
        if (options.once) {
          this.once(type, wrapper);
        } else {
          this.on(type, wrapper);
        }
      },
      /**
       * Remove an event listener.
       *
       * @param {String} type A string representing the event type to remove
       * @param {(Function|Object)} handler The listener to remove
       * @public
       */
      removeEventListener(type, handler) {
        for (const listener of this.listeners(type)) {
          if (listener[kListener] === handler && !listener[kForOnEventAttribute]) {
            this.removeListener(type, listener);
            break;
          }
        }
      }
    };
    module.exports = {
      CloseEvent,
      ErrorEvent,
      Event,
      EventTarget,
      MessageEvent
    };
    function callListener(listener, thisArg, event) {
      if (typeof listener === "object" && listener.handleEvent) {
        listener.handleEvent.call(listener, event);
      } else {
        listener.call(thisArg, event);
      }
    }
  }
});

// node_modules/ws/lib/extension.js
var require_extension = __commonJS({
  "node_modules/ws/lib/extension.js"(exports, module) {
    "use strict";
    var { tokenChars } = require_validation();
    function push(dest, name, elem) {
      if (dest[name] === void 0) dest[name] = [elem];
      else dest[name].push(elem);
    }
    function parse(header) {
      const offers = /* @__PURE__ */ Object.create(null);
      let params = /* @__PURE__ */ Object.create(null);
      let mustUnescape = false;
      let isEscaping = false;
      let inQuotes = false;
      let extensionName;
      let paramName;
      let start = -1;
      let code = -1;
      let end = -1;
      let i = 0;
      for (; i < header.length; i++) {
        code = header.charCodeAt(i);
        if (extensionName === void 0) {
          if (end === -1 && tokenChars[code] === 1) {
            if (start === -1) start = i;
          } else if (i !== 0 && (code === 32 || code === 9)) {
            if (end === -1 && start !== -1) end = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (end === -1) end = i;
            const name = header.slice(start, end);
            if (code === 44) {
              push(offers, name, params);
              params = /* @__PURE__ */ Object.create(null);
            } else {
              extensionName = name;
            }
            start = end = -1;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        } else if (paramName === void 0) {
          if (end === -1 && tokenChars[code] === 1) {
            if (start === -1) start = i;
          } else if (code === 32 || code === 9) {
            if (end === -1 && start !== -1) end = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (end === -1) end = i;
            push(params, header.slice(start, end), true);
            if (code === 44) {
              push(offers, extensionName, params);
              params = /* @__PURE__ */ Object.create(null);
              extensionName = void 0;
            }
            start = end = -1;
          } else if (code === 61 && start !== -1 && end === -1) {
            paramName = header.slice(start, i);
            start = end = -1;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        } else {
          if (isEscaping) {
            if (tokenChars[code] !== 1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (start === -1) start = i;
            else if (!mustUnescape) mustUnescape = true;
            isEscaping = false;
          } else if (inQuotes) {
            if (tokenChars[code] === 1) {
              if (start === -1) start = i;
            } else if (code === 34 && start !== -1) {
              inQuotes = false;
              end = i;
            } else if (code === 92) {
              isEscaping = true;
            } else {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
          } else if (code === 34 && header.charCodeAt(i - 1) === 61) {
            inQuotes = true;
          } else if (end === -1 && tokenChars[code] === 1) {
            if (start === -1) start = i;
          } else if (start !== -1 && (code === 32 || code === 9)) {
            if (end === -1) end = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (end === -1) end = i;
            let value = header.slice(start, end);
            if (mustUnescape) {
              value = value.replace(/\\/g, "");
              mustUnescape = false;
            }
            push(params, paramName, value);
            if (code === 44) {
              push(offers, extensionName, params);
              params = /* @__PURE__ */ Object.create(null);
              extensionName = void 0;
            }
            paramName = void 0;
            start = end = -1;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        }
      }
      if (start === -1 || inQuotes || code === 32 || code === 9) {
        throw new SyntaxError("Unexpected end of input");
      }
      if (end === -1) end = i;
      const token = header.slice(start, end);
      if (extensionName === void 0) {
        push(offers, token, params);
      } else {
        if (paramName === void 0) {
          push(params, token, true);
        } else if (mustUnescape) {
          push(params, paramName, token.replace(/\\/g, ""));
        } else {
          push(params, paramName, token);
        }
        push(offers, extensionName, params);
      }
      return offers;
    }
    function format(extensions) {
      return Object.keys(extensions).map((extension2) => {
        let configurations = extensions[extension2];
        if (!Array.isArray(configurations)) configurations = [configurations];
        return configurations.map((params) => {
          return [extension2].concat(
            Object.keys(params).map((k) => {
              let values = params[k];
              if (!Array.isArray(values)) values = [values];
              return values.map((v) => v === true ? k : `${k}=${v}`).join("; ");
            })
          ).join("; ");
        }).join(", ");
      }).join(", ");
    }
    module.exports = { format, parse };
  }
});

// node_modules/ws/lib/websocket.js
var require_websocket = __commonJS({
  "node_modules/ws/lib/websocket.js"(exports, module) {
    "use strict";
    var EventEmitter = __require("events");
    var https = __require("https");
    var http = __require("http");
    var net = __require("net");
    var tls = __require("tls");
    var { randomBytes, createHash } = __require("crypto");
    var { Duplex, Readable } = __require("stream");
    var { URL: URL2 } = __require("url");
    var PerMessageDeflate2 = require_permessage_deflate();
    var Receiver2 = require_receiver();
    var Sender2 = require_sender();
    var { isBlob } = require_validation();
    var {
      BINARY_TYPES,
      CLOSE_TIMEOUT,
      EMPTY_BUFFER,
      GUID,
      kForOnEventAttribute,
      kListener,
      kStatusCode,
      kWebSocket,
      NOOP
    } = require_constants();
    var {
      EventTarget: { addEventListener, removeEventListener }
    } = require_event_target();
    var { format, parse } = require_extension();
    var { toBuffer } = require_buffer_util();
    var kAborted = /* @__PURE__ */ Symbol("kAborted");
    var protocolVersions = [8, 13];
    var readyStates = ["CONNECTING", "OPEN", "CLOSING", "CLOSED"];
    var subprotocolRegex = /^[!#$%&'*+\-.0-9A-Z^_`|a-z~]+$/;
    var WebSocket2 = class _WebSocket extends EventEmitter {
      /**
       * Create a new `WebSocket`.
       *
       * @param {(String|URL)} address The URL to which to connect
       * @param {(String|String[])} [protocols] The subprotocols
       * @param {Object} [options] Connection options
       */
      constructor(address, protocols, options) {
        super();
        this._binaryType = BINARY_TYPES[0];
        this._closeCode = 1006;
        this._closeFrameReceived = false;
        this._closeFrameSent = false;
        this._closeMessage = EMPTY_BUFFER;
        this._closeTimer = null;
        this._errorEmitted = false;
        this._extensions = {};
        this._paused = false;
        this._protocol = "";
        this._readyState = _WebSocket.CONNECTING;
        this._receiver = null;
        this._sender = null;
        this._socket = null;
        if (address !== null) {
          this._bufferedAmount = 0;
          this._isServer = false;
          this._redirects = 0;
          if (protocols === void 0) {
            if (!options || options.protocols === void 0) {
              protocols = [];
            } else if (Array.isArray(options.protocols)) {
              protocols = options.protocols;
            } else {
              protocols = [options.protocols];
            }
          } else if (!Array.isArray(protocols)) {
            if (typeof protocols === "object" && protocols !== null) {
              options = protocols;
              if (options.protocols === void 0) {
                protocols = [];
              } else if (Array.isArray(options.protocols)) {
                protocols = options.protocols;
              } else {
                protocols = [options.protocols];
              }
            } else {
              protocols = [protocols];
            }
          }
          initAsClient(this, address, protocols, options);
        } else {
          this._autoPong = options.autoPong;
          this._closeTimeout = options.closeTimeout;
          this._isServer = true;
        }
      }
      /**
       * For historical reasons, the custom "nodebuffer" type is used by the default
       * instead of "blob".
       *
       * @type {String}
       */
      get binaryType() {
        return this._binaryType;
      }
      set binaryType(type) {
        if (!BINARY_TYPES.includes(type)) return;
        this._binaryType = type;
        if (this._receiver) this._receiver._binaryType = type;
      }
      /**
       * @type {Number}
       */
      get bufferedAmount() {
        if (!this._socket) return this._bufferedAmount;
        return this._socket._writableState.length + this._sender._bufferedBytes;
      }
      /**
       * @type {String}
       */
      get extensions() {
        return Object.keys(this._extensions).join();
      }
      /**
       * @type {Boolean}
       */
      get isPaused() {
        return this._paused;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onclose() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onerror() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onopen() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onmessage() {
        return null;
      }
      /**
       * @type {String}
       */
      get protocol() {
        return this._protocol;
      }
      /**
       * @type {Number}
       */
      get readyState() {
        return this._readyState;
      }
      /**
       * @type {String}
       */
      get url() {
        return this._url;
      }
      /**
       * Set up the socket and the internal resources.
       *
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Object} options Options object
       * @param {Boolean} [options.allowSynchronousEvents=false] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Number} [options.maxBufferedChunks=0] The maximum number of
       *     buffered data chunks
       * @param {Number} [options.maxFragments=0] The maximum number of message
       *     fragments
       * @param {Number} [options.maxPayload=0] The maximum allowed message size
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       * @private
       */
      setSocket(socket, head, options) {
        const receiver = new Receiver2({
          allowSynchronousEvents: options.allowSynchronousEvents,
          binaryType: this.binaryType,
          extensions: this._extensions,
          isServer: this._isServer,
          maxBufferedChunks: options.maxBufferedChunks,
          maxFragments: options.maxFragments,
          maxPayload: options.maxPayload,
          skipUTF8Validation: options.skipUTF8Validation
        });
        const sender = new Sender2(socket, this._extensions, options.generateMask);
        this._receiver = receiver;
        this._sender = sender;
        this._socket = socket;
        receiver[kWebSocket] = this;
        sender[kWebSocket] = this;
        socket[kWebSocket] = this;
        receiver.on("conclude", receiverOnConclude);
        receiver.on("drain", receiverOnDrain);
        receiver.on("error", receiverOnError);
        receiver.on("message", receiverOnMessage);
        receiver.on("ping", receiverOnPing);
        receiver.on("pong", receiverOnPong);
        sender.onerror = senderOnError;
        if (socket.setTimeout) socket.setTimeout(0);
        if (socket.setNoDelay) socket.setNoDelay();
        if (head.length > 0) socket.unshift(head);
        socket.on("close", socketOnClose);
        socket.on("data", socketOnData);
        socket.on("end", socketOnEnd);
        socket.on("error", socketOnError);
        this._readyState = _WebSocket.OPEN;
        this.emit("open");
      }
      /**
       * Emit the `'close'` event.
       *
       * @private
       */
      emitClose() {
        if (!this._socket) {
          this._readyState = _WebSocket.CLOSED;
          this.emit("close", this._closeCode, this._closeMessage);
          return;
        }
        if (this._extensions[PerMessageDeflate2.extensionName]) {
          this._extensions[PerMessageDeflate2.extensionName].cleanup();
        }
        this._receiver.removeAllListeners();
        this._readyState = _WebSocket.CLOSED;
        this.emit("close", this._closeCode, this._closeMessage);
      }
      /**
       * Start a closing handshake.
       *
       *          +----------+   +-----------+   +----------+
       *     - - -|ws.close()|-->|close frame|-->|ws.close()|- - -
       *    |     +----------+   +-----------+   +----------+     |
       *          +----------+   +-----------+         |
       * CLOSING  |ws.close()|<--|close frame|<--+-----+       CLOSING
       *          +----------+   +-----------+   |
       *    |           |                        |   +---+        |
       *                +------------------------+-->|fin| - - - -
       *    |         +---+                      |   +---+
       *     - - - - -|fin|<---------------------+
       *              +---+
       *
       * @param {Number} [code] Status code explaining why the connection is closing
       * @param {(String|Buffer)} [data] The reason why the connection is
       *     closing
       * @public
       */
      close(code, data) {
        if (this.readyState === _WebSocket.CLOSED) return;
        if (this.readyState === _WebSocket.CONNECTING) {
          const msg = "WebSocket was closed before the connection was established";
          abortHandshake(this, this._req, msg);
          return;
        }
        if (this.readyState === _WebSocket.CLOSING) {
          if (this._closeFrameSent && (this._closeFrameReceived || this._receiver._writableState.errorEmitted)) {
            this._socket.end();
          }
          return;
        }
        this._sender.close(code, data, !this._isServer, (err) => {
          if (err) return;
          this._closeFrameSent = true;
          if (this._closeFrameReceived || this._receiver._writableState.errorEmitted) {
            this._socket.end();
          }
        });
        this._readyState = _WebSocket.CLOSING;
        setCloseTimer(this);
      }
      /**
       * Pause the socket.
       *
       * @public
       */
      pause() {
        if (this.readyState === _WebSocket.CONNECTING || this.readyState === _WebSocket.CLOSED) {
          return;
        }
        this._paused = true;
        this._socket.pause();
      }
      /**
       * Send a ping.
       *
       * @param {*} [data] The data to send
       * @param {Boolean} [mask] Indicates whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when the ping is sent
       * @public
       */
      ping(data, mask, cb) {
        if (this.readyState === _WebSocket.CONNECTING) {
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        }
        if (typeof data === "function") {
          cb = data;
          data = mask = void 0;
        } else if (typeof mask === "function") {
          cb = mask;
          mask = void 0;
        }
        if (typeof data === "number") data = data.toString();
        if (this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        if (mask === void 0) mask = !this._isServer;
        this._sender.ping(data || EMPTY_BUFFER, mask, cb);
      }
      /**
       * Send a pong.
       *
       * @param {*} [data] The data to send
       * @param {Boolean} [mask] Indicates whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when the pong is sent
       * @public
       */
      pong(data, mask, cb) {
        if (this.readyState === _WebSocket.CONNECTING) {
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        }
        if (typeof data === "function") {
          cb = data;
          data = mask = void 0;
        } else if (typeof mask === "function") {
          cb = mask;
          mask = void 0;
        }
        if (typeof data === "number") data = data.toString();
        if (this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        if (mask === void 0) mask = !this._isServer;
        this._sender.pong(data || EMPTY_BUFFER, mask, cb);
      }
      /**
       * Resume the socket.
       *
       * @public
       */
      resume() {
        if (this.readyState === _WebSocket.CONNECTING || this.readyState === _WebSocket.CLOSED) {
          return;
        }
        this._paused = false;
        if (!this._receiver._writableState.needDrain) this._socket.resume();
      }
      /**
       * Send a data message.
       *
       * @param {*} data The message to send
       * @param {Object} [options] Options object
       * @param {Boolean} [options.binary] Specifies whether `data` is binary or
       *     text
       * @param {Boolean} [options.compress] Specifies whether or not to compress
       *     `data`
       * @param {Boolean} [options.fin=true] Specifies whether the fragment is the
       *     last one
       * @param {Boolean} [options.mask] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when data is written out
       * @public
       */
      send(data, options, cb) {
        if (this.readyState === _WebSocket.CONNECTING) {
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        }
        if (typeof options === "function") {
          cb = options;
          options = {};
        }
        if (typeof data === "number") data = data.toString();
        if (this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        const opts = {
          binary: typeof data !== "string",
          mask: !this._isServer,
          compress: true,
          fin: true,
          ...options
        };
        if (!this._extensions[PerMessageDeflate2.extensionName]) {
          opts.compress = false;
        }
        this._sender.send(data || EMPTY_BUFFER, opts, cb);
      }
      /**
       * Forcibly close the connection.
       *
       * @public
       */
      terminate() {
        if (this.readyState === _WebSocket.CLOSED) return;
        if (this.readyState === _WebSocket.CONNECTING) {
          const msg = "WebSocket was closed before the connection was established";
          abortHandshake(this, this._req, msg);
          return;
        }
        if (this._socket) {
          this._readyState = _WebSocket.CLOSING;
          this._socket.destroy();
        }
      }
    };
    Object.defineProperty(WebSocket2, "CONNECTING", {
      enumerable: true,
      value: readyStates.indexOf("CONNECTING")
    });
    Object.defineProperty(WebSocket2.prototype, "CONNECTING", {
      enumerable: true,
      value: readyStates.indexOf("CONNECTING")
    });
    Object.defineProperty(WebSocket2, "OPEN", {
      enumerable: true,
      value: readyStates.indexOf("OPEN")
    });
    Object.defineProperty(WebSocket2.prototype, "OPEN", {
      enumerable: true,
      value: readyStates.indexOf("OPEN")
    });
    Object.defineProperty(WebSocket2, "CLOSING", {
      enumerable: true,
      value: readyStates.indexOf("CLOSING")
    });
    Object.defineProperty(WebSocket2.prototype, "CLOSING", {
      enumerable: true,
      value: readyStates.indexOf("CLOSING")
    });
    Object.defineProperty(WebSocket2, "CLOSED", {
      enumerable: true,
      value: readyStates.indexOf("CLOSED")
    });
    Object.defineProperty(WebSocket2.prototype, "CLOSED", {
      enumerable: true,
      value: readyStates.indexOf("CLOSED")
    });
    [
      "binaryType",
      "bufferedAmount",
      "extensions",
      "isPaused",
      "protocol",
      "readyState",
      "url"
    ].forEach((property) => {
      Object.defineProperty(WebSocket2.prototype, property, { enumerable: true });
    });
    ["open", "error", "close", "message"].forEach((method) => {
      Object.defineProperty(WebSocket2.prototype, `on${method}`, {
        enumerable: true,
        get() {
          for (const listener of this.listeners(method)) {
            if (listener[kForOnEventAttribute]) return listener[kListener];
          }
          return null;
        },
        set(handler) {
          for (const listener of this.listeners(method)) {
            if (listener[kForOnEventAttribute]) {
              this.removeListener(method, listener);
              break;
            }
          }
          if (typeof handler !== "function") return;
          this.addEventListener(method, handler, {
            [kForOnEventAttribute]: true
          });
        }
      });
    });
    WebSocket2.prototype.addEventListener = addEventListener;
    WebSocket2.prototype.removeEventListener = removeEventListener;
    module.exports = WebSocket2;
    function initAsClient(websocket, address, protocols, options) {
      const opts = {
        allowSynchronousEvents: true,
        autoPong: true,
        closeTimeout: CLOSE_TIMEOUT,
        protocolVersion: protocolVersions[1],
        maxBufferedChunks: 256 * 1024,
        maxFragments: 16 * 1024,
        maxPayload: 100 * 1024 * 1024,
        skipUTF8Validation: false,
        perMessageDeflate: true,
        followRedirects: false,
        maxRedirects: 10,
        ...options,
        socketPath: void 0,
        hostname: void 0,
        protocol: void 0,
        protocols: void 0,
        timeout: void 0,
        method: "GET",
        host: void 0,
        path: void 0,
        port: void 0
      };
      websocket._autoPong = opts.autoPong;
      websocket._closeTimeout = opts.closeTimeout;
      if (!protocolVersions.includes(opts.protocolVersion)) {
        throw new RangeError(
          `Unsupported protocol version: ${opts.protocolVersion} (supported versions: ${protocolVersions.join(", ")})`
        );
      }
      let parsedUrl;
      if (address instanceof URL2) {
        parsedUrl = address;
      } else {
        try {
          parsedUrl = new URL2(address);
        } catch {
          throw new SyntaxError(`Invalid URL: ${address}`);
        }
      }
      if (parsedUrl.protocol === "http:") {
        parsedUrl.protocol = "ws:";
      } else if (parsedUrl.protocol === "https:") {
        parsedUrl.protocol = "wss:";
      }
      websocket._url = parsedUrl.href;
      const isSecure = parsedUrl.protocol === "wss:";
      const isIpcUrl = parsedUrl.protocol === "ws+unix:";
      let invalidUrlMessage;
      if (parsedUrl.protocol !== "ws:" && !isSecure && !isIpcUrl) {
        invalidUrlMessage = `The URL's protocol must be one of "ws:", "wss:", "http:", "https:", or "ws+unix:"`;
      } else if (isIpcUrl && !parsedUrl.pathname) {
        invalidUrlMessage = "The URL's pathname is empty";
      } else if (parsedUrl.hash) {
        invalidUrlMessage = "The URL contains a fragment identifier";
      }
      if (invalidUrlMessage) {
        const err = new SyntaxError(invalidUrlMessage);
        if (websocket._redirects === 0) {
          throw err;
        } else {
          emitErrorAndClose(websocket, err);
          return;
        }
      }
      const defaultPort = isSecure ? 443 : 80;
      const key = randomBytes(16).toString("base64");
      const request = isSecure ? https.request : http.request;
      const protocolSet = /* @__PURE__ */ new Set();
      let perMessageDeflate;
      opts.createConnection = opts.createConnection || (isSecure ? tlsConnect : netConnect);
      opts.defaultPort = opts.defaultPort || defaultPort;
      opts.port = parsedUrl.port || defaultPort;
      opts.host = parsedUrl.hostname.startsWith("[") ? parsedUrl.hostname.slice(1, -1) : parsedUrl.hostname;
      opts.headers = {
        ...opts.headers,
        "Sec-WebSocket-Version": opts.protocolVersion,
        "Sec-WebSocket-Key": key,
        Connection: "Upgrade",
        Upgrade: "websocket"
      };
      opts.path = parsedUrl.pathname + parsedUrl.search;
      opts.timeout = opts.handshakeTimeout;
      if (opts.perMessageDeflate) {
        perMessageDeflate = new PerMessageDeflate2({
          ...opts.perMessageDeflate,
          isServer: false,
          maxPayload: opts.maxPayload
        });
        opts.headers["Sec-WebSocket-Extensions"] = format({
          [PerMessageDeflate2.extensionName]: perMessageDeflate.offer()
        });
      }
      if (protocols.length) {
        for (const protocol of protocols) {
          if (typeof protocol !== "string" || !subprotocolRegex.test(protocol) || protocolSet.has(protocol)) {
            throw new SyntaxError(
              "An invalid or duplicated subprotocol was specified"
            );
          }
          protocolSet.add(protocol);
        }
        opts.headers["Sec-WebSocket-Protocol"] = protocols.join(",");
      }
      if (opts.origin) {
        if (opts.protocolVersion < 13) {
          opts.headers["Sec-WebSocket-Origin"] = opts.origin;
        } else {
          opts.headers.Origin = opts.origin;
        }
      }
      if (parsedUrl.username || parsedUrl.password) {
        opts.auth = `${parsedUrl.username}:${parsedUrl.password}`;
      }
      if (isIpcUrl) {
        const parts = opts.path.split(":");
        opts.socketPath = parts[0];
        opts.path = parts[1];
      }
      let req;
      if (opts.followRedirects) {
        if (websocket._redirects === 0) {
          websocket._originalIpc = isIpcUrl;
          websocket._originalSecure = isSecure;
          websocket._originalHostOrSocketPath = isIpcUrl ? opts.socketPath : parsedUrl.host;
          const headers = options && options.headers;
          options = { ...options, headers: {} };
          if (headers) {
            for (const [key2, value] of Object.entries(headers)) {
              options.headers[key2.toLowerCase()] = value;
            }
          }
        } else if (websocket.listenerCount("redirect") === 0) {
          const isSameHost = isIpcUrl ? websocket._originalIpc ? opts.socketPath === websocket._originalHostOrSocketPath : false : websocket._originalIpc ? false : parsedUrl.host === websocket._originalHostOrSocketPath;
          if (!isSameHost || websocket._originalSecure && !isSecure) {
            delete opts.headers.authorization;
            delete opts.headers.cookie;
            if (!isSameHost) delete opts.headers.host;
            opts.auth = void 0;
          }
        }
        if (opts.auth && !options.headers.authorization) {
          options.headers.authorization = "Basic " + Buffer.from(opts.auth).toString("base64");
        }
        req = websocket._req = request(opts);
        if (websocket._redirects) {
          websocket.emit("redirect", websocket.url, req);
        }
      } else {
        req = websocket._req = request(opts);
      }
      if (opts.timeout) {
        req.on("timeout", () => {
          abortHandshake(websocket, req, "Opening handshake has timed out");
        });
      }
      req.on("error", (err) => {
        if (req === null || req[kAborted]) return;
        req = websocket._req = null;
        emitErrorAndClose(websocket, err);
      });
      req.on("response", (res) => {
        const location = res.headers.location;
        const statusCode = res.statusCode;
        if (location && opts.followRedirects && statusCode >= 300 && statusCode < 400) {
          if (++websocket._redirects > opts.maxRedirects) {
            abortHandshake(websocket, req, "Maximum redirects exceeded");
            return;
          }
          req.abort();
          let addr;
          try {
            addr = new URL2(location, address);
          } catch (e) {
            const err = new SyntaxError(`Invalid URL: ${location}`);
            emitErrorAndClose(websocket, err);
            return;
          }
          initAsClient(websocket, addr, protocols, options);
        } else if (!websocket.emit("unexpected-response", req, res)) {
          abortHandshake(
            websocket,
            req,
            `Unexpected server response: ${res.statusCode}`
          );
        }
      });
      req.on("upgrade", (res, socket, head) => {
        websocket.emit("upgrade", res);
        if (websocket.readyState !== WebSocket2.CONNECTING) return;
        req = websocket._req = null;
        const upgrade = res.headers.upgrade;
        if (upgrade === void 0 || upgrade.toLowerCase() !== "websocket") {
          abortHandshake(websocket, socket, "Invalid Upgrade header");
          return;
        }
        const digest = createHash("sha1").update(key + GUID).digest("base64");
        if (res.headers["sec-websocket-accept"] !== digest) {
          abortHandshake(websocket, socket, "Invalid Sec-WebSocket-Accept header");
          return;
        }
        const serverProt = res.headers["sec-websocket-protocol"];
        let protError;
        if (serverProt !== void 0) {
          if (!protocolSet.size) {
            protError = "Server sent a subprotocol but none was requested";
          } else if (!protocolSet.has(serverProt)) {
            protError = "Server sent an invalid subprotocol";
          }
        } else if (protocolSet.size) {
          protError = "Server sent no subprotocol";
        }
        if (protError) {
          abortHandshake(websocket, socket, protError);
          return;
        }
        if (serverProt) websocket._protocol = serverProt;
        const secWebSocketExtensions = res.headers["sec-websocket-extensions"];
        if (secWebSocketExtensions !== void 0) {
          if (!perMessageDeflate) {
            const message = "Server sent a Sec-WebSocket-Extensions header but no extension was requested";
            abortHandshake(websocket, socket, message);
            return;
          }
          let extensions;
          try {
            extensions = parse(secWebSocketExtensions);
          } catch (err) {
            const message = "Invalid Sec-WebSocket-Extensions header";
            abortHandshake(websocket, socket, message);
            return;
          }
          const extensionNames = Object.keys(extensions);
          if (extensionNames.length !== 1 || extensionNames[0] !== PerMessageDeflate2.extensionName) {
            const message = "Server indicated an extension that was not requested";
            abortHandshake(websocket, socket, message);
            return;
          }
          try {
            perMessageDeflate.accept(extensions[PerMessageDeflate2.extensionName]);
          } catch (err) {
            const message = "Invalid Sec-WebSocket-Extensions header";
            abortHandshake(websocket, socket, message);
            return;
          }
          websocket._extensions[PerMessageDeflate2.extensionName] = perMessageDeflate;
        }
        websocket.setSocket(socket, head, {
          allowSynchronousEvents: opts.allowSynchronousEvents,
          generateMask: opts.generateMask,
          maxBufferedChunks: opts.maxBufferedChunks,
          maxFragments: opts.maxFragments,
          maxPayload: opts.maxPayload,
          skipUTF8Validation: opts.skipUTF8Validation
        });
      });
      if (opts.finishRequest) {
        opts.finishRequest(req, websocket);
      } else {
        req.end();
      }
    }
    function emitErrorAndClose(websocket, err) {
      websocket._readyState = WebSocket2.CLOSING;
      websocket._errorEmitted = true;
      websocket.emit("error", err);
      websocket.emitClose();
    }
    function netConnect(options) {
      options.path = options.socketPath;
      return net.connect(options);
    }
    function tlsConnect(options) {
      options.path = void 0;
      if (!options.servername && options.servername !== "") {
        options.servername = net.isIP(options.host) ? "" : options.host;
      }
      return tls.connect(options);
    }
    function abortHandshake(websocket, stream, message) {
      websocket._readyState = WebSocket2.CLOSING;
      const err = new Error(message);
      Error.captureStackTrace(err, abortHandshake);
      if (stream.setHeader) {
        stream[kAborted] = true;
        stream.abort();
        if (stream.socket && !stream.socket.destroyed) {
          stream.socket.destroy();
        }
        process.nextTick(emitErrorAndClose, websocket, err);
      } else {
        stream.destroy(err);
        stream.once("error", websocket.emit.bind(websocket, "error"));
        stream.once("close", websocket.emitClose.bind(websocket));
      }
    }
    function sendAfterClose(websocket, data, cb) {
      if (data) {
        const length = isBlob(data) ? data.size : toBuffer(data).length;
        if (websocket._socket) websocket._sender._bufferedBytes += length;
        else websocket._bufferedAmount += length;
      }
      if (cb) {
        const err = new Error(
          `WebSocket is not open: readyState ${websocket.readyState} (${readyStates[websocket.readyState]})`
        );
        process.nextTick(cb, err);
      }
    }
    function receiverOnConclude(code, reason) {
      const websocket = this[kWebSocket];
      websocket._closeFrameReceived = true;
      websocket._closeMessage = reason;
      websocket._closeCode = code;
      if (websocket._socket[kWebSocket] === void 0) return;
      websocket._socket.removeListener("data", socketOnData);
      process.nextTick(resume, websocket._socket);
      if (code === 1005) websocket.close();
      else websocket.close(code, reason);
    }
    function receiverOnDrain() {
      const websocket = this[kWebSocket];
      if (!websocket.isPaused) websocket._socket.resume();
    }
    function receiverOnError(err) {
      const websocket = this[kWebSocket];
      if (websocket._socket[kWebSocket] !== void 0) {
        websocket._socket.removeListener("data", socketOnData);
        process.nextTick(resume, websocket._socket);
        websocket.close(err[kStatusCode]);
      }
      if (!websocket._errorEmitted) {
        websocket._errorEmitted = true;
        websocket.emit("error", err);
      }
    }
    function receiverOnFinish() {
      this[kWebSocket].emitClose();
    }
    function receiverOnMessage(data, isBinary) {
      this[kWebSocket].emit("message", data, isBinary);
    }
    function receiverOnPing(data) {
      const websocket = this[kWebSocket];
      if (websocket._autoPong) websocket.pong(data, !this._isServer, NOOP);
      websocket.emit("ping", data);
    }
    function receiverOnPong(data) {
      this[kWebSocket].emit("pong", data);
    }
    function resume(stream) {
      stream.resume();
    }
    function senderOnError(err) {
      const websocket = this[kWebSocket];
      if (websocket.readyState === WebSocket2.CLOSED) return;
      if (websocket.readyState === WebSocket2.OPEN) {
        websocket._readyState = WebSocket2.CLOSING;
        setCloseTimer(websocket);
      }
      this._socket.end();
      if (!websocket._errorEmitted) {
        websocket._errorEmitted = true;
        websocket.emit("error", err);
      }
    }
    function setCloseTimer(websocket) {
      websocket._closeTimer = setTimeout(
        websocket._socket.destroy.bind(websocket._socket),
        websocket._closeTimeout
      );
    }
    function socketOnClose() {
      const websocket = this[kWebSocket];
      this.removeListener("close", socketOnClose);
      this.removeListener("data", socketOnData);
      this.removeListener("end", socketOnEnd);
      websocket._readyState = WebSocket2.CLOSING;
      if (!this._readableState.endEmitted && !websocket._closeFrameReceived && !websocket._receiver._writableState.errorEmitted && this._readableState.length !== 0) {
        const chunk = this.read(this._readableState.length);
        websocket._receiver.write(chunk);
      }
      websocket._receiver.end();
      this[kWebSocket] = void 0;
      clearTimeout(websocket._closeTimer);
      if (websocket._receiver._writableState.finished || websocket._receiver._writableState.errorEmitted) {
        websocket.emitClose();
      } else {
        websocket._receiver.on("error", receiverOnFinish);
        websocket._receiver.on("finish", receiverOnFinish);
      }
    }
    function socketOnData(chunk) {
      if (!this[kWebSocket]._receiver.write(chunk)) {
        this.pause();
      }
    }
    function socketOnEnd() {
      const websocket = this[kWebSocket];
      websocket._readyState = WebSocket2.CLOSING;
      websocket._receiver.end();
      this.end();
    }
    function socketOnError() {
      const websocket = this[kWebSocket];
      this.removeListener("error", socketOnError);
      this.on("error", NOOP);
      if (websocket) {
        websocket._readyState = WebSocket2.CLOSING;
        this.destroy();
      }
    }
  }
});

// node_modules/ws/lib/stream.js
var require_stream = __commonJS({
  "node_modules/ws/lib/stream.js"(exports, module) {
    "use strict";
    var WebSocket2 = require_websocket();
    var { Duplex } = __require("stream");
    function emitClose(stream) {
      stream.emit("close");
    }
    function duplexOnEnd() {
      if (!this.destroyed && this._writableState.finished) {
        this.destroy();
      }
    }
    function duplexOnError(err) {
      this.removeListener("error", duplexOnError);
      this.destroy();
      if (this.listenerCount("error") === 0) {
        this.emit("error", err);
      }
    }
    function createWebSocketStream2(ws, options) {
      let terminateOnDestroy = true;
      const duplex = new Duplex({
        ...options,
        autoDestroy: false,
        emitClose: false,
        objectMode: false,
        writableObjectMode: false
      });
      ws.on("message", function message(msg, isBinary) {
        const data = !isBinary && duplex._readableState.objectMode ? msg.toString() : msg;
        if (!duplex.push(data)) ws.pause();
      });
      ws.once("error", function error(err) {
        if (duplex.destroyed) return;
        terminateOnDestroy = false;
        duplex.destroy(err);
      });
      ws.once("close", function close() {
        if (duplex.destroyed) return;
        duplex.push(null);
      });
      duplex._destroy = function(err, callback) {
        if (ws.readyState === ws.CLOSED) {
          callback(err);
          process.nextTick(emitClose, duplex);
          return;
        }
        let called = false;
        ws.once("error", function error(err2) {
          called = true;
          callback(err2);
        });
        ws.once("close", function close() {
          if (!called) callback(err);
          process.nextTick(emitClose, duplex);
        });
        if (terminateOnDestroy) ws.terminate();
      };
      duplex._final = function(callback) {
        if (ws.readyState === ws.CONNECTING) {
          ws.once("open", function open() {
            duplex._final(callback);
          });
          return;
        }
        if (ws._socket === null) return;
        if (ws._socket._writableState.finished) {
          callback();
          if (duplex._readableState.endEmitted) duplex.destroy();
        } else {
          ws._socket.once("finish", function finish() {
            callback();
          });
          ws.close();
        }
      };
      duplex._read = function() {
        if (ws.isPaused) ws.resume();
      };
      duplex._write = function(chunk, encoding, callback) {
        if (ws.readyState === ws.CONNECTING) {
          ws.once("open", function open() {
            duplex._write(chunk, encoding, callback);
          });
          return;
        }
        ws.send(chunk, callback);
      };
      duplex.on("end", duplexOnEnd);
      duplex.on("error", duplexOnError);
      return duplex;
    }
    module.exports = createWebSocketStream2;
  }
});

// node_modules/ws/lib/subprotocol.js
var require_subprotocol = __commonJS({
  "node_modules/ws/lib/subprotocol.js"(exports, module) {
    "use strict";
    var { tokenChars } = require_validation();
    function parse(header) {
      const protocols = /* @__PURE__ */ new Set();
      let start = -1;
      let end = -1;
      let i = 0;
      for (i; i < header.length; i++) {
        const code = header.charCodeAt(i);
        if (end === -1 && tokenChars[code] === 1) {
          if (start === -1) start = i;
        } else if (i !== 0 && (code === 32 || code === 9)) {
          if (end === -1 && start !== -1) end = i;
        } else if (code === 44) {
          if (start === -1) {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
          if (end === -1) end = i;
          const protocol2 = header.slice(start, end);
          if (protocols.has(protocol2)) {
            throw new SyntaxError(`The "${protocol2}" subprotocol is duplicated`);
          }
          protocols.add(protocol2);
          start = end = -1;
        } else {
          throw new SyntaxError(`Unexpected character at index ${i}`);
        }
      }
      if (start === -1 || end !== -1) {
        throw new SyntaxError("Unexpected end of input");
      }
      const protocol = header.slice(start, i);
      if (protocols.has(protocol)) {
        throw new SyntaxError(`The "${protocol}" subprotocol is duplicated`);
      }
      protocols.add(protocol);
      return protocols;
    }
    module.exports = { parse };
  }
});

// node_modules/ws/lib/websocket-server.js
var require_websocket_server = __commonJS({
  "node_modules/ws/lib/websocket-server.js"(exports, module) {
    "use strict";
    var EventEmitter = __require("events");
    var http = __require("http");
    var { Duplex } = __require("stream");
    var { createHash } = __require("crypto");
    var extension2 = require_extension();
    var PerMessageDeflate2 = require_permessage_deflate();
    var subprotocol2 = require_subprotocol();
    var WebSocket2 = require_websocket();
    var { CLOSE_TIMEOUT, GUID, kWebSocket } = require_constants();
    var keyRegex = /^[+/0-9A-Za-z]{22}==$/;
    var RUNNING = 0;
    var CLOSING = 1;
    var CLOSED = 2;
    var WebSocketServer2 = class extends EventEmitter {
      /**
       * Create a `WebSocketServer` instance.
       *
       * @param {Object} options Configuration options
       * @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {Boolean} [options.autoPong=true] Specifies whether or not to
       *     automatically send a pong in response to a ping
       * @param {Number} [options.backlog=511] The maximum length of the queue of
       *     pending connections
       * @param {Boolean} [options.clientTracking=true] Specifies whether or not to
       *     track clients
       * @param {Number} [options.closeTimeout=30000] Duration in milliseconds to
       *     wait for the closing handshake to finish after `websocket.close()` is
       *     called
       * @param {Function} [options.handleProtocols] A hook to handle protocols
       * @param {String} [options.host] The hostname where to bind the server
       * @param {Number} [options.maxBufferedChunks=262144] The maximum number of
       *     buffered data chunks
       * @param {Number} [options.maxFragments=16384] The maximum number of message
       *     fragments
       * @param {Number} [options.maxPayload=104857600] The maximum allowed message
       *     size
       * @param {Boolean} [options.noServer=false] Enable no server mode
       * @param {String} [options.path] Accept only connections matching this path
       * @param {(Boolean|Object)} [options.perMessageDeflate=false] Enable/disable
       *     permessage-deflate
       * @param {Number} [options.port] The port where to bind the server
       * @param {(http.Server|https.Server)} [options.server] A pre-created HTTP/S
       *     server to use
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       * @param {Function} [options.verifyClient] A hook to reject connections
       * @param {Function} [options.WebSocket=WebSocket] Specifies the `WebSocket`
       *     class to use. It must be the `WebSocket` class or class that extends it
       * @param {Function} [callback] A listener for the `listening` event
       */
      constructor(options, callback) {
        super();
        options = {
          allowSynchronousEvents: true,
          autoPong: true,
          maxBufferedChunks: 256 * 1024,
          maxFragments: 16 * 1024,
          maxPayload: 100 * 1024 * 1024,
          skipUTF8Validation: false,
          perMessageDeflate: false,
          handleProtocols: null,
          clientTracking: true,
          closeTimeout: CLOSE_TIMEOUT,
          verifyClient: null,
          noServer: false,
          backlog: null,
          // use default (511 as implemented in net.js)
          server: null,
          host: null,
          path: null,
          port: null,
          WebSocket: WebSocket2,
          ...options
        };
        if (options.port == null && !options.server && !options.noServer || options.port != null && (options.server || options.noServer) || options.server && options.noServer) {
          throw new TypeError(
            'One and only one of the "port", "server", or "noServer" options must be specified'
          );
        }
        if (options.port != null) {
          this._server = http.createServer((req, res) => {
            const body = http.STATUS_CODES[426];
            res.writeHead(426, {
              "Content-Length": body.length,
              "Content-Type": "text/plain"
            });
            res.end(body);
          });
          this._server.listen(
            options.port,
            options.host,
            options.backlog,
            callback
          );
        } else if (options.server) {
          this._server = options.server;
        }
        if (this._server) {
          const emitConnection = this.emit.bind(this, "connection");
          this._removeListeners = addListeners(this._server, {
            listening: this.emit.bind(this, "listening"),
            error: this.emit.bind(this, "error"),
            upgrade: (req, socket, head) => {
              this.handleUpgrade(req, socket, head, emitConnection);
            }
          });
        }
        if (options.perMessageDeflate === true) options.perMessageDeflate = {};
        if (options.clientTracking) {
          this.clients = /* @__PURE__ */ new Set();
          this._shouldEmitClose = false;
        }
        this.options = options;
        this._state = RUNNING;
      }
      /**
       * Returns the bound address, the address family name, and port of the server
       * as reported by the operating system if listening on an IP socket.
       * If the server is listening on a pipe or UNIX domain socket, the name is
       * returned as a string.
       *
       * @return {(Object|String|null)} The address of the server
       * @public
       */
      address() {
        if (this.options.noServer) {
          throw new Error('The server is operating in "noServer" mode');
        }
        if (!this._server) return null;
        return this._server.address();
      }
      /**
       * Stop the server from accepting new connections and emit the `'close'` event
       * when all existing connections are closed.
       *
       * @param {Function} [cb] A one-time listener for the `'close'` event
       * @public
       */
      close(cb) {
        if (this._state === CLOSED) {
          if (cb) {
            this.once("close", () => {
              cb(new Error("The server is not running"));
            });
          }
          process.nextTick(emitClose, this);
          return;
        }
        if (cb) this.once("close", cb);
        if (this._state === CLOSING) return;
        this._state = CLOSING;
        if (this.options.noServer || this.options.server) {
          if (this._server) {
            this._removeListeners();
            this._removeListeners = this._server = null;
          }
          if (this.clients) {
            if (!this.clients.size) {
              process.nextTick(emitClose, this);
            } else {
              this._shouldEmitClose = true;
            }
          } else {
            process.nextTick(emitClose, this);
          }
        } else {
          const server2 = this._server;
          this._removeListeners();
          this._removeListeners = this._server = null;
          server2.close(() => {
            emitClose(this);
          });
        }
      }
      /**
       * See if a given request should be handled by this server instance.
       *
       * @param {http.IncomingMessage} req Request object to inspect
       * @return {Boolean} `true` if the request is valid, else `false`
       * @public
       */
      shouldHandle(req) {
        if (this.options.path) {
          const index = req.url.indexOf("?");
          const pathname = index !== -1 ? req.url.slice(0, index) : req.url;
          if (pathname !== this.options.path) return false;
        }
        return true;
      }
      /**
       * Handle a HTTP Upgrade request.
       *
       * @param {http.IncomingMessage} req The request object
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Function} cb Callback
       * @public
       */
      handleUpgrade(req, socket, head, cb) {
        socket.on("error", socketOnError);
        const key = req.headers["sec-websocket-key"];
        const upgrade = req.headers.upgrade;
        const version = +req.headers["sec-websocket-version"];
        if (req.method !== "GET") {
          const message = "Invalid HTTP method";
          abortHandshakeOrEmitwsClientError(this, req, socket, 405, message);
          return;
        }
        if (upgrade === void 0 || upgrade.toLowerCase() !== "websocket") {
          const message = "Invalid Upgrade header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
          return;
        }
        if (key === void 0 || !keyRegex.test(key)) {
          const message = "Missing or invalid Sec-WebSocket-Key header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
          return;
        }
        if (version !== 13 && version !== 8) {
          const message = "Missing or invalid Sec-WebSocket-Version header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message, {
            "Sec-WebSocket-Version": "13, 8"
          });
          return;
        }
        if (!this.shouldHandle(req)) {
          abortHandshake(socket, 400);
          return;
        }
        const secWebSocketProtocol = req.headers["sec-websocket-protocol"];
        let protocols = /* @__PURE__ */ new Set();
        if (secWebSocketProtocol !== void 0) {
          try {
            protocols = subprotocol2.parse(secWebSocketProtocol);
          } catch (err) {
            const message = "Invalid Sec-WebSocket-Protocol header";
            abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
            return;
          }
        }
        const secWebSocketExtensions = req.headers["sec-websocket-extensions"];
        const extensions = {};
        if (this.options.perMessageDeflate && secWebSocketExtensions !== void 0) {
          const perMessageDeflate = new PerMessageDeflate2({
            ...this.options.perMessageDeflate,
            isServer: true,
            maxPayload: this.options.maxPayload
          });
          try {
            const offers = extension2.parse(secWebSocketExtensions);
            if (offers[PerMessageDeflate2.extensionName]) {
              perMessageDeflate.accept(offers[PerMessageDeflate2.extensionName]);
              extensions[PerMessageDeflate2.extensionName] = perMessageDeflate;
            }
          } catch (err) {
            const message = "Invalid or unacceptable Sec-WebSocket-Extensions header";
            abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
            return;
          }
        }
        if (this.options.verifyClient) {
          const info = {
            origin: req.headers[`${version === 8 ? "sec-websocket-origin" : "origin"}`],
            secure: !!(req.socket.authorized || req.socket.encrypted),
            req
          };
          if (this.options.verifyClient.length === 2) {
            this.options.verifyClient(info, (verified, code, message, headers) => {
              if (!verified) {
                return abortHandshake(socket, code || 401, message, headers);
              }
              this.completeUpgrade(
                extensions,
                key,
                protocols,
                req,
                socket,
                head,
                cb
              );
            });
            return;
          }
          if (!this.options.verifyClient(info)) return abortHandshake(socket, 401);
        }
        this.completeUpgrade(extensions, key, protocols, req, socket, head, cb);
      }
      /**
       * Upgrade the connection to WebSocket.
       *
       * @param {Object} extensions The accepted extensions
       * @param {String} key The value of the `Sec-WebSocket-Key` header
       * @param {Set} protocols The subprotocols
       * @param {http.IncomingMessage} req The request object
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Function} cb Callback
       * @throws {Error} If called more than once with the same socket
       * @private
       */
      completeUpgrade(extensions, key, protocols, req, socket, head, cb) {
        if (!socket.readable || !socket.writable) return socket.destroy();
        if (socket[kWebSocket]) {
          throw new Error(
            "server.handleUpgrade() was called more than once with the same socket, possibly due to a misconfiguration"
          );
        }
        if (this._state > RUNNING) return abortHandshake(socket, 503);
        const digest = createHash("sha1").update(key + GUID).digest("base64");
        const headers = [
          "HTTP/1.1 101 Switching Protocols",
          "Upgrade: websocket",
          "Connection: Upgrade",
          `Sec-WebSocket-Accept: ${digest}`
        ];
        const ws = new this.options.WebSocket(null, void 0, this.options);
        if (protocols.size) {
          const protocol = this.options.handleProtocols ? this.options.handleProtocols(protocols, req) : protocols.values().next().value;
          if (protocol) {
            headers.push(`Sec-WebSocket-Protocol: ${protocol}`);
            ws._protocol = protocol;
          }
        }
        if (extensions[PerMessageDeflate2.extensionName]) {
          const params = extensions[PerMessageDeflate2.extensionName].params;
          const value = extension2.format({
            [PerMessageDeflate2.extensionName]: [params]
          });
          headers.push(`Sec-WebSocket-Extensions: ${value}`);
          ws._extensions = extensions;
        }
        this.emit("headers", headers, req);
        socket.write(headers.concat("\r\n").join("\r\n"));
        socket.removeListener("error", socketOnError);
        ws.setSocket(socket, head, {
          allowSynchronousEvents: this.options.allowSynchronousEvents,
          maxBufferedChunks: this.options.maxBufferedChunks,
          maxFragments: this.options.maxFragments,
          maxPayload: this.options.maxPayload,
          skipUTF8Validation: this.options.skipUTF8Validation
        });
        if (this.clients) {
          this.clients.add(ws);
          ws.on("close", () => {
            this.clients.delete(ws);
            if (this._shouldEmitClose && !this.clients.size) {
              process.nextTick(emitClose, this);
            }
          });
        }
        cb(ws, req);
      }
    };
    module.exports = WebSocketServer2;
    function addListeners(server2, map) {
      for (const event of Object.keys(map)) server2.on(event, map[event]);
      return function removeListeners() {
        for (const event of Object.keys(map)) {
          server2.removeListener(event, map[event]);
        }
      };
    }
    function emitClose(server2) {
      server2._state = CLOSED;
      server2.emit("close");
    }
    function socketOnError() {
      this.destroy();
    }
    function abortHandshake(socket, code, message, headers) {
      message = message || http.STATUS_CODES[code];
      headers = {
        Connection: "close",
        "Content-Type": "text/html",
        "Content-Length": Buffer.byteLength(message),
        ...headers
      };
      socket.once("finish", socket.destroy);
      socket.end(
        `HTTP/1.1 ${code} ${http.STATUS_CODES[code]}\r
` + Object.keys(headers).map((h) => `${h}: ${headers[h]}`).join("\r\n") + "\r\n\r\n" + message
      );
    }
    function abortHandshakeOrEmitwsClientError(server2, req, socket, code, message, headers) {
      if (server2.listenerCount("wsClientError")) {
        const err = new Error(message);
        Error.captureStackTrace(err, abortHandshakeOrEmitwsClientError);
        server2.emit("wsClientError", err, socket, req);
      } else {
        abortHandshake(socket, code, message, headers);
      }
    }
  }
});

// src/server/index.ts
import { existsSync } from "node:fs";
import { dirname, join as join2 } from "node:path";
import { fileURLToPath } from "node:url";

// src/server/gateway.ts
import { createServer } from "node:http";

// node_modules/ws/wrapper.mjs
var import_stream = __toESM(require_stream(), 1);
var import_extension = __toESM(require_extension(), 1);
var import_permessage_deflate = __toESM(require_permessage_deflate(), 1);
var import_receiver = __toESM(require_receiver(), 1);
var import_sender = __toESM(require_sender(), 1);
var import_subprotocol = __toESM(require_subprotocol(), 1);
var import_websocket = __toESM(require_websocket(), 1);
var import_websocket_server = __toESM(require_websocket_server(), 1);

// src/shared/types.ts
var ANIMS = ["idle", "run", "throw", "catch", "dive", "down", "block", "celebrate"];
var NEUTRAL_INPUT = {
  seq: 0,
  mx: 0,
  my: 0,
  ax: 0,
  ay: 0,
  throwHeld: false,
  actionCount: 0,
  switchCount: 0,
  passType: 1
};

// src/shared/constants.ts
var TICK_RATE = 30;
var DT = 1 / TICK_RATE;
var FIELD_LEN = 100;
var ENDZONE = 10;
var FIELD_W = 53.3;
var MID_Y = FIELD_W / 2;
var HALF_SECONDS = 120;
var PLAYCALL_SECONDS = 15;
var PLAY_CLOCK_SECONDS = 10;
var HIKE_MIN_SECONDS = 0.6;
var ALIGN_SPEED = 7.5;
var DEAD_SECONDS = 2;
var SCORE_SECONDS = 3;
var HALFTIME_SECONDS = 4;
var PREGAME_SECONDS = 2.5;
var MAX_PLAY_SECONDS = 15;
var TIMEOUTS_PER_HALF = 3;
var START_LOS = 25;
var TOUCHBACK_LOS = 20;
var PUNT_NET = 40;
var PUNT_SNAP_SECONDS = 1.3;
var PUNT_DEPTH = 13;
var PUNT_MIN = 38;
var PUNT_MAX = 50;
var PUNT_DRIFT = 5;
function puntHang(flight) {
  return 2.4 + flight * 0.03;
}
var OT_START_LOS = 75;
var TD_POINTS = 6;
var SAFETY_POINTS = 2;
var FG_POINTS = 3;
var XP_POINTS = 1;
var TWO_POINTS = 2;
var XP_LOS = 85;
var TWO_LOS = 98;
var KO_FROM = 35;
var KO_SAFETY_FROM = 20;
var KO_TOUCHBACK_LOS = 25;
var KO_OOB_LOS = 40;
var KO_MIN_DIST = 30;
var KO_MAX_DIST = 72;
var KO_DEEP_TB = -2.5;
function koDistance(p) {
  return KO_MIN_DIST + (KO_MAX_DIST - KO_MIN_DIST) * Math.max(0, Math.min(1, p));
}
function koHang(dist2) {
  return 2.2 + dist2 * 0.028;
}
var KO_DRIFT = 4;
var WEAR_BASE = 0.015;
var WEAR_FROM_FATIGUE = 0.22;
var WEAR_RECOVER = 0.035;
var WEAR_HALFTIME = 0.5;
var WEAR_TIMEOUT = 0.12;
var WEAR_SLOW = 0.1;
var INJURY_CHANCE = 0.035;
var INJURY_PLAYS_MIN = 3;
var INJURY_PLAYS_MAX = 6;
var INJURY_SLOW = 0.12;
var FG_SNAP_BACK = 7;
var FG_MAX_DIST = 57;
function fgDistance(los) {
  return Math.round(FIELD_LEN - los + FG_SNAP_BACK + ENDZONE);
}
function kickMeter(t, hz) {
  const ph = (t * hz % 1 + 1) % 1;
  return ph < 0.5 ? -1 + 4 * ph : 3 - 4 * ph;
}
function fgMeterHz(dist2) {
  return 0.5 + dist2 / 120;
}
function fgZone(dist2) {
  return Math.max(0.14, Math.min(0.42, 0.42 - (dist2 - 20) * 68e-4));
}
var KO_METER_HZ = 0.55;
var KICK_TIME_LIMIT = 6;
var FG_POWER_HZ = 0.7;
function fgPowerAt(t) {
  const ph = t * FG_POWER_HZ % 1;
  return ph < 0.5 ? ph * 2 : 2 - ph * 2;
}
function fgPowerNeed(dist2, leg = 1) {
  return Math.min(0.97, Math.max(0.22, (0.22 + (dist2 - 18) * 0.0155) / Math.max(0.6, leg)));
}
var UPRIGHT_HALF = 3.1;
var PACE = 1.08;
var SPEED = {
  QB: 6.9 * PACE,
  QB_CHARGING: 4.2 * PACE,
  OL: 6 * PACE,
  RB: 7.9 * PACE,
  WR: 8.1 * PACE,
  TE: 7.5 * PACE,
  DL: 6.1 * PACE,
  LB: 7.3 * PACE,
  CB: 8 * PACE,
  S: 7.8 * PACE,
  HUMAN_DEF_BONUS: 0.3
};
var CARRY_MULT = 0.96;
var ACCEL = 20.5;
var DECEL = 26;
var FATIGUE_CARRY = 0.17;
var FATIGUE_OTHER = 0.05;
var FATIGUE_SLOW = 0.22;
var FATIGUE_GRACE = 1.2;
var JUKE_TIME = 0.3;
var JUKE_MULT = 1.25;
var JUKE_MISS_SLOW = 0.7;
var JUKE_SIDESTEP = 0.6;
var JUKE_COOLDOWN = 1.2;
var JUKE_WINDOW_R = 2.4;
var JUKE_PERFECT_TTC = 0.3;
var JUKE_IMMUNE = 0.4;
var JUKE_STUN = 0.7;
var STIFF_TIME = 0.45;
var STIFF_COOLDOWN = 2;
var STIFF_WIN = 0.7;
var STIFF_WIN_DIVE = 0.3;
var STIFF_SLOW = 0.9;
var STUMBLE_TIME = 0.4;
var STUMBLE_SLOW = 0.55;
var JAM_SLOW = 0.35;
var DIVE_SPEED = 11;
var DIVE_TIME = 0.35;
var CARRIER_DIVE_TIME = 0.3;
var DIVE_RECOVER = 0.6;
var DIVE_COOLDOWN = 1.6;
var THROW_MIN_DIST = 3;
var PRESSURE_R = 2.6;
var THROW_SNAP_R = 4.5;
var ACC_BASE = 0.45;
var ACC_PER_YARD = 0.04;
var ACC_PRESSURE_MULT = 1.75;
var ACC_MOVING_MULT = 1.35;
var PASS_SPECS = [
  { name: "BULLET", maxDist: 28, dur: (d) => 0.22 + d / 34, peak: (d) => 0.45 + d * 0.035, acc: 1.2, catchR: 1.25 },
  { name: "TOUCH", maxDist: 38, dur: (d) => 0.35 + d / 24, peak: (d) => 1.2 + d * 0.12, acc: 1, catchR: 1.4 },
  { name: "FLOATER", maxDist: 46, dur: (d) => 0.55 + d / 17, peak: (d) => 2.4 + d * 0.22, acc: 1, catchR: 1.55 }
];
var THROW_MAX_DIST = PASS_SPECS[2].maxDist;
var LONG_THROW_FROM = 25;
var ACC_LONG_PER_YARD = 0.05;
function maxThrowDist(passType = 1, arm = 1) {
  return (PASS_SPECS[passType] ?? PASS_SPECS[1]).maxDist * arm;
}
function throwLoft(dist2) {
  return Math.max(0, Math.min(1, (dist2 - LOFT_FROM) / (LOFT_TO - LOFT_FROM)));
}
var LOFT_FROM = 6;
var LOFT_TO = 40;
function blendSpec(dist2, f) {
  const u = throwLoft(dist2) * 2;
  return u <= 1 ? f(PASS_SPECS[0]) + (f(PASS_SPECS[1]) - f(PASS_SPECS[0])) * u : f(PASS_SPECS[1]) + (f(PASS_SPECS[2]) - f(PASS_SPECS[1])) * (u - 1);
}
function catchRadius(dist2) {
  return blendSpec(dist2, (s) => s.catchR);
}
function accuracyRadius(dist2, pressured, moving, _passType = 1, qbAcc = 1) {
  const acc = blendSpec(dist2, (s) => s.acc);
  const long = Math.max(0, dist2 - LONG_THROW_FROM) * ACC_LONG_PER_YARD;
  return (ACC_BASE + ACC_PER_YARD * dist2 + long) * acc * (pressured ? ACC_PRESSURE_MULT : 1) * (moving ? ACC_MOVING_MULT : 1) / Math.max(0.4, qbAcc);
}
function clampThrowDist(d, passType = 1, arm = 1) {
  return Math.max(THROW_MIN_DIST, Math.min(maxThrowDist(passType, arm), d));
}
function throwDuration(dist2, _passType = 1) {
  return blendSpec(dist2, (s) => s.dur(dist2)) * THROW_TIME_SCALE;
}
var THROW_TIME_SCALE = 0.75;
function throwPeak(dist2, _passType = 1) {
  return blendSpec(dist2, (s) => s.peak(dist2));
}
function ballZ(f, peak) {
  return 1.5 * (1 - f) + 4 * peak * f * (1 - f);
}
var CATCHABLE_FROM = 0.12;
var INT_JUMP_TIME = 0.55;
var INT_JUMP_R = 1.7;
var INT_JUMP_CHANCE_CLOSE = 0.9;
var INT_JUMP_CHANCE = 0.62;
var INT_STANDING_CHANCE = 0.45;
var REACH_OFF = 2.9;
var REACH_DEF = 2.6;
var REACH_DEF_HUMAN = 2.75;
var CATCH_R_DEF_HUMAN = 1.2;
var HUMAN_INT_R = 0.7;
var CONTEST_R = 1.8;
var AI_INT_CHANCE = 0.42;
var CONTEST_BREAKUP_CHANCE = 0.7;
var TACKLE_R_HUMAN = 0.95;
var TACKLE_R_DIVE = 1.6;
var BROKEN_TACKLE_CHANCE = 0.12;
var BROKEN_TACKLE_CHANCE_BURST = 0.3;
var STUN_TIME = 0.6;
var BLOCK_R = 1.25;
var BLOCK_R_LINE = 1.65;
var BLOCK_SLOW = 0.18;
var SHED_TIME_LINE = 3.1;
var SHED_TIME_OPEN = 1.1;
var SHED_IMMUNE = 1;
var SEPARATION_R = 0.7;
var RECONNECT_GRACE_SECONDS = 20;
var PAUSE_BUDGET_SECONDS = 60;
var RESUME_COUNTDOWN_SECONDS = 3;
var START_COUNTDOWN_SECONDS = 3;

// src/shared/teams.ts
var TEAMS = [
  { id: 0, city: "Bayside", name: "Gulls", abbr: "BAY", primary: "#18b5b0", secondary: "#f4f1e8", accent: "#0b4f6c" },
  { id: 1, city: "Copper City", name: "Coyotes", abbr: "COP", primary: "#e8742a", secondary: "#4a2b1a", accent: "#ffd27a" },
  { id: 2, city: "Iron Valley", name: "Moose", abbr: "IRV", primary: "#8c2340", secondary: "#e9b949", accent: "#f6e7c1" },
  { id: 3, city: "Neon Harbor", name: "Sharks", abbr: "NEO", primary: "#e03fa8", secondary: "#1b1b3a", accent: "#57e3ff" },
  { id: 4, city: "Thunder Ridge", name: "Rams", abbr: "THR", primary: "#25327a", secondary: "#f5d000", accent: "#ffffff" },
  { id: 5, city: "Pine Hollow", name: "Owls", abbr: "PIN", primary: "#2f7a3a", secondary: "#efe6c8", accent: "#b5852b" },
  { id: 6, city: "Sunfall", name: "Comets", abbr: "SUN", primary: "#d8c07a", secondary: "#c3262e", accent: "#2a1a12" },
  { id: 7, city: "Frostport", name: "Yetis", abbr: "FRO", primary: "#9fd8ff", secondary: "#ffffff", accent: "#2b4a8c" },
  { id: 8, city: "Granite Falls", name: "Grizzlies", abbr: "GRF", primary: "#6b4226", secondary: "#f2b134", accent: "#1d1d1d" },
  { id: 9, city: "Redwood", name: "Lumberjacks", abbr: "RED", primary: "#a8322d", secondary: "#1f4d2b", accent: "#f3e2c3" },
  { id: 10, city: "Mesa", name: "Scorpions", abbr: "MES", primary: "#c9a46b", secondary: "#1a1a1a", accent: "#e04f2f" },
  { id: 11, city: "Midnight", name: "Vipers", abbr: "VIP", primary: "#4b2a7a", secondary: "#b6f23b", accent: "#0e0e1a" },
  { id: 12, city: "Coral Coast", name: "Marlins", abbr: "CRL", primary: "#ff6f61", secondary: "#13315c", accent: "#ffffff" },
  { id: 13, city: "Steelton", name: "Titans", abbr: "STL", primary: "#5f6b73", secondary: "#ff8c1a", accent: "#111111" },
  { id: 14, city: "Ember Hills", name: "Dragons", abbr: "EMB", primary: "#262626", secondary: "#e2321f", accent: "#f6b93b" },
  { id: 15, city: "Kingsgate", name: "Lions", abbr: "KNG", primary: "#e2b13c", secondary: "#5a2d82", accent: "#ffffff" },
  { id: 16, city: "Silver Lake", name: "Lynx", abbr: "SLX", primary: "#9aa6b2", secondary: "#1c2a44", accent: "#7fe0ff" },
  { id: 17, city: "Dusty Gulch", name: "Prospectors", abbr: "DUS", primary: "#b5651d", secondary: "#2b2b2b", accent: "#f2d16b" },
  { id: 18, city: "Twin Rivers", name: "Otters", abbr: "TWR", primary: "#3d6f5f", secondary: "#e9e1c6", accent: "#c96b2c" },
  { id: 19, city: "Blue Mesa", name: "Condors", abbr: "BMC", primary: "#2f5fbf", secondary: "#f2a541", accent: "#ffffff" },
  { id: 20, city: "Northgate", name: "Wolves", abbr: "NTH", primary: "#3b3f4a", secondary: "#c4cdd6", accent: "#e8432e" },
  { id: 21, city: "Sandport", name: "Sandpipers", abbr: "SND", primary: "#e7c98f", secondary: "#1e5f8c", accent: "#ffffff" },
  { id: 22, city: "Cedar Hill", name: "Badgers", abbr: "CED", primary: "#7b1e2b", secondary: "#f0e6d2", accent: "#2a2a2a" },
  { id: 23, city: "Lakeshore", name: "Loons", abbr: "LAK", primary: "#0f4c5c", secondary: "#f6f6f6", accent: "#e36414" },
  { id: 24, city: "Ironwood", name: "Bison", abbr: "IRW", primary: "#5a3d2b", secondary: "#d9a441", accent: "#f4ecdc" },
  { id: 25, city: "Crescent City", name: "Gators", abbr: "CRS", primary: "#2e8b57", secondary: "#f4a300", accent: "#0d2b1d" },
  { id: 26, city: "Highline", name: "Hurricanes", abbr: "HGH", primary: "#d7263d", secondary: "#1b1b3a", accent: "#f4f4f4" },
  { id: 27, city: "Moonvale", name: "Mustangs", abbr: "MVL", primary: "#2d1e6b", secondary: "#c0c0d8", accent: "#ffd23f" },
  { id: 28, city: "Port Aurora", name: "Narwhals", abbr: "PAU", primary: "#3fb8af", secondary: "#273043", accent: "#ffffff" },
  { id: 29, city: "Red Rock", name: "Roadrunners", abbr: "RRK", primary: "#c1440e", secondary: "#f2d0a4", accent: "#3c1f0f" },
  { id: 30, city: "Summit", name: "Sasquatch", abbr: "SUM", primary: "#4a6741", secondary: "#d8d8c8", accent: "#8b5e3c" },
  { id: 31, city: "Gold Coast", name: "Flamingos", abbr: "GLD", primary: "#ff7eb6", secondary: "#0b3d91", accent: "#ffe066" }
];
function kitColors(teamId, kit) {
  const t = team(teamId);
  const k = kit === 1 || kit === 2 ? kit : 0;
  const key = `${t.id}.${k}`;
  if (k === 1) {
    const white = luminance(t.secondary) > 0.75 ? "#f4f1e8" : "#f6f3ec";
    return { key, helmet: t.primary, jersey: white, pants: luminance(t.secondary) > 0.6 ? t.primary : t.secondary, trim: t.primary, sock: t.primary };
  }
  if (k === 2) {
    return { key, helmet: t.secondary, jersey: t.secondary, pants: t.primary, trim: t.accent === t.secondary ? t.primary : t.accent, sock: t.secondary };
  }
  return { key, helmet: t.primary, jersey: t.primary, pants: t.secondary, trim: t.accent, sock: t.primary };
}
function hexRgb(hex) {
  const n = parseInt(hex.slice(1), 16);
  return [n >> 16 & 255, n >> 8 & 255, n & 255];
}
function colorDist(a, b) {
  const [r1, g1, b1] = hexRgb(a);
  const [r22, g2, b2] = hexRgb(b);
  return Math.hypot(r1 - r22, g1 - g2, b1 - b2);
}
function kitsClash(teams, kits) {
  return colorDist(kitColors(teams[0], kits[0]).jersey, kitColors(teams[1], kits[1]).jersey) < 110;
}
function resolveKits(teams, kits) {
  if (!kitsClash(teams, kits)) return kits;
  let best = kits[1];
  let bestD = -1;
  for (const k of [1, 0, 2]) {
    const d = colorDist(kitColors(teams[0], kits[0]).jersey, kitColors(teams[1], k).jersey);
    if (d > bestD) {
      bestD = d;
      best = k;
    }
  }
  if (bestD < 110) {
    for (const k0 of [0, 1, 2])
      for (const k1 of [1, 0, 2])
        if (!kitsClash(teams, [k0, k1])) return [k0, k1];
  }
  return [kits[0], best];
}
function isValidKit(k) {
  return k === 0 || k === 1 || k === 2;
}
function team(id) {
  return TEAMS[id] ?? TEAMS[0];
}
function isValidTeamId(id) {
  return typeof id === "number" && Number.isInteger(id) && id >= 0 && id < TEAMS.length;
}
function luminance(hex) {
  const n = parseInt(hex.slice(1), 16);
  const c = [n >> 16 & 255, n >> 8 & 255, n & 255].map((v) => {
    const s = v / 255;
    return s <= 0.03928 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * c[0] + 0.7152 * c[1] + 0.0722 * c[2];
}

// src/shared/rules.ts
function other(side) {
  return side === 0 ? 1 : 0;
}
function emptyStats() {
  return {
    passYds: 0,
    rushYds: 0,
    completions: 0,
    attempts: 0,
    ints: 0,
    sacks: 0,
    tds: 0,
    perfectThrows: 0,
    perfectJukes: 0,
    fgMade: 0,
    fgAtt: 0,
    xpMade: 0,
    xpAtt: 0,
    twoMade: 0,
    twoAtt: 0,
    retYds: 0
  };
}
function fieldPosText(los) {
  const y = Math.round(los);
  if (y === 50) return "MIDFIELD";
  return y < 50 ? `OWN ${y}` : `OPP ${FIELD_LEN - y}`;
}
function startPossession(s, side, los) {
  s.possession = side;
  s.los = clampLos(los);
  s.down = 1;
  s.toGo = Math.min(10, FIELD_LEN - s.los);
  s.clockRunsBetween = false;
  s.calls = [null, null];
}
function clampLos(x) {
  return Math.max(0.5, Math.min(FIELD_LEN - 0.5, Math.round(x * 2) / 2));
}
function result(kind, text, sub, side, yards = 0) {
  return { kind, text, sub, team: side, yards };
}
function afterScore(s, kicker, from = KO_FROM) {
  const recv = other(kicker);
  if (s.ot || !s.kickoffs) {
    endPossession(s, recv, START_LOS);
    return;
  }
  startPossession(s, recv, START_LOS);
  s.pendingKick = { kicker, from };
}
function startConversion(s, scorer) {
  s.conv = { team: scorer };
  startPossession(s, scorer, TWO_LOS);
}
function endPossession(s, nextSide, nextLos) {
  if (s.ot) {
    s.ot.possDone += 1;
    if (s.ot.possDone >= 2) {
      if (s.score[0] !== s.score[1]) {
        s.winner = s.score[0] > s.score[1] ? 0 : 1;
        return;
      }
      s.ot.round += 1;
      s.ot.first = other(s.ot.first);
      s.ot.possDone = 0;
      startPossession(s, s.ot.first, OT_START_LOS);
      return;
    }
    startPossession(s, other(s.possession), OT_START_LOS);
    return;
  }
  startPossession(s, nextSide, nextLos);
}
function applyOutcome(s, o) {
  const off = s.possession;
  const def = other(off);
  const offName = team(s.teams[off]).name.toUpperCase();
  const defName = team(s.teams[def]).name.toUpperCase();
  const startLos = s.los;
  let res;
  let scored = false;
  let changed = false;
  s.clockRunsBetween = false;
  const addYards = (via, yds) => {
    if (via === "pass") s.stats[off].passYds += yds;
    else if (via === "return") s.stats[off].retYds += yds;
    else s.stats[off].rushYds += yds;
  };
  if (s.conv) {
    const scorer = s.conv.team;
    const name = team(s.teams[scorer]).name.toUpperCase();
    s.conv = null;
    if (o.kind === "fg") {
      s.stats[scorer].xpAtt += 1;
      if (o.good) {
        s.score[scorer] += XP_POINTS;
        s.stats[scorer].xpMade += 1;
        res = result("xp_good", "EXTRA POINT GOOD", `${name} +1`, scorer);
        scored = true;
      } else res = result("xp_miss", "NO GOOD!", "Extra point missed", other(scorer));
    } else {
      s.stats[scorer].twoAtt += 1;
      if (o.kind === "td") {
        s.score[scorer] += TWO_POINTS;
        s.stats[scorer].twoMade += 1;
        res = result("two_good", "2-POINT GOOD!", `${name} +2`, scorer);
        scored = true;
      } else res = result("two_fail", "NO GOOD!", "2-point try stopped", other(scorer));
    }
    changed = true;
    afterScore(s, scorer);
    s.lastResult = res;
    s.resultSeq += 1;
    return { result: res, scored, possessionChanged: changed };
  }
  switch (o.kind) {
    case "td": {
      addYards(o.via, Math.round(FIELD_LEN - startLos));
      s.score[off] += TD_POINTS;
      s.stats[off].tds += 1;
      res = result("touchdown", o.via === "return" ? "RETURN TOUCHDOWN!" : "TOUCHDOWN!", `${offName} +6`, off, Math.round(FIELD_LEN - startLos));
      scored = true;
      startConversion(s, off);
      break;
    }
    case "int_td": {
      s.score[def] += TD_POINTS;
      s.stats[def].tds += 1;
      s.stats[off].ints += 1;
      res = result("touchdown", "PICK SIX!", `${defName} +6`, def);
      scored = true;
      changed = true;
      startConversion(s, def);
      break;
    }
    case "int_return": {
      s.stats[off].ints += 1;
      changed = true;
      if (s.ot) {
        res = result("interception", "INTERCEPTED!", "Overtime possession over", def);
        endPossession(s, def, OT_START_LOS);
      } else if (o.spot >= FIELD_LEN) {
        res = result("interception", "INTERCEPTED!", "Touchback", def);
        endPossession(s, def, TOUCHBACK_LOS);
      } else {
        res = result("interception", "INTERCEPTED!", `${defName} ball`, def);
        endPossession(s, def, FIELD_LEN - o.spot);
      }
      break;
    }
    case "punt": {
      const land = startLos + PUNT_NET;
      changed = true;
      if (land >= FIELD_LEN) {
        res = result("punt", "PUNT", "Touchback", def);
        endPossession(s, def, TOUCHBACK_LOS);
      } else {
        res = result("punt", "PUNT", `${PUNT_NET} yards`, def);
        endPossession(s, def, FIELD_LEN - land);
      }
      break;
    }
    case "fg": {
      const dist2 = fgDistance(startLos);
      s.stats[off].fgAtt += 1;
      changed = true;
      if (o.good) {
        s.score[off] += FG_POINTS;
        s.stats[off].fgMade += 1;
        res = result("fg_good", "IT'S GOOD!", `${dist2}-yard field goal \xB7 ${offName} +3`, off);
        scored = true;
        afterScore(s, off);
      } else {
        res = result("fg_miss", "NO GOOD!", `${dist2}-yard try missed \xB7 ${defName} ball`, def);
        endPossession(s, def, Math.max(TOUCHBACK_LOS, FIELD_LEN - (startLos - FG_SNAP_BACK)));
      }
      break;
    }
    case "ko_return": {
      const punt = !!s.play?.punt;
      const tb = punt ? TOUCHBACK_LOS : KO_TOUCHBACK_LOS;
      if (o.spot <= 0) {
        res = result("touchback", "TOUCHBACK", `Ball at the ${tb}`, off);
        startPossession(s, off, tb);
      } else {
        const spot = clampLos(o.spot);
        const ret = Math.round(spot - startLos);
        s.stats[off].retYds += Math.max(0, ret);
        const what = punt ? ret > 0 ? `${ret}-YARD PUNT RETURN` : "PUNT DOWNED" : `${Math.max(0, ret)}-YARD RETURN`;
        res = result("gain", what, `${offName} ball at the ${fieldPosText(spot).toLowerCase()}`, off, ret);
        startPossession(s, off, spot);
      }
      break;
    }
    case "ko_dead": {
      const punt = !!s.play?.punt;
      if (o.why === "touchback") {
        const tb = punt ? TOUCHBACK_LOS : KO_TOUCHBACK_LOS;
        res = result("touchback", "TOUCHBACK", `Ball at the ${tb}`, off);
        startPossession(s, off, tb);
      } else if (punt) {
        const spot = clampLos(s.ball.x);
        res = result("out_of_bounds", "PUNT OUT OF BOUNDS", `${offName} ball at the ${fieldPosText(spot).toLowerCase()}`, off);
        startPossession(s, off, spot);
      } else {
        res = result("out_of_bounds", "KICK OUT OF BOUNDS", `${offName} ball at the ${KO_OOB_LOS}`, off);
        startPossession(s, off, KO_OOB_LOS);
      }
      break;
    }
    case "incomplete": {
      res = o.breakup ? result("breakup", "BROKEN UP!", "", def) : result("incomplete", "INCOMPLETE", "", def);
      s.down += 1;
      if (s.down > 4) {
        res = result("turnover_downs", "TURNOVER ON DOWNS", `${defName} ball`, def);
        changed = true;
        endPossession(s, def, FIELD_LEN - startLos);
      }
      break;
    }
    case "down": {
      if (o.spot >= FIELD_LEN) {
        return applyOutcome(s, { kind: "td", via: o.via });
      }
      if (o.spot <= 0) {
        s.score[def] += SAFETY_POINTS;
        res = result("safety", "SAFETY!", `${defName} +2`, def);
        scored = true;
        changed = true;
        if (o.sack) s.stats[def].sacks += 1;
        afterScore(s, off, KO_SAFETY_FROM);
        break;
      }
      const spot = clampLos(o.spot);
      const gained = spot - startLos;
      if (!o.sack) addYards(o.via, Math.round(gained));
      const firstDownLine = startLos + s.toGo;
      const ydsText = `${gained >= 0 ? "+" : ""}${Math.round(gained)} yards`;
      if (spot >= firstDownLine - 0.01) {
        s.los = spot;
        s.down = 1;
        s.toGo = Math.min(10, FIELD_LEN - spot);
        res = result("first_down", "FIRST DOWN!", ydsText, off, gained);
      } else {
        s.down += 1;
        s.toGo = firstDownLine - spot;
        s.los = spot;
        if (s.down > 4) {
          res = result("turnover_downs", "TURNOVER ON DOWNS", `${defName} ball`, def, gained);
          changed = true;
          endPossession(s, def, FIELD_LEN - spot);
          break;
        }
        if (o.sack) res = result("sack", "SACKED!", ydsText, def, gained);
        else if (o.oob) res = result("out_of_bounds", "OUT OF BOUNDS", ydsText, off, gained);
        else res = result(gained >= 0 ? "gain" : "loss", ydsText.toUpperCase(), "", off, gained);
      }
      if (o.sack) s.stats[def].sacks += 1;
      s.clockRunsBetween = !o.oob && !s.ot;
      break;
    }
  }
  s.lastResult = res;
  s.resultSeq += 1;
  return { result: res, scored, possessionChanged: changed };
}
function endOfHalf(s) {
  const q = s.quarter ?? (s.half === 2 ? 4 : 2);
  if (q === 1 || q === 3) {
    s.quarter = q + 1;
    s.clock = s.halfSeconds;
    s.clockRunsBetween = false;
    return "quarter";
  }
  s.timeouts = [TIMEOUTS_PER_HALF, TIMEOUTS_PER_HALF];
  s.pendingKick = null;
  if (q === 2) {
    s.half = 2;
    s.quarter = 3;
    s.clock = s.halfSeconds;
    startPossession(s, other(s.firstPossession), START_LOS);
    if (s.kickoffs) s.pendingKick = { kicker: s.firstPossession, from: KO_FROM };
    return "halftime";
  }
  if (s.score[0] !== s.score[1]) {
    s.winner = s.score[0] > s.score[1] ? 0 : 1;
    return "final";
  }
  s.ot = { round: 1, first: s.firstPossession, possDone: 0 };
  s.clock = 0;
  startPossession(s, s.firstPossession, OT_START_LOS);
  return "overtime";
}
function canPunt(s) {
  return s.down === 4 && !s.ot && !s.conv;
}
function canKickFG(s) {
  return !s.conv && fgDistance(s.los) <= FG_MAX_DIST;
}

// src/shared/protocol.ts
var PROTOCOL_VERSION = 3;
var MAX_MESSAGE_BYTES = 1024;
var isNum = (v) => typeof v === "number" && Number.isFinite(v);
var isInt = (v) => isNum(v) && Number.isInteger(v);
var clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
var ROOM_CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ";
var ROOM_CODE_LENGTH = 4;
var CODE_RE = new RegExp(`^[${ROOM_CODE_ALPHABET}]{${ROOM_CODE_LENGTH}}$`);
var TOKEN_RE = /^[0-9a-f]{32}$/;
var CALL_RE = /^[a-z0-9]{2,16}$/;
function parseClientMsg(raw) {
  if (typeof raw !== "string" || raw.length > MAX_MESSAGE_BYTES) return null;
  let m;
  try {
    m = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!m || typeof m !== "object" || Array.isArray(m)) return null;
  const o = m;
  switch (o.t) {
    case "hello":
      return isInt(o.v) ? { t: "hello", v: o.v } : null;
    case "create":
      return { t: "create" };
    case "join":
      return typeof o.code === "string" && CODE_RE.test(o.code) ? { t: "join", code: o.code } : null;
    case "resume":
      return typeof o.token === "string" && TOKEN_RE.test(o.token) ? { t: "resume", token: o.token } : null;
    case "team":
      if (!isInt(o.team) || o.team < 0 || o.team >= 64) return null;
      if (o.kit !== void 0 && (!isInt(o.kit) || o.kit < 0 || o.kit > 2)) return null;
      return { t: "team", team: o.team, kit: o.kit ?? 0 };
    case "ready":
      return typeof o.ready === "boolean" ? { t: "ready", ready: o.ready } : null;
    case "call":
      return typeof o.id === "string" && CALL_RE.test(o.id) ? { t: "call", id: o.id } : null;
    case "pick":
      return isInt(o.slot) && o.slot >= 0 && o.slot < 7 ? { t: "pick", slot: o.slot } : null;
    case "timeout":
      return { t: "timeout" };
    case "opt":
      return typeof o.aiDefense === "boolean" ? { t: "opt", aiDefense: o.aiDefense } : null;
    case "in": {
      const i = o.i;
      if (!Array.isArray(i) || i.length < 9 || i.length > 11 || !i.every(isNum)) return null;
      const [seq, mx, my, ax, ay, th, ac, sc, pt, dc = 0, vt = -1] = i;
      if (!isInt(vt) || vt < -1) return null;
      if (!isInt(dc) || dc < 0 || dc > 255) return null;
      if (!isInt(seq) || seq < 0 || seq > 2 ** 31) return null;
      if (!isInt(ac) || !isInt(sc) || ac < 0 || ac > 255 || sc < 0 || sc > 255) return null;
      if (th !== 0 && th !== 1) return null;
      if (!isInt(pt) || pt < 0 || pt > 5) return null;
      return {
        t: "in",
        i: [
          seq,
          clamp(mx, -1, 1),
          clamp(my, -1, 1),
          clamp(ax, -ENDZONE - 20, FIELD_LEN + ENDZONE + 20),
          clamp(ay, -20, FIELD_W + 20),
          th,
          ac,
          sc,
          pt,
          dc,
          vt
        ]
      };
    }
    case "rematch":
      return { t: "rematch" };
    case "leave":
      return { t: "leave" };
    case "ping":
      return isNum(o.c) ? { t: "ping", c: o.c } : null;
    default:
      return null;
  }
}
function wireToInput(w) {
  let [, mx, my] = w;
  const m = Math.hypot(mx, my);
  if (m > 1) {
    mx /= m;
    my /= m;
  }
  return {
    seq: w[0],
    mx,
    my,
    ax: w[3],
    ay: w[4],
    throwHeld: w[5] === 1,
    actionCount: w[6],
    switchCount: w[7],
    passType: w[8] % 3 === 0 || w[8] % 3 === 2 ? w[8] % 3 : 1,
    aimExact: w[8] >= 3,
    diveCount: w[9] ?? 0,
    viewTick: w[10] !== void 0 && w[10] >= 0 ? w[10] : void 0
  };
}
var ERROR_TEXT = {
  bad_msg: "The server could not understand that request.",
  bad_version: "Your game is out of date. Refresh the page to update.",
  no_room: "That room code doesn't exist. Check the code and try again.",
  room_full: "That room is already full (2 players).",
  bad_token: "Your previous session has expired.",
  not_in_room: "You are not in a room.",
  team_taken: "Your opponent already picked that team.",
  bad_team: "Unknown team.",
  wrong_phase: "You can't do that right now.",
  rate_limited: "Too many messages \u2014 slow down.",
  server_full: "The server is full right now. Try again in a minute."
};
var PHASES = ["pregame", "playcall", "presnap", "live", "kick", "kickair", "dead", "score", "halftime", "final", "kickoff"];
var BALL_STATES = ["held", "air", "loose", "dead"];
var EVENT_TYPES = [
  "snap",
  "throw",
  "catch",
  "tackle",
  "sack",
  "incomplete",
  "interception",
  "touchdown",
  "safety",
  "first_down",
  "turnover_downs",
  "punt",
  "out_of_bounds",
  "halftime",
  "overtime",
  "final",
  "whistle",
  "broken_tackle",
  "dive",
  "burst",
  "handoff",
  "breakup",
  "switch",
  "juke",
  "stiff_arm",
  "audible",
  "timeout",
  "kick",
  "fg_good",
  "fg_miss",
  "jam",
  "xp_good",
  "xp_miss",
  "two_good",
  "two_fail",
  "kickoff",
  "touchback",
  "injury",
  "kick_power"
];
function snapshotToWire(s, includeStats) {
  const p = [];
  for (const q of s.players) {
    const flags = (q.burst ? 1 : 0) | (q.diving ? 2 : 0) | (q.down ? 4 : 0) | (q.engaged ? 8 : 0) | (q.burstReady ? 16 : 0) | (q.diveReady ? 32 : 0) | (q.facing === -1 ? 64 : 0) | (q.stiff ? 128 : 0) | (q.stiffReady ? 256 : 0) | (q.hurt ? 512 : 0);
    p.push(q.x, q.y, ANIMS.indexOf(q.anim), q.animT, flags, q.tired, q.stamina);
  }
  const b = s.ball;
  return [
    s.tick,
    PHASES.indexOf(s.phase),
    s.phaseT,
    s.clock,
    s.half,
    s.ot,
    s.score,
    s.poss,
    s.down,
    s.toGo,
    s.los,
    s.teams,
    p,
    [BALL_STATES.indexOf(b.state), b.carrier, b.x, b.y, b.z, b.tx, b.ty, b.sx, b.sy, b.target, b.passType, b.kick ? 1 : 0],
    s.control,
    s.myCall,
    s.oppReady ? 1 : 0,
    s.playId,
    s.thrown ? 1 : 0,
    s.chargeT,
    s.canThrow ? 1 : 0,
    s.defPref,
    s.result ? [s.result.seq, s.result.kind, s.result.text, s.result.sub, s.result.team] : null,
    s.events.map((e) => [EVENT_TYPES.indexOf(e.type), e.team ?? -1, e.x ?? null, e.y ?? null, e.v ?? null]),
    s.fseq,
    s.winner,
    s.ack,
    s.mySpeed,
    includeStats ? s.stats : null,
    s.frame,
    s.myVel,
    s.timeouts,
    s.hikeReady ? 1 : 0,
    [s.audibled[0] ? 1 : 0, s.audibled[1] ? 1 : 0],
    s.kick ? [s.kick.dist, s.kick.holdT, s.kick.holding ? 1 : 0, s.kick.released ? 1 : 0, s.kick.needle, s.kick.good ? 1 : 0, s.kick.punt ? 2 : s.kick.ko ? 1 : 0, s.kick.zone, s.kick.stage, s.kick.power, s.kick.need] : null,
    s.kits,
    s.conv ? 1 : 0,
    s.koPlay ? 1 : 0,
    s.aiDef ? 1 : 0,
    s.myAuto,
    s.changesLeft,
    s.throwMax,
    s.callMax,
    s.quarter
  ];
}

// src/shared/plays.ts
var OFF = { QB: 0, OL1: 1, OL2: 2, RB: 3, WR1: 4, WR2: 5, TE: 6 };
var DEF = { DL1: 0, DL2: 1, LB: 2, CB1: 3, CB2: 4, S1: 5, S2: 6 };
var TEAM_SIZE = 7;
var OFF_ROLES = ["QB", "OL", "OL", "RB", "WR", "WR", "TE"];
var DEF_ROLES = ["DL", "DL", "LB", "CB", "CB", "S", "S"];
var W = FIELD_W;
var M = MID_Y;
var FORMATIONS = {
  //       QB          OL1             OL2             RB              WR1         WR2            TE
  gun: [[-4, M], [-0.8, M - 1.6], [-0.8, M + 1.6], [-4.2, M + 2.4], [-0.8, 7], [-0.8, W - 7], [-0.8, M + 9]],
  trips: [[-4, M], [-0.8, M - 1.6], [-0.8, M + 1.6], [-4.2, M - 2.4], [-0.8, W - 5], [-0.8, M + 11], [-0.8, M + 5]],
  spread: [[-4, M], [-0.8, M - 1.6], [-0.8, M + 1.6], [-0.8, M - 9], [-0.8, 6], [-0.8, W - 6], [-0.8, M + 9]],
  single: [[-1.2, M], [-0.8, M - 1.6], [-0.8, M + 1.6], [-5.5, M], [-0.8, 7], [-0.8, W - 7], [-0.8, M + 3.4]],
  heavy: [[-1.2, M], [-0.8, M - 1.6], [-0.8, M + 1.6], [-6, M], [-0.8, 11], [-0.8, M + 3.6], [-0.8, M - 3.4]]
};
var OFF_FORMATION = FORMATIONS.gun;
var DEF_FORMATION = [
  [1, M - 1.8],
  [1, M + 1.8],
  [5, M],
  [6, 7],
  [6, W - 7],
  [9, M + 9],
  [14, M - 4]
];
var r = (pts, end = "stop", delay) => ({ pts, end, delay });
var R = {
  slant: () => r([[2.5, 0], [9, 6]], "go"),
  hitch: (n = 5) => r([[n, 0], [n - 1.2, 0.6]]),
  out: (n = 5) => r([[n, 0], [n + 0.3, -8]]),
  dig: (n = 12) => r([[n, 0], [n + 0.5, 14]], "go"),
  inn: (n = 5) => r([[n, 0], [n + 0.3, 10]], "go"),
  curl: (n = 11) => r([[n, 0], [n - 1.5, 1]]),
  post: (n = 12) => r([[n, 0], [n + 14, 8]], "go"),
  corner: (n = 10) => r([[n, 0], [n + 12, -8]], "go"),
  go: () => r([[40, 0]], "go"),
  seam: () => r([[38, 0.6]], "go"),
  flat: () => r([[1.5, -6], [3, -10]]),
  arrow: () => r([[2, -5], [4, -9]]),
  drag: () => r([[2, 0], [3, 20]], "go"),
  comeback: (n = 13) => r([[n, 0], [n - 3, -3]]),
  wheel: () => r([[1, -5], [4, -9], [30, -9.5]], "go"),
  sluggo: () => r([[2.5, 0], [5, 3], [36, 3]], "go"),
  stick: (n = 5) => r([[n, 0], [n, 1.5]]),
  cross: () => r([[10, 0], [18, 16]], "go"),
  bubble: () => r([[-1.5, -2.5], [-1, -6]]),
  check: () => r([[0.5, -6], [2, -9]]),
  screen: () => r([[-2, -4], [-1.5, -7.5]], "stop", 0.9),
  outShort: () => r([[4, -3], [5, -3]])
};
var P = (p) => p;
var PLAYS = [
  // QUICK -------------------------------------------------------------------
  P({
    id: "slant",
    name: "Slants",
    kind: "pass",
    cat: "quick",
    formation: "gun",
    desc: "Fast inside cuts. Beats the blitz.",
    routes: { [OFF.WR1]: R.slant(), [OFF.WR2]: R.slant(), [OFF.TE]: r([[4, 0], [5, -7]]) }
  }),
  P({
    id: "stick",
    name: "Stick",
    kind: "pass",
    cat: "quick",
    formation: "trips",
    desc: "Stick, flat and a clear-out. Easy reads.",
    routes: { [OFF.TE]: R.stick(5), [OFF.WR2]: R.arrow(), [OFF.WR1]: R.go(), [OFF.RB]: R.check() }
  }),
  P({
    id: "outs",
    name: "Quick Out",
    kind: "pass",
    cat: "quick",
    formation: "spread",
    desc: "Everyone breaks out at 5. Beats soft zone.",
    routes: { [OFF.WR1]: R.out(5), [OFF.WR2]: R.out(5), [OFF.RB]: R.hitch(5), [OFF.TE]: R.hitch(5) }
  }),
  P({
    id: "bubble",
    name: "WR Bubble Screen",
    kind: "pass",
    cat: "quick",
    formation: "trips",
    desc: "Throw it out wide behind two blockers.",
    routes: { [OFF.WR2]: R.bubble() }
  }),
  P({
    id: "hbscreen",
    name: "HB Slip Screen",
    kind: "pass",
    cat: "quick",
    formation: "gun",
    desc: "Let the rush in, dump it to the back.",
    routes: { [OFF.RB]: R.screen(), [OFF.WR1]: R.go(), [OFF.WR2]: R.go(), [OFF.TE]: R.inn(6) },
    screenRelease: 0.9
  }),
  P({
    id: "hitches",
    name: "Four Hitches",
    kind: "pass",
    cat: "quick",
    formation: "spread",
    desc: "Everyone stops at 6 and turns. Beats soft coverage.",
    routes: { [OFF.WR1]: R.hitch(6), [OFF.WR2]: R.hitch(6), [OFF.RB]: R.hitch(5), [OFF.TE]: R.hitch(5) }
  }),
  P({
    id: "snag",
    name: "Snag",
    kind: "pass",
    cat: "quick",
    formation: "trips",
    desc: "Corner, flat and a sit-down snag. A triangle read vs zone.",
    routes: { [OFF.WR2]: r([[5, 0], [6, 3]]), [OFF.WR1]: R.corner(9), [OFF.TE]: R.flat(), [OFF.RB]: R.check() }
  }),
  P({
    id: "shallow",
    name: "Shallow Cross",
    kind: "pass",
    cat: "quick",
    formation: "gun",
    desc: "A quick drag underneath a dig. Good vs man.",
    routes: { [OFF.WR1]: R.drag(), [OFF.TE]: R.dig(10), [OFF.WR2]: R.go(), [OFF.RB]: R.check() }
  }),
  // MEDIUM ------------------------------------------------------------------
  P({
    id: "flood",
    name: "Flood",
    kind: "pass",
    cat: "medium",
    formation: "gun",
    desc: "Curl, out, corner and a flat. Good vs zone.",
    routes: { [OFF.WR1]: R.curl(11), [OFF.WR2]: r([[9, 0], [9, -6]]), [OFF.TE]: r([[10, 0], [20, -9]], "go"), [OFF.RB]: r([[1.5, -7], [4, -10]]) }
  }),
  P({
    id: "cross",
    name: "Mesh",
    kind: "pass",
    cat: "medium",
    formation: "gun",
    desc: "Crossing routes. Good vs man.",
    routes: { [OFF.WR1]: r([[4, 0], [6, 24]], "go"), [OFF.WR2]: r([[12, 0], [12, 14]], "go"), [OFF.TE]: r([[12, 0], [26, 7]], "go"), [OFF.RB]: r([[3, 3]]) }
  }),
  P({
    id: "smash",
    name: "Smash",
    kind: "pass",
    cat: "medium",
    formation: "spread",
    desc: "Hitch under, corner over on one side. Beats Cover 2.",
    routes: { [OFF.WR1]: R.hitch(5), [OFF.WR2]: R.hitch(5), [OFF.RB]: R.corner(10), [OFF.TE]: R.arrow() }
  }),
  P({
    id: "levels",
    name: "Levels",
    kind: "pass",
    cat: "medium",
    formation: "trips",
    desc: "Two in-breakers stacked. Read high to low.",
    routes: { [OFF.TE]: R.inn(5), [OFF.WR2]: R.dig(12), [OFF.WR1]: R.post(14), [OFF.RB]: R.check() }
  }),
  P({
    id: "ycross",
    name: "Y-Cross",
    kind: "pass",
    cat: "medium",
    formation: "single",
    desc: "Tight end deep cross with a post on top.",
    routes: { [OFF.TE]: R.cross(), [OFF.WR1]: R.post(12), [OFF.WR2]: R.comeback(13), [OFF.RB]: R.flat() }
  }),
  P({
    id: "dagger",
    name: "Dagger",
    kind: "pass",
    cat: "medium",
    formation: "trips",
    desc: "Slot clears the seam, the outside man digs in behind it.",
    routes: { [OFF.WR2]: R.seam(), [OFF.WR1]: R.dig(14), [OFF.TE]: R.hitch(6), [OFF.RB]: R.check() }
  }),
  P({
    id: "curlflat",
    name: "Curl Flat",
    kind: "pass",
    cat: "medium",
    formation: "gun",
    desc: "Curls outside, a flat underneath. Safe vs Cover 3.",
    routes: { [OFF.WR1]: R.curl(10), [OFF.WR2]: R.curl(10), [OFF.TE]: R.flat(), [OFF.RB]: R.check() }
  }),
  P({
    id: "scat",
    name: "Comebacks",
    kind: "pass",
    cat: "medium",
    formation: "spread",
    desc: "Outside comebacks at 14 with a short in. Beats off coverage.",
    routes: { [OFF.WR1]: R.comeback(14), [OFF.WR2]: R.comeback(14), [OFF.TE]: R.inn(6), [OFF.RB]: R.flat() }
  }),
  // DEEP --------------------------------------------------------------------
  P({
    id: "verts",
    name: "Four Verticals",
    kind: "pass",
    cat: "deep",
    formation: "spread",
    desc: "Four go routes. Stretch the safeties.",
    routes: { [OFF.WR1]: R.go(), [OFF.WR2]: R.go(), [OFF.RB]: R.seam(), [OFF.TE]: R.seam() }
  }),
  P({
    id: "deep",
    name: "Streaks",
    kind: "pass",
    cat: "deep",
    formation: "gun",
    desc: "Everyone runs long. Needs time.",
    routes: { [OFF.WR1]: R.go(), [OFF.WR2]: R.go(), [OFF.TE]: r([[30, 3]], "go") }
  }),
  P({
    id: "postwheel",
    name: "Post Wheel",
    kind: "pass",
    cat: "deep",
    formation: "trips",
    desc: "Post clears the corner, slot wheels up the sideline.",
    routes: { [OFF.WR1]: R.post(14), [OFF.WR2]: R.wheel(), [OFF.TE]: R.drag(), [OFF.RB]: R.check() }
  }),
  P({
    id: "sluggo",
    name: "Slant & Go",
    kind: "pass",
    cat: "deep",
    formation: "single",
    desc: "Fake the slant, go deep. Beats jumpy corners.",
    routes: { [OFF.WR1]: R.sluggo(), [OFF.WR2]: R.sluggo(), [OFF.TE]: R.hitch(6), [OFF.RB]: R.check() }
  }),
  P({
    id: "seams",
    name: "Curl Seams",
    kind: "pass",
    cat: "deep",
    formation: "spread",
    desc: "Two seams split the safeties, curls underneath.",
    routes: { [OFF.WR1]: R.curl(12), [OFF.WR2]: R.curl(12), [OFF.RB]: R.seam(), [OFF.TE]: R.seam() }
  }),
  P({
    id: "twopost",
    name: "Double Posts",
    kind: "pass",
    cat: "deep",
    formation: "spread",
    desc: "Two posts split the safeties, a hitch underneath.",
    routes: { [OFF.WR1]: R.post(13), [OFF.WR2]: R.post(13), [OFF.TE]: R.hitch(5), [OFF.RB]: R.check() }
  }),
  P({
    id: "scissors",
    name: "Scissors",
    kind: "pass",
    cat: "deep",
    formation: "trips",
    desc: "A post and a corner cross over each other. Beats quarters.",
    routes: { [OFF.WR1]: R.post(12), [OFF.WR2]: R.corner(10), [OFF.TE]: R.hitch(6), [OFF.RB]: R.check() }
  }),
  P({
    id: "hailmary",
    name: "Hail Mary",
    kind: "pass",
    cat: "deep",
    formation: "trips",
    desc: "Everybody to the end zone. Throw it up and hope.",
    routes: { [OFF.WR1]: R.go(), [OFF.WR2]: R.go(), [OFF.TE]: R.seam(), [OFF.RB]: R.seam() }
  }),
  // PLAY ACTION -------------------------------------------------------------
  // Fake the handoff first: linebackers and safeties near the line bite and
  // step up, opening the middle behind them. Takes longer to develop.
  P({
    id: "pacross",
    name: "PA Crossers",
    kind: "pass",
    cat: "pa",
    formation: "single",
    desc: "Fake it, then two deep crossers behind the linebackers.",
    routes: { [OFF.WR1]: r([[7, 0], [15, 10], [17, 26]], "go"), [OFF.TE]: r([[3, 0], [6, 12], [7, 28]], "go"), [OFF.WR2]: R.post(13) },
    playAction: [[-3, 0.5], [0.5, 0.5]]
  }),
  P({
    id: "paflood",
    name: "PA Flood",
    kind: "pass",
    cat: "pa",
    formation: "single",
    desc: "Three levels to one side after the fake.",
    routes: { [OFF.WR1]: R.corner(11), [OFF.TE]: r([[8, 0], [10, -8]]), [OFF.WR2]: r([[6, 0], [12, 18], [13, 30]], "go") },
    playAction: [[-3, -0.5], [0.5, -1]]
  }),
  P({
    id: "pa",
    name: "PA Post",
    kind: "pass",
    cat: "pa",
    formation: "single",
    desc: "Fake the handoff, linebackers bite, go over the top.",
    routes: { [OFF.WR1]: R.post(14), [OFF.WR2]: R.go(), [OFF.TE]: r([[6, 0], [8, 16]], "go") },
    playAction: [[-3, 0.5], [0.5, 0.5]]
  }),
  P({
    id: "padig",
    name: "PA Dig",
    kind: "pass",
    cat: "pa",
    formation: "gun",
    desc: "Fake, then a deep dig under a post. Beats Cover 3.",
    routes: { [OFF.WR1]: R.dig(15), [OFF.WR2]: R.post(15), [OFF.TE]: R.seam() },
    playAction: [[-2.5, -2.5], [0.5, -2.5]]
  }),
  P({
    id: "pashot",
    name: "PA Max Shot",
    kind: "pass",
    cat: "pa",
    formation: "heavy",
    desc: "Looks like a goal-line run. Everyone bites, go long.",
    routes: { [OFF.WR1]: R.go(), [OFF.WR2]: R.corner(10), [OFF.TE]: r([[4, 0], [6, 14], [8, 24]], "go") },
    playAction: [[-3.5, 0.5], [0.5, 0.5]]
  }),
  P({
    id: "paboot",
    name: "PA Waggle",
    kind: "pass",
    cat: "pa",
    formation: "single",
    desc: "Fake one way, everything floods the other side.",
    routes: { [OFF.WR1]: R.corner(12), [OFF.TE]: R.flat(), [OFF.WR2]: r([[8, 0], [10, 16], [11, 26]], "go") },
    playAction: [[-3, -0.5], [0.5, -1]]
  }),
  P({
    id: "paseam",
    name: "PA Seam",
    kind: "pass",
    cat: "pa",
    formation: "gun",
    desc: "Fake the run, the tight end splits the safeties.",
    routes: { [OFF.WR1]: R.go(), [OFF.WR2]: R.out(10), [OFF.TE]: R.seam() },
    playAction: [[-2.5, -2.5], [0.5, -2.5]]
  }),
  P({
    id: "pawheel",
    name: "PA TE Wheel",
    kind: "pass",
    cat: "pa",
    formation: "single",
    desc: "Fake, then the tight end wheels up the sideline.",
    routes: { [OFF.WR1]: R.post(14), [OFF.WR2]: R.dig(12), [OFF.TE]: R.wheel() },
    playAction: [[-3, 0.5], [0.5, 0.5]]
  }),
  // RUN ---------------------------------------------------------------------
  P({
    id: "dive",
    name: "HB Dive",
    kind: "run",
    cat: "run",
    formation: "single",
    desc: "Hand off straight up the middle.",
    routes: {},
    runner: "RB",
    runPath: [[-3.2, 0.6], [2, 0.4], [10, 0]]
  }),
  P({
    id: "sweep",
    name: "HB Toss",
    kind: "run",
    cat: "run",
    formation: "heavy",
    desc: "Pitch it outside and race the edge.",
    routes: {},
    runner: "RB",
    runPath: [[-5, 4], [-3, 13], [6, 20]]
  }),
  P({
    id: "counter",
    name: "HB Counter",
    kind: "run",
    cat: "run",
    formation: "gun",
    desc: "Step one way, cut back the other.",
    routes: {},
    runner: "RB",
    runPath: [[-4.5, 4.5], [-3, -1], [2, -4], [10, -8]]
  }),
  P({
    id: "draw",
    name: "HB Draw",
    kind: "run",
    cat: "run",
    formation: "gun",
    desc: "Looks like a pass, then a late handoff. Beats the blitz.",
    routes: { [OFF.WR1]: R.go(), [OFF.WR2]: R.go() },
    runner: "RB",
    runPath: [[-4.2, 0.6], [2, 0.4], [12, 0]],
    handoffAt: 0.9
  }),
  P({
    id: "qbpower",
    name: "QB Power",
    kind: "run",
    cat: "run",
    formation: "heavy",
    desc: "QB keeps it behind a lead blocker. Short yardage.",
    routes: {},
    runner: "QB",
    runPath: [[0.5, -2], [3, -3], [10, -4]]
  }),
  P({
    id: "stretch",
    name: "HB Stretch",
    kind: "run",
    cat: "run",
    formation: "single",
    desc: "Run wide along the line, then cut up the first crease.",
    routes: {},
    runner: "RB",
    runPath: [[-4.5, 2], [-1.5, 8], [3, 12], [12, 14]]
  }),
  P({
    id: "tossweak",
    name: "HB Toss Weak",
    kind: "run",
    cat: "run",
    formation: "heavy",
    desc: "Pitch it outside, away from the tight end.",
    routes: {},
    runner: "RB",
    runPath: [[-5, -4], [-3, -13], [6, -20]]
  }),
  P({
    id: "sneak",
    name: "QB Sneak",
    kind: "run",
    cat: "run",
    formation: "heavy",
    desc: "The QB pushes straight ahead. Best on 3rd or 4th and 1.",
    routes: {},
    runner: "QB",
    runPath: [[0.6, 0], [3, 0.3], [8, 0]]
  })
];
var PUNT_PLAY = {
  id: "punt",
  name: "Punt",
  kind: "punt",
  cat: "special",
  formation: "gun",
  desc: "Punt it away. The other team can field it and return it.",
  routes: {}
};
var FG_PLAY = {
  id: "fg",
  name: "Field Goal",
  kind: "fg",
  cat: "special",
  formation: "gun",
  desc: "Kick it through for 3. Time the meter.",
  routes: {}
};
var XP_PLAY = {
  id: "xp",
  name: "Extra Point",
  kind: "fg",
  cat: "special",
  formation: "gun",
  desc: "A 32-yard kick for 1. Time the meter.",
  routes: {}
};
function playById(id) {
  if (id === "punt") return PUNT_PLAY;
  if (id === "fg") return FG_PLAY;
  if (id === "xp") return XP_PLAY;
  return PLAYS.find((p) => p.id === id);
}
function inwardSign(slotY) {
  return slotY < MID_Y ? 1 : -1;
}
function routeWaypoints(route, x, y) {
  const sign = inwardSign(y);
  return route.pts.map(([dx, din]) => [x + dx, y + din * sign]);
}
var ZONES = {
  flatT: { dx: 4, y: 7, r: 7, deep: false },
  flatB: { dx: 4, y: W - 7, r: 7, deep: false },
  hookT: { dx: 7, y: M - 8, r: 7, deep: false },
  hookB: { dx: 7, y: M + 8, r: 7, deep: false },
  hookM: { dx: 6.5, y: M, r: 7.5, deep: false },
  curlT: { dx: 10, y: 11, r: 7, deep: false },
  curlB: { dx: 10, y: W - 11, r: 7, deep: false },
  deep3T: { dx: 17, y: 8, r: 11, deep: true },
  deep3M: { dx: 19, y: M, r: 12, deep: true },
  deep3B: { dx: 17, y: W - 8, r: 11, deep: true },
  deep2T: { dx: 16, y: 14, r: 14, deep: true },
  deep2B: { dx: 16, y: W - 14, r: 14, deep: true },
  deep4a: { dx: 14, y: 6.5, r: 9, deep: true },
  deep4b: { dx: 15, y: 20, r: 9, deep: true },
  deep4c: { dx: 15, y: W - 20, r: 9, deep: true },
  deep4d: { dx: 14, y: W - 6.5, r: 9, deep: true },
  tampa: { dx: 13, y: M, r: 9, deep: true },
  robber: { dx: 9, y: M, r: 9, deep: false },
  box: { dx: 3.5, y: M, r: 6, deep: false },
  prevT: { dx: 24, y: 10, r: 14, deep: true },
  prevM: { dx: 27, y: M, r: 14, deep: true },
  prevB: { dx: 24, y: W - 10, r: 14, deep: true }
};
var rush = (lane) => ({ t: "rush", lane });
var man = (target, press = false, over = false) => ({ t: "man", target, press, over });
var zone = (z) => ({ t: "zone", z });
var spy = { t: "spy" };
var O = OFF;
var D = (d) => d;
var DEF_CALLS = [
  // MAN ---------------------------------------------------------------------
  D({
    id: "man",
    name: "Cover 1",
    cat: "man",
    desc: "Man everywhere, one free safety deep.",
    a: [rush(), rush(), man(O.RB), man(O.WR1), man(O.WR2), man(O.TE), zone("deep3M")]
  }),
  D({
    id: "press",
    name: "Cover 1 Press",
    cat: "man",
    desc: "Jam receivers at the line. Risky if they win.",
    a: [rush(), rush(), man(O.RB), man(O.WR1, true), man(O.WR2, true), man(O.TE, true), zone("deep3M")]
  }),
  D({
    id: "twoman",
    name: "2 Man Under",
    cat: "man",
    desc: "Tight man underneath, two safeties over the top.",
    a: [rush(), rush(), man(O.TE), man(O.WR1), man(O.WR2), zone("deep2T"), zone("deep2B")]
  }),
  D({
    id: "cover0",
    name: "Cover 0",
    cat: "man",
    desc: "Press man, no deep help, extra rusher.",
    a: [rush(), rush(), man(O.RB), man(O.WR1, true), man(O.WR2, true), man(O.TE, true), rush("in")]
  }),
  D({
    id: "spy",
    name: "Cover 1 QB Spy",
    cat: "man",
    desc: "Man coverage with a linebacker watching the QB.",
    a: [rush(), rush(), spy, man(O.WR1), man(O.WR2), man(O.TE), zone("deep3M")]
  }),
  D({
    id: "twopress",
    name: "2 Man Press",
    cat: "man",
    desc: "Jam them at the line with two safeties deep behind.",
    a: [rush(), rush(), man(O.TE), man(O.WR1, true), man(O.WR2, true), zone("deep2T"), zone("deep2B")]
  }),
  // ZONE --------------------------------------------------------------------
  D({
    id: "cover2",
    name: "Cover 2",
    cat: "zone",
    desc: "Corners sit in the flats, two deep halves.",
    a: [rush(), rush(), zone("hookM"), zone("flatT"), zone("flatB"), zone("deep2T"), zone("deep2B")]
  }),
  D({
    id: "zone",
    name: "Cover 3",
    cat: "zone",
    desc: "Three deep, two underneath. Flats are soft.",
    a: [rush(), rush(), zone("hookT"), zone("deep3T"), zone("deep3B"), zone("hookB"), zone("deep3M")]
  }),
  D({
    id: "cover4",
    name: "Cover 4 Quarters",
    cat: "zone",
    desc: "Four deep quarters. Nothing gets over the top.",
    a: [rush(), rush(), zone("hookM"), zone("deep4a"), zone("deep4d"), zone("deep4c"), zone("deep4b")]
  }),
  D({
    id: "tampa",
    name: "Tampa 2",
    cat: "zone",
    desc: "Cover 2 with the linebacker running deep middle.",
    a: [rush(), rush(), zone("tampa"), zone("flatT"), zone("flatB"), zone("deep2T"), zone("deep2B")]
  }),
  D({
    id: "prevent",
    name: "Prevent",
    cat: "zone",
    desc: "Everyone deep. Give up short stuff, never the big one.",
    a: [rush(), rush(), zone("hookM"), zone("prevT"), zone("prevB"), zone("curlB"), zone("prevM")]
  }),
  D({
    id: "cover6",
    name: "Cover 6",
    cat: "zone",
    desc: "Quarters on one side, Cover 2 on the other.",
    a: [rush(), rush(), zone("hookM"), zone("deep4a"), zone("flatB"), zone("deep4b"), zone("deep2B")]
  }),
  D({
    id: "buzz",
    name: "Cover 3 Buzz",
    cat: "zone",
    desc: "Cover 3 with a safety buzzing down to take the hook.",
    a: [rush(), rush(), zone("hookB"), zone("deep3T"), zone("deep3B"), zone("curlT"), zone("deep3M")]
  }),
  // BLITZ -------------------------------------------------------------------
  D({
    id: "blitz",
    name: "Mike Blitz",
    cat: "blitz",
    desc: "Send the linebacker, man behind.",
    a: [rush(), rush(), rush("in"), man(O.WR1), man(O.WR2), man(O.TE), zone("deep3M")]
  }),
  D({
    id: "sblitz",
    name: "SS Blitz",
    cat: "blitz",
    desc: "Safety comes off the edge.",
    a: [rush(), rush(), man(O.TE), man(O.WR1), man(O.WR2), rush("edgeB"), zone("deep3M")]
  }),
  D({
    id: "cblitz",
    name: "Corner Blitz",
    cat: "blitz",
    desc: "Corner rushes, safety rotates down onto his man.",
    a: [rush(), rush(), man(O.TE), rush("edgeT"), man(O.WR2), man(O.WR1), zone("deep3M")]
  }),
  D({
    id: "zeroblitz",
    name: "Cover 0 Blitz",
    cat: "blitz",
    desc: "All-out pressure, press man, no safety.",
    a: [rush(), rush(), rush("in"), man(O.WR1, true), man(O.WR2, true), rush("edgeB"), man(O.TE)]
  }),
  D({
    id: "firezone",
    name: "Fire Zone 3",
    cat: "blitz",
    desc: "Zone blitz: a lineman drops, linebacker and safety rush.",
    a: [rush(), zone("hookM"), rush("in"), zone("deep3T"), zone("deep3B"), rush("edgeB"), zone("deep3M")]
  }),
  D({
    id: "overload",
    name: "Overload Blitz",
    cat: "blitz",
    desc: "Linebacker and corner both come off the same edge.",
    a: [rush(), rush(), rush("edgeT"), rush("edgeT"), man(O.WR2), man(O.WR1), zone("deep3M")]
  }),
  // SPECIAL -----------------------------------------------------------------
  D({
    id: "runstuff",
    name: "Run Commit",
    cat: "special",
    desc: "Load the box and attack the run.",
    runFirst: true,
    a: [rush(), rush(), rush("in"), man(O.WR1, true), man(O.WR2, true), zone("box"), man(O.TE)]
  }),
  D({
    id: "goalline",
    name: "Goal Line 5-3",
    cat: "special",
    desc: "Everyone tight. Built for the last few yards.",
    runFirst: true,
    a: [rush(), rush(), rush("in"), man(O.WR1, true), man(O.WR2, true), man(O.TE, true), man(O.RB)]
  }),
  D({
    id: "contain",
    name: "Cover 1 Contain",
    cat: "special",
    desc: "Rushers keep the QB in the pocket, spy behind.",
    a: [rush("edgeT"), rush("edgeB"), spy, man(O.WR1), man(O.WR2), man(O.TE), zone("deep3M")]
  }),
  D({
    id: "robber",
    name: "Cover 1 Robber",
    cat: "special",
    desc: "A safety lurks in the middle to jump crossers.",
    a: [rush(), rush(), man(O.TE), man(O.WR1), man(O.WR2), zone("robber"), zone("deep3M")]
  }),
  D({
    id: "bracket",
    name: "Cover 2 Bracket",
    cat: "special",
    desc: "Corner under, safety over the top on their best receiver.",
    a: [rush(), rush(), man(O.TE), man(O.WR1), man(O.WR2), man(O.WR1, false, true), zone("deep3M")]
  }),
  D({
    id: "bear",
    name: "Bear Front",
    cat: "special",
    desc: "Three across the middle to stuff inside runs.",
    runFirst: true,
    a: [rush("in"), rush("in"), rush("in"), man(O.WR1), man(O.WR2), man(O.TE), zone("box")]
  })
];
function defCallById(id) {
  return DEF_CALLS.find((d) => d.id === id);
}

// src/shared/rng.ts
function nextRandom(state) {
  let t = state.rng = state.rng + 1831565813 | 0;
  t = Math.imul(t ^ t >>> 15, t | 1);
  t ^= t + Math.imul(t ^ t >>> 7, t | 61);
  return ((t ^ t >>> 14) >>> 0) / 4294967296;
}

// src/shared/tuning.ts
var AI_TUNING = {
  rookie: {
    id: "rookie",
    reactThrow: 0.4,
    breakSpeed: 0.92,
    manCushion: 1.2,
    manLead: 0.05,
    deepCushion: 1.5,
    zoneSqueeze: 0.5,
    contestMult: 0.65,
    intMult: 0.6,
    tackleR: 0.74,
    brokenTackleMult: 1.5,
    shedMult: 1.25,
    scrambleReact: 1.4,
    contain: false,
    pursuitLead: 0.5,
    adapt: 0.15,
    disguise: 0,
    blitzRate: 0.12,
    jukeResist: 0,
    ballReach: 0.8,
    jamBeat: 0.5,
    jamTime: 0.4,
    help: 0,
    coordinator: 0,
    callTemp: 6,
    paBite: 1,
    jump: 0.4
  },
  pro: {
    id: "pro",
    reactThrow: 0.2,
    breakSpeed: 1,
    manCushion: 0.3,
    manLead: 0.18,
    deepCushion: 3.2,
    zoneSqueeze: 0.78,
    contestMult: 1.15,
    intMult: 1.05,
    tackleR: 0.84,
    brokenTackleMult: 0.85,
    shedMult: 0.92,
    scrambleReact: 0.4,
    contain: false,
    pursuitLead: 0.9,
    adapt: 0.8,
    disguise: 0.18,
    blitzRate: 0.24,
    jukeResist: 0.3,
    ballReach: 1,
    jamBeat: 0.26,
    jamTime: 0.55,
    help: 0.5,
    coordinator: 0.75,
    callTemp: 3,
    paBite: 0.65,
    jump: 1.1
  },
  allpro: {
    id: "allpro",
    reactThrow: 0.1,
    breakSpeed: 1,
    manCushion: -0.5,
    manLead: 0.4,
    deepCushion: 6,
    zoneSqueeze: 1,
    contestMult: 1.35,
    intMult: 0.85,
    tackleR: 0.94,
    brokenTackleMult: 0.4,
    shedMult: 0.7,
    scrambleReact: 0.25,
    contain: true,
    pursuitLead: 1,
    adapt: 1,
    disguise: 0.4,
    blitzRate: 0.35,
    jukeResist: 0.6,
    ballReach: 1.2,
    jamBeat: 0.12,
    jamTime: 0.75,
    help: 0.5,
    coordinator: 0.85,
    callTemp: 2,
    paBite: 0.5,
    jump: 1.1
  },
  nohuddle: {
    id: "nohuddle",
    reactThrow: 0.08,
    breakSpeed: 1,
    manCushion: -0.7,
    manLead: 0.5,
    deepCushion: 5.5,
    zoneSqueeze: 1,
    contestMult: 1.5,
    intMult: 0.95,
    tackleR: 1,
    brokenTackleMult: 0.25,
    shedMult: 0.55,
    scrambleReact: 0.15,
    contain: true,
    pursuitLead: 1,
    adapt: 1,
    disguise: 0.5,
    blitzRate: 0.42,
    jukeResist: 0.75,
    ballReach: 1.25,
    jamBeat: 0.08,
    jamTime: 0.85,
    help: 0.65,
    coordinator: 1,
    callTemp: 1.1,
    paBite: 0.35,
    jump: 1.3
  }
};
var STANDARD_TUNING = {
  id: "standard",
  reactThrow: 0.16,
  breakSpeed: 1,
  manCushion: 0.45,
  manLead: 0.15,
  deepCushion: 3,
  zoneSqueeze: 0.72,
  contestMult: 1.35,
  intMult: 1.5,
  tackleR: 0.82,
  brokenTackleMult: 1,
  shedMult: 1,
  scrambleReact: 0.45,
  contain: false,
  pursuitLead: 0.85,
  adapt: 0.75,
  disguise: 0.15,
  blitzRate: 0.22,
  jukeResist: 0.2,
  ballReach: 1.15,
  jamBeat: 0.3,
  jamTime: 0.5,
  help: 0.3,
  coordinator: 0.5,
  callTemp: 3,
  paBite: 0.8,
  jump: 0.9
};
var FRIENDLY_TUNING = {
  id: "friendly",
  reactThrow: 0.3,
  breakSpeed: 0.95,
  manCushion: 0.95,
  manLead: 0.08,
  deepCushion: 2.2,
  zoneSqueeze: 0.6,
  contestMult: 0.85,
  intMult: 0.85,
  tackleR: 0.78,
  brokenTackleMult: 1.25,
  shedMult: 1.5,
  scrambleReact: 1,
  contain: false,
  pursuitLead: 0.7,
  adapt: 0.3,
  disguise: 0.05,
  blitzRate: 0.12,
  jukeResist: 0.1,
  ballReach: 0.95,
  jamBeat: 0.45,
  jamTime: 0.4,
  help: 0.1,
  coordinator: 0.2,
  callTemp: 4,
  paBite: 1,
  jump: 0.4
};
var override = null;
function tuningFor(id) {
  const t = id === "rookie" || id === "pro" || id === "allpro" || id === "nohuddle" ? AI_TUNING[id] : id === "friendly" ? FRIENDLY_TUNING : STANDARD_TUNING;
  return override ? { ...t, ...override } : t;
}

// src/shared/sim.ts
function emptySlotStats() {
  return { passAtt: 0, passCmp: 0, passYds: 0, passTd: 0, intThrown: 0, sacked: 0, rushAtt: 0, rushYds: 0, rushTd: 0, rec: 0, recYds: 0, recTd: 0, tkl: 0, sck: 0, defInt: 0, retYds: 0 };
}
function emptySideStats() {
  return { off: Array.from({ length: TEAM_SIZE }, emptySlotStats), def: Array.from({ length: TEAM_SIZE }, emptySlotStats) };
}
function makePlayer(idx) {
  return {
    idx,
    team: idx < TEAM_SIZE ? 0 : 1,
    slot: idx % TEAM_SIZE,
    x: 0,
    y: 0,
    vx: 0,
    vy: 0,
    facing: 1,
    anim: "idle",
    animT: 0,
    mode: "idle",
    engagedT: 0,
    shedT: 0,
    stunT: 0,
    diveT: 0,
    downT: 0,
    diveCd: 0,
    burstT: 0,
    burstCd: 0,
    jukeImmuneT: 0,
    jukeSide: 1,
    jukeFrom: -1,
    stiffT: 0,
    stiffCd: 0,
    stumbleT: 0,
    jamT: 0,
    biteT: 0,
    catchT: 0,
    carrySince: -1,
    fatigue: 0,
    stamina: 1,
    injuryPlays: 0,
    alignX: 0,
    alignY: 0,
    routeStep: 0,
    routeTargets: [],
    routeEnd: "stop",
    routeDelay: 0,
    homeX: 0,
    homeY: 0,
    zoneR: 8,
    zoneDeep: false,
    cover: -1,
    press: false,
    over: false,
    lane: "",
    blockTarget: -1,
    lastDx: 1,
    lastDy: 0
  };
}
function createMatch(opts) {
  const s = {
    tick: 0,
    phase: "pregame",
    phaseT: PREGAME_SECONDS,
    teams: [opts.teams[0], opts.teams[1]],
    score: [0, 0],
    possession: 0,
    frameSide: 0,
    firstPossession: 0,
    half: 1,
    quarter: 1,
    clock: opts.halfSeconds ?? HALF_SECONDS,
    halfSeconds: opts.halfSeconds ?? HALF_SECONDS,
    teamSpeed: opts.teamSpeed ?? [1, 1],
    clockRunsBetween: false,
    ot: null,
    down: 1,
    toGo: 10,
    los: START_LOS,
    calls: [null, null],
    lastPlay: ["slant", "man"],
    play: null,
    players: Array.from({ length: TEAM_SIZE * 2 }, (_, i) => makePlayer(i)),
    ball: {
      state: "dead",
      carrier: -1,
      x: START_LOS,
      y: MID_Y,
      z: 0,
      sx: 0,
      sy: 0,
      tx: 0,
      ty: 0,
      t: 0,
      dur: 0,
      peak: 0,
      thrower: -1,
      target: -1,
      passType: 1,
      kick: false
    },
    control: [-1, -1],
    defPref: [DEF.LB, DEF.LB],
    lastInput: [{ ...NEUTRAL_INPUT }, { ...NEUTRAL_INPUT }],
    lastResult: null,
    resultSeq: 0,
    formationSeq: 0,
    winner: null,
    rng: opts.seed >>> 0,
    events: [],
    stats: [emptyStats(), emptyStats()],
    timeouts: [TIMEOUTS_PER_HALF, TIMEOUTS_PER_HALF],
    presnap: { setT: 0, elapsed: 0, hikeQueued: false, audibled: [false, false] },
    kick: null,
    kits: [opts.kits?.[0] ?? 0, opts.kits?.[1] ?? 1],
    kickoffs: opts.kickoffs ?? true,
    injuries: opts.injuries ?? true,
    conv: null,
    pendingKick: null,
    aiLevel: [opts.difficulty?.[0] ?? null, opts.difficulty?.[1] ?? null],
    friendly: !!opts.friendly,
    scout: [{}, {}],
    scrambles: [0, 0],
    disguise: null,
    pstats: [emptySideStats(), emptySideStats()],
    maxAudibles: opts.maxAudibles ? [opts.maxAudibles[0], opts.maxAudibles[1]] : [1, 1],
    audibleCount: [0, 0]
  };
  if (opts.mods) {
    s.mods = [opts.mods[0], opts.mods[1]];
    s.qbAcc = [opts.mods[0]?.qbAcc ?? 1, opts.mods[1]?.qbAcc ?? 1];
    s.qbArm = [opts.mods[0]?.qbArm ?? 1, opts.mods[1]?.qbArm ?? 1];
  }
  const first = opts.firstPossession ?? (nextRandom(s) < 0.5 ? 0 : 1);
  s.firstPossession = first;
  startPossession(s, first, START_LOS);
  if (s.kickoffs) s.pendingKick = { kicker: other(first), from: KO_FROM };
  resetFormation(s);
  return s;
}
function pidx(team2, slot) {
  return team2 * TEAM_SIZE + slot;
}
function offTeam(s) {
  return s.possession;
}
function defTeam(s) {
  return other(s.possession);
}
function ballTeam(s) {
  return s.play?.intercepted ? defTeam(s) : offTeam(s);
}
function roleOf(s, p) {
  return p.team === s.possession ? OFF_ROLES[p.slot] : DEF_ROLES[p.slot];
}
function T(s, side) {
  return tuningFor(s.aiLevel?.[side] ?? (s.friendly ? "friendly" : null));
}
function dist(ax, ay, bx, by) {
  return Math.hypot(ax - bx, ay - by);
}
function emit(s, e) {
  s.events.push(e);
}
function baseSpeed(s, p) {
  const m = s.mods?.[p.team];
  const r3 = m ? p.team === s.possession ? m.spdOff[p.slot] : m.spdDef[p.slot] : 1;
  const edge = p.team === s.possession ? 1 : s.teamSpeed?.[p.team] ?? 1;
  return roleSpeed(s, p) * edge * r3;
}
function modOf(s, p, key) {
  const m = s.mods?.[p.team];
  if (!m) return 1;
  const onOff = p.team === s.possession;
  if ((key === "hands" || key === "power") !== onOff) return 1;
  return m[key][p.slot] ?? 1;
}
function kickZone(s, side, dist2) {
  return fgZone(dist2) * (s.mods?.[side]?.kickAcc ?? 1);
}
function roleSpeed(s, p) {
  switch (roleOf(s, p)) {
    case "QB":
      return SPEED.QB;
    case "OL":
      return SPEED.OL;
    case "RB":
      return SPEED.RB;
    case "WR":
      return SPEED.WR;
    case "TE":
      return SPEED.TE;
    case "DL":
      return SPEED.DL;
    case "LB":
      return SPEED.LB;
    case "CB":
      return SPEED.CB;
    default:
      return SPEED.S;
  }
}
function isCarrier(s, p) {
  return s.ball.state === "held" && s.ball.carrier === p.idx;
}
function speedMods(p, carrying) {
  let m = (1 - FATIGUE_SLOW * p.fatigue) * (1 - WEAR_SLOW * (1 - p.stamina));
  if (p.injuryPlays > 0) m *= 1 - INJURY_SLOW;
  if (carrying) m *= CARRY_MULT;
  if (p.burstT > 0) m *= p.jukeImmuneT > 0 ? JUKE_MULT : JUKE_MISS_SLOW;
  if (p.stumbleT > 0) m *= STUMBLE_SLOW;
  if (p.stiffT > 0) m *= STIFF_SLOW;
  if (p.jamT > 0) m *= JAM_SLOW;
  if (p.stunT > 0) m *= 0.45;
  return m;
}
function humanSpeed(s, p) {
  const carrying = isCarrier(s, p);
  let sp;
  const preThrowRush = !!s.friendly && !!s.play && !s.play.thrown && !s.play.handedOff && !s.play.kickoff;
  if (p.team !== ballTeamSafe(s) && !carrying) sp = preThrowRush ? baseSpeed(s, p) : Math.max(6.2 * PACE, baseSpeed(s, p)) + SPEED.HUMAN_DEF_BONUS;
  else sp = baseSpeed(s, p);
  if (preThrowRush && p.team !== s.possession && s.play && p.x < s.play.startLos + 1) {
    const t = s.play.t;
    sp *= t < FRIENDLY_HOLD_FULL ? FRIENDLY_HOLD_MIN : t < FRIENDLY_HOLD_END ? FRIENDLY_HOLD_MIN + (1 - FRIENDLY_HOLD_MIN) * (t - FRIENDLY_HOLD_FULL) / (FRIENDLY_HOLD_END - FRIENDLY_HOLD_FULL) : 1;
  }
  if (carrying && roleOf(s, p) === "QB" && s.play && !s.play.thrown && s.play.chargeT > 0) sp = SPEED.QB_CHARGING;
  return sp * speedMods(p, carrying);
}
function autoRunDir(s, p) {
  const play2 = s.play;
  if (!play2 || s.phase !== "live" || !isCarrier(s, p)) return 0;
  if (play2.intercepted) return -1;
  const pdef = playById(play2.playId);
  const inPocket = p.slot === OFF.QB && p.team === s.possession && !play2.kickoff && !play2.thrown && !play2.handedOff && pdef?.kind === "pass" && !play2.qbCrossed && !play2.scramble;
  if (inPocket) return 0;
  return p.team === s.possession ? 1 : -1;
}
var AUTO_STEER = 0.85;
var AUTO_MIN_FWD = 0.55;
function steerVelocity(mx, my, sp, dir) {
  if (dir === 0) {
    const m = Math.hypot(mx, my);
    if (m > 1) {
      mx /= m;
      my /= m;
    }
    return [mx * sp, my * sp];
  }
  const sy = Math.max(-1, Math.min(1, my)) * AUTO_STEER;
  const fwd = Math.max(AUTO_MIN_FWD, Math.sqrt(Math.max(0, 1 - sy * sy)));
  const brake = mx * dir < -0.5 ? 0.4 : 1;
  return [dir * fwd * sp * brake, sy * sp];
}
function ballTeamSafe(s) {
  return s.play ? ballTeam(s) : s.possession;
}
function applyMomentum(vx, vy, dvx, dvy, dt) {
  const ex = dvx - vx;
  const ey = dvy - vy;
  const e = Math.hypot(ex, ey);
  if (e < 1e-6) return [dvx, dvy];
  const slowing = Math.hypot(dvx, dvy) < Math.hypot(vx, vy) - 0.01;
  const lim = (slowing ? DECEL : ACCEL) * dt;
  if (e <= lim) return [dvx, dvy];
  return [vx + ex / e * lim, vy + ey / e * lim];
}
function resetFormation(s) {
  const off = s.possession;
  const def = other(off);
  const L = s.los;
  const prevOff = playById(s.lastPlay[off] ?? "");
  const form = FORMATIONS[prevOff && prevOff.kind !== "punt" && prevOff.kind !== "fg" ? prevOff.formation : "gun"];
  for (let k = 0; k < TEAM_SIZE; k++) {
    const po = s.players[pidx(off, k)];
    const [dx, y] = form[k];
    placePlayer(po, L + dx, y, 1);
    const pd = s.players[pidx(def, k)];
    const [ddx, dy] = DEF_FORMATION[k];
    placePlayer(pd, Math.min(FIELD_LEN + ENDZONE - 1, L + ddx), dy, -1);
  }
  s.ball.state = "dead";
  s.ball.carrier = -1;
  s.ball.x = L;
  s.ball.y = MID_Y;
  s.ball.z = 0;
  s.ball.target = -1;
  s.ball.kick = false;
  s.play = null;
  s.kick = null;
  s.frameSide = off;
  s.formationSeq += 1;
  s.control[off] = pidx(off, OFF.QB);
  s.control[def] = pidx(def, s.defPref[def]);
}
function placePlayer(p, x, y, facing) {
  p.x = Math.max(-ENDZONE + 0.5, x);
  p.y = y;
  p.vx = 0;
  p.vy = 0;
  p.facing = facing;
  p.anim = "idle";
  p.animT = 0;
  p.mode = "idle";
  p.engagedT = 0;
  p.shedT = 0;
  p.stunT = 0;
  p.diveT = 0;
  p.carrierDive = false;
  p.downT = 0;
  p.diveCd = 0;
  p.burstT = 0;
  p.burstCd = 0;
  p.jukeImmuneT = 0;
  p.jukeFrom = -1;
  p.stiffT = 0;
  p.stiffCd = 0;
  p.stumbleT = 0;
  p.jamT = 0;
  p.biteT = 0;
  p.catchT = 0;
  p.carrySince = -1;
  p.fatigue = 0;
  p.alignX = p.x;
  p.alignY = y;
  p.routeStep = 0;
  p.routeTargets = [];
  p.routeEnd = "stop";
  p.routeDelay = 0;
  p.homeX = x;
  p.homeY = y;
  p.cover = -1;
  p.press = false;
  p.over = false;
  p.lane = "";
  p.blockTarget = -1;
  p.lastDx = facing;
  p.lastDy = 0;
}
function offPlay(s) {
  return playById(s.calls[s.possession] ?? "") ?? PLAYS[0];
}
function defCall(s) {
  return defCallById(s.calls[defTeam(s)] ?? "") ?? DEF_CALLS[0];
}
var maxX = () => FIELD_LEN + ENDZONE - 1;
function computeAlignment(s) {
  const off = offTeam(s);
  const def = defTeam(s);
  const L = s.los;
  const form = FORMATIONS[offPlay(s).formation];
  for (let k = 0; k < TEAM_SIZE; k++) {
    const p = s.players[pidx(off, k)];
    p.alignX = L + form[k][0];
    p.alignY = form[k][1];
  }
  const call = s.disguise && defCallById(s.disguise) || defCall(s);
  const human = s.control[def];
  for (let k = 0; k < TEAM_SIZE; k++) {
    const p = s.players[pidx(def, k)];
    if (p.idx === human && p.lane === "manual") continue;
    const [x, y] = alignFor(s, k, call.a[k], L);
    p.alignX = Math.min(maxX(), x);
    p.alignY = y;
  }
  const tune = T(s, def);
  const hot = s.aiLevel[def] !== null ? hotSpots(s, def, tendencyGuess(s, def, offPlay(s).formation), L) : null;
  if (hot && tune.help >= 0.5) {
    const taken = /* @__PURE__ */ new Set();
    for (const [hx, hy] of hot) {
      let best = null;
      let bd = Infinity;
      for (let k = 0; k < TEAM_SIZE; k++) {
        const a = call.a[k];
        const p = s.players[pidx(def, k)];
        if (p.idx === human || taken.has(k) || !(a.t === "zone" || a.t === "spy" || a.t === "man" && a.over)) continue;
        const d = dist(p.alignX, p.alignY, hx, hy);
        if (d < bd) {
          bd = d;
          best = p;
        }
      }
      if (!best) continue;
      taken.add(best.slot);
      const shade = 0.35 + 0.35 * tune.help;
      best.alignX = Math.min(maxX(), Math.max(L + 4, best.alignX + (hx - 2 - best.alignX) * shade));
      best.alignY = best.alignY + (hy - best.alignY) * shade;
    }
  }
}
function alignFor(s, slot, a, L) {
  const off = offTeam(s);
  const base = DEF_FORMATION[slot];
  switch (a.t) {
    case "rush": {
      if (slot === DEF.DL1 || slot === DEF.DL2) {
        const y = base[1] + (a.lane === "edgeT" ? -3 : a.lane === "edgeB" ? 3 : 0);
        return [L + 1, y];
      }
      if (slot === DEF.LB) return [L + 4.5, MID_Y + (a.lane === "edgeB" ? 4 : 0)];
      if (slot === DEF.CB1 || slot === DEF.CB2) {
        const r3 = s.players[pidx(off, slot === DEF.CB1 ? OFF.WR1 : OFF.WR2)];
        return [L + 4.5, r3.alignY + (r3.alignY < MID_Y ? 1.5 : -1.5)];
      }
      return [L + 7.5, MID_Y + (slot === DEF.S1 ? 7 : -6)];
    }
    case "man": {
      const r3 = s.players[pidx(off, a.target)];
      const rdx = r3.alignX - L;
      const inward = r3.alignY < MID_Y ? 1 : -1;
      if (rdx < -2) return [L + 4.5, Math.max(MID_Y - 5, Math.min(MID_Y + 5, r3.alignY))];
      if (a.over) return [L + 10, r3.alignY + inward * 2];
      const depth = a.press ? 1.2 : Math.abs(r3.alignY - MID_Y) < 5 ? 3.5 : 5.5;
      return [L + depth, r3.alignY + inward * 0.8];
    }
    case "zone": {
      const z = ZONES[a.z];
      const depth = z.deep ? Math.min(z.dx, 12) : Math.min(z.dx, 5);
      return [L + depth, z.y];
    }
    case "spy":
      return [L + 5.5, MID_Y];
  }
}
function offenseOptions(s) {
  const ids = PLAYS.map((p) => p.id);
  if (s.conv) {
    ids.push("xp");
    return ids;
  }
  if (canPunt(s)) ids.push("punt");
  if (canKickFG(s)) ids.push("fg");
  return ids;
}
function defenseOptions() {
  return DEF_CALLS.map((d) => d.id);
}
function submitCall(s, side, id) {
  const isOff = side === s.possession;
  const valid = isOff ? offenseOptions(s) : defenseOptions();
  if (!valid.includes(id)) return false;
  if (s.phase === "playcall") {
    if (s.calls[side] !== null) return false;
    s.calls[side] = id;
    return true;
  }
  if (s.phase === "presnap") {
    const used = s.audibleCount?.[side] ?? (s.presnap.audibled[side] ? 1 : 0);
    const max = s.maxAudibles?.[side] ?? 1;
    if (used >= max || s.calls[side] === id) return false;
    if (isOff && (id === "punt" || id === "fg" || id === "xp")) return false;
    s.calls[side] = id;
    s.lastPlay[side] = id;
    if (!s.audibleCount) s.audibleCount = [0, 0];
    s.audibleCount[side] = used + 1;
    s.presnap.audibled[side] = s.audibleCount[side] >= max;
    if (isOff) {
      s.presnap.setT = 0;
      s.presnap.hikeQueued = false;
    }
    computeAlignment(s);
    emit(s, { type: "audible", team: side });
    return true;
  }
  return false;
}
function callTimeout(s, side) {
  if (s.ot || s.conv || s.timeouts[side] <= 0) return false;
  if (s.phase !== "playcall" && s.phase !== "presnap" && s.phase !== "dead") return false;
  if (s.clock <= 0) return false;
  s.timeouts[side] -= 1;
  for (const p of s.players) if (p.team === side) p.stamina = Math.min(1, p.stamina + WEAR_TIMEOUT);
  s.clockRunsBetween = false;
  if (s.phase === "presnap") s.phaseT = playClockSeconds(s);
  if (s.phase === "playcall") s.phaseT = Math.max(s.phaseT, playcallSeconds(s) * 0.6);
  emit(s, { type: "timeout", team: side });
  return true;
}
var DEF_CYCLE = [DEF.LB, DEF.S2, DEF.S1, DEF.CB1, DEF.CB2, DEF.DL1, DEF.DL2];
function stepMatch(s, inputs, dt = DT) {
  s.events = [];
  s.tick += 1;
  const prev = [s.lastInput[0], s.lastInput[1]];
  const cur = [inputs[0] ?? prev[0], inputs[1] ?? prev[1]];
  const pressed = (side, key) => ((cur[side][key] ?? 0) - (prev[side][key] ?? 0) & 255) > 0;
  const tapped = (side) => pressed(side, "actionCount") || cur[side].throwHeld && !prev[side].throwHeld;
  switch (s.phase) {
    case "pregame":
      s.phaseT -= dt;
      if (s.phaseT <= 0) nextSeries(s);
      break;
    case "playcall": {
      s.phaseT -= dt;
      runBetweenClock(s, dt);
      if (!s.ot && !s.conv && s.clock <= 0) {
        afterPlay(s);
        break;
      }
      const def = defTeam(s);
      if (pressed(def, "switchCount")) cycleDefender(s, def);
      if (s.calls[0] !== null && s.calls[1] !== null || s.phaseT <= 0) finishPlaycall(s);
      break;
    }
    case "presnap":
      stepPresnap(s, cur, pressed, dt);
      break;
    case "live":
      stepLive(s, cur, pressed, dt);
      break;
    case "kick":
      stepKick(s, tapped, dt, cur);
      break;
    case "kickair":
      stepKickAir(s, dt);
      break;
    case "kickoff":
      stepKickoff(s, tapped, dt, cur);
      break;
    case "dead":
      s.phaseT -= dt;
      runBetweenClock(s, dt);
      animateDead(s, dt);
      if (s.phaseT <= 0) afterPlay(s);
      break;
    case "score":
      s.phaseT -= dt;
      animateDead(s, dt);
      if (s.phaseT <= 0) afterPlay(s);
      break;
    case "halftime":
      s.phaseT -= dt;
      if (s.phaseT <= 0) nextSeries(s);
      break;
    case "final":
      break;
  }
  s.lastInput = [cur[0], cur[1]];
}
function cycleDefender(s, def) {
  const i = DEF_CYCLE.indexOf(s.defPref[def]);
  s.defPref[def] = DEF_CYCLE[(i + 1) % DEF_CYCLE.length];
  s.control[def] = pidx(def, s.defPref[def]);
  emit(s, { type: "switch", team: def });
}
function runBetweenClock(s, dt) {
  if (s.clockRunsBetween && !s.ot && s.clock > 0) s.clock = Math.max(0, s.clock - dt);
}
function nextSeries(s) {
  if (s.pendingKick && !s.conv) enterKickoff(s);
  else enterPlaycall(s);
}
function enterPlaycall(s) {
  s.phase = "playcall";
  s.phaseT = playcallSeconds(s);
  s.calls = [null, null];
  for (const p of s.players) p.stamina = Math.min(1, p.stamina + WEAR_RECOVER);
  resetFormation(s);
}
function finishPlaycall(s) {
  const off = offTeam(s);
  const def = defTeam(s);
  if (s.calls[off] === null) {
    const last = s.lastPlay[off];
    if (s.conv) s.calls[off] = "xp";
    else s.calls[off] = offenseOptions(s).includes(last) && last !== "punt" && last !== "fg" && last !== "xp" ? last : "slant";
  }
  if (s.calls[def] === null) {
    const last = s.lastPlay[def];
    s.calls[def] = defenseOptions().includes(last) ? last : "man";
  }
  s.lastPlay[off] = s.calls[off];
  s.lastPlay[def] = s.calls[def];
  if (s.calls[off] === "punt") {
    s.pendingKick = { kicker: off, from: s.los, punt: true };
    enterKickoff(s);
    return;
  }
  if (s.calls[off] === "fg" || s.calls[off] === "xp") {
    if (s.calls[off] === "xp") s.los = XP_LOS;
    enterKick(s);
    return;
  }
  enterPresnap(s);
}
function enterPresnap(s) {
  s.phase = "presnap";
  s.phaseT = playClockSeconds(s);
  s.presnap = { setT: 0, elapsed: 0, hikeQueued: false, audibled: [false, false] };
  s.audibleCount = [0, 0];
  s.disguise = pickDisguise(s);
  for (const p of s.players) if (p.lane === "manual") p.lane = "";
  computeAlignment(s);
}
function pickDisguise(s) {
  const def = defTeam(s);
  if (s.aiLevel[def] === null) return null;
  const t = T(s, def);
  if (nextRandom(s) >= t.disguise) return null;
  const real = defCall(s);
  const looks = {
    man: ["cover2", "zone", "cover4"],
    zone: ["man", "press", "twoman"],
    blitz: ["cover2", "zone", "cover4"],
    special: ["zone", "man"]
  };
  const opts = looks[real.cat] ?? [];
  return opts.length ? opts[Math.floor(nextRandom(s) * opts.length)] : null;
}
function afterPlay(s) {
  if (s.winner !== null) {
    s.phase = "final";
    s.phaseT = 0;
    emit(s, { type: "final", team: s.winner });
    return;
  }
  if (s.conv) {
    enterPlaycall(s);
    return;
  }
  if (!s.ot && s.clock <= 0) {
    const r3 = endOfHalf(s);
    if (r3 === "quarter") {
      s.lastResult = { kind: "gain", text: `END OF ${s.quarter === 2 ? "1ST" : "3RD"} QUARTER`, sub: s.quarter === 2 ? "2nd quarter" : "4th quarter", team: s.possession, yards: 0 };
      s.resultSeq += 1;
      emit(s, { type: "whistle" });
      nextSeries(s);
      return;
    }
    if (r3 === "halftime") for (const p of s.players) p.stamina = Math.min(1, p.stamina + WEAR_HALFTIME);
    if (r3 === "final") {
      s.phase = "final";
      s.phaseT = 0;
      emit(s, { type: "final", team: s.winner ?? 0 });
      return;
    }
    s.phase = "halftime";
    s.phaseT = HALFTIME_SECONDS;
    s.lastResult = r3 === "overtime" ? { kind: "gain", text: "OVERTIME", sub: "One possession each from the 25", team: s.possession, yards: 0 } : { kind: "gain", text: "HALFTIME", sub: "Second half: other team receives", team: s.possession, yards: 0 };
    s.resultSeq += 1;
    resetFormation(s);
    emit(s, { type: r3 === "overtime" ? "overtime" : "halftime" });
    return;
  }
  nextSeries(s);
}
function stepPresnap(s, inputs, pressed, dt) {
  const off = offTeam(s);
  const def = defTeam(s);
  const ps = s.presnap;
  s.phaseT -= dt;
  ps.elapsed += dt;
  runBetweenClock(s, dt);
  if (!s.ot && !s.conv && s.clock <= 0) {
    afterPlay(s);
    return;
  }
  if (pressed(def, "switchCount")) cycleDefender(s, def);
  if (pressed(off, "actionCount")) ps.hikeQueued = true;
  let offSet = true;
  const humanDef = s.control[def];
  for (const p of s.players) {
    p.animT += dt;
    if (p.idx === humanDef) {
      const inp = inputs[def];
      let mx = inp.mx;
      let my = inp.my;
      const m = Math.hypot(mx, my);
      if (m > 0.15) {
        if (m > 1) {
          mx /= m;
          my /= m;
        }
        p.lane = "manual";
        p.x = Math.max(s.los + 1, Math.min(maxX(), p.x + mx * 5 * dt));
        p.y = Math.max(0.5, Math.min(FIELD_W - 0.5, p.y + my * 5 * dt));
        p.alignX = p.x;
        p.alignY = p.y;
        p.vx = mx * 5;
        p.vy = my * 5;
        p.anim = "run";
        continue;
      }
    }
    const d = dist(p.x, p.y, p.alignX, p.alignY);
    if (d > 0.05) {
      const v = Math.max(ALIGN_SPEED, d * 2);
      const step = Math.min(d, v * dt);
      p.vx = (p.alignX - p.x) / d * v;
      p.vy = (p.alignY - p.y) / d * v;
      p.x += (p.alignX - p.x) / d * step;
      p.y += (p.alignY - p.y) / d * step;
      if (d > 0.3) p.anim = "run";
      if (Math.abs(p.vx) > 0.5) p.facing = p.vx > 0 ? 1 : -1;
    } else {
      p.vx = 0;
      p.vy = 0;
      p.anim = "idle";
      p.facing = p.team === off ? 1 : -1;
    }
    if (p.team === off && d > 0.3) offSet = false;
  }
  if (offSet) ps.setT += dt;
  else ps.setT = 0;
  const canHike2 = offSet && ps.setT >= HIKE_MIN_SECONDS * 0.5 && ps.elapsed >= HIKE_MIN_SECONDS;
  if (ps.hikeQueued && canHike2 || s.phaseT <= 0 && offSet || s.phaseT < -3) snap(s);
}
function enterKick(s) {
  const off = offTeam(s);
  const def = defTeam(s);
  const L = s.los;
  s.phase = "kick";
  s.phaseT = KICK_TIME_LIMIT;
  const d = fgDistance(L);
  s.kick = { holdT: 0, holding: false, released: false, dist: d, good: false, dev: 0, power: 0, stage: 0, type: "fg" };
  for (let k = 0; k < TEAM_SIZE; k++) {
    const po = s.players[pidx(off, k)];
    if (k === OFF.QB) placePlayer(po, L - FG_SNAP_BACK - 1, MID_Y - 1.2, 1);
    else if (k === OFF.RB) placePlayer(po, L - FG_SNAP_BACK, MID_Y + 0.6, 1);
    else placePlayer(po, L - 0.8, MID_Y + (k - 3.5) * 1.8, 1);
    const pd = s.players[pidx(def, k)];
    placePlayer(pd, L + 1 + (k > 4 ? 3 : 0), MID_Y + (k - 3) * 2.2, -1);
  }
  s.control[off] = pidx(off, OFF.QB);
  s.control[def] = -1;
  s.ball.state = "dead";
  s.ball.carrier = -1;
  s.ball.x = L - FG_SNAP_BACK;
  s.ball.y = MID_Y;
  s.ball.z = 0;
  s.formationSeq += 1;
}
var MAX_KICK_LAG_TICKS = 24;
function kickLag(s, inp) {
  const vt = inp?.viewTick;
  if (vt === void 0 || vt < 0) return 0;
  return Math.max(0, Math.min(MAX_KICK_LAG_TICKS, s.tick - vt)) * DT;
}
function stepKick(s, tapped, dt, cur) {
  const k = s.kick;
  s.phaseT -= dt;
  for (const p of s.players) p.animT += dt;
  k.holdT += dt;
  const tap = tapped(offTeam(s)) && k.holdT > 0.15;
  const lag = tap ? kickLag(s, cur?.[offTeam(s)]) : 0;
  if ((k.stage ?? 0) === 0) {
    if (!tap && s.phaseT > 0) return;
    k.lockedPower = fgPowerAt(Math.max(0, k.holdT - lag));
    k.stage = 1;
    k.holdT = 0;
    s.phaseT = Math.max(s.phaseT, 3);
    emit(s, { type: "kick_power", team: offTeam(s) });
    return;
  }
  if (!tap && s.phaseT > 0) return;
  k.released = true;
  if (!s.ot && !s.conv) s.clock = Math.max(0, s.clock - 3);
  const m = kickMeter(Math.max(0, k.holdT - lag), fgMeterHz(k.dist));
  const zone2 = kickZone(s, offTeam(s), k.dist);
  const powerNeed = fgPowerNeed(k.dist, s.mods?.[offTeam(s)]?.kickPow ?? 1);
  const longEnough = (k.lockedPower ?? 0) >= powerNeed;
  k.power = m;
  k.good = longEnough && Math.abs(m) <= zone2;
  k.dev = Math.abs(m) <= zone2 ? m / zone2 * UPRIGHT_HALF * 0.8 : Math.sign(m || 1) * (UPRIGHT_HALF + 1 + (Math.abs(m) - zone2) * 14);
  const need = longEnough ? 1 : Math.max(0.45, (k.lockedPower ?? 0) / powerNeed) * 0.92;
  const b = s.ball;
  const kicker = s.players[pidx(offTeam(s), OFF.QB)];
  kicker.anim = "throw";
  kicker.animT = 0;
  b.state = "air";
  b.kick = true;
  b.sx = s.los - FG_SNAP_BACK;
  b.sy = MID_Y;
  const reach = need;
  const postX = FIELD_LEN + ENDZONE;
  b.tx = b.sx + (postX + 4 - b.sx) * Math.min(1, reach);
  b.ty = MID_Y + k.dev * Math.min(1.3, (b.tx - b.sx) / Math.max(1, postX - b.sx));
  b.t = 0;
  b.dur = 1 + k.dist / 60;
  b.peak = 4 + k.dist * 0.12;
  b.thrower = kicker.idx;
  b.target = -1;
  s.phase = "kickair";
  s.phaseT = b.dur;
  emit(s, { type: "kick", team: offTeam(s), x: b.sx, y: b.sy });
}
function stepKickAir(s, dt) {
  const b = s.ball;
  b.t += dt;
  const f = Math.min(1, b.t / b.dur);
  b.x = b.sx + (b.tx - b.sx) * f;
  b.y = b.sy + (b.ty - b.sy) * f;
  b.z = 4 * b.peak * f * (1 - f) + 0.3 * (1 - f);
  for (const p of s.players) p.animT += dt;
  if (f < 1) return;
  b.state = "loose";
  b.z = 0;
  const good = s.kick.good;
  const off = offTeam(s);
  if (s.conv) emit(s, { type: good ? "xp_good" : "xp_miss", team: off, x: b.x, y: b.y });
  else emit(s, { type: good ? "fg_good" : "fg_miss", team: off, x: b.x, y: b.y });
  const r3 = applyOutcome(s, { kind: "fg", good });
  if (good) {
    for (const p of s.players) if (p.team === off) {
      p.anim = "celebrate";
      p.animT = 0;
    }
  }
  s.phase = r3.scored ? "score" : "dead";
  s.phaseT = r3.scored ? SCORE_SECONDS : DEAD_SECONDS;
}
function koSpot(s) {
  return FIELD_LEN - (s.pendingKick?.from ?? KO_FROM);
}
function enterKickoff(s) {
  const pk = s.pendingKick;
  const kickTeam = pk.kicker;
  const recv = other(kickTeam);
  s.possession = recv;
  const K = koSpot(s);
  s.phase = "kickoff";
  s.phaseT = pk.punt ? PUNT_SNAP_SECONDS : KICK_TIME_LIMIT;
  s.calls = [null, null];
  s.play = null;
  s.los = K;
  s.kick = { holdT: 0, holding: false, released: false, dist: 0, good: false, dev: 0, power: 0, type: pk.punt ? "punt" : "ko" };
  for (const p of s.players) p.stamina = Math.min(1, p.stamina + WEAR_RECOVER);
  if (pk.punt) {
    placePuntTeams(s, kickTeam, recv, K);
    return;
  }
  const lane = [4, 11.5, 19, 34.3, 41.8, 49.3];
  let n = 0;
  for (let k = 0; k < TEAM_SIZE; k++) {
    const p = s.players[pidx(kickTeam, k)];
    if (k === OFF.QB) placePlayer(p, K + 7, MID_Y - 1, -1);
    else placePlayer(p, K + 1, lane[n++], -1);
  }
  const front = [8, 17, MID_Y, 36.3, 45.3];
  n = 0;
  for (let k = 0; k < TEAM_SIZE; k++) {
    const p = s.players[pidx(recv, k)];
    if (k === OFF.RB) placePlayer(p, Math.max(-3, K - 61), MID_Y, 1);
    else if (k === OFF.WR1) placePlayer(p, K - 48, MID_Y - 6, 1);
    else placePlayer(p, K - 13, front[n++], 1);
  }
  s.control[kickTeam] = pidx(kickTeam, OFF.QB);
  s.control[recv] = -1;
  s.frameSide = recv;
  s.ball.state = "dead";
  s.ball.carrier = -1;
  s.ball.x = K;
  s.ball.y = MID_Y;
  s.ball.z = 0;
  s.ball.kick = false;
  s.ball.target = -1;
  s.formationSeq += 1;
}
function stepKickoff(s, tapped, dt, cur) {
  const k = s.kick;
  const kickTeam = defTeam(s);
  s.phaseT -= dt;
  for (const p of s.players) p.animT += dt;
  k.holdT += dt;
  const kicker = s.players[pidx(kickTeam, OFF.QB)];
  if (k.type === "punt") {
    const f = Math.min(1, k.holdT / 0.45);
    s.ball.x = koSpot(s) + (kicker.x - koSpot(s)) * f;
    s.ball.z = 0.4 + Math.sin(f * Math.PI) * 0.6;
    if (s.phaseT <= 0) launchPunt(s);
    return;
  }
  kicker.x = Math.max(koSpot(s) + 1.5, kicker.x - 1.2 * dt);
  const tap = tapped(kickTeam) && k.holdT > 0.15;
  if (!tap && s.phaseT > 0) return;
  launchKickoff(s, tap ? kickLag(s, cur?.[kickTeam]) : 0);
}
function launchKickoff(s, lag = 0) {
  const k = s.kick;
  const kickTeam = defTeam(s);
  const recv = offTeam(s);
  const K = koSpot(s);
  k.released = true;
  const m = kickMeter(Math.max(0, k.holdT - lag), KO_METER_HZ);
  k.power = (m + 1) / 2;
  const kd = koDistance(k.power) * (s.mods?.[kickTeam]?.kickPow ?? 1);
  const drift = (nextRandom(s) * 2 - 1) * KO_DRIFT;
  k.dist = Math.round(kd);
  k.dev = drift;
  const b = s.ball;
  const kicker = s.players[pidx(kickTeam, OFF.QB)];
  kicker.anim = "throw";
  kicker.animT = 0;
  b.state = "air";
  b.kick = true;
  b.carrier = -1;
  b.passType = 2;
  b.sx = K;
  b.sy = MID_Y;
  b.tx = K - kd;
  b.ty = MID_Y + drift;
  b.t = 0;
  b.dur = koHang(kd);
  b.peak = 6 + kd * 0.12;
  b.thrower = kicker.idx;
  b.x = K;
  b.y = MID_Y;
  b.z = 0.3;
  s.pendingKick = null;
  s.play = {
    playId: "kickoff",
    defId: "",
    t: 0,
    thrown: true,
    handedOff: true,
    intercepted: false,
    qbCrossed: true,
    startLos: b.tx,
    chargeT: 0,
    lastThrowHeld: false,
    catchContested: true,
    ended: false,
    via: "rush",
    tackler: -1,
    kickoff: true,
    scrambleT: 0
  };
  s.phase = "live";
  s.phaseT = 0;
  let ret = pidx(recv, OFF.RB);
  let bd = Infinity;
  for (const k2 of [OFF.RB, OFF.WR1]) {
    const p = s.players[pidx(recv, k2)];
    const d = dist(p.x, p.y, b.tx, Math.max(0, Math.min(FIELD_W, b.ty)));
    if (d < bd) {
      bd = d;
      ret = p.idx;
    }
  }
  b.target = ret;
  const ly = Math.max(6, Math.min(FIELD_W - 6, b.ty));
  const wall = [-10, -5, 0, 5, 10];
  let n = 0;
  for (const p of s.players) {
    p.vx = 0;
    p.vy = 0;
    if (p.team === kickTeam) {
      p.mode = "pursue";
      continue;
    }
    if (p.idx === ret) {
      p.mode = "carry";
      continue;
    }
    p.mode = "route";
    const wx = Math.max(b.tx + 6, Math.min(p.x, b.tx + 14)) + (p.slot === OFF.WR1 || p.slot === OFF.RB ? -4 : 0);
    p.routeTargets = [[wx, Math.max(2, Math.min(FIELD_W - 2, ly + wall[n++ % wall.length]))]];
    p.routeEnd = "stop";
    p.routeStep = 0;
    p.routeDelay = 0;
  }
  s.control[recv] = -1;
  s.control[kickTeam] = pidx(kickTeam, 3);
  emit(s, { type: "kickoff", team: kickTeam, x: K, y: MID_Y });
}
function placePuntTeams(s, kickTeam, recv, K) {
  const line = [6, 13, 20, 33.3, 40.3, 47.3];
  let n = 0;
  for (let k = 0; k < TEAM_SIZE; k++) {
    const p = s.players[pidx(kickTeam, k)];
    if (k === OFF.QB) placePlayer(p, K + PUNT_DEPTH, MID_Y, -1);
    else placePlayer(p, K + 1, line[n++], -1);
  }
  const front = [9, 18, MID_Y, 35.3, 44.3];
  n = 0;
  const deep = Math.max(1, K - 44);
  for (let k = 0; k < TEAM_SIZE; k++) {
    const p = s.players[pidx(recv, k)];
    if (k === OFF.RB) placePlayer(p, deep, MID_Y, 1);
    else if (k === OFF.WR1) placePlayer(p, Math.max(deep + 6, K - 30), MID_Y + 5, 1);
    else placePlayer(p, K - 1, front[n++], 1);
  }
  s.control[kickTeam] = -1;
  s.control[recv] = -1;
  s.frameSide = recv;
  const b = s.ball;
  b.state = "dead";
  b.carrier = -1;
  b.x = K;
  b.y = MID_Y;
  b.z = 0.4;
  b.kick = false;
  b.target = -1;
  s.formationSeq += 1;
}
function launchPunt(s) {
  const k = s.kick;
  const kickTeam = defTeam(s);
  const K = koSpot(s);
  const punter = s.players[pidx(kickTeam, OFF.QB)];
  let d = (PUNT_MIN + nextRandom(s) * (PUNT_MAX - PUNT_MIN)) * (s.mods?.[kickTeam]?.kickPow ?? 1);
  if (K - d < 4) d = K - (3 + nextRandom(s) * 7);
  d = Math.max(10, d);
  const drift = (nextRandom(s) * 2 - 1) * PUNT_DRIFT;
  const b = s.ball;
  const from = punter.x;
  const flight = from - (K - d);
  k.released = true;
  k.dist = Math.round(d);
  k.dev = drift;
  punter.anim = "throw";
  punter.animT = 0;
  b.state = "air";
  b.kick = true;
  b.carrier = -1;
  b.passType = 2;
  b.sx = from;
  b.sy = punter.y;
  b.tx = K - d;
  b.ty = MID_Y + drift;
  b.t = 0;
  b.dur = puntHang(flight);
  b.peak = 9 + flight * 0.15;
  b.thrower = punter.idx;
  b.x = from;
  b.y = punter.y;
  b.z = 1;
  s.pendingKick = null;
  s.play = {
    playId: "punt",
    defId: "",
    t: 0,
    thrown: true,
    handedOff: true,
    intercepted: false,
    qbCrossed: true,
    startLos: b.tx,
    chargeT: 0,
    lastThrowHeld: false,
    catchContested: true,
    ended: false,
    via: "rush",
    tackler: -1,
    kickoff: true,
    punt: true,
    scrambleT: 0
  };
  s.phase = "live";
  s.phaseT = 0;
  setReturnJobs(s, kickTeam, b);
  emit(s, { type: "punt", team: kickTeam, x: from, y: punter.y });
}
function setReturnJobs(s, kickTeam, b) {
  const recv = other(kickTeam);
  let ret = pidx(recv, OFF.RB);
  let bd = Infinity;
  for (const k2 of [OFF.RB, OFF.WR1]) {
    const p = s.players[pidx(recv, k2)];
    const d = dist(p.x, p.y, b.tx, Math.max(0, Math.min(FIELD_W, b.ty)));
    if (d < bd) {
      bd = d;
      ret = p.idx;
    }
  }
  b.target = ret;
  const ly = Math.max(6, Math.min(FIELD_W - 6, b.ty));
  const wall = [-10, -5, 0, 5, 10];
  let n = 0;
  for (const p of s.players) {
    p.vx = 0;
    p.vy = 0;
    if (p.team === kickTeam) {
      p.mode = "pursue";
      continue;
    }
    if (p.idx === ret) {
      p.mode = "carry";
      continue;
    }
    p.mode = "route";
    const wx = Math.max(b.tx + 6, Math.min(p.x, b.tx + 14)) + (p.slot === OFF.WR1 || p.slot === OFF.RB ? -4 : 0);
    p.routeTargets = [[wx, Math.max(2, Math.min(FIELD_W - 2, ly + wall[n++ % wall.length]))]];
    p.routeEnd = "stop";
    p.routeStep = 0;
    p.routeDelay = 0;
  }
  s.control[recv] = -1;
  s.control[kickTeam] = pidx(kickTeam, 3);
}
function updateKickoffBall(s, dt) {
  const b = s.ball;
  b.t += dt;
  const f = Math.min(1, b.t / b.dur);
  b.x = b.sx + (b.tx - b.sx) * f;
  b.y = b.sy + (b.ty - b.sy) * f;
  b.z = ballZ(f, b.peak) * (1 - 0.25 * f) + 0.3 * (1 - f);
  if (f < 1) return;
  if (b.ty < 0 || b.ty > FIELD_W) {
    b.state = "loose";
    b.z = 0;
    emit(s, { type: "out_of_bounds", x: b.x, y: b.y });
    endPlay(s, { kind: "ko_dead", why: "oob" });
    return;
  }
  if (b.tx <= KO_DEEP_TB) {
    b.state = "loose";
    b.z = 0;
    emit(s, { type: "touchback", x: b.x, y: b.y });
    endPlay(s, { kind: "ko_dead", why: "touchback" });
    return;
  }
  const ret = s.players[b.target];
  if (ret && dist(ret.x, ret.y, b.x, b.y) < 3.5 && ret.downT <= 0) {
    b.state = "held";
    b.carrier = ret.idx;
    b.z = 1;
    ret.anim = "catch";
    ret.animT = 0;
    ret.x = b.x;
    ret.y = b.y;
    s.los = Math.max(0.5, Math.min(FIELD_LEN - 0.5, b.x));
    s.play.startLos = b.x;
    emit(s, { type: "catch", team: ret.team, x: b.x, y: b.y });
    onNewCarrier(s, ret);
    return;
  }
  b.state = "loose";
  b.z = 0;
  s.los = Math.max(0.5, b.x);
  endPlay(s, { kind: "ko_return", spot: b.x, oob: false });
}
function snap(s) {
  const off = offTeam(s);
  const def = defTeam(s);
  const play2 = offPlay(s);
  const call = defCall(s);
  s.play = {
    playId: play2.id,
    defId: call.id,
    t: 0,
    thrown: false,
    handedOff: false,
    intercepted: false,
    qbCrossed: false,
    startLos: s.los,
    chargeT: 0,
    lastThrowHeld: false,
    catchContested: false,
    ended: false,
    via: play2.kind === "run" ? "rush" : "pass",
    tackler: -1,
    scrambleT: 0
  };
  readTendency(s, def, play2);
  s.scout[def][`${play2.formation}:${play2.id}`] = (s.scout[def][`${play2.formation}:${play2.id}`] ?? 0) + 1;
  s.disguise = null;
  s.phase = "live";
  s.phaseT = 0;
  const L = s.los;
  const qb = s.players[pidx(off, OFF.QB)];
  s.ball.state = "held";
  s.ball.carrier = qb.idx;
  s.ball.target = -1;
  s.ball.kick = false;
  qb.carrySince = 0;
  emit(s, { type: "snap", team: off, x: L, y: MID_Y });
  for (const p of s.players) {
    p.vx = 0;
    p.vy = 0;
    p.facing = p.team === off ? 1 : -1;
  }
  for (let k = 0; k < TEAM_SIZE; k++) {
    const p = s.players[pidx(off, k)];
    const route = play2.routes[k];
    p.routeStep = 0;
    p.routeDelay = 0;
    if (k === OFF.QB) {
      if (play2.kind === "run" && play2.runner === "QB") setPath(p, play2.runPath ?? [], L);
      else if (play2.handoffAt) {
        p.mode = "fake";
        p.routeTargets = [[L - 6.5, MID_Y]];
        p.routeEnd = "stop";
      } else p.mode = "qb";
    } else if (play2.kind === "run" && k === OFF.RB && play2.runner !== "QB") {
      setPath(p, play2.runPath ?? [], L);
      p.routeDelay = play2.handoffAt ?? 0;
    } else if (play2.playAction && k === OFF.RB) {
      p.mode = "fake";
      p.routeTargets = play2.playAction.map(([dx, dy]) => [L + dx, MID_Y + dy]);
      p.routeEnd = "stop";
    } else if (route) {
      p.mode = "route";
      p.routeTargets = routeWaypoints(route, p.x, p.y);
      p.routeEnd = route.end;
      p.routeDelay = route.delay ?? 0;
    } else p.mode = "block";
  }
  if (play2.kind === "run" && play2.runner === "QB") {
    s.play.handedOff = true;
    s.play.via = "rush";
  }
  for (let k = 0; k < TEAM_SIZE; k++) {
    const p = s.players[pidx(def, k)];
    const a = call.a[k];
    p.cover = -1;
    p.press = false;
    p.over = false;
    p.lane = "";
    switch (a.t) {
      case "rush":
        p.mode = "rush";
        p.lane = a.lane ?? "";
        break;
      case "man": {
        p.mode = "cover";
        p.cover = pidx(off, a.target);
        p.press = !!a.press;
        p.over = !!a.over;
        const r3 = s.players[p.cover];
        if (p.press && dist(p.x, p.y, r3.x, r3.y) < 2.6 && r3.slot !== OFF.RB) {
          if (nextRandom(s) < T(s, def).jamBeat) {
            p.stunT = 0.45;
          } else {
            r3.jamT = T(s, def).jamTime;
            emit(s, { type: "jam", team: def, x: r3.x, y: r3.y });
          }
        }
        break;
      }
      case "zone": {
        const z = ZONES[a.z];
        p.mode = "zone";
        const cap = L > 82 ? FIELD_LEN + 7 : maxX();
        p.homeX = Math.min(cap, L + z.dx);
        p.homeY = z.y;
        p.zoneR = z.r;
        p.zoneDeep = z.deep;
        break;
      }
      case "spy":
        p.mode = "spy";
        p.cover = qb.idx;
        break;
    }
    if (play2.playAction && p.mode !== "rush" && !(p.mode === "zone" && p.zoneDeep && p.homeX - L > 15) && p.x - L < 12) {
      p.biteT = (call.runFirst ? 0.85 : 0.55) * T(s, def).paBite;
    }
  }
  if (play2.kind === "run" && play2.runner === "QB") s.control[off] = qb.idx;
  else s.control[off] = play2.kind === "run" ? pidx(off, OFF.RB) : qb.idx;
  s.control[def] = pidx(def, s.defPref[def]);
}
function tendencyGuess(s, def, formation) {
  const tune = T(s, def);
  const seen = s.scout[def];
  let total = 0;
  let best = "";
  let bestN = 0;
  for (const [k, n] of Object.entries(seen)) {
    const [form, id] = k.split(":");
    if (form !== formation) continue;
    total += n;
    if (n > bestN) {
      bestN = n;
      best = id;
    }
  }
  if (bestN < 2 || bestN / total < 0.5) return null;
  return { id: best, conf: Math.min(1, (bestN - 1) / 3) * (bestN / total) * tune.adapt };
}
function hotSpots(s, def, guess, L) {
  const tune = T(s, def);
  if (!guess || tune.help <= 0 || guess.conf <= 0.15) return null;
  const spots = s.burned?.[def]?.[guess.id];
  if (!spots || !spots.length) return null;
  const hot = [];
  for (let i = spots.length - 1; i >= 0 && hot.length < (tune.help >= 0.8 ? 2 : 1); i--) {
    const x = L + spots[i][0];
    const y = spots[i][1];
    if (hot.every(([hx, hy]) => dist(hx, hy, x, y) > 6)) hot.push([x, y]);
  }
  return hot;
}
function readTendency(s, def, play2) {
  const g = tendencyGuess(s, def, play2.formation);
  if (!g) return;
  s.play.guess = g.id;
  s.play.guessConf = g.conf;
  const hot = hotSpots(s, def, g, s.play.startLos);
  if (hot) s.play.hot = hot;
}
function guessedSpot(s, r3) {
  const play2 = s.play;
  if (!play2?.guess || !play2.guessConf) return null;
  const g = playById(play2.guess);
  const route = g?.routes[r3.slot];
  if (!route) return null;
  const pts = routeWaypoints(route, r3.alignX, r3.alignY);
  const i = Math.min(pts.length - 1, 1);
  const [ax, ay] = pts[i - 1] ?? pts[0];
  const [bx, by] = pts[i];
  const m = Math.hypot(bx - ax, by - ay) || 1;
  const t = Math.min(5.5, m * 0.7);
  const qb = s.players[pidx(offTeam(s), OFF.QB)];
  let gx = ax + (bx - ax) / m * t;
  let gy = ay + (by - ay) / m * t;
  const q = Math.hypot(qb.x - gx, qb.y - gy) || 1;
  gx += (qb.x - gx) / q * 1;
  gy += (qb.y - gy) / q * 1;
  return [gx, gy];
}
function guessedRouteDist(s, r3) {
  const play2 = s.play;
  if (!play2?.guess || !play2.guessConf) return Infinity;
  const route = playById(play2.guess)?.routes[r3.slot];
  if (!route) return Infinity;
  const pts = [[r3.alignX, r3.alignY], ...routeWaypoints(route, r3.alignX, r3.alignY)];
  if (route.end === "go" && pts.length >= 2) {
    const [ax, ay] = pts[pts.length - 2];
    const [bx, by] = pts[pts.length - 1];
    const m = Math.hypot(bx - ax, by - ay) || 1;
    pts.push([bx + (bx - ax) / m * 15, by + (by - ay) / m * 15]);
  }
  let best = Infinity;
  for (let i = 0; i + 1 < pts.length; i++) {
    const [ax, ay] = pts[i];
    const [bx, by] = pts[i + 1];
    const vx = bx - ax;
    const vy = by - ay;
    const l2 = vx * vx + vy * vy || 1;
    const t = Math.max(0, Math.min(1, ((r3.x - ax) * vx + (r3.y - ay) * vy) / l2));
    best = Math.min(best, Math.hypot(r3.x - (ax + vx * t), r3.y - (ay + vy * t)));
  }
  return best;
}
function setPath(p, path, L) {
  p.mode = "carry";
  p.routeTargets = path.map(([dx, dy]) => [L + dx, MID_Y + dy]);
  p.routeStep = 0;
  p.routeEnd = "go";
}
function stepLive(s, inputs, pressed, dt) {
  const play2 = s.play;
  play2.t += dt;
  if (!s.ot && !s.conv && !(play2.kickoff && s.ball.state === "air")) s.clock = Math.max(0, s.clock - dt);
  const pdef = playById(play2.playId);
  const bt = ballTeam(s);
  const nbt = other(bt);
  const carrier = s.ball.state === "held" ? s.players[s.ball.carrier] : null;
  for (const side of [0, 1]) {
    const ctrl = s.control[side];
    const p = ctrl >= 0 ? s.players[ctrl] : null;
    const isCar = !!(carrier && p && carrier.idx === p.idx);
    if (pressed(side, "switchCount")) {
      if (side === nbt) switchDefender(s, side);
      else if (isCar && p.stiffCd <= 0 && !(p.slot === OFF.QB && !play2.thrown && pdef?.kind === "pass")) {
        p.stiffT = STIFF_TIME;
        p.stiffCd = STIFF_COOLDOWN;
        emit(s, { type: "stiff_arm", team: side, x: p.x, y: p.y, v: 0 });
      }
    }
    const qbInPocket = !!p && isCar && p.slot === OFF.QB && !play2.thrown && pdef?.kind === "pass" && !play2.qbCrossed && !play2.scramble && !play2.handedOff && !play2.kickoff;
    if (pressed(side, "diveCount") && qbInPocket && p.downT <= 0) {
      play2.scramble = true;
      emit(s, { type: "juke", team: side, x: p.x, y: p.y, v: 0 });
    } else if (pressed(side, "diveCount") && p && isCar && p.diveT <= 0 && p.downT <= 0 && !qbInPocket) {
      const dir = p.team === s.possession && !play2.intercepted ? 1 : -1;
      p.lastDx = dir;
      p.lastDy = 0;
      p.diveT = CARRIER_DIVE_TIME;
      p.carrierDive = true;
      emit(s, { type: "dive", team: side, x: p.x, y: p.y });
    }
    if (pressed(side, "actionCount") && p) {
      if (isCar) {
        if (p.burstCd <= 0) juke(s, p, inputs[side]);
      } else if (side === nbt && s.ball.state === "air" && !play2.kickoff && p.downT <= 0 && p.diveT <= 0) {
        p.catchT = INT_JUMP_TIME;
        p.anim = "catch";
        p.animT = 0;
        emit(s, { type: "burst", team: side, x: p.x, y: p.y });
      } else if (side === nbt && p.diveCd <= 0 && p.downT <= 0 && p.diveT <= 0) {
        const inp = inputs[side];
        let dx = inp.mx;
        let dy = inp.my;
        if (Math.hypot(dx, dy) < 0.2) {
          const tx = carrier ? carrier.x : s.ball.x;
          const ty = carrier ? carrier.y : s.ball.y;
          dx = tx - p.x;
          dy = ty - p.y;
        }
        const m = Math.hypot(dx, dy) || 1;
        p.lastDx = dx / m;
        p.lastDy = dy / m;
        p.diveT = DIVE_TIME;
        p.diveCd = DIVE_COOLDOWN;
        emit(s, { type: "dive", team: side, x: p.x, y: p.y });
      }
    }
  }
  const off = offTeam(s);
  const qb = s.players[pidx(off, OFF.QB)];
  if (pdef?.kind === "pass" && !play2.thrown && !play2.intercepted && carrier && carrier.idx === qb.idx && s.control[off] === qb.idx) {
    const inp = inputs[off];
    const behind = qb.x < play2.startLos + 0.3;
    if (!behind && !play2.qbCrossed) {
      play2.qbCrossed = true;
      play2.via = "rush";
      autoSwitch(s, defTeam(s), qb.x, qb.y, 0);
    }
    const outside = Math.abs(qb.y - MID_Y) > 5.5 || qb.x > play2.startLos - 1.5;
    if (outside && Math.hypot(qb.vx, qb.vy) > 2.5) {
      if (play2.scrambleT === 0) s.scrambles[off] += 1;
      play2.scrambleT += dt;
    }
    if (inp.throwHeld && !play2.lastThrowHeld && behind && play2.t > 0.15) throwBall(s, qb, inp.ax, inp.ay, inp.passType, !!inp.aimExact);
    play2.chargeT = 0;
    play2.lastThrowHeld = inp.throwHeld;
  }
  if (pdef?.kind === "run" && pdef.runner !== "QB" && !play2.handedOff && !play2.intercepted && s.ball.carrier === qb.idx) {
    const rb = s.players[pidx(off, OFF.RB)];
    const at = pdef.handoffAt ?? 0;
    if (play2.t > at && dist(rb.x, rb.y, qb.x, qb.y) < 1.3 || play2.t > at + 0.8) {
      s.ball.carrier = rb.idx;
      play2.handedOff = true;
      qb.mode = "idle";
      emit(s, { type: "handoff", team: off, x: rb.x, y: rb.y });
      onNewCarrier(s, rb);
    }
  }
  if (pdef?.screenRelease && !play2.thrown && play2.t >= pdef.screenRelease && play2.t - dt < pdef.screenRelease) {
    const rb = s.players[pidx(off, OFF.RB)];
    const side = rb.y < MID_Y ? -1 : 1;
    for (const k of [OFF.OL1, OFF.OL2]) {
      const ol = s.players[pidx(off, k)];
      ol.mode = "fake";
      ol.routeTargets = [[play2.startLos + 2, MID_Y + side * (7 + (k - 1) * 3)]];
      ol.routeEnd = "stop";
      ol.engagedT = 0;
    }
  }
  const curCarrier = s.ball.state === "held" ? s.players[s.ball.carrier] : null;
  const desired = [];
  for (const p of s.players) {
    tickTimers(p, dt);
    if (p.downT > 0) {
      desired.push([0, 0]);
      p.vx = 0;
      p.vy = 0;
      continue;
    }
    if (p.diveT > 0) {
      p.vx = p.lastDx * DIVE_SPEED;
      p.vy = p.lastDy * DIVE_SPEED;
      desired.push([p.vx, p.vy]);
      if (p.diveT - dt <= 0) {
        if (p.carrierDive) play2.carrierLanded = true;
        else p.downT = DIVE_RECOVER;
      }
      continue;
    }
    const human = s.control[p.team] === p.idx && !(p.mode === "carry" && !play2.handedOff);
    const idle = inputs[p.team].mx === 0 && inputs[p.team].my === 0;
    if (human && idle && s.aiLevel[p.team] === null && p.team !== ballTeamSafe(s) && !isCarrier(s, p) && p.burstT <= 0) {
      desired.push(aiVelocity(s, p, curCarrier));
    } else if (human) {
      const inp = inputs[p.team];
      const mx = inp.mx;
      const my = inp.my;
      const sp = humanSpeed(s, p);
      let [vx, vy] = steerVelocity(mx, my, sp, autoRunDir(s, p));
      if (p.burstT > 0) {
        const base = sp / (p.jukeImmuneT > 0 ? JUKE_MULT : JUKE_MISS_SLOW);
        vy += p.jukeSide * base * JUKE_SIDESTEP;
        if (Math.hypot(mx, my) < 0.2) vx += (p.team === offTeam(s) && !play2.intercepted ? 1 : -1) * base;
      }
      desired.push([vx, vy]);
    } else {
      desired.push(aiVelocity(s, p, curCarrier));
    }
  }
  applyBlocking(s, desired, dt);
  for (const p of s.players) {
    const [dvx, dvy] = desired[p.idx];
    if (p.diveT <= 0 && p.downT <= 0) [p.vx, p.vy] = applyMomentum(p.vx, p.vy, dvx, dvy, dt);
    const sp = Math.hypot(p.vx, p.vy);
    const carrying = isCarrier(s, p);
    const top = baseSpeed(s, p);
    if (sp > top * 0.7) {
      const graceOk = !carrying || p.carrySince < 0 || play2.t - p.carrySince > FATIGUE_GRACE;
      if (graceOk) p.fatigue = Math.min(1, p.fatigue + (carrying ? FATIGUE_CARRY : FATIGUE_OTHER) * dt);
    }
    p.x += p.vx * dt;
    p.y += p.vy * dt;
    p.x = Math.max(-ENDZONE - 0.5, Math.min(FIELD_LEN + ENDZONE + 0.5, p.x));
    p.y = Math.max(-2, Math.min(FIELD_W + 2, p.y));
    if (Math.abs(p.vx) > 0.3) p.facing = p.vx > 0 ? 1 : -1;
  }
  separate(s, curCarrier);
  updateBall(s, dt);
  if (play2.ended) return;
  const c = s.ball.state === "held" ? s.players[s.ball.carrier] : null;
  if (c) {
    if (checkCarrier(s, c)) return;
    if (checkTackles(s, c)) return;
    if (play2.carrierLanded && c.carrierDive) {
      endPlayDown(s, c, false, -1);
      return;
    }
  }
  if (play2.t > MAX_PLAY_SECONDS) {
    if (c) endPlayDown(s, c, false, -1);
    else endPlay(s, { kind: "incomplete", breakup: false });
    return;
  }
  updateAnims(s);
}
function tickTimers(p, dt) {
  p.shedT = Math.max(0, p.shedT - dt);
  p.stunT = Math.max(0, p.stunT - dt);
  p.diveT = Math.max(0, p.diveT - dt);
  p.downT = Math.max(0, p.downT - dt);
  p.diveCd = Math.max(0, p.diveCd - dt);
  p.burstT = Math.max(0, p.burstT - dt);
  p.burstCd = Math.max(0, p.burstCd - dt);
  p.jukeImmuneT = Math.max(0, p.jukeImmuneT - dt);
  p.stiffT = Math.max(0, p.stiffT - dt);
  p.stiffCd = Math.max(0, p.stiffCd - dt);
  p.stumbleT = Math.max(0, p.stumbleT - dt);
  p.jamT = Math.max(0, p.jamT - dt);
  p.biteT = Math.max(0, p.biteT - dt);
  p.catchT = Math.max(0, p.catchT - dt);
  p.animT += dt;
}
function switchDefender(s, side) {
  const cur = s.control[side];
  const tx = s.ball.state === "air" ? s.ball.tx : s.ball.x;
  const ty = s.ball.state === "air" ? s.ball.ty : s.ball.y;
  let best = -1;
  let bestD = Infinity;
  for (let k = 0; k < TEAM_SIZE; k++) {
    const p = s.players[pidx(side, k)];
    if (p.idx === cur || p.downT > 0) continue;
    const d = dist(p.x, p.y, tx, ty);
    if (d < bestD) {
      bestD = d;
      best = p.idx;
    }
  }
  if (best >= 0) {
    s.control[side] = best;
    emit(s, { type: "switch", team: side });
  }
}
function isEligible(p) {
  return p.slot !== OFF.QB && p.slot !== OFF.OL1 && p.slot !== OFF.OL2;
}
function leadPoint(s, qb, r3, pt) {
  let lx = r3.x;
  let ly = r3.y;
  for (let i = 0; i < 4; i++) {
    const d = clampThrowDist(Math.hypot(lx - qb.x, ly - qb.y), pt, armOf(s, qb.team));
    const tb = throwDuration(d, pt);
    lx = r3.x + r3.vx * tb * 0.92;
    ly = r3.y + r3.vy * tb * 0.92;
  }
  return [lx, Math.max(1, Math.min(FIELD_W - 1, ly))];
}
function noHuddle(s) {
  return s.aiLevel[0] === "nohuddle" || s.aiLevel[1] === "nohuddle";
}
function playcallSeconds(s) {
  return noHuddle(s) ? 8 : PLAYCALL_SECONDS;
}
function playClockSeconds(s) {
  return noHuddle(s) ? 6 : PLAY_CLOCK_SECONDS;
}
function armOf(s, side) {
  return s.qbArm?.[side] ?? 1;
}
var FRIENDLY_HOLD_MIN = 0.4;
var FRIENDLY_HOLD_FULL = 2.5;
var FRIENDLY_HOLD_END = 3.8;
var SIDELINE_IN = 1.3;
function throwBall(s, qb, ax, ay, passType, exact = false) {
  const play2 = s.play;
  const arm = armOf(s, qb.team);
  const pt = passType === 0 || passType === 2 ? passType : 1;
  if (!Number.isFinite(ax) || !Number.isFinite(ay)) {
    ax = qb.x + 10;
    ay = qb.y;
  }
  let snapTo = null;
  let sd = exact ? 0 : THROW_SNAP_R;
  for (let k = 0; k < TEAM_SIZE; k++) {
    const r4 = s.players[pidx(offTeam(s), k)];
    if (!isEligible(r4) || r4.downT > 0) continue;
    const d2 = dist(r4.x, r4.y, ax, ay);
    if (d2 < sd) {
      sd = d2;
      snapTo = r4;
    }
  }
  if (snapTo) [ax, ay] = leadPoint(s, qb, snapTo, pt);
  let dx = ax - qb.x;
  let dy = ay - qb.y;
  const m = Math.hypot(dx, dy);
  if (m < 0.01) {
    dx = 1;
    dy = 0;
  } else {
    dx /= m;
    dy /= m;
  }
  const aimDist = clampThrowDist(m, pt, arm);
  const pressured = s.players.some((d2) => d2.team !== qb.team && dist(d2.x, d2.y, qb.x, qb.y) < PRESSURE_R);
  const moving = Math.hypot(qb.vx, qb.vy) > 1.5;
  const r3 = accuracyRadius(aimDist, pressured, moving, pt, s.qbAcc?.[qb.team] ?? 1);
  const ang = nextRandom(s) * Math.PI * 2;
  const rr = r3 * Math.sqrt(nextRandom(s));
  let tx = qb.x + dx * aimDist + Math.cos(ang) * rr;
  let ty = qb.y + dy * aimDist + Math.sin(ang) * rr;
  const d0 = clampThrowDist(Math.hypot(tx - qb.x, ty - qb.y), pt, arm);
  const m2 = Math.hypot(tx - qb.x, ty - qb.y) || 1;
  tx = qb.x + (tx - qb.x) / m2 * d0;
  ty = qb.y + (ty - qb.y) / m2 * d0;
  ty = Math.max(SIDELINE_IN, Math.min(FIELD_W - SIDELINE_IN, ty));
  const d = Math.hypot(tx - qb.x, ty - qb.y);
  const catchR = catchRadius(d);
  const b = s.ball;
  b.state = "air";
  b.carrier = -1;
  b.kick = false;
  b.passType = pt;
  b.sx = qb.x;
  b.sy = qb.y;
  b.tx = tx;
  b.ty = ty;
  b.t = 0;
  b.dur = throwDuration(d, pt);
  b.peak = throwPeak(d, pt);
  b.thrower = qb.idx;
  b.x = qb.x;
  b.y = qb.y;
  b.z = 1.5;
  play2.thrown = true;
  qb.anim = "throw";
  qb.animT = 0;
  qb.mode = "idle";
  qb.carrySince = -1;
  s.stats[offTeam(s)].attempts += 1;
  if (!s.conv) s.pstats[qb.team].off[qb.slot].passAtt += 1;
  let best = snapTo ? snapTo.idx : -1;
  let bestSlack = snapTo ? Infinity : -2;
  for (let k = 0; k < TEAM_SIZE && !snapTo; k++) {
    if (k === OFF.QB || k === OFF.OL1 || k === OFF.OL2) continue;
    const p = s.players[pidx(offTeam(s), k)];
    const reach = baseSpeed(s, p) * b.dur * 0.95 + catchR;
    const slack = reach - dist(p.x, p.y, b.tx, b.ty);
    if (slack > bestSlack) {
      bestSlack = slack;
      best = p.idx;
    }
  }
  b.target = best;
  s.control[offTeam(s)] = -1;
  emit(s, { type: "throw", team: offTeam(s), x: qb.x, y: qb.y, v: 1 });
  autoSwitch(s, defTeam(s), b.tx, b.ty, 1.5);
}
function updateBall(s, dt) {
  const b = s.ball;
  const play2 = s.play;
  if (b.state === "held") {
    const c = s.players[b.carrier];
    b.x = c.x;
    b.y = c.y;
    b.z = 1;
    return;
  }
  if (b.state !== "air") return;
  if (play2.kickoff) {
    updateKickoffBall(s, dt);
    return;
  }
  b.t += dt;
  const f = Math.min(1, b.t / b.dur);
  b.x = b.sx + (b.tx - b.sx) * f;
  b.y = b.sy + (b.ty - b.sy) * f;
  b.z = ballZ(f, b.peak);
  if (f >= CATCHABLE_FROM) {
    const off = offTeam(s);
    const catchR = catchRadius(dist(b.sx, b.sy, b.tx, b.ty));
    let best = null;
    let bestScore = Infinity;
    for (const p of s.players) {
      if (p.idx === b.thrower || p.downT > 0) continue;
      const isOff = p.team === off;
      if (isOff && (p.slot === OFF.OL1 || p.slot === OFF.OL2 || p.slot === OFF.QB)) continue;
      const human = s.control[p.team] === p.idx;
      const jumping = human && !isOff && p.catchT > 0;
      const reach = p.diveT > 0 ? 1.6 : isOff ? REACH_OFF : jumping ? REACH_DEF_HUMAN + 0.5 : human ? REACH_DEF_HUMAN : REACH_DEF;
      if (b.z > reach) continue;
      const aiDef = !isOff && !(human && s.aiLevel[p.team] === null);
      const r3 = isOff ? catchR : jumping ? INT_JUMP_R : p.diveT > 0 ? TACKLE_R_DIVE * 0.85 : aiDef ? T(s, p.team).ballReach : CATCH_R_DEF_HUMAN;
      const d = dist(p.x, p.y, b.x, b.y);
      if (d <= r3 && d / r3 < bestScore) {
        bestScore = d / r3;
        best = p;
      }
    }
    if (best) {
      if (best.team === off) {
        if (!play2.catchContested) {
          play2.catchContested = true;
          const read = guessedRouteDist(s, best) < 2.5 ? play2.guessConf ?? 0 : 0;
          let keep = 1;
          for (const p of s.players) {
            if (p.team === off || p.downT > 0) continue;
            const dd = Math.min(dist(p.x, p.y, b.x, b.y), dist(p.x, p.y, best.x, best.y) + 0.3);
            if (dd > CONTEST_R) continue;
            const human = s.control[p.team] === p.idx;
            const rate = modOf(s, p, "cover") / modOf(s, best, "hands");
            const c = Math.min(0.9, CONTEST_BREAKUP_CHANCE * (1 - (dd / CONTEST_R) ** 2) * rate * (human ? 1.4 : T(s, p.team).contestMult * (1 + 0.7 * T(s, p.team).jump * read)));
            keep *= 1 - c;
          }
          const chance = 1 - keep;
          if (chance > 0 && nextRandom(s) < chance) {
            let cut = null;
            let cd = dist(best.x, best.y, b.x, b.y);
            for (const p of s.players) {
              if (p.team === off || p.downT > 0) continue;
              const dd = dist(p.x, p.y, b.x, b.y);
              if (dd < cd) {
                cd = dd;
                cut = p;
              }
            }
            const human = cut !== null && s.control[cut.team] === cut.idx && s.aiLevel[cut.team] === null;
            if (cut && !human && nextRandom(s) < AI_INT_CHANCE * T(s, cut.team).intMult * modOf(s, cut, "cover")) {
              catchBall(s, cut, true);
              return;
            }
            emit(s, { type: "breakup", team: other(off), x: b.x, y: b.y });
            endPlay(s, { kind: "incomplete", breakup: true });
            return;
          }
        }
        catchBall(s, best, false);
      } else if (defenderPicksIt(s, best, dist(best.x, best.y, b.x, b.y))) {
        catchBall(s, best, true);
      } else {
        emit(s, { type: "breakup", team: best.team, x: b.x, y: b.y });
        endPlay(s, { kind: "incomplete", breakup: true });
      }
      return;
    }
  }
  if (f >= 1) {
    b.state = "loose";
    b.z = 0;
    emit(s, { type: "incomplete", x: b.x, y: b.y });
    endPlay(s, { kind: "incomplete", breakup: false });
  }
}
function defenderPicksIt(s, p, d) {
  const human = s.control[p.team] === p.idx && s.aiLevel[p.team] === null;
  if (!human) return nextRandom(s) < AI_INT_CHANCE * T(s, p.team).intMult * modOf(s, p, "cover");
  if (p.catchT > 0) return nextRandom(s) < (d <= 1 ? INT_JUMP_CHANCE_CLOSE : INT_JUMP_CHANCE);
  if (p.diveT > 0) return nextRandom(s) < INT_JUMP_CHANCE;
  return d <= HUMAN_INT_R && nextRandom(s) < INT_STANDING_CHANCE;
}
function catchBall(s, p, interception) {
  const b = s.ball;
  const play2 = s.play;
  b.state = "held";
  b.carrier = p.idx;
  b.z = 1;
  p.anim = "catch";
  p.animT = 0;
  p.vx *= 0.75;
  p.vy *= 0.75;
  if (interception) {
    play2.intercepted = true;
    emit(s, { type: "interception", team: p.team, x: p.x, y: p.y });
    if (s.ot) {
      endPlay(s, { kind: "int_return", spot: p.x, oob: false });
      return;
    }
  } else {
    s.stats[offTeam(s)].completions += 1;
    if (!s.conv && !play2.kickoff) {
      const def = defTeam(s);
      if (!s.burned) s.burned = [{}, {}];
      const list = s.burned[def][play2.playId] ??= [];
      list.push([Math.round((p.x - play2.startLos) * 10) / 10, Math.round(p.y * 10) / 10]);
      if (list.length > 3) list.shift();
    }
    if (!s.conv) {
      s.pstats[p.team].off[p.slot].rec += 1;
      const qb = s.players[b.thrower];
      if (qb) s.pstats[qb.team].off[qb.slot].passCmp += 1;
    }
    emit(s, { type: "catch", team: p.team, x: p.x, y: p.y });
  }
  onNewCarrier(s, p);
}
function onNewCarrier(s, c) {
  const bt = c.team;
  const nbt = other(bt);
  c.carrySince = s.play?.t ?? 0;
  for (const p of s.players) {
    if (p.idx === c.idx) {
      p.mode = "carry";
      p.routeTargets = [];
      continue;
    }
    if (p.team === bt) {
      p.mode = "block";
      p.blockTarget = -1;
    } else {
      p.mode = "pursue";
    }
  }
  s.control[bt] = c.idx;
  autoSwitch(s, nbt, c.x, c.y, 2);
}
function autoSwitch(s, side, tx, ty, keep) {
  const cur = s.control[side];
  void keep;
  if (cur >= 0 && s.players[cur].team === side && s.ball.carrier !== cur) return;
  let best = -1;
  let bestD = Infinity;
  for (let k = 0; k < TEAM_SIZE; k++) {
    const p = s.players[pidx(side, k)];
    if (p.downT > 0) continue;
    const d = dist(p.x, p.y, tx, ty);
    if (d < bestD) {
      bestD = d;
      best = p.idx;
    }
  }
  if (best < 0) return;
  const curOk = cur >= 0 && s.players[cur].team === side && s.players[cur].downT <= 0;
  if (curOk && dist(s.players[cur].x, s.players[cur].y, tx, ty) <= bestD + keep) return;
  if (best !== cur) {
    s.control[side] = best;
    emit(s, { type: "switch", team: side });
  }
}
function juke(s, p, inp) {
  p.burstT = JUKE_TIME;
  p.burstCd = JUKE_COOLDOWN;
  let threat = null;
  let bestTtc = JUKE_PERFECT_TTC;
  let nearest = null;
  let nd = JUKE_WINDOW_R;
  for (const d of s.players) {
    if (d.team === p.team || d.downT > 0 || d.stunT > 0) continue;
    const dd = dist(d.x, d.y, p.x, p.y);
    if (dd > JUKE_WINDOW_R) continue;
    if (dd < nd) {
      nd = dd;
      nearest = d;
    }
    const closing = ((d.vx - p.vx) * (p.x - d.x) + (d.vy - p.vy) * (p.y - d.y)) / Math.max(0.01, dd);
    const ttc = closing > 0.5 ? Math.max(0, dd - 0.8) / closing : dd < 1.05 ? 0 : Infinity;
    if (ttc <= bestTtc) {
      bestTtc = ttc;
      threat = d;
    }
  }
  const side = threat ?? nearest;
  if (Math.abs(inp.my) > 0.3) p.jukeSide = inp.my > 0 ? 1 : -1;
  else if (side) p.jukeSide = p.y >= side.y ? 1 : -1;
  else p.jukeSide = p.y < MID_Y ? 1 : -1;
  if (p.y < 3) p.jukeSide = 1;
  if (p.y > FIELD_W - 3) p.jukeSide = -1;
  const aiPlayer = threat !== null && (s.aiLevel[threat.team] !== null || s.control[threat.team] !== threat.idx);
  const resisted = aiPlayer && nextRandom(s) < T(s, threat.team).jukeResist;
  const perfect = threat !== null && !resisted;
  p.vy += p.jukeSide * baseSpeed(s, p) * 0.5;
  if (perfect) {
    p.jukeImmuneT = JUKE_IMMUNE;
    p.jukeFrom = threat.idx;
    threat.stunT = Math.max(threat.stunT, JUKE_STUN);
    s.stats[p.team].perfectJukes += 1;
  }
  emit(s, { type: "juke", team: p.team, x: p.x, y: p.y, v: perfect ? 1 : 0 });
}
function selectDefender(s, side, slot) {
  if (!Number.isInteger(slot) || slot < 0 || slot >= TEAM_SIZE) return false;
  if (s.phase === "live" && s.play) {
    if (side === ballTeam(s)) return false;
    const p = s.players[pidx(side, slot)];
    if (p.downT > 0 || s.control[side] === p.idx) return false;
    s.control[side] = p.idx;
    emit(s, { type: "switch", team: side });
    return true;
  }
  if (s.phase !== "playcall" && s.phase !== "presnap") return false;
  if (side === s.possession) return false;
  s.defPref[side] = slot;
  s.control[side] = pidx(side, slot);
  emit(s, { type: "switch", team: side });
  return true;
}
function checkCarrier(s, c) {
  const play2 = s.play;
  const off = offTeam(s);
  if (!play2.intercepted) {
    if (c.x >= FIELD_LEN) {
      if (!s.conv) emit(s, { type: "touchdown", team: off, x: c.x, y: c.y });
      c.anim = "celebrate";
      endPlay(s, { kind: "td", via: play2.kickoff ? "return" : play2.via });
      return true;
    }
    if (c.y < 0 || c.y > FIELD_W) {
      emit(s, { type: "out_of_bounds", x: c.x, y: c.y });
      if (play2.kickoff) endPlay(s, { kind: "ko_return", spot: c.x, oob: true });
      else endPlay(s, { kind: "down", spot: c.x, oob: true, sack: false, via: play2.via });
      return true;
    }
  } else {
    if (c.x <= 0) {
      if (!s.conv) emit(s, { type: "touchdown", team: c.team, x: c.x, y: c.y });
      c.anim = "celebrate";
      endPlay(s, { kind: "int_td" });
      return true;
    }
    if (c.y < 0 || c.y > FIELD_W) {
      emit(s, { type: "out_of_bounds", x: c.x, y: c.y });
      endPlay(s, { kind: "int_return", spot: c.x, oob: true });
      return true;
    }
  }
  return false;
}
function checkTackles(s, c) {
  for (const p of s.players) {
    if (p.team === c.team || p.stunT > 0 || p.downT > 0) continue;
    const human = s.control[p.team] === p.idx;
    const tune = T(s, p.team);
    const r3 = p.diveT > 0 ? TACKLE_R_DIVE : human ? TACKLE_R_HUMAN : tune.tackleR * Math.sqrt(modOf(s, p, "tackle"));
    if (dist(p.x, p.y, c.x, c.y) > r3) continue;
    if (c.jukeImmuneT > 0 && c.jukeFrom === p.idx && p.diveT <= 0) continue;
    if (c.stiffT > 0) {
      if (nextRandom(s) < (p.diveT > 0 ? STIFF_WIN_DIVE : STIFF_WIN)) {
        p.stunT = 0.8;
        p.downT = Math.max(p.downT, 0.5);
        c.stiffT = 0;
        c.stumbleT = Math.max(c.stumbleT, 0.15);
        emit(s, { type: "stiff_arm", team: c.team, x: c.x, y: c.y, v: 1 });
        continue;
      }
      c.stiffT = 0;
    }
    if (p.diveT <= 0) {
      const rate = modOf(s, c, "power") / modOf(s, p, "tackle");
      const chance = (human ? c.burstT > 0 ? 0.2 : 0 : (c.burstT > 0 ? BROKEN_TACKLE_CHANCE_BURST : BROKEN_TACKLE_CHANCE) * tune.brokenTackleMult) * rate;
      if (chance > 0 && nextRandom(s) < chance) {
        p.stunT = STUN_TIME;
        c.stumbleT = STUMBLE_TIME;
        emit(s, { type: "broken_tackle", team: c.team, x: c.x, y: c.y });
        continue;
      }
    }
    endPlayDown(s, c, false, p.idx);
    return true;
  }
  return false;
}
function endPlayDown(s, c, oob, tackler) {
  const play2 = s.play;
  c.anim = "down";
  c.animT = 0;
  if (tackler >= 0) {
    const t = s.players[tackler];
    t.anim = t.diveT > 0 ? "dive" : "down";
    t.animT = 0;
    play2.tackler = tackler;
    if (s.injuries && c.injuryPlays <= 0) {
      const closing = Math.hypot(t.vx - c.vx, t.vy - c.vy);
      const big = t.diveT > 0 || closing > 9;
      const chance = big ? INJURY_CHANCE * (1.5 - 0.5 * c.stamina) : 0;
      if (chance > 0 && nextRandom(s) < chance) {
        c.injuryPlays = INJURY_PLAYS_MIN + Math.floor(nextRandom(s) * (INJURY_PLAYS_MAX - INJURY_PLAYS_MIN + 1)) + 1;
        emit(s, { type: "injury", team: c.team, x: c.x, y: c.y, v: c.slot });
        (s.injuredLog ??= []).push({ team: c.team, slot: c.slot, off: c.team === s.possession });
      }
    }
  }
  if (play2.kickoff) {
    emit(s, { type: "tackle", team: other(c.team), x: c.x, y: c.y });
    endPlay(s, { kind: "ko_return", spot: c.x, oob });
    return;
  }
  if (play2.intercepted) {
    emit(s, { type: "tackle", team: other(c.team), x: c.x, y: c.y });
    endPlay(s, { kind: "int_return", spot: c.x, oob });
    return;
  }
  const off = offTeam(s);
  const pdef = playById(play2.playId);
  const sack = pdef?.kind === "pass" && !play2.thrown && !play2.handedOff && c.slot === OFF.QB && c.team === off && c.x < play2.startLos;
  emit(s, { type: sack ? "sack" : "tackle", team: other(off), x: c.x, y: c.y });
  endPlay(s, { kind: "down", spot: c.x, oob, sack, via: play2.via });
}
function endPlay(s, o) {
  const play2 = s.play;
  if (play2.ended) return;
  play2.ended = true;
  recordSlotStats(s, o);
  if (s.ball.state === "held") {
    s.ball.state = "dead";
  } else if (s.ball.state === "air") {
    s.ball.state = "loose";
    s.ball.z = 0;
  }
  for (const p of s.players) {
    p.vx = 0;
    p.vy = 0;
    p.stamina = Math.max(0, p.stamina - WEAR_BASE - WEAR_FROM_FATIGUE * p.fatigue);
    if (p.injuryPlays > 0) p.injuryPlays -= 1;
  }
  const off = offTeam(s);
  const r3 = applyOutcome(s, o);
  if (r3.result.kind === "first_down") emit(s, { type: "first_down", team: off });
  if (r3.result.kind === "turnover_downs") emit(s, { type: "turnover_downs", team: other(off) });
  if (r3.result.kind === "safety") emit(s, { type: "safety", team: other(off) });
  if (r3.result.kind === "two_good" || r3.result.kind === "two_fail") emit(s, { type: r3.result.kind, team: r3.result.team });
  emit(s, { type: "whistle" });
  s.phase = r3.scored ? "score" : "dead";
  s.phaseT = r3.scored ? SCORE_SECONDS : DEAD_SECONDS;
  if (r3.scored) {
    for (const p of s.players) {
      if (p.team === r3.result.team && p.anim !== "down") {
        p.anim = "celebrate";
        p.animT = 0;
      }
    }
  }
}
function recordSlotStats(s, o) {
  const play2 = s.play;
  if (s.conv) return;
  const off = offTeam(s);
  const def = other(off);
  const st = s.pstats;
  const carrier = s.ball.carrier >= 0 && s.ball.state === "held" ? s.players[s.ball.carrier] : null;
  if (play2.tackler !== void 0 && play2.tackler >= 0) {
    const t = s.players[play2.tackler];
    const side = t.team === off ? st[t.team].off : st[t.team].def;
    side[t.slot].tkl += 1;
    if (o.kind === "down" && o.sack) side[t.slot].sck += 1;
  }
  if (o.kind === "down" && o.sack) {
    const qb = s.players[pidx(off, OFF.QB)];
    st[off].off[qb.slot].sacked += 1;
    return;
  }
  if (play2.kickoff) {
    if (carrier && carrier.team === off && (o.kind === "ko_return" || o.kind === "td")) {
      if (o.kind === "td") st[off].off[carrier.slot].retYds += Math.round(FIELD_LEN - s.los);
      else if (o.spot > 0) st[off].off[carrier.slot].retYds += Math.max(0, Math.round(clampLos(o.spot) - s.los));
    }
    return;
  }
  if (play2.intercepted) {
    const qb = s.players[s.ball.thrower];
    if (qb && qb.team === off) st[off].off[qb.slot].intThrown += 1;
    if (carrier && carrier.team === def) st[def].def[carrier.slot].defInt += 1;
    return;
  }
  if ((o.kind === "down" || o.kind === "td") && carrier && carrier.team === off) {
    if (o.kind === "down" && (o.spot <= 0 || o.spot >= FIELD_LEN)) return;
    const gained = o.kind === "td" ? Math.round(FIELD_LEN - s.los) : Math.round(clampLos(o.spot) - s.los);
    const td = o.kind === "td" ? 1 : 0;
    if (o.via === "pass") {
      const qb = s.players[s.ball.thrower];
      if (qb && qb.team === off) {
        st[off].off[qb.slot].passYds += gained;
        st[off].off[qb.slot].passTd += td;
      }
      st[off].off[carrier.slot].recYds += gained;
      st[off].off[carrier.slot].recTd += td;
    } else {
      st[off].off[carrier.slot].rushAtt += 1;
      st[off].off[carrier.slot].rushYds += gained;
      st[off].off[carrier.slot].rushTd += td;
    }
  }
}
function animateDead(s, dt) {
  for (const p of s.players) {
    p.animT += dt;
    if (p.anim === "run" || p.anim === "block") p.anim = "idle";
  }
}
function updateAnims(s) {
  const b = s.ball;
  for (const p of s.players) {
    let a;
    if (p.diveT > 0) a = "dive";
    else if (p.downT > 0) a = p.anim === "dive" ? "dive" : "down";
    else if ((p.anim === "throw" || p.anim === "catch") && p.animT < 0.35) a = p.anim;
    else if (b.state === "held" && b.carrier === p.idx && s.play && !s.play.thrown && p.slot === OFF.QB && s.play.chargeT > 0)
      a = "throw";
    else if (p.engagedT > 0.05 || p.stiffT > 0) a = "block";
    else if (Math.hypot(p.vx, p.vy) > 0.5) a = "run";
    else a = "idle";
    if (a !== p.anim) {
      p.anim = a;
      p.animT = 0;
    }
  }
}
function isBlocker(s, p) {
  if (s.ball.state === "held" && s.ball.carrier === p.idx) return false;
  if (p.team !== ballTeam(s)) return false;
  if (s.control[p.team] === p.idx) return false;
  if (p.mode === "route" && s.play && s.play.t < p.routeDelay) return true;
  return p.mode === "block";
}
function applyBlocking(s, desired, dt) {
  const bt = ballTeam(s);
  const preThrow = s.play && !s.play.thrown && !s.play.handedOff && !s.play.intercepted;
  for (const d of s.players) {
    if (d.team === bt) continue;
    if (d.shedT > 0 || d.diveT > 0) {
      d.engagedT = 0;
      continue;
    }
    let engaged = false;
    for (const b of s.players) {
      if (!isBlocker(s, b)) continue;
      const reach = b.team === offTeam(s) && (b.slot === OFF.OL1 || b.slot === OFF.OL2) && preThrow ? BLOCK_R_LINE : BLOCK_R;
      if (dist(b.x, b.y, d.x, d.y) < reach) {
        engaged = true;
        desired[b.idx][0] *= 0.25;
        desired[b.idx][1] *= 0.25;
        break;
      }
    }
    if (!engaged) {
      d.engagedT = Math.max(0, d.engagedT - dt * 2);
      continue;
    }
    const human = s.control[d.team] === d.idx;
    d.engagedT += dt;
    const slow = human ? 0.3 : BLOCK_SLOW;
    desired[d.idx][0] *= slow;
    desired[d.idx][1] *= slow;
    d.vx *= 0.8;
    d.vy *= 0.8;
    const hold = (s.mods?.[bt]?.block ?? 1) / modOf(s, d, "rush");
    const shed = (preThrow ? SHED_TIME_LINE : SHED_TIME_OPEN) * (human ? s.friendly ? 1.3 : 0.8 : T(s, d.team).shedMult) * hold;
    if (d.engagedT >= shed) {
      d.engagedT = 0;
      d.shedT = SHED_IMMUNE;
    }
  }
}
function separate(s, carrier) {
  const ps = s.players;
  for (let i = 0; i < ps.length; i++) {
    for (let j = i + 1; j < ps.length; j++) {
      const a = ps[i];
      const b = ps[j];
      if (carrier && (a === carrier && b.team !== a.team || b === carrier && a.team !== b.team)) continue;
      const dx = b.x - a.x;
      const dy = b.y - a.y;
      const d = Math.hypot(dx, dy);
      if (d >= SEPARATION_R || d < 1e-6) continue;
      const push = (SEPARATION_R - d) / 2;
      const nx = dx / d;
      const ny = dy / d;
      a.x -= nx * push;
      a.y -= ny * push;
      b.x += nx * push;
      b.y += ny * push;
    }
  }
}
function seek(p, tx, ty, speed) {
  const dx = tx - p.x;
  const dy = ty - p.y;
  const d = Math.hypot(dx, dy);
  if (d < 0.15) return [0, 0];
  const sp = speed * Math.min(1, d / 0.6);
  return [dx / d * sp, dy / d * sp];
}
function aiVelocity(s, p, carrier) {
  const play2 = s.play;
  if (!play2) return [0, 0];
  const sp = baseSpeed(s, p) * speedMods(p, isCarrier(s, p));
  const b = s.ball;
  const off = offTeam(s);
  const L = play2.startLos;
  const qb = s.players[pidx(off, OFF.QB)];
  const runStarted = carrier && carrier.team === off && (play2.handedOff || play2.qbCrossed);
  const tune = T(s, p.team);
  const fam = p.team !== off ? play2.guessConf ?? 0 : 0;
  const seenScrambles = Math.min(1, (s.scrambles[off] ?? 0) / 3) * tune.adapt;
  const scrambling = carrier === qb && !play2.thrown && !play2.handedOff && play2.scrambleT > tune.scrambleReact * (1 - 0.6 * seenScrambles);
  if (p.team !== off && p.biteT > 0 && b.state !== "air") return seek(p, Math.max(L + 1.5, p.x - 2), p.y, sp * 0.6);
  if (p.team !== off && play2.hot && b.state !== "air" && !runStarted && play2.t > 0.3) {
    const i = hotSpotFor(s, p);
    if (i >= 0) {
      const [hx, hy] = play2.hot[i];
      return seek(p, hx - 0.8, hy, sp);
    }
  }
  if (b.state === "air") {
    if (p.idx === b.target) {
      const bx = b.tx - b.sx;
      const by = b.ty - b.sy;
      const bl = Math.hypot(bx, by) || 1;
      const back = Math.min(2.4, bl * 0.25);
      return seek(p, b.tx - bx / bl * back, b.ty - by / bl * back, sp);
    }
    if (p.team !== off) {
      const remaining = Math.max(0, b.dur - b.t);
      const d = dist(p.x, p.y, b.tx, b.ty);
      const target = b.target >= 0 ? s.players[b.target] : null;
      const reacting = b.t > tune.reactThrow;
      const breakSp = sp * Math.min(1, tune.breakSpeed);
      const bl = Math.hypot(b.tx - b.sx, b.ty - b.sy) || 1;
      const cutX = b.tx - (b.tx - b.sx) / bl * Math.min(1.6, bl * 0.2);
      const cutY = b.ty - (b.ty - b.sy) / bl * Math.min(1.6, bl * 0.2);
      if (p.mode === "cover" && p.cover >= 0) {
        const r3 = s.players[p.cover];
        if (reacting && (d < 7 || p.cover === b.target)) return seek(p, cutX, cutY, breakSp);
        const inward = r3.y < MID_Y ? 1 : -1;
        return seek(p, r3.x - 0.3, r3.y + 1 * inward, sp);
      }
      if (reacting && p.mode !== "rush" && d - remaining * sp < 1.5) return seek(p, cutX, cutY, breakSp * 0.97);
      if (target) return pursue(s, p, target, p.mode === "rush" ? sp * 0.5 : sp * 0.9);
      return [0, 0];
    }
    if (p.mode === "route") return routeVelocity(s, p, sp);
    return [0, 0];
  }
  switch (p.mode) {
    case "carry": {
      if (!carrier || carrier.idx !== p.idx) {
        if (play2.t < p.routeDelay) return seek(p, p.x, p.y, 0);
        return routeVelocity(s, p, sp);
      }
      return carrierAi(s, p, sp);
    }
    case "fake": {
      const v = routeVelocity(s, p, sp * 0.85);
      if (v[0] === 0 && v[1] === 0 && p.slot !== OFF.QB) {
        p.mode = "block";
        p.blockTarget = -1;
      }
      return v;
    }
    case "qb":
    case "idle":
      return [0, 0];
    case "route":
      if (play2.t < p.routeDelay) return blockVelocity(s, p, carrier, sp);
      return routeVelocity(s, p, sp);
    case "block":
      return blockVelocity(s, p, carrier, sp);
    case "rush":
      return rushVelocity(s, p, carrier ?? qb, sp);
    case "cover": {
      if (play2.t < 0.15) return [0, 0];
      const r3 = s.players[p.cover];
      if (runStarted) return pursue(s, p, carrier, sp);
      if (r3.mode === "block" || r3.mode === "fake" || r3.mode === "route" && play2.t < r3.routeDelay) {
        if (carrier && carrier.team === off && carrier.slot !== OFF.QB) return pursue(s, p, carrier, sp);
        return seek(p, L + 4, r3.y * 0.6 + qb.y * 0.4, sp * 0.6);
      }
      const inward = r3.y < MID_Y ? 1 : -1;
      if (p.over) return seek(p, Math.max(r3.x + 3, L + 8), r3.y + 1.5 * inward, sp);
      if (scrambling && tune.contain && dist(r3.x, r3.y, qb.x, qb.y) < 8) return pursue(s, p, qb, sp);
      const lead = tune.manLead;
      const cushion = r3.x - L < 4 ? 1.1 : -tune.manCushion;
      let tx = r3.x + r3.vx * lead + cushion;
      let ty = r3.y + r3.vy * lead + 0.8 * inward;
      const g = fam > 0 && play2.t < 2 ? guessedSpot(s, r3) : null;
      if (g) {
        const gx = g[0] - r3.x;
        const gy = g[1] - r3.y;
        const gm = Math.hypot(gx, gy);
        if (gm > 0.5) {
          const step = Math.min(gm, 1.6 * tune.jump) * Math.min(1, fam);
          tx = r3.x + r3.vx * lead + gx / gm * step;
          ty = r3.y + r3.vy * lead + gy / gm * step;
        }
      }
      return seek(p, tx, ty, sp);
    }
    case "spy": {
      if (runStarted || scrambling || carrier && carrier.team === off && carrier.slot !== OFF.QB) return pursue(s, p, carrier, sp);
      if (dist(p.x, p.y, qb.x, qb.y) < 3 && carrier === qb) return pursue(s, p, qb, sp);
      if (tune.help > 0.5 && play2.t > 0.9) {
        const h = helpTarget(s, p, L, (tune.help - 0.5) * 2);
        if (h) return seek(p, h[0], h[1], sp);
      }
      return seek(p, L + 4.5, qb.y, sp * 0.8);
    }
    case "zone": {
      if (runStarted || carrier && carrier.team === off && carrier.slot !== OFF.QB) return pursue(s, p, carrier, sp);
      if (scrambling && !p.zoneDeep) return pursue(s, p, qb, sp);
      if (play2.t < 0.12) return [0, 0];
      let threat = null;
      let best = Infinity;
      for (let k = 0; k < TEAM_SIZE; k++) {
        const r3 = s.players[pidx(off, k)];
        if (r3.mode !== "route" || play2.t < r3.routeDelay) continue;
        const look = 0.4 + fam * 0.25;
        const px = r3.x + r3.vx * look;
        const py = r3.y + r3.vy * look;
        const d = dist(px, py, p.homeX, p.homeY);
        if (d > p.zoneR) continue;
        const score = d - (p.zoneDeep ? (r3.x - L) * 0.3 : 0);
        if (score < best) {
          best = score;
          threat = r3;
        }
      }
      if (!threat && fam > 0 && !p.zoneDeep) {
        for (let k = 0; k < TEAM_SIZE; k++) {
          const r3 = s.players[pidx(off, k)];
          const g = guessedSpot(s, r3);
          if (g && dist(g[0], g[1], p.homeX, p.homeY) < p.zoneR + 2) {
            const w = Math.min(0.9, fam * Math.min(1.3, tune.jump));
            return seek(p, p.homeX * (1 - w) + g[0] * w, p.homeY * (1 - w) + g[1] * w, sp);
          }
        }
      }
      if ((!threat || p.zoneDeep && threat.x - L < 8) && tune.help > 0 && play2.t > 0.5) {
        const h = helpTarget(s, p, L, tune.help);
        if (h) return seek(p, h[0], h[1], sp);
      }
      if (threat) {
        if (p.zoneDeep) {
          const tx2 = Math.max(threat.x + tune.deepCushion, p.homeX - 4);
          return seek(p, tx2, p.homeY * 0.35 + threat.y * 0.65, sp);
        }
        const sq = Math.min(0.95, tune.zoneSqueeze + fam * 0.1);
        const tx = Math.max(L + 2.5, Math.min(p.homeX + 3, threat.x + 0.5 + threat.vx * 0.2 * sq));
        return seek(p, tx, p.homeY * (1 - sq) + (threat.y + threat.vy * 0.2 * sq) * sq, sp);
      }
      return seek(p, p.homeX, p.homeY * 0.85 + qb.y * 0.15, sp * 0.85);
    }
    case "pursue":
      return carrier ? pursue(s, p, carrier, sp) : [0, 0];
    default:
      return [0, 0];
  }
}
function hotSpotFor(s, p) {
  const play2 = s.play;
  if (!play2.hot) return -1;
  if (!play2.hotBy) {
    const taken = /* @__PURE__ */ new Set();
    play2.hotBy = play2.hot.map(([hx, hy]) => {
      let best = -1;
      let bd = Infinity;
      for (const d of s.players) {
        if (d.team !== p.team || taken.has(d.idx) || s.control[d.team] === d.idx) continue;
        const free = d.mode === "zone" || d.mode === "spy" || d.over || d.mode === "cover" && d.cover >= 0 && s.players[d.cover].mode === "block";
        if (!free) continue;
        const dd = dist(d.x, d.y, hx, hy);
        if (dd < bd) {
          bd = dd;
          best = d.idx;
        }
      }
      if (best >= 0) taken.add(best);
      return best;
    });
  }
  return play2.hotBy.indexOf(p.idx);
}
function helpTarget(s, p, L, help) {
  const off = offTeam(s);
  let best = null;
  let bestOpen = 2.5;
  for (let k = 0; k < TEAM_SIZE; k++) {
    const r3 = s.players[pidx(off, k)];
    if (r3.mode !== "route" || r3.x < L + 2) continue;
    const px = r3.x + r3.vx * 0.5;
    const py = r3.y + r3.vy * 0.5;
    let open = Infinity;
    for (const d of s.players) {
      if (d.team === off || d.idx === p.idx || d.mode === "rush") continue;
      open = Math.min(open, dist(d.x, d.y, px, py));
    }
    if (dist(p.x, p.y, px, py) > 8 + 14 * help) continue;
    const score = open + (r3.x - L) * 0.08;
    if (score > bestOpen) {
      bestOpen = score;
      best = r3;
    }
  }
  if (!best) return null;
  const deep = best.x - L > 12;
  const lead = 0.4 + 0.3 * help;
  const tx = best.x + best.vx * lead + (deep ? 1.5 : -1);
  const ty = best.y + best.vy * lead;
  return [p.homeX + (tx - p.homeX) * Math.min(1, 0.5 + help * 0.5), p.homeY + (ty - p.homeY) * Math.min(1, 0.5 + help * 0.5)];
}
function routeVelocity(s, p, sp) {
  void s;
  while (p.routeStep < p.routeTargets.length) {
    const [tx, ty] = p.routeTargets[p.routeStep];
    if (dist(p.x, p.y, tx, ty) < 0.7) {
      p.routeStep += 1;
      continue;
    }
    const dx = tx - p.x;
    const dy = ty - p.y;
    const d = Math.hypot(dx, dy);
    p.lastDx = dx / d;
    p.lastDy = dy / d;
    return [p.lastDx * sp, p.lastDy * sp];
  }
  if (p.routeEnd === "go") {
    const vx = p.lastDx * sp;
    let vy = p.lastDy * sp;
    if (p.y < 2 && vy < 0 || p.y > FIELD_W - 2 && vy > 0) vy = 0;
    return [vx, vy];
  }
  return [0, 0];
}
function pursue(s, p, c, sp) {
  const off = offTeam(s);
  const dir = c.team === off && !s.play?.intercepted ? 1 : -1;
  const lead = T(s, p.team).pursuitLead;
  let t = dist(p.x, p.y, c.x, c.y) / Math.max(1, sp);
  let px = c.x;
  let py = c.y;
  for (let i = 0; i < 4; i++) {
    t = Math.min(2.2, t) * lead;
    px = c.x + c.vx * t;
    py = c.y + c.vy * t;
    t = dist(p.x, p.y, px, py) / Math.max(1, sp);
  }
  if (dist(p.x, p.y, c.x, c.y) < 2.2) return seek(p, c.x + c.vx * 0.1, c.y + c.vy * 0.1, sp);
  px += dir * 0.8;
  py = Math.max(0, Math.min(FIELD_W, py));
  return seek(p, px, py, sp);
}
function rushVelocity(s, p, target, sp) {
  const play2 = s.play;
  let tx = target.x;
  let ty = target.y;
  const pocket = target.slot === OFF.QB && !play2.thrown && !play2.handedOff;
  const lane = p.lane === "" && T(s, p.team).contain && (p.slot === DEF.DL1 || p.slot === DEF.DL2) ? p.slot === DEF.DL1 ? "edgeT" : "edgeB" : p.lane;
  if (pocket && (lane === "edgeT" || lane === "edgeB" || lane === "contain")) {
    const side = lane === "edgeT" ? -1 : lane === "edgeB" ? 1 : p.y < MID_Y ? -1 : 1;
    const wide = lane === "contain" ? 3 : lane !== p.lane ? 2.5 : 4.5;
    if (p.x > target.x + 1.5) {
      tx = target.x + 1;
      ty = target.y + side * wide;
    }
    if ((lane === "contain" || lane !== p.lane) && (target.y - p.y) * side > -1) ty = target.y + side * wide;
  }
  let [vx, vy] = seek(p, tx, ty, sp);
  for (const b of s.players) {
    if (b.team === p.team || !isBlocker(s, b)) continue;
    const d = dist(p.x, p.y, b.x, b.y);
    if (d > 1.9 || d < 1e-3) continue;
    const toT = Math.hypot(tx - p.x, ty - p.y) || 1;
    const ahead = ((b.x - p.x) * (tx - p.x) + (b.y - p.y) * (ty - p.y)) / (d * toT);
    if (ahead < 0.3) continue;
    const side = b.y - p.y >= 0 ? -1 : 1;
    vy += side * sp * 0.45;
    const m = Math.hypot(vx, vy) || 1;
    vx = vx / m * sp;
    vy = vy / m * sp;
    break;
  }
  return [vx, vy];
}
function blockVelocity(s, p, carrier, sp) {
  const play2 = s.play;
  const preThrow = !play2.thrown && !play2.handedOff && !play2.intercepted;
  const protect = carrier ?? s.players[pidx(offTeam(s), OFF.QB)];
  let t = p.blockTarget >= 0 ? s.players[p.blockTarget] : null;
  if (!t || t.team === p.team || t.downT > 0 || dist(t.x, t.y, protect.x, protect.y) > 14 || play2.t % 0.25 < DT) {
    t = null;
    let bestD = Infinity;
    for (const d2 of s.players) {
      if (d2.team === p.team) continue;
      const threat = d2.mode === "rush" || s.control[d2.team] === d2.idx || dist(d2.x, d2.y, protect.x, protect.y) < 6;
      if (preThrow && !threat) continue;
      const taken = s.players.some((o) => o !== p && o.team === p.team && o.blockTarget === d2.idx);
      const dd = dist(d2.x, d2.y, p.x, p.y) + dist(d2.x, d2.y, protect.x, protect.y) * 0.6 + (taken ? 12 : 0);
      if (dd < bestD) {
        bestD = dd;
        t = d2;
      }
    }
    p.blockTarget = t ? t.idx : -1;
  }
  if (!t) return seek(p, protect.x + 2 * (p.team === offTeam(s) ? 1 : -1), protect.y, sp * 0.6);
  const dx = protect.x - t.x;
  const dy = protect.y - t.y;
  const d = Math.hypot(dx, dy) || 1;
  let tx = t.x + dx / d * 0.8;
  const ty = t.y + dy / d * 0.8;
  if (preThrow && p.team === offTeam(s)) tx = Math.min(tx, play2.startLos + 2.5);
  return seek(p, tx, ty, sp);
}
function carrierAi(s, p, sp) {
  const dir = p.team === offTeam(s) && !s.play?.intercepted ? 1 : -1;
  let vx = dir * sp;
  let vy = 0;
  let nearest = null;
  let nd = 6;
  for (const d of s.players) {
    if (d.team === p.team) continue;
    const dd = dist(d.x, d.y, p.x, p.y);
    if ((d.x - p.x) * dir > -1 && dd < nd) {
      nd = dd;
      nearest = d;
    }
  }
  if (nearest) {
    const away = p.y - nearest.y >= 0 ? 1 : -1;
    vy = away * sp * 0.75;
    if (p.y < 4) vy = Math.abs(vy);
    if (p.y > FIELD_W - 4) vy = -Math.abs(vy);
    const m = Math.hypot(vx, vy);
    vx = vx / m * sp;
    vy = vy / m * sp;
  }
  return [vx, vy];
}
function canHike(s) {
  return s.phase === "presnap" && s.presnap.elapsed >= HIKE_MIN_SECONDS && s.presnap.setT > 0;
}

// src/shared/snapshot.ts
var r2 = (v) => Math.round(v * 100) / 100;
function buildSnapshot(s, side, ack) {
  const isOff = side === s.possession;
  const ctrl = s.control[side];
  const play2 = s.play;
  const pdef = playById(play2?.playId ?? s.calls[s.possession] ?? "");
  const me = ctrl >= 0 ? s.players[ctrl] : null;
  return {
    tick: s.tick,
    phase: s.phase,
    phaseT: r2(s.phaseT),
    clock: r2(s.clock),
    half: s.half,
    quarter: s.quarter ?? (s.half === 2 ? 3 : 1),
    ot: s.ot ? s.ot.round : 0,
    score: [s.score[0], s.score[1]],
    poss: s.possession,
    frame: s.frameSide,
    down: s.down,
    toGo: r2(s.toGo),
    los: r2(s.los),
    teams: [s.teams[0], s.teams[1]],
    players: s.players.map((p) => ({
      x: r2(p.x),
      y: r2(p.y),
      anim: p.anim,
      animT: r2(p.animT),
      facing: p.facing,
      burst: p.burstT > 0,
      diving: p.diveT > 0,
      down: p.downT > 0,
      engaged: p.engagedT > 0.05,
      burstReady: p.burstCd <= 0,
      diveReady: p.diveCd <= 0,
      stiff: p.stiffT > 0,
      stiffReady: p.stiffCd <= 0,
      tired: Math.round(p.fatigue * 10) / 10,
      stamina: Math.round(p.stamina * 10) / 10,
      hurt: p.injuryPlays > 0
    })),
    ball: {
      state: s.ball.state,
      carrier: s.ball.carrier,
      x: r2(s.ball.x),
      y: r2(s.ball.y),
      z: r2(s.ball.z),
      tx: r2(s.ball.tx),
      ty: r2(s.ball.ty),
      sx: r2(s.ball.sx),
      sy: r2(s.ball.sy),
      target: s.ball.target,
      passType: s.ball.passType,
      kick: s.ball.kick
    },
    control: [s.control[0], s.control[1]],
    myCall: s.calls[side],
    oppReady: s.calls[side === 0 ? 1 : 0] !== null,
    playId: isOff ? play2?.playId ?? s.calls[side] : null,
    thrown: play2?.thrown ?? false,
    chargeT: isOff ? r2(play2?.chargeT ?? 0) : 0,
    canThrow: isOff && s.phase === "live" && !!play2 && !play2.thrown && !play2.intercepted && !play2.scramble && pdef?.kind === "pass" && s.ball.state === "held" && s.ball.carrier === ctrl && s.players[ctrl]?.slot === 0 && s.players[ctrl].x < play2.startLos + 0.3,
    defPref: s.defPref[side],
    result: s.lastResult ? { seq: s.resultSeq, kind: s.lastResult.kind, text: s.lastResult.text, sub: s.lastResult.sub, team: s.lastResult.team } : null,
    events: s.events.slice(),
    fseq: s.formationSeq,
    winner: s.winner,
    ack,
    mySpeed: me ? r2(humanSpeed(s, me)) : 0,
    myAuto: me ? autoRunDir(s, me) : 0,
    myVel: me ? [r2(me.vx), r2(me.vy)] : [0, 0],
    stats: s.stats,
    timeouts: [s.timeouts[0], s.timeouts[1]],
    hikeReady: canHike(s),
    audibled: [s.presnap.audibled[0], s.presnap.audibled[1]],
    kick: s.kick ? {
      dist: s.kick.dist,
      holdT: r2(s.kick.holdT),
      holding: s.kick.holding,
      released: s.kick.released,
      needle: r2(
        s.kick.released ? s.kick.type === "ko" ? s.kick.power * 2 - 1 : s.kick.power : s.kick.type !== "ko" && (s.kick.stage ?? 0) === 0 ? 0 : kickMeter(s.kick.holdT, s.kick.type === "ko" ? KO_METER_HZ : fgMeterHz(s.kick.dist))
      ),
      stage: s.kick.type === "ko" ? 1 : s.kick.stage ?? 0,
      power: r2(s.kick.type === "ko" ? 0 : (s.kick.stage ?? 0) === 0 ? fgPowerAt(s.kick.holdT) : s.kick.lockedPower ?? 0),
      need: s.kick.type === "ko" ? 0 : r2(fgPowerNeed(s.kick.dist, s.mods?.[s.possession]?.kickPow ?? 1)),
      zone: s.kick.type === "ko" ? 0 : r2(kickZone(s, s.possession, s.kick.dist)),
      good: s.kick.released ? s.kick.good : false,
      ko: s.kick.type === "ko",
      punt: s.kick.type === "punt"
    } : null,
    kits: [s.kits[0], s.kits[1]],
    conv: !!s.conv,
    koPlay: !!play2?.kickoff,
    aiDef: !!s.aiDefense,
    callMax: playcallSeconds(s),
    throwMax: Math.round(maxThrowDist(2, armOf(s, side)) * 10) / 10,
    changesLeft: Math.max(0, (s.maxAudibles?.[side] ?? 1) - (s.audibleCount?.[side] ?? (s.presnap.audibled[side] ? 1 : 0)))
  };
}

// src/shared/counters.ts
var DEF_CALL_IDS = ["man", "press", "twoman", "cover0", "spy", "twopress", "cover2", "zone", "cover4", "tampa", "prevent", "cover6", "buzz", "blitz", "sblitz", "cblitz", "zeroblitz", "firezone", "overload", "runstuff", "goalline", "contain", "robber", "bracket", "bear"];
var COUNTER_TABLE = {
  slant: [0.3, 0.4, -3.3, 0.7, 2.6, -1.4, 4.1, 0.2, -0.7, 0.9, -3.1, 3.1, 1.5, 0.3, -2.7, 3.6, 0.4, 9.2, 6.9, -1.7, -0.6, -2.1, -1.9, 0.4, 0.8],
  stick: [10.4, 12.7, 15.1, 21.4, 18.1, 11.5, 3.7, 7.6, 12.4, 1, 1.7, 1.5, 6.6, 33.5, 25.3, 14.5, 41, 12.5, 6.9, 19.5, 15.9, 8, 19.4, 2.1, 17.7],
  outs: [-4.9, 1.3, -2.3, 1.6, -0.7, 0.8, -0.9, 2.7, 0.6, -2.7, 3.4, 0, 5.4, 5.6, -0.8, 5.2, 6.6, 8.7, 7.8, -1.1, 2.9, 0.7, -4.2, -2.4, 5.8],
  bubble: [2.3, 0.5, 0.9, 1, -1.6, 0.8, -1, -2.7, 0.5, 1.2, 1.1, 1.2, 1.4, 0.7, 1.7, 1, 0.4, 5.2, -0.1, 0.5, 1.5, 1.2, 1.6, 0.9, 1.5],
  hbscreen: [2.7, 3.8, 3.7, 5.5, 2.7, 1.1, 0.9, 2.4, 2.5, 0.9, -0.7, 3.1, -0.3, 1.9, 4.1, 5.5, 6.2, 1.4, 8.1, 4.6, 7.8, 2.5, 1.5, 1.7, 1.5],
  hitches: [2.9, 1.2, -0.9, 0.9, 3.3, 2.4, 1.6, 3.3, 0.9, 1.3, 2.5, 2.4, 3.7, 2.6, 3.7, 3.8, 11.3, 6.9, 7.1, 3.9, 5.5, 4.3, -0.5, 3.8, 4.4],
  snag: [11.4, 9.3, 13.8, 23, 24, 16.9, 8.6, 1.8, 6.6, 6.7, 0.3, 5.2, 10.9, 34.7, 33.1, 15.6, 39.8, 10.8, 12.5, 23.6, 15.9, 10.5, 13, 5.5, 11.3],
  shallow: [0.9, 2.2, 3, 3.8, 2.1, 3.3, 3.4, 0.5, 4.9, -1, 0.4, 1.7, 2.5, 3.4, 5.2, 4.2, 5.9, 4.1, 3.4, 4.6, 4.1, 2, 1.1, 1.6, 2.6],
  flood: [2.7, 5, 1, 5.4, 6.2, 4.5, 4.7, 3, 4.9, 1.7, 3.8, 2, 1.4, 4.9, 3.7, 11.7, 9.5, 9, 41.3, 4.5, 8.1, 4.2, 0.2, 1.4, 6.3],
  cross: [3.4, 5.4, 3.8, 5.8, 2.7, 4.2, 12.7, -0.4, 5.4, 3.1, 1.9, 7.1, -1.5, 4.5, 3.7, 4.7, 8.2, 2.1, 6, 4.5, 8.3, 2.6, 1.1, 3.9, 3.4],
  smash: [0.6, 3.7, 5.1, 2.4, 14.4, 13.8, -0.7, 0.9, 0.4, 0, 1.8, -0.8, 2.9, 35, 23.6, 31, 63, 10.7, 30.2, 31.7, 8.8, 13.6, 3.3, 11.3, 15.6],
  levels: [4.7, 8.4, 12.8, 24.4, 17.6, 8.2, 6.2, -0.8, -1.4, 2.1, -2.7, 2.9, -1.3, 27.2, 27.7, 9.2, 40.5, 3.9, 9.5, 13.7, 12.9, 11.6, 12.7, 3.1, 12],
  ycross: [9, 9.5, 4, 7.2, 6.6, 4.9, 5.4, 0.8, 4.9, 4.5, 2.7, 5.5, 4.5, 2.3, 7.1, 11.7, 1, 8.3, 21.3, 6.5, 3.6, 8.2, 6.9, 0.8, 2],
  dagger: [5.1, 4.1, 4.5, 17, 19, 7.6, 5.3, -0.4, 1.3, 3, -1.1, 2.3, -2.5, 16.6, 20.5, 11.1, 30.3, 4.4, 8.2, 11.5, 12.4, 9.7, 8.4, 4.1, 11.5],
  curlflat: [3.1, 5, 1.9, 4.8, 4.6, 3.6, 7.3, 2.6, 7.3, 6.2, 5.1, 4.2, 6, 3.7, 3.4, 8.2, 7.9, 11.4, 14.7, 8.2, 7.2, 0.7, 4, 4.3, 6.2],
  scat: [8.8, 2.9, 3.7, 5.8, 6, 4.4, 6, 10.1, 17.4, -1.7, 21.9, 15, 4.3, 3.5, 4.8, 9.7, 7.1, 24.6, 10.1, 5.7, 5.7, 6, 5, 4.6, 4.5],
  verts: [4.5, 2.5, 2.1, 4.6, 9, 2.3, 4, 0.4, 3, 2.5, 3, -0.2, 6.5, 0.6, 8.8, 10.2, 32.3, 9.4, 39.5, 7.4, 4.6, 3.9, 4.2, 7.7, 5.7],
  deep: [5.3, 3.9, -0.6, 4.4, 5.3, 3, 0.2, -0.3, 0.9, 2, -0.5, 2, 1.2, 1.4, 3.9, 6.6, 5.5, 1.7, 8.9, 1.4, 3.5, 3.8, 2.6, 2, 3.7],
  postwheel: [7.4, 3.9, 10.7, 16.7, 12.3, 10.3, 1.4, 11, 5.4, 3.8, 1.6, 2.3, 9, 16.3, 16.2, 6.3, 31.8, 8.8, 6.3, 12.3, 8.9, 10.7, 16.9, 3.1, 10.6],
  sluggo: [7.5, 9, 2, 9.5, 11.2, 1.7, 14.4, -0.5, 1.6, 9.2, -4.4, 5, 1.9, 6.1, 14.2, 15, 1.9, -5.6, 17.1, 5.4, 5.8, 4.5, 10.9, 9.5, 4],
  seams: [6.9, 5.3, 5.1, 6.3, 12.3, 3.6, 5.4, -2, 4, 3.7, 2.7, 3, 5.7, 7.9, 9.6, 13.6, 31.5, 16.1, 45.5, 12.5, 6.6, 6.4, 6.6, 8.1, 10.1],
  twopost: [4.3, 3.8, 3.3, 2.7, 4.9, 3.3, 4.5, 10.3, 7, 2.6, 23.7, 12.1, 6, 3.6, 4.6, 5.2, 8.4, 26.6, 6.7, 5.1, 4, 4.4, 6, 3.8, 1.4],
  scissors: [4.1, 2.4, -0.6, 8.7, 7.2, 6, 3.5, 2.4, -0.3, 0.6, -1.7, 2.7, -2.2, 12.8, 17, 10.8, 19.7, 4.5, 4.1, 9.9, 8.7, 3.2, 4.6, -2.7, 4.5],
  hailmary: [7.1, 7.5, 2.9, 16.5, 5.7, 4.4, 1.2, -0.6, 0.3, -0.7, -2.3, 2.1, 2.7, 8.7, 5.9, 0.6, 20.8, 5.2, 6.9, 4.1, 7.7, 1.7, 1.1, -1.6, 4],
  pacross: [4.9, 11.2, 3.5, 5.7, 2, 4.1, 3.2, -0.4, 4, 2.2, -0.3, 0.6, 6.9, 13.4, 2.5, 5.9, 7.5, 8.1, 6.9, 6.7, 14, 1.3, 1.7, 0, 16.2],
  paflood: [3.3, 9.3, 3.6, 9.1, 1.4, 6.7, 4.5, -0.3, 5.7, 2.9, 2.8, 6.8, 0.8, 12.1, 3.7, 12.3, 5.3, 5.4, 18.1, 3.9, 10.2, 3.5, 4.4, 2.2, 17.9],
  pa: [6.5, 6, 0.3, 7.5, 6.5, 5.2, 3.9, 8.6, 5.7, 3.5, 4.6, 4.9, 8.1, 9.2, 3.3, 16.6, -0.6, 10.4, 14.5, 3.5, 12.6, 3.9, 5, 6.1, 17.9],
  padig: [4.9, 4.3, 1.9, 5.6, 4.9, 1.6, 2.5, -0.8, -0.3, 1.3, -1.6, 0.7, -5.2, 4.2, 7.2, 11.5, 5.6, 2.4, 20.3, 3.5, 6.3, 3.2, 3.2, 4.9, 3.6],
  pashot: [4, 1.6, 0.5, 5.8, 3.4, -0.7, 4.5, 5.2, 1, 4.7, 1.1, 1.1, -1.8, -1.7, 2.7, 7.8, 0.7, 1.2, 12.8, -1, 6.7, 2.1, -0.4, 0.5, 11.6],
  paboot: [5.3, 7.4, 1.3, 11.4, 5.2, 3.7, 8.7, 5.1, 3.4, 4, 4.6, 4.2, 5.8, 9.7, 5.6, 13, -3.1, 12.8, 9.5, 3.6, 1.5, 6, 3.6, 3.5, 18.4],
  paseam: [4.4, 6.4, 1, 5, 4.4, 3.3, 0.9, 0.5, -0.1, 2.6, 1.6, -0.2, 1.2, 4.8, 5.1, 13.4, 4.3, 4.8, 31, 1.5, 6.7, 2, 2.8, 2.7, 4.1],
  pawheel: [5.8, 8.4, 3.7, 10.9, 5.8, 3.9, 5.6, 6.3, 7.6, 5.1, 3.1, 8.7, 3.6, 10.6, 5.1, 10.6, 0.7, 12.7, 16.7, 8, 5.9, 3.9, 8.8, 0.1, 17.1],
  dive: [1, 1.5, 2, -1, 0, 1.5, 3, 3, 5, 3, 7, 4, 2.5, -2, -2, -1, -1.5, -1, -1.5, -3, -3, 0, 1, 2, -3.5],
  sweep: [1, 1.5, 2, -1, 0, 1.5, 3, 3, 5, 3, 7, 4, 2.5, -2, -2, -1, -1.5, -1, -1.5, -3, -3, 0, 1, 2, -3.5],
  counter: [1, 1.5, 2, -1, 0, 1.5, 3, 3, 5, 3, 7, 4, 2.5, -2, -2, -1, -1.5, -1, -1.5, -3, -3, 0, 1, 2, -3.5],
  draw: [1, 1.5, 2, -1, 0, 1.5, 3, 3, 5, 3, 7, 4, 2.5, 0, 0, 1, 0.5, -1, 0.5, -3, -3, 0, 1, 2, -3.5],
  qbpower: [1, 1.5, 2, -1, 0, 1.5, 3, 3, 5, 3, 7, 4, 2.5, -2, -2, -1, -1.5, -1, -1.5, -3, -3, 0, 1, 2, -3.5],
  stretch: [1, 1.5, 2, -1, 0, 1.5, 3, 3, 5, 3, 7, 4, 2.5, -2, -2, -1, -1.5, -1, -1.5, -3, -3, 0, 1, 2, -3.5],
  tossweak: [1, 1.5, 2, -1, 0, 1.5, 3, 3, 5, 3, 7, 4, 2.5, -2, -2, -1, -1.5, -1, -1.5, -3, -3, 0, 1, 2, -3.5],
  sneak: [1, 1.5, 2, -1, 0, 1.5, 3, 3, 5, 3, 7, 4, 2.5, -2, -2, -1, -1.5, -1, -1.5, -3, -3, 0, 1, 2, -3.5]
};
function counterValue(play2, call) {
  const i = DEF_CALL_IDS.indexOf(call);
  return i >= 0 ? COUNTER_TABLE[play2]?.[i] ?? 0 : 0;
}

// src/shared/cpu.ts
var CpuBrain = class {
  constructor(side, opts = { skill: 0.6 }, seed = 1234) {
    this.side = side;
    this.opts = opts;
    let st = seed >>> 0;
    this.rand = () => {
      st = st + 1831565813 | 0;
      let t = st;
      t = Math.imul(t ^ t >>> 15, t | 1);
      t ^= t + Math.imul(t ^ t >>> 7, t | 61);
      return ((t ^ t >>> 14) >>> 0) / 4294967296;
    };
  }
  side;
  opts;
  seq = 0;
  actionCount = 0;
  switchCount = 0;
  formationSeq = -1;
  readTime = 2;
  plan = null;
  holding = 0;
  hikeAt = 1;
  audibleTried = false;
  /** Target spot on the kick meter (-1..1); below -5 means "not chosen yet". */
  kickHold = -9;
  /** Field goals: target power for the first tap (below 0 = not chosen yet). */
  kickPower = -1;
  recent = [];
  rand;
  pick(a) {
    return a[Math.floor(this.rand() * a.length)];
  }
  weighted(w) {
    const keys = Object.keys(w);
    const total = keys.reduce((a, k) => a + w[k], 0);
    let r3 = this.rand() * total;
    for (const k of keys) {
      r3 -= w[k];
      if (r3 <= 0) return k;
    }
    return keys[keys.length - 1];
  }
  /** Play call for the play-call phase (null = not yet / nothing to do). */
  pickCall(s) {
    if (s.phase !== "playcall" || s.calls[this.side] !== null) return null;
    if (s.phaseT > 13.6) return null;
    const id = this.side === s.possession ? this.offenseCall(s) : this.defenseCall(s);
    this.recent.push(id);
    if (this.recent.length > 3) this.recent.shift();
    return id;
  }
  offenseCall(s) {
    const opts = offenseOptions(s);
    const me = this.side;
    const diff = s.score[me] - s.score[me === 0 ? 1 : 0];
    const late = s.quarter === 4 && !s.ot && s.clock < 40;
    if (s.conv) {
      const chart = [-2, -5, -9, -10, -12, 1, 5];
      const goForTwo = chart.includes(diff) && (s.quarter === 4 || s.ot) || this.rand() < 0.06;
      if (!goForTwo) return "xp";
      const near = PLAYS.filter((p) => ["slant", "stick", "outs", "levels", "qbpower", "dive", "pacross", "cross"].includes(p.id));
      return this.pick(near).id;
    }
    if (opts.includes("fg")) {
      const d = fgDistance(s.los);
      if (s.down === 4 && d <= 48 + this.opts.skill * 8) return "fg";
      if (!s.ot && s.clock < 7 && (s.quarter === 2 || s.quarter === 4 && diff >= -3)) return "fg";
      if (s.ot && s.down === 4) return "fg";
    }
    if (s.down === 4) {
      const goForIt = s.toGo <= 1.5 || s.los >= 60 && s.toGo <= 3 || late && diff < 0 || !!s.ot;
      if (!goForIt && opts.includes("punt")) return "punt";
    }
    let cat;
    if (late && diff < 0) cat = this.weighted({ quick: 3, medium: 4, deep: 3, pa: 0.5, run: 0.3 });
    else if (s.toGo <= 2) cat = this.weighted({ run: 6, quick: 4, medium: 1, deep: 0.5, pa: 1.5 });
    else if (s.toGo >= 8 && s.down >= 2) cat = this.weighted({ quick: 2, medium: 4.5, deep: 3, pa: 1, run: 0.7 });
    else cat = this.weighted({ run: 4, quick: 2.5, medium: 2.5, deep: 1, pa: 1.5 });
    let pool = PLAYS.filter((p) => p.cat === cat && opts.includes(p.id));
    if (this.rand() < (1 - this.opts.skill) * 0.5) pool = PLAYS.filter((p) => opts.includes(p.id));
    const desperate = !s.ot && s.clock < 12 && (s.quarter === 2 || s.quarter === 4 && diff < 0) && s.los < 75;
    const situational = pool.filter((p) => (p.id !== "hailmary" || desperate) && (p.id !== "sneak" || s.toGo <= 1.5));
    if (situational.length) pool = situational;
    if (desperate && s.los < 70 && this.rand() < 0.6) {
      const hm = PLAYS.find((p) => p.id === "hailmary" && opts.includes(p.id));
      if (hm) return hm.id;
    }
    const fresh = pool.filter((p) => !this.recent.includes(p.id));
    return this.pick(fresh.length ? fresh : pool).id;
  }
  /**
   * The defensive coordinator: estimate what the offense will run (its play
   * history this game, plus down and distance), then pick among the calls that
   * do best against that mix (from the scouting table). `temp` is how loosely it
   * picks: higher = more variety, lower = sharper.
   */
  coordinatorCall(s, temp) {
    const r3 = this.coordinatorRank(s);
    const min = Math.min(...r3.map((x) => x.ev));
    const weights = r3.map((x) => Math.exp(-(x.ev - min) / temp));
    let roll = this.rand() * weights.reduce((a, b) => a + b, 0);
    for (let i = 0; i < r3.length; i++) {
      roll -= weights[i];
      if (roll <= 0) return r3[i].id;
    }
    return r3[0].id;
  }
  /** Every defensive call with its expected result against what the offense is likely to run (optionally only plays from this formation). */
  coordinatorRank(s, formation) {
    const seen = s.scout[this.side] ?? {};
    const counts = {};
    for (const [k, n] of Object.entries(seen)) {
      const id = k.split(":")[1];
      counts[id] = (counts[id] ?? 0) + n;
    }
    const shortYds = s.toGo <= 2;
    const passingDown = s.toGo >= 8 && s.down >= 2;
    let wsum = 0;
    const w = {};
    for (const p of PLAYS) {
      if (p.kind !== "pass" && p.kind !== "run") continue;
      if (formation && p.formation !== formation) continue;
      let prior = 0.5;
      if (p.kind === "run") prior *= shortYds ? 2.5 : passingDown ? 0.3 : 1;
      const v = prior + (counts[p.id] ?? 0) * 1.5;
      w[p.id] = v;
      wsum += v;
    }
    const calls = DEF_CALLS.filter((d) => !(d.id === "goalline" && s.los < 90) && !(d.id === "prevent" && s.los >= 85));
    return calls.map((d) => {
      let e = 0;
      for (const [id, v] of Object.entries(w)) e += v / wsum * counterValue(id, d.id);
      if (!formation && this.recent.includes(d.id)) e += 1.5;
      return { id: d.id, ev: e };
    });
  }
  defenseCall(s) {
    const tune = tuningFor(s.aiLevel?.[this.side]);
    const scr = s.scrambles?.[s.possession] ?? 0;
    const plays = Object.values(s.scout[this.side] ?? {}).reduce((a, b) => a + b, 0);
    if (scr >= 2 && scr / Math.max(1, plays) > 0.3 && this.rand() < tune.adapt * 0.8) return this.pick(["spy", "contain", "spy", "robber"]);
    if (this.rand() < tune.coordinator) return this.coordinatorCall(s, tune.callTemp);
    if (s.down >= 2 && s.toGo >= 5 && s.toGo <= 10 && this.rand() < tune.blitzRate) return this.pick(["blitz", "sblitz", "firezone", "cblitz"]);
    let cat;
    const goalLine = s.los >= 94;
    if (goalLine) cat = this.weighted({ special: 5, man: 3, blitz: 2, zone: 0.5 });
    else if (s.toGo <= 2) cat = this.weighted({ special: 4, blitz: 3, man: 2, zone: 1 });
    else if (s.toGo >= 9 && s.down >= 3) cat = this.weighted({ zone: 5, man: 2, blitz: 1.5, special: 1 });
    else cat = this.weighted({ man: 3, zone: 3.5, blitz: 2, special: 1.5 });
    let pool = DEF_CALLS.filter((d) => d.cat === cat);
    if (goalLine) pool = pool.filter((d) => d.id !== "prevent");
    else if (cat === "special" && s.toGo > 3) pool = pool.filter((d) => !d.runFirst);
    if (this.rand() < (1 - this.opts.skill) * 0.5) pool = DEF_CALLS.slice();
    const fresh = pool.filter((d) => !this.recent.includes(d.id));
    const id = this.pick(fresh.length ? fresh : pool).id;
    return defenseOptions().includes(id) ? id : "man";
  }
  /** Pre-snap audible (defense reads the formation), or null. */
  presnapAudible(s) {
    if (s.phase !== "presnap" || this.audibleTried || s.presnap.audibled[this.side]) return null;
    if (s.presnap.elapsed < 0.6) return null;
    this.audibleTried = true;
    if (this.side === s.possession) return null;
    const form = playById(s.calls[s.possession] ?? "")?.formation;
    const cur = defCallById(s.calls[this.side] ?? "");
    if (!cur || !form) return null;
    const tune = tuningFor(s.aiLevel?.[this.side]);
    if (this.rand() < tune.coordinator) {
      const rank = this.coordinatorRank(s, form);
      const now = rank.find((x) => x.id === cur.id)?.ev ?? 0;
      const best = rank.reduce((a, b) => b.ev < a.ev ? b : a);
      return best.id !== cur.id && best.ev < now - 2.5 ? best.id : null;
    }
    if (this.rand() > this.opts.skill * 0.6) return null;
    if ((form === "spread" || form === "trips") && cur.runFirst) return this.pick(["zone", "cover2", "man"]);
    if (form === "heavy" && (cur.id === "prevent" || cur.id === "cover4")) return "runstuff";
    return null;
  }
  /** Should this CPU call a timeout right now? */
  wantsTimeout(s) {
    if (s.ot || s.quarter !== 2 && s.quarter !== 4 || s.timeouts[this.side] <= 0) return false;
    if (s.phase !== "dead" || !s.clockRunsBetween || s.clock > 50 || s.clock < 1) return false;
    const me = this.side;
    const diff = s.score[me] - s.score[me === 0 ? 1 : 0];
    return diff < 0 && s.possession === me;
  }
  press() {
    this.actionCount = this.actionCount + 1 & 255;
  }
  press2() {
    this.switchCount = this.switchCount + 1 & 255;
  }
  input(s) {
    this.seq += 1;
    const skill = this.opts.skill;
    const inp = {
      seq: this.seq,
      mx: 0,
      my: 0,
      ax: 0,
      ay: 0,
      throwHeld: false,
      actionCount: this.actionCount,
      switchCount: this.switchCount,
      passType: 1
    };
    if (s.formationSeq !== this.formationSeq) {
      this.formationSeq = s.formationSeq;
      this.plan = null;
      this.holding = 0;
      this.kickHold = -9;
      this.audibleTried = false;
      this.readTime = 0.5 + (1 - skill) * 0.8 + this.rand() * 1;
      const me = this.side;
      const hurry = s.quarter === 4 && s.clock < 40 && s.score[me] < s.score[me === 0 ? 1 : 0];
      this.hikeAt = hurry ? 0.7 : 0.8 + this.rand() * 1.8;
    }
    if (s.phase === "presnap") {
      if (this.side === s.possession && canHike(s) && s.presnap.elapsed >= this.hikeAt) {
        this.press();
        inp.actionCount = this.actionCount;
      }
      return inp;
    }
    if (s.phase === "kick" && this.side === s.possession) {
      const k = s.kick;
      if ((k.stage ?? 0) === 0) {
        if (this.kickPower < 0) {
          const need = fgPowerNeed(k.dist, s.mods?.[this.side]?.kickPow ?? 1);
          this.kickPower = Math.min(0.99, need + 0.04 + this.rand() * 0.08 - (1 - this.opts.skill) * this.rand() * 0.1);
        }
        if (k.holdT > 0.4 && Math.abs(fgPowerAt(k.holdT + DT) - this.kickPower) < 0.03) this.press();
      } else {
        this.kickPower = -1;
        if (this.kickHold < -5) this.kickHold = (this.rand() - 0.5) * 2 * fgZone(k.dist) * (0.35 + (1 - this.opts.skill) * 1.4);
        if (!k.released && k.holdT > 0.5 && Math.abs(kickMeter(k.holdT + DT, fgMeterHz(k.dist)) - this.kickHold) < 0.07) this.press();
      }
      inp.actionCount = this.actionCount;
      return inp;
    }
    if (s.phase === "kickoff" && this.side !== s.possession) {
      const k = s.kick;
      if (this.kickHold < -5) this.kickHold = this.kickoffPlan();
      if (!k.released && k.holdT > 0.6 && Math.abs(kickMeter(k.holdT + DT, KO_METER_HZ) - this.kickHold) < 0.05) this.press();
      inp.actionCount = this.actionCount;
      return inp;
    }
    if (s.phase !== "live" || !s.play) return inp;
    const ctrl = s.control[this.side];
    if (ctrl < 0) return inp;
    const p = s.players[ctrl];
    const b = s.ball;
    const carrier = b.state === "held" ? s.players[b.carrier] : null;
    if (carrier && carrier.idx === p.idx) {
      const play2 = s.play;
      const isQbPre = p.slot === OFF.QB && p.team === offTeam(s) && !play2.thrown && !play2.intercepted && playById(play2.playId)?.kind === "pass";
      if (isQbPre) {
        this.qb(s, p, inp);
        inp.actionCount = this.actionCount;
        return inp;
      }
      const [vx, vy] = aiVelocity(s, p, carrier);
      this.setMove(inp, vx, vy);
      const dir = p.team === offTeam(s) && !play2.intercepted ? 1 : -1;
      for (const d of s.players) {
        if (d.team === p.team || d.downT > 0 || d.stunT > 0) continue;
        const dd = Math.hypot(d.x - p.x, d.y - p.y);
        if (dd > JUKE_WINDOW_R + 1) continue;
        const closing = ((d.vx - p.vx) * (p.x - d.x) + (d.vy - p.vy) * (p.y - d.y)) / Math.max(0.01, dd);
        const ttc = closing > 0.5 ? Math.max(0, dd - 0.8) / closing : 9;
        if (p.burstCd <= 0) {
          const want = JUKE_PERFECT_TTC * (0.5 + 0.5 * skill) + (1 - skill) * (0.4 + this.rand() * 0.8);
          if (ttc < want && this.rand() < 0.1 + 0.6 * skill) {
            this.press();
            break;
          }
        } else if (p.stiffCd <= 0 && (d.x - p.x) * dir > 0 && ttc < 0.35 && this.rand() < 0.3 * skill) {
          this.press2();
          break;
        }
      }
      inp.actionCount = this.actionCount;
      inp.switchCount = this.switchCount;
      return inp;
    }
    if (this.side !== ballTeam(s)) {
      if (carrier) {
        const lead = 0.35;
        const tx = carrier.x + carrier.vx * lead;
        const ty = carrier.y + carrier.vy * lead;
        const dd = Math.hypot(p.x - carrier.x, p.y - carrier.y);
        const preThrowQb = carrier.slot === OFF.QB && !s.play.thrown && !s.play.handedOff;
        if (dd < 1.6 && p.diveCd <= 0 && this.rand() < 0.05 + 0.45 * skill) this.press();
        if (preThrowQb || dd > 6) {
          const [vx, vy] = aiVelocity(s, p, carrier);
          this.setMove(inp, vx, vy, Math.min(1, Math.hypot(vx, vy) / 6.5));
        } else this.setMove(inp, tx - p.x, ty - p.y);
      } else if (b.state === "air") {
        const [vx, vy] = aiVelocity(s, p, null);
        this.setMove(inp, vx, vy, Math.min(1, Math.hypot(vx, vy) / 6.5));
      }
      inp.actionCount = this.actionCount;
      inp.switchCount = this.switchCount;
      return inp;
    }
    return inp;
  }
  /** Kickoff: usually a returnable kick near the goal line, sometimes a deep touchback. */
  kickoffPlan() {
    const p = this.rand() < 0.3 ? 0.9 + this.rand() * 0.1 : 0.7 + this.rand() * 0.16;
    return p * 2 - 1;
  }
  qb(s, qb, inp) {
    const play2 = s.play;
    let nd = Infinity;
    let nx = 0;
    let ny = 0;
    for (const d of s.players) {
      if (d.team === qb.team) continue;
      const dd = Math.hypot(d.x - qb.x, d.y - qb.y);
      if (dd < nd) {
        nd = dd;
        nx = d.x;
        ny = d.y;
      }
    }
    if (!this.plan) {
      const pressured = nd < 3.5;
      if (play2.t >= this.readTime || pressured) {
        this.plan = this.choose(s, qb, play2.t > this.readTime + 1.2 || pressured);
      }
      if (nd < 5) this.setMove(inp, qb.x > s.los - 8 ? (qb.x - nx) * 0.3 : 0.2, (qb.y - ny) * 1.5 || 1);
      else if (qb.x > s.los - 7) this.setMove(inp, -1, 0, 0.2);
    }
    if (this.plan) {
      inp.ax = this.plan.ax;
      inp.ay = this.plan.ay;
      inp.passType = this.plan.passType;
      inp.throwHeld = this.holding < this.plan.holdT;
      this.holding += DT;
      inp.mx = 0;
      inp.my = 0;
    }
  }
  /** Throws go on the click now; this is just the tiny delay before the ball leaves. */
  holdTime() {
    return DT * 2;
  }
  choose(s, qb, force) {
    const off = offTeam(s);
    const hold = this.holdTime();
    let best = null;
    const options = [];
    for (let k = 0; k < TEAM_SIZE; k++) {
      if (k === OFF.QB || k === OFF.OL1 || k === OFF.OL2) continue;
      const r3 = s.players[pidx(off, k)];
      if (r3.mode !== "route" || play(s).t < r3.routeDelay) continue;
      const rsp = Math.max(1, Math.hypot(r3.vx, r3.vy));
      for (const pt of [0, 1, 2]) {
        const spec = PASS_SPECS[pt];
        for (const lead of [0.35, 0.65, 1]) {
          let lx = r3.x;
          let ly = r3.y;
          let tb = 0;
          for (let i = 0; i < 3; i++) {
            const d = Math.hypot(lx - qb.x, ly - qb.y);
            tb = spec.dur(d) + hold;
            lx = r3.x + r3.vx * tb * lead;
            ly = r3.y + r3.vy * tb * lead;
          }
          ly = Math.max(1, Math.min(FIELD_W - 1, ly));
          const dist2 = Math.hypot(lx - qb.x, ly - qb.y);
          if (dist2 > maxThrowDist(pt, armOf(s, qb.team)) || dist2 < 4) continue;
          if (pt === 0 && dist2 > 24) continue;
          const tr = Math.hypot(lx - r3.x, ly - r3.y) / Math.max(rsp, 7);
          if (tr > tb + 0.15) continue;
          let td = Infinity;
          for (const d of s.players) {
            if (d.team === off) continue;
            td = Math.min(td, Math.max(0, Math.hypot(d.x - lx, d.y - ly) - 1) / 7.5 + 0.3);
          }
          let margin = td - tb;
          margin -= this.laneRisk(s, qb, lx, ly, pt);
          const need = dist2 > 25 ? (dist2 - 25) * 0.02 : 0;
          const score = margin - need + (lx - s.los) * 0.012 + (pt === 1 ? 0.05 : 0);
          options.push({ ax: lx, ay: ly, score, pt });
          if (!best || score > best.score) best = { ax: lx, ay: ly, score, pt };
        }
      }
    }
    if ((!best || best.score < -0.4) && force) {
      return { ax: qb.x + 12, ay: qb.y < FIELD_W / 2 ? -6 : FIELD_W + 6, holdT: hold, passType: 1 };
    }
    if (!best || best.score < 0.25 && !force) return null;
    if (options.length > 1 && this.rand() < (1 - this.opts.skill) * 0.8) best = options[Math.floor(this.rand() * options.length)];
    const err = (1 - this.opts.skill) * 3;
    return { ax: best.ax + (this.rand() - 0.5) * err, ay: best.ay + (this.rand() - 0.5) * err, holdT: hold, passType: best.pt };
  }
  laneRisk(s, qb, tx, ty, pt) {
    const spec = PASS_SPECS[pt];
    const d = Math.hypot(tx - qb.x, ty - qb.y);
    const peak = spec.peak(d);
    let risk = 0;
    for (const def of s.players) {
      if (def.team === qb.team) continue;
      for (let f = 0.2; f < 0.9; f += 0.1) {
        if (ballZ(f, peak) > 2.6) continue;
        const bx = qb.x + (tx - qb.x) * f;
        const by = qb.y + (ty - qb.y) * f;
        if (Math.hypot(def.x - bx, def.y - by) < 1.6) {
          risk = Math.max(risk, 1.2);
          break;
        }
      }
    }
    return risk;
  }
  setMove(inp, dx, dy, mag = 1) {
    const m = Math.hypot(dx, dy);
    if (m < 0.05) return;
    inp.mx = dx / m * mag;
    inp.my = dy / m * mag;
  }
};
function play(s) {
  return s.play;
}

// src/shared/room.ts
function randomInt(lo, hi) {
  const a = new Uint32Array(1);
  globalThis.crypto.getRandomValues(a);
  return lo + a[0] % (hi - lo);
}
function randomHex(bytes) {
  const a = new Uint8Array(bytes);
  globalThis.crypto.getRandomValues(a);
  return Array.from(a, (b) => b.toString(16).padStart(2, "0")).join("");
}
var MAX_QUEUE = 3;
var INPUT_STALE_MS = 250;
var Room = class {
  code;
  phase = "lobby";
  seats = [null, null];
  match = null;
  matchId = 0;
  countdown = 0;
  paused = null;
  pauseUsed = [0, 0];
  resumeIn = 0;
  lastActive;
  endedAt = 0;
  lastRoomBroadcast = 0;
  /** Computer players for the agreed AI-defense option (one per side). */
  aiBrains = null;
  cfg;
  constructor(code, now, cfg = {}) {
    this.code = code;
    this.lastActive = now;
    this.cfg = {
      halfSeconds: cfg.halfSeconds ?? 120,
      graceSeconds: cfg.graceSeconds ?? RECONNECT_GRACE_SECONDS,
      pauseBudgetSeconds: cfg.pauseBudgetSeconds ?? PAUSE_BUDGET_SECONDS,
      lobbyIdleSeconds: cfg.lobbyIdleSeconds ?? 30 * 60,
      inputStaleMs: cfg.inputStaleMs ?? INPUT_STALE_MS
    };
  }
  get grace() {
    return this.cfg.graceSeconds;
  }
  isFull() {
    return this.seats[0] !== null && this.seats[1] !== null;
  }
  isEmpty() {
    return this.seats[0] === null && this.seats[1] === null;
  }
  connectedCount() {
    return this.seats.filter((s) => s?.client).length;
  }
  // --------------------------------------------------------------------------
  // Membership
  /** Seat a new player. Returns their token, or an error code. */
  join(client, now) {
    if (this.phase !== "lobby") return this.isFull() ? "room_full" : "wrong_phase";
    const slot = this.seats[0] === null ? 0 : this.seats[1] === null ? 1 : -1;
    if (slot === -1) return "room_full";
    const other2 = this.seats[slot === 0 ? 1 : 0];
    let team2 = slot;
    if (other2 && other2.team === team2) team2 = TEAMS.map((t) => t.id).find((t) => t !== other2.team) ?? 0;
    const token = randomHex(16);
    this.seats[slot] = {
      slot,
      token,
      client,
      team: team2,
      kit: slot === 0 ? 0 : 1,
      ready: false,
      aiDefense: false,
      rematch: false,
      disconnectedAt: null,
      queue: [],
      last: { ...NEUTRAL_INPUT },
      lastInputAt: 0,
      ack: 0
    };
    this.lastActive = now;
    this.broadcastRoom(now);
    return { slot, token };
  }
  /** A known player reconnects with their session token. */
  resume(slot, client, now) {
    const seat = this.seats[slot];
    if (!seat) return;
    seat.client = client;
    seat.disconnectedAt = null;
    seat.queue = [];
    seat.last = { ...NEUTRAL_INPUT, actionCount: seat.last.actionCount, switchCount: seat.last.switchCount };
    this.lastActive = now;
    if (this.phase === "match" && this.match) {
      client.send({ t: "start", teams: [this.match.teams[0], this.match.teams[1]], kits: [this.match.kits[0], this.match.kits[1]], you: slot, matchId: this.matchId });
      if (this.paused && this.paused.by === slot) {
        this.pauseUsed[slot] += (now - this.paused.since) / 1e3;
        const otherSlot = slot === 0 ? 1 : 0;
        const otherSeat = this.seats[otherSlot];
        if (otherSeat && !otherSeat.client) {
          this.paused = { by: otherSlot, since: now, deadline: this.pauseDeadline(otherSlot, now) };
        } else {
          this.paused = null;
          this.resumeIn = RESUME_COUNTDOWN_SECONDS;
        }
      }
      this.sendSnapshot(seat, true);
    }
    this.broadcastRoom(now);
  }
  disconnect(slot, now) {
    const seat = this.seats[slot];
    if (!seat || !seat.client) return;
    seat.client = null;
    seat.disconnectedAt = now;
    seat.queue = [];
    if (this.phase === "countdown") {
      this.phase = "lobby";
      seat.ready = false;
    }
    if (this.phase === "match" && !this.paused) {
      this.paused = { by: slot, since: now, deadline: this.pauseDeadline(slot, now) };
      this.resumeIn = 0;
    }
    this.broadcastRoom(now);
  }
  pauseDeadline(slot, now) {
    const budget = Math.max(0, this.cfg.pauseBudgetSeconds - this.pauseUsed[slot]);
    return now + Math.min(this.cfg.graceSeconds, budget) * 1e3;
  }
  /** Player deliberately leaves. In a match this is a forfeit. */
  leave(slot, now) {
    const seat = this.seats[slot];
    if (!seat) return;
    if (this.phase === "match") {
      this.endMatch(slot === 0 ? 1 : 0, "forfeit", now);
    }
    this.seats[slot] = null;
    if (this.phase === "countdown") this.phase = "lobby";
    const other2 = this.seats[slot === 0 ? 1 : 0];
    if (other2) {
      other2.ready = false;
      other2.rematch = false;
    }
    this.broadcastRoom(now);
  }
  // --------------------------------------------------------------------------
  // Lobby actions
  setTeam(slot, team2, now, kit = seat0Kit(slot)) {
    const seat = this.seats[slot];
    if (!seat) return "not_in_room";
    if (this.phase !== "lobby") return "wrong_phase";
    if (!isValidTeamId(team2) || !isValidKit(kit)) return "bad_team";
    const other2 = this.seats[slot === 0 ? 1 : 0];
    if (other2 && other2.team === team2) return "team_taken";
    seat.team = team2;
    seat.kit = kit;
    seat.ready = false;
    this.lastActive = now;
    this.broadcastRoom(now);
    return null;
  }
  setReady(slot, ready, now) {
    const seat = this.seats[slot];
    if (!seat) return "not_in_room";
    if (this.phase !== "lobby" && this.phase !== "countdown") return "wrong_phase";
    if (ready && seat.team === null) return "bad_team";
    seat.ready = ready;
    this.lastActive = now;
    if (!ready && this.phase === "countdown") this.phase = "lobby";
    this.maybeStartCountdown();
    this.broadcastRoom(now);
    return null;
  }
  /** Vote for AI defense. Changing it un-readies both players so nobody starts a match they didn't agree to. */
  setAiDefense(slot, on, now) {
    const seat = this.seats[slot];
    if (!seat) return "not_in_room";
    if (this.phase !== "lobby") return "wrong_phase";
    if (seat.aiDefense === on) return null;
    seat.aiDefense = on;
    for (const s of this.seats) if (s) s.ready = false;
    this.lastActive = now;
    this.broadcastRoom(now);
    return null;
  }
  /** Both players voted for AI defense. */
  get aiDefenseAgreed() {
    return !!(this.seats[0]?.aiDefense && this.seats[1]?.aiDefense);
  }
  requestRematch(slot, now) {
    const seat = this.seats[slot];
    if (!seat) return "not_in_room";
    if (this.phase !== "result") return "wrong_phase";
    if (this.endReason !== "final") return "wrong_phase";
    seat.rematch = true;
    this.lastActive = now;
    const [a, b] = this.seats;
    if (a?.rematch && b?.rematch && a.client && b.client) {
      a.rematch = false;
      b.rematch = false;
      a.ready = true;
      b.ready = true;
      this.phase = "countdown";
      this.countdown = START_COUNTDOWN_SECONDS;
    }
    this.broadcastRoom(now);
    return null;
  }
  maybeStartCountdown() {
    const [a, b] = this.seats;
    if (this.phase === "lobby" && a && b && a.ready && b.ready && a.client && b.client && a.team !== null && b.team !== null && a.team !== b.team) {
      this.phase = "countdown";
      this.countdown = START_COUNTDOWN_SECONDS;
    }
  }
  // --------------------------------------------------------------------------
  // Match actions
  call(slot, id) {
    if (this.phase !== "match" || !this.match) return "wrong_phase";
    if (this.paused || this.resumeIn > 0) return "wrong_phase";
    if (this.match.aiDefense && slot === defTeam(this.match)) return "wrong_phase";
    return submitCall(this.match, slot, id) ? null : "wrong_phase";
  }
  timeout(slot) {
    if (this.phase !== "match" || !this.match || this.paused || this.resumeIn > 0) return "wrong_phase";
    return callTimeout(this.match, slot) ? null : "wrong_phase";
  }
  pickDefender(slot, defSlot) {
    if (this.phase !== "match" || !this.match) return "wrong_phase";
    return selectDefender(this.match, slot, defSlot) ? null : "wrong_phase";
  }
  input(slot, input, now) {
    const seat = this.seats[slot];
    if (!seat || this.phase !== "match") return;
    if (input.seq <= seat.ack || seat.queue.length && input.seq <= seat.queue[seat.queue.length - 1].seq) return;
    seat.queue.push(input);
    while (seat.queue.length > MAX_QUEUE) seat.queue.shift();
    seat.lastInputAt = now;
  }
  // --------------------------------------------------------------------------
  // Clock
  /** Advance the room by one simulation tick. */
  tick(now) {
    if (this.phase === "countdown") {
      const before = Math.ceil(this.countdown);
      this.countdown -= DT;
      if (this.countdown <= 0) this.startMatch(now);
      else if (Math.ceil(this.countdown) !== before) this.broadcastRoom(now);
      return;
    }
    if (this.phase !== "match" || !this.match) return;
    if (this.paused) {
      const bothGone = !this.seats[0]?.client && !this.seats[1]?.client;
      if (now >= this.paused.deadline) {
        if (bothGone) this.endMatch(null, "abandoned", now);
        else this.endMatch(this.paused.by === 0 ? 1 : 0, "forfeit", now);
        return;
      }
      if (now - this.lastRoomBroadcast > 1e3) this.broadcastRoom(now);
      return;
    }
    if (this.resumeIn > 0) {
      const before = Math.ceil(this.resumeIn);
      this.resumeIn = Math.max(0, this.resumeIn - DT);
      if (Math.ceil(this.resumeIn) !== before) this.broadcastRoom(now);
      return;
    }
    const inputs = [this.nextInput(0, now), this.nextInput(1, now)];
    if (this.match.aiDefense && this.aiBrains) {
      const d = defTeam(this.match);
      const brain = this.aiBrains[d];
      const c = brain.pickCall(this.match) ?? brain.presnapAudible(this.match);
      if (c) submitCall(this.match, d, c);
      inputs[d] = brain.input(this.match);
    }
    stepMatch(this.match, inputs);
    for (const seat of this.seats) if (seat?.client) this.sendSnapshot(seat, this.match.phase === "final");
    if (this.match.phase === "final") this.endMatch(this.match.winner, "final", now);
  }
  nextInput(slot, now) {
    const seat = this.seats[slot];
    if (!seat) return { ...NEUTRAL_INPUT };
    const next = seat.queue.shift();
    if (next) {
      seat.last = next;
      seat.ack = next.seq;
      return next;
    }
    if (now - seat.lastInputAt > this.cfg.inputStaleMs) {
      seat.last = { ...seat.last, mx: 0, my: 0, throwHeld: false };
    }
    return seat.last;
  }
  sendSnapshot(seat, includeStats) {
    if (!this.match || !seat.client) return;
    if (seat.client.congested?.()) return;
    const snap2 = buildSnapshot(this.match, seat.slot, seat.ack);
    seat.client.send({ t: "s", s: snapshotToWire(snap2, includeStats || this.match.tick % 30 === 0) });
  }
  startMatch(now) {
    const [a, b] = this.seats;
    if (!a || !b || a.team === null || b.team === null || this.phase !== "countdown") {
      this.phase = "lobby";
      this.broadcastRoom(now);
      return;
    }
    this.matchId += 1;
    const kits = resolveKits([a.team, b.team], [a.kit, b.kit]);
    this.match = createMatch({ teams: [a.team, b.team], kits, seed: randomInt(0, 2 ** 31), halfSeconds: this.cfg.halfSeconds, maxAudibles: [2, 2], friendly: true });
    this.match.aiDefense = this.aiDefenseAgreed;
    this.aiBrains = this.match.aiDefense ? [new CpuBrain(0, { skill: 0.7 }, randomInt(1, 1e9)), new CpuBrain(1, { skill: 0.7 }, randomInt(1, 1e9))] : null;
    this.phase = "match";
    this.paused = null;
    this.resumeIn = 0;
    this.pauseUsed = [0, 0];
    this.endReason = null;
    for (const s of [a, b]) {
      s.queue = [];
      s.ack = 0;
      s.last = { ...NEUTRAL_INPUT };
      s.lastInputAt = now;
      s.rematch = false;
      s.ready = false;
      s.client?.send({ t: "start", teams: [a.team, b.team], kits, you: s.slot, matchId: this.matchId });
    }
    this.lastActive = now;
    this.broadcastRoom(now);
  }
  endReason = null;
  endMatch(winner, reason, now) {
    if (this.phase !== "match") return;
    this.phase = "result";
    this.endReason = reason;
    this.endedAt = now;
    this.paused = null;
    this.resumeIn = 0;
    const m = this.match;
    const score = m ? [m.score[0], m.score[1]] : [0, 0];
    const stats = m ? m.stats : [emptyStats2(), emptyStats2()];
    for (const s of this.seats) s?.client?.send({ t: "end", winner, score, reason, stats });
    this.broadcastRoom(now);
  }
  // --------------------------------------------------------------------------
  // Info
  info(now) {
    const lp = (slot) => {
      const s = this.seats[slot];
      return {
        slot,
        team: s?.team ?? null,
        kit: s?.kit ?? 0,
        ready: s?.ready ?? false,
        aiDefense: s?.aiDefense ?? false,
        connected: !!s?.client,
        rematch: s?.rematch ?? false,
        present: !!s
      };
    };
    return {
      code: this.code,
      phase: this.phase,
      players: [lp(0), lp(1)],
      countdown: Math.max(0, this.countdown),
      aiDefense: this.aiDefenseAgreed,
      paused: this.paused ? { by: this.paused.by, secondsLeft: Math.max(0, (this.paused.deadline - now) / 1e3) } : null,
      resumeIn: this.resumeIn
    };
  }
  broadcastRoom(now) {
    this.lastRoomBroadcast = now;
    const room = this.info(now);
    for (const s of this.seats) s?.client?.send({ t: "room", room, you: s.slot });
  }
  /** Housekeeping. Returns true if the room should be deleted. */
  sweep(now) {
    const graceMs = this.cfg.graceSeconds * 1e3;
    if (this.phase !== "match") {
      for (const slot of [0, 1]) {
        const s = this.seats[slot];
        if (s && !s.client && s.disconnectedAt !== null && now - s.disconnectedAt > graceMs) {
          this.seats[slot] = null;
          const other2 = this.seats[slot === 0 ? 1 : 0];
          if (other2) {
            other2.ready = false;
            other2.rematch = false;
          }
          if (this.phase === "countdown") this.phase = "lobby";
          this.broadcastRoom(now);
        }
      }
    }
    if (this.isEmpty()) return true;
    if (this.connectedCount() === 0 && this.phase !== "match" && now - this.lastActive > graceMs) return true;
    if (now - this.lastActive > this.cfg.lobbyIdleSeconds * 1e3 && this.phase !== "match") return true;
    if (this.phase === "result" && now - this.endedAt > 10 * 60 * 1e3) return true;
    return false;
  }
  tokenFor(slot) {
    return this.seats[slot]?.token ?? null;
  }
  clientFor(slot) {
    return this.seats[slot]?.client ?? null;
  }
};
function emptyStats2() {
  return { passYds: 0, rushYds: 0, completions: 0, attempts: 0, ints: 0, sacks: 0, tds: 0, perfectThrows: 0, perfectJukes: 0, fgMade: 0, fgAtt: 0 };
}
function makeRoomCode(exists) {
  for (let i = 0; i < 50; i++) {
    let c = "";
    for (let k = 0; k < ROOM_CODE_LENGTH; k++) c += ROOM_CODE_ALPHABET[randomInt(0, ROOM_CODE_ALPHABET.length)];
    if (!exists(c)) return c;
  }
  throw new Error("Could not allocate a room code");
}
var RoomManager = class {
  constructor(cfg = {}, maxRooms = 1e3) {
    this.cfg = cfg;
    this.maxRooms = maxRooms;
  }
  cfg;
  maxRooms;
  rooms = /* @__PURE__ */ new Map();
  create(now) {
    if (this.rooms.size >= this.maxRooms) return null;
    const code = makeRoomCode((c) => this.rooms.has(c));
    const room = new Room(code, now, this.cfg);
    this.rooms.set(code, room);
    return room;
  }
  get(code) {
    return this.rooms.get(code);
  }
  /** Find the room & seat for a session token. */
  byToken(token) {
    for (const room of this.rooms.values()) {
      for (const slot of [0, 1]) if (room.tokenFor(slot) === token) return { room, slot };
    }
    return null;
  }
  tick(now) {
    for (const r3 of this.rooms.values()) r3.tick(now);
  }
  sweep(now) {
    for (const [code, r3] of this.rooms) if (r3.sweep(now)) this.rooms.delete(code);
  }
  stats() {
    let players = 0;
    let matches = 0;
    for (const r3 of this.rooms.values()) {
      players += r3.connectedCount();
      if (r3.phase === "match") matches++;
    }
    return { rooms: this.rooms.size, players, matches };
  }
};
function seat0Kit(slot) {
  return slot === 0 ? 0 : 1;
}

// src/shared/hub.ts
var BUCKET_MAX = 120;
var BUCKET_REFILL = 60;
var MAX_DROPPED = 600;
var Hub = class {
  manager;
  nextId = 1;
  log;
  loop = null;
  sweeper = null;
  constructor(opts = {}) {
    this.manager = new RoomManager(opts.room ?? {}, opts.maxRooms ?? 1e3);
    this.log = opts.log ?? (() => {
    });
  }
  /**
   * A player connects. `send` delivers a message to them; `kick` closes their
   * connection (rate-limit abuse); `congested` says they can't keep up.
   */
  connect(send, kick = () => {
  }, congested) {
    const conn = {
      id: this.nextId++,
      room: null,
      slot: null,
      tokens: BUCKET_MAX,
      dropped: 0,
      lastRefill: Date.now(),
      send,
      congested,
      kick
    };
    return {
      receive: (raw) => this.handleRaw(conn, raw),
      closed: () => this.detach(conn, Date.now(), false)
    };
  }
  err(conn, code) {
    conn.send({ t: "err", code, msg: ERROR_TEXT[code] });
  }
  rateOk(conn) {
    const now = Date.now();
    conn.tokens = Math.min(BUCKET_MAX, conn.tokens + (now - conn.lastRefill) / 1e3 * BUCKET_REFILL);
    conn.lastRefill = now;
    if (conn.tokens >= 1) {
      conn.tokens -= 1;
      return true;
    }
    conn.dropped += 1;
    if (conn.dropped === 1 || conn.dropped % 100 === 0) this.err(conn, "rate_limited");
    if (conn.dropped > MAX_DROPPED) conn.kick();
    return false;
  }
  handleRaw(conn, raw) {
    if (!this.rateOk(conn)) return;
    const msg = parseClientMsg(raw);
    if (!msg) return this.err(conn, "bad_msg");
    try {
      this.handle(conn, msg);
    } catch (e) {
      this.log("handler error", e);
      this.err(conn, "bad_msg");
    }
  }
  bind(conn, room, slot) {
    conn.room = room;
    conn.slot = slot;
  }
  detach(conn, now, leaving) {
    if (conn.room && conn.slot !== null) {
      const room = conn.room;
      const slot = conn.slot;
      if (room.clientFor(slot) === conn) {
        if (leaving) room.leave(slot, now);
        else room.disconnect(slot, now);
      }
    }
    conn.room = null;
    conn.slot = null;
  }
  handle(conn, msg) {
    const now = Date.now();
    const manager = this.manager;
    switch (msg.t) {
      case "hello":
        if (msg.v !== PROTOCOL_VERSION) return this.err(conn, "bad_version");
        conn.send({ t: "welcome", v: PROTOCOL_VERSION });
        return;
      case "ping":
        conn.send({ t: "pong", c: msg.c, s: now });
        return;
      case "create": {
        this.detach(conn, now, true);
        const room2 = manager.create(now);
        if (!room2) return this.err(conn, "server_full");
        const r3 = room2.join(conn, now);
        if (typeof r3 === "string") return this.err(conn, r3);
        this.bind(conn, room2, r3.slot);
        conn.send({ t: "joined", code: room2.code, token: r3.token, slot: r3.slot, resumed: false });
        room2.broadcastRoom(now);
        this.log(`room ${room2.code} created`);
        return;
      }
      case "join": {
        const room2 = manager.get(msg.code);
        if (!room2) return this.err(conn, "no_room");
        if (conn.room === room2) return;
        if (room2.isFull()) return this.err(conn, "room_full");
        this.detach(conn, now, true);
        const r3 = room2.join(conn, now);
        if (typeof r3 === "string") return this.err(conn, r3);
        this.bind(conn, room2, r3.slot);
        conn.send({ t: "joined", code: room2.code, token: r3.token, slot: r3.slot, resumed: false });
        room2.broadcastRoom(now);
        this.log(`room ${room2.code} joined`);
        return;
      }
      case "resume": {
        const found = manager.byToken(msg.token);
        if (!found) return this.err(conn, "bad_token");
        const { room: room2, slot: slot2 } = found;
        const prev = room2.clientFor(slot2);
        if (prev && prev !== conn) {
          prev.room = null;
          prev.slot = null;
          prev.kick();
        }
        this.bind(conn, room2, slot2);
        conn.send({ t: "joined", code: room2.code, token: msg.token, slot: slot2, resumed: true });
        room2.resume(slot2, conn, now);
        this.log(`room ${room2.code} slot ${slot2} resumed`);
        return;
      }
      default:
        break;
    }
    const room = conn.room;
    const slot = conn.slot;
    if (!room || slot === null || room.clientFor(slot) !== conn) return this.err(conn, "not_in_room");
    let e = null;
    switch (msg.t) {
      case "team":
        e = room.setTeam(slot, msg.team, now, msg.kit);
        break;
      case "ready":
        e = room.setReady(slot, msg.ready, now);
        break;
      case "call":
        e = room.call(slot, msg.id);
        break;
      case "pick":
        e = room.pickDefender(slot, msg.slot);
        break;
      case "timeout":
        e = room.timeout(slot);
        break;
      case "opt":
        e = room.setAiDefense(slot, msg.aiDefense, now);
        break;
      case "in":
        room.input(slot, wireToInput(msg.i), now);
        break;
      case "rematch":
        e = room.requestRematch(slot, now);
        break;
      case "leave":
        this.detach(conn, now, true);
        break;
    }
    if (e && e !== "wrong_phase") this.err(conn, e);
  }
  /** Run the fixed-rate game loop (with drift compensation) and the room sweeper. */
  start() {
    if (this.loop) return;
    let last = performance.now();
    let acc = 0;
    this.loop = setInterval(() => {
      const t = performance.now();
      acc += t - last;
      last = t;
      let steps = 0;
      while (acc >= DT * 1e3 && steps < 5) {
        acc -= DT * 1e3;
        steps++;
        this.manager.tick(Date.now());
      }
      if (acc > DT * 1e3 * 5) acc = 0;
    }, 1e3 / TICK_RATE / 2);
    this.sweeper = setInterval(() => this.manager.sweep(Date.now()), 1e3);
  }
  stop() {
    if (this.loop) clearInterval(this.loop);
    if (this.sweeper) clearInterval(this.sweeper);
    this.loop = null;
    this.sweeper = null;
  }
};

// src/server/static.ts
import { createReadStream, statSync } from "node:fs";
import { extname, join, normalize, resolve, sep } from "node:path";
var TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".ico": "image/x-icon",
  ".json": "application/json",
  ".woff2": "font/woff2",
  ".woff": "font/woff",
  ".webmanifest": "application/manifest+json",
  ".txt": "text/plain; charset=utf-8"
};
function serveStatic(rootDir, req, res) {
  if (req.method !== "GET" && req.method !== "HEAD") {
    res.writeHead(405);
    res.end();
    return;
  }
  const root = resolve(rootDir);
  let pathname;
  try {
    pathname = decodeURIComponent(new URL(req.url ?? "/", "http://x").pathname);
  } catch {
    res.writeHead(400);
    res.end();
    return;
  }
  const rel = normalize(pathname).replace(/^([/\\])+/, "");
  let file = join(root, rel);
  if (!file.startsWith(root + sep) && file !== root) {
    res.writeHead(403);
    res.end();
    return;
  }
  let st = safeStat(file);
  if (st?.isDirectory()) {
    file = join(file, "index.html");
    st = safeStat(file);
  }
  if (!st) {
    file = join(root, "index.html");
    st = safeStat(file);
    if (!st) {
      res.writeHead(404, { "content-type": "text/plain" });
      res.end("Client not built. Run: npm run build");
      return;
    }
  }
  const type = TYPES[extname(file)] ?? "application/octet-stream";
  const immutable = file.includes(`${sep}assets${sep}`);
  res.writeHead(200, {
    "content-type": type,
    "content-length": st.size,
    "cache-control": immutable ? "public, max-age=31536000, immutable" : "no-cache",
    "x-content-type-options": "nosniff"
  });
  if (req.method === "HEAD") {
    res.end();
    return;
  }
  createReadStream(file).pipe(res);
}
function safeStat(p) {
  try {
    return statSync(p);
  } catch {
    return null;
  }
}

// src/server/gateway.ts
function createGameServer(opts) {
  const log = opts.log ?? (() => {
  });
  const hub = new Hub({ room: opts.room, maxRooms: opts.maxRooms, log });
  const manager = hub.manager;
  const startedAt = Date.now();
  const http = createServer((req, res) => {
    const url = new URL(req.url ?? "/", "http://x");
    if (url.pathname === "/healthz") {
      res.writeHead(200, { "content-type": "application/json", "cache-control": "no-store", "access-control-allow-origin": "*" });
      res.end(JSON.stringify({ ok: true, uptime: Math.round((Date.now() - startedAt) / 1e3), ...manager.stats() }));
      return;
    }
    if (opts.staticDir) {
      serveStatic(opts.staticDir, req, res);
      return;
    }
    res.writeHead(404, { "content-type": "text/plain" });
    res.end("No Huddle game server. The web client is served separately in development (npm run dev).");
  });
  const wss = new import_websocket_server.default({ noServer: true, maxPayload: MAX_MESSAGE_BYTES });
  http.on("upgrade", (req, socket, head) => {
    const url = new URL(req.url ?? "/", "http://x");
    if (url.pathname !== "/ws") {
      socket.destroy();
      return;
    }
    const origin = req.headers.origin;
    if (opts.allowedOrigins && opts.allowedOrigins.length > 0 && (!origin || !opts.allowedOrigins.includes(origin))) {
      log("rejected origin", origin);
      socket.write("HTTP/1.1 403 Forbidden\r\n\r\n");
      socket.destroy();
      return;
    }
    wss.handleUpgrade(req, socket, head, (ws) => onConnection(ws));
  });
  const delay = opts.simLatencyMs ?? 0;
  const later = (fn) => delay > 0 ? setTimeout(fn, delay) : fn();
  function onConnection(ws) {
    const conn = hub.connect(
      (msg) => {
        const data = JSON.stringify(msg);
        later(() => {
          if (ws.readyState === import_websocket.default.OPEN) ws.send(data);
        });
      },
      () => ws.close(4e3, "closed by server"),
      () => ws.bufferedAmount > 256 * 1024
    );
    ws.on("message", (data, isBinary) => {
      const raw = isBinary ? "" : data.toString();
      later(() => conn.receive(raw));
    });
    ws.on("close", () => later(() => conn.closed()));
    ws.on("error", () => {
    });
  }
  hub.start();
  return new Promise((resolve2) => {
    http.listen(opts.port, opts.host ?? "0.0.0.0", () => {
      resolve2({
        http,
        manager,
        port: () => {
          const a = http.address();
          return typeof a === "object" && a ? a.port : opts.port;
        },
        close: () => new Promise((done) => {
          hub.stop();
          for (const c of wss.clients) c.terminate();
          wss.close();
          http.close(() => done());
        })
      });
    });
  });
}

// src/server/index.ts
var env = process.env;
var num = (v, d) => v !== void 0 && v !== "" && Number.isFinite(Number(v)) ? Number(v) : d;
var here = dirname(fileURLToPath(import.meta.url));
var builtClient = join2(here, "..", "client");
var staticDir = env.STATIC_DIR ?? (existsSync(join2(builtClient, "index.html")) ? builtClient : null);
var port = num(env.PORT, num(env.SERVER_PORT, 8787));
var allowedOrigins = (env.ALLOWED_ORIGINS ?? "").split(",").map((s) => s.trim()).filter(Boolean);
var server = await createGameServer({
  port,
  host: env.HOST ?? "0.0.0.0",
  staticDir,
  allowedOrigins,
  maxRooms: num(env.MAX_ROOMS, 1e3),
  simLatencyMs: num(env.SIM_LATENCY_MS, 0),
  room: {
    halfSeconds: num(env.MATCH_HALF_SECONDS, 120),
    graceSeconds: num(env.RECONNECT_GRACE_SECONDS, 20)
  },
  log: (...a) => console.log((/* @__PURE__ */ new Date()).toISOString(), ...a)
});
console.log(`No Huddle server listening on :${server.port()}`);
console.log(staticDir ? `Serving web client from ${staticDir}` : "No built client found; run the client with Vite (npm run dev).");
if (allowedOrigins.length) console.log(`Allowed origins: ${allowedOrigins.join(", ")}`);
if (num(env.SIM_LATENCY_MS, 0) > 0) console.log(`Simulating ${env.SIM_LATENCY_MS} ms one-way latency`);
var shutdown = () => {
  console.log("Shutting down\u2026");
  server.close().then(() => process.exit(0));
  setTimeout(() => process.exit(0), 3e3).unref();
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
