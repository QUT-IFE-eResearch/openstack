"use strict";

var request = require('request');
var crypto = require('crypto');
var fs = require('fs');
var url = require('url');
/*
var client = require('openstack').createClient()
*/
function Swift(options, cb) {
  var opt = this.opt = {};
  opt.auth = { 
    "auth": { "passwordCredentials": { "username": options.username, "password": options.password },
      "tenantName": options.tenantName }
  };
  opt.authUrl = options.url;
  opt.tenantId = options.tenantId;
  opt.serviceName = options.serviceName;
  opt.access = {};
  swiftEnsureAccess(opt, function() {
    if (typeof cb === 'function') cb();
  });
}

function noop(){}

function finish(cb, url) {
  if (typeof cb === 'function') {
    return function finish(err, res) {
      //console.log(res.statusCode);
      var code;
      if (res) code = res.statusCode;
      cb(err, code, url);
    };
  }
}
Object.defineProperties(Swift.prototype, {
  serviceUrl : { enumerable:true, get: function(){ return this.opt.serviceUrl; } }
});

Swift.prototype.upload = function swiftUpload(options, cb) {
  var opt = this.opt;
  swiftEnsureAccess(opt, function() {
    var container = options.container ? ('/' + options.container) : '';
    var objectUrl = opt.serviceUrl + container + '/' + options.remote;
    var headers = options.headers || {};
    headers['X-Auth-Token'] = opt.access.token.id;
    var reqOpt = { url:objectUrl, headers:headers };
    var streamObj = options.stream;
    if (options.local) {
      streamObj = fs.createReadStream(options.local);
    }
    if (streamObj) {
      streamObj.pipe(request.put(reqOpt, finish(cb, objectUrl)));
    } else {
      reqOpt.body = options.body;
      request.put(reqOpt, finish(cb, objectUrl));
    }
  });
};

Swift.prototype.download = function swiftDownload(options, cb) {
  var opt = this.opt;
  swiftEnsureAccess(opt, function() {
    var container = options.container ? ('/' + options.container) : '';
    var objectUrl = opt.serviceUrl + container + '/' + options.remote;
    var headers = options.headers || {};
    headers['X-Auth-Token'] = opt.access.token.id;
    var reqOpt = { url:objectUrl, headers:headers };
    request.get(reqOpt, cb);
  });
};

Swift.prototype.getAccountMetadata = function(cb) {
  var opt = this.opt;
  swiftEnsureAccess(opt, function(headers, statusCode) {
    if (headers) {
      return cb(null, headers, statusCode);
    } else {
      var reqOpt = { url:opt.serviceUrl, headers:{'X-Auth-Token':opt.access.token.id} };
      request.head(reqOpt, function(err, res) {
        if (typeof cb === 'function') {
          res = res || {};
          cb(err, res.headers, res.statusCode);
        }
      });
    }
  });
};

Swift.prototype.setAccountMetadata = function(headers, cb) {
  var opt = this.opt;
  swiftEnsureAccess(opt, function() {
    headers['X-Auth-Token'] = opt.access.token.id;
    var reqOpt = { url:opt.serviceUrl, headers:headers };
    request.post(reqOpt, finish(cb));
  });
};

Swift.prototype.setContainerMetadata = function(container, headers, cb) {
  var opt = this.opt;
  swiftEnsureAccess(opt, function() {
    var objectUrl = opt.serviceUrl + '/' + container + '/';
    headers['X-Auth-Token'] = opt.access.token.id;
    var reqOpt = { url:objectUrl, headers:headers };
    request.post(reqOpt, finish(cb));
  });
};

Swift.prototype.getContainerMetadata = function(container, cb) {
  var opt = this.opt;
  swiftEnsureAccess(opt, function() {
    var objectUrl = opt.serviceUrl + '/' + container + '/';
    var headers = {};
    headers['X-Auth-Token'] = opt.access.token.id;
    var reqOpt = { url:objectUrl, headers:headers };
    request.head(reqOpt, function(err, res){
      if (typeof cb === 'function') {
        res = res || {};
        cb(err, res.headers, res.statusCode);
      }
    });
  });
};
// set public read: '.r:*'
Swift.prototype.setMetaReadAcl = function swiftSetMetaReadAcl(container, acl, cb) {
  this.setContainerMetadata(container, {"X-Container-Read":acl}, cb); 
};
Swift.prototype.setMetaVersionsLocation = function swiftSetMetaVersionsLocation(container, versionsContainer, cb) {
  this.setContainerMetadata(container, {"X-Versions-Location":versionsContainer}, cb); 
};
Swift.prototype.setMetaTempUrl = function(key, cb) {
  this.setAccountMetadata({"X-Account-Meta-Temp-URL-Key":key}, cb); 
};

Swift.prototype.listContainers = function swiftListContainer(options, cb) {
  var opt = this.opt;
  if (typeof cb !== 'function') {
    if (typeof options === 'function') {
      cb = options;
      options = {};
    } else {
      cb = noop;
    }
  }
  options.format = options.format || 'json';
  swiftEnsureAccess(opt, function() {
    var objectUrl = opt.serviceUrl + '?format=' + options.format;
    if (options.limit) objectUrl += ('&limit=' + options.limit);
    if (options.marker) objectUrl += ('&marker=' + options.marker);
    if (options.end_marker) objectUrl += ('&end_marker=' + options.end_marker);
    var headers = options.headers || {};
    headers['X-Auth-Token'] = opt.access.token.id;
    var reqOpt = { url:objectUrl, headers:headers };
    request.get(reqOpt, function(err, res, body){
      res = res || {};
      var result = body;
      if (options.format === 'json') result = JSON.parse(body);
      cb(err, result, res.statusCode);
    });
  });
};

Swift.prototype.createContainer = function swiftCreateContainer(options, cb) {
  var opt = this.opt;
  swiftEnsureAccess(opt, function() {
    var objectUrl = opt.serviceUrl + '/' + options.container;
    var headers = options.headers || {};
    headers['X-Auth-Token'] = opt.access.token.id;
    var reqOpt = { url:objectUrl, headers:headers };
    request.put(reqOpt, finish(cb));
  });
};

Swift.prototype.removeFile = function(container, file, cb) {
  var opt = this.opt;
  swiftEnsureAccess(opt, function() {
    var objectUrl = opt.serviceUrl + '/' + container + '/' + file;
    var headers = {};
    headers['X-Auth-Token'] = opt.access.token.id;
    var reqOpt = { url:objectUrl, headers:headers };
    request.del(reqOpt, finish(cb));
  });
};

Swift.prototype.removeAllFiles = function(container, cb) {
  var opt = this.opt;
  if (typeof cb !== 'function') cb = noop;
  swiftEnsureAccess(opt, function() {
    var objectUrl = opt.serviceUrl + '/' + container;
    var headers = {};
    headers['X-Auth-Token'] = opt.access.token.id;
    headers['Content-Type'] = 'text/plain';
    var reqOpt = { url:objectUrl, headers:headers };
    request.get(reqOpt, function(err, res, body){
      if (err) return cb(err);
      var files = [];
      if (body) files = body.split('\n');
      if (res.statusCode < 300) {
        files.forEach(function(file) {
          reqOpt.url = objectUrl + '/' + file;
          request.del(reqOpt, function(err, res, body){});
        });
        cb(null);
      }
    });
  });
};

Swift.prototype.copyFile = function(srcContainer, srcFile, destContainer, destFile, cb) {
  var opt = this.opt;
  swiftEnsureAccess(opt, function() {
    var destObjectUrl = opt.serviceUrl + '/' + destContainer + '/' + destFile;
    var headers = {};
    headers['X-Auth-Token'] = opt.access.token.id;
    headers['X-Copy-From'] = '/' + srcContainer + '/' + srcFile;
    var reqOpt = { url:destObjectUrl, headers:headers };
    request.put(reqOpt, finish(cb));
  });
};

Swift.prototype.listFiles = function(container, options, cb) {
  var opt = this.opt;
  if (typeof cb !== 'function') {
    if (typeof options === 'function') {
      cb = options;
      options = {};
    } else {
      cb = noop;
    }
  }
  options.format = options.format || 'json';
  swiftEnsureAccess(opt, function() {
    var objectUrl = opt.serviceUrl + '/' + container + '?format=' + options.format;
    if (options.limit) objectUrl += ('&limit=' + options.limit);
    if (options.marker) objectUrl += ('&marker=' + options.marker);
    if (options.end_marker) objectUrl += ('&end_marker=' + options.end_marker);
    if (options.prefix) objectUrl += ('&prefix=' + options.prefix);
    if (options.delimiter) objectUrl += ('&delimiter=' + options.delimiter);
    if (options.path) objectUrl += ('&path=' + options.path);
    var headers = {};
    headers['X-Auth-Token'] = opt.access.token.id;
    var reqOpt = { url:objectUrl, headers:headers };
    request.get(reqOpt, function(err, res, body){
      res = res || {};
      var result = body;
      if (options.format === 'json') result = JSON.parse(body);
      //if (body) files = body.split('\n');
      cb(err, result, res.statusCode);
    });
  });
};


/** 
method
duration 
expires:unix timestamp in milliseconds
container
remote
key
 */
Swift.prototype.createTempUrl = function(options) {
  var duration = options.duration || 1;
  var expires = options.expires || (~~(Date.now()/1000) + duration);
  //console.log(duration);
  //console.log(expires);
  var pathSuffix = '/' + options.container + '/' + options.remote;
  var urlpath = url.parse(this.opt.serviceUrl).path + pathSuffix;
  var hmacBody = options.method.toUpperCase() + '\n' + expires + '\n' + urlpath;
  var sig = crypto.createHmac('sha1', options.key).update(hmacBody).digest('hex');
  var tempurl = this.opt.serviceUrl + pathSuffix + '?temp_url_sig=' + sig + '&temp_url_expires=' + expires;
  if (options.filename) tempurl += ('&filename=' + options.filename);
  return tempurl;
};

function swiftEnsureAccess(opt, cb) {
  if (opt.access.token) {
    //Checking the token expiry time has proven unreliable because the storage server is not guaranteed to respect that.
    //A token can be invalidated at will by the server. Therefore, we always check token validity by a HEAD request for account metadata.
/* 
    var tokenValid = false;
    var ed = new Date(opt.access.token.expires);
    ed.setHours(ed.getHours()-2);
    if (ed > new Date()) tokenValid = true;
*/
    var reqOpt = { url:opt.serviceUrl, headers:{'X-Auth-Token':opt.access.token.id} };
    request.head(reqOpt, function(err, res) {
      if (err) throw err;
      if (res.statusCode == 401) getToken();
      else if (res.statusCode < 300) cb(res.headers, res.statusCode);
      else throw new Error('Unknown Error when accessing ' + reqOpt.url + ': HTTP' + res.statusCode);
    });
  } else {
    getToken();
  }
  function getToken() {
    request.post( 
      { url: opt.authUrl + 'tokens',
        headers: {'accept': 'application/json'},
        json: opt.auth 
      }, 
      function(e, r, body) {
        if (e) throw e;
        if (r.statusCode == 200 && body && body.access) {
          opt.access = body.access;
          var serviceExists = opt.access.serviceCatalog.some(function(cat) {
            if (cat.name === opt.serviceName) {
              try {
                var regionExists = cat.endpoints.some(function(ep){
                  if (ep.region === opt.region) {
                    opt.serviceUrl = ep.internalURL;
                    return true;
                  }
                });
                if (!regionExists) opt.serviceUrl = cat.endpoints[0].internalURL;
                return true;
              } catch (e) {
              }
            }
          });
          if (!serviceExists) throw new Error('Service not found');
          cb();
        } else {
          throw new Error('Unknown Error when getting token: HTTP' + r.statusCode);
        }
      }
    );
  }
}

exports.createClient = function(options, cb){
  return new Swift(options, cb);
};

