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
  swiftEnsureAccess(opt, function(err) {
    if (err) throw err;
    if (typeof cb === 'function') cb();
  });
}

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
  swiftEnsureAccess(opt, function(err) {
    if (err) throw err;
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
  swiftEnsureAccess(opt, function(err) {
    if (err) throw err;
    var container = options.container ? ('/' + options.container) : '';
    var objectUrl = opt.serviceUrl + container + '/' + options.remote;
    var headers = options.headers || {};
    headers['X-Auth-Token'] = opt.access.token.id;
    var reqOpt = { url:objectUrl, headers:headers };
    request.get(reqOpt, cb);
  });
};

Swift.prototype.setAccountMetadata = function(headers, cb) {
  var opt = this.opt;
  swiftEnsureAccess(opt, function(err) {
    if (err) throw err;
    headers['X-Auth-Token'] = opt.access.token.id;
    var reqOpt = { url:opt.serviceUrl, headers:headers };
    request.post(reqOpt, finish(cb));
  });
};

Swift.prototype.setContainerMetadata = function(container, headers, cb) {
  var opt = this.opt;
  swiftEnsureAccess(opt, function(err) {
    if (err) throw err;
    var objectUrl = opt.serviceUrl + '/' + container + '/';
    headers['X-Auth-Token'] = opt.access.token.id;
    var reqOpt = { url:objectUrl, headers:headers };
    request.post(reqOpt, finish(cb));
  });
};

Swift.prototype.getContainerMetadata = function(container, cb) {
  var opt = this.opt;
  swiftEnsureAccess(opt, function(err) {
    if (err) throw err;
    var objectUrl = opt.serviceUrl + '/' + container + '/';
    var headers = {};
    headers['X-Auth-Token'] = opt.access.token.id;
    var reqOpt = { url:objectUrl, headers:headers };
    request.head(reqOpt, function(err, res){
      if (typeof cb === 'function') {
        var code, headers;
        if (res) {
          code = res.statusCode;
          headers = res.headers;
        }
        cb(err, headers, code);
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

Swift.prototype.createContainer = function swiftCreateContainer(options, cb) {
  var opt = this.opt;
  swiftEnsureAccess(opt, function(err) {
    if (err) throw err;
    var objectUrl = opt.serviceUrl + '/' + options.container;
    var headers = options.headers || {};
    headers['X-Auth-Token'] = opt.access.token.id;
    var reqOpt = { url:objectUrl, headers:headers };
    request.put(reqOpt, finish(cb));
  });
};

Swift.prototype.removeFile = function(container, file, cb) {
  var opt = this.opt;
  swiftEnsureAccess(opt, function(err) {
    if (err) throw err;
    var objectUrl = opt.serviceUrl + '/' + container + '/' + file;
    var headers = {};
    headers['X-Auth-Token'] = opt.access.token.id;
    var reqOpt = { url:objectUrl, headers:headers };
    request.del(reqOpt, finish(cb));
  });
};

Swift.prototype.copyFile = function(srcContainer, srcFile, destContainer, destFile, cb) {
  var opt = this.opt;
  swiftEnsureAccess(opt, function(err) {
    if (err) throw err;
    var destObjectUrl = opt.serviceUrl + '/' + destContainer + '/' + destFile;
    var headers = {};
    headers['X-Auth-Token'] = opt.access.token.id;
    headers['X-Copy-From'] = '/' + srcContainer + '/' + srcFile;
    var reqOpt = { url:destObjectUrl, headers:headers };
    request.put(reqOpt, finish(cb));
  });
};

Swift.prototype.listFiles = function(container, cb) {
  var opt = this.opt;
  swiftEnsureAccess(opt, function(err) {
    if (err) throw err;
    var objectUrl = opt.serviceUrl + '/' + container;
    var headers = {};
    headers['X-Auth-Token'] = opt.access.token.id;
    var reqOpt = { url:objectUrl, headers:headers };
    request.get(reqOpt, function(err, res, body){
      if (typeof cb === 'function') {
        var files, code;
        if (body) files = body.split('\n');
        if (res) code = res.statusCode;
        cb(err, files, code);
      }
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
  var tokenValid = false;
  if (opt.access.token) {
    var ed = new Date(opt.access.token.expires);
    ed.setHours(ed.getHours()-2);
    if (ed > new Date()) tokenValid = true;
  }
  if (tokenValid) {
    cb();
  } else {
    request.post( 
      { url: opt.authUrl + 'tokens',
        headers: {'accept': 'application/json'},
        json: opt.auth 
      }, 
      function(e, r, body) {
        if (e) {
          cb(e);
        } else if (r.statusCode === 200 && body && body.access) {
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
          cb(new Error('Unknown Error'), r);
        }
      }
    );
  }
}

exports.createClient = function(options, cb){
  return new Swift(options, cb);
};

