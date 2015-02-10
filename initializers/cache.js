var fs = require('fs');

var cache = function(api, next){

  api.cache = {};
  api.cache.redisPrefix  = api.config.general.cachePrefix;
  api.cache.lockPrefix   = api.config.general.lockPrefix;
  api.cache.lockDuration = api.config.general.lockDuration;
  api.cache.useLocking   = api.config.general.useLocks;
  api.cache.lockName     = api.id;
  api.cache.lockRetry    = 100;

  api.cache._start = function(api, callback){
    if ( !api.config.general.developmentMode ) {
      // do not run this on startup in the production environment, as KEYS may ruin redis performance:
      //
      // http://redis.io/commands/KEYS
      //
      // "Warning: consider KEYS as a command that should only be used in production environments with
      // extreme care. It may ruin performance when it is executed against large databases. This
      // command is intended for debugging and special operations, such as changing your keyspace
      // layout. Don't use KEYS in your regular application code. If you're looking for a way to find
      // keys in a subset of your keyspace, consider using SCAN or sets.
      callback();
      return;
    }

    api.cache.size(function(err, count){
      if(err){
        api.log('error connecting to the cache: ' + String(err), 'fatal');
      }
      api.log('connected to the cache with ' + count + ' existing objects', 'debug');
      callback();
    });
  }

  api.cache.keys = function(next){
    api.redis.client.keys(api.cache.redisPrefix + '*', function(err, keys){
      next(err, keys);
    });
  }

  api.cache.locks = function(next){
    api.redis.client.keys(api.cache.lockPrefix + '*', function(err, keys){
      next(err, keys);
    });
  }
 
  api.cache.size = function(next){
    api.cache.keys(function(err, keys){
      var length = 0;
      if(keys){
        length = keys.length;
      }
      next(null, length);
    });
  }

  api.cache.clear = function(next){
    api.cache.keys(function(err, keys){
      if(keys.length > 0){
        var stared = 0;
        keys.forEach(function(key){
          stared++;
          api.redis.client.del(key, function(err){
            stared--;
            if(stared === 0 && typeof next === 'function'){
              next(err, keys.length);
            }
          });
        });
      }else{
        if(typeof next === 'function'){ next(err, keys.length); }
      }
    });
  }

  api.cache.dumpWrite = function(file, next){
    api.cache.keys(function(err, keys){
      var data = {};
      var stared = 0;
      keys.forEach(function(key){
        stared++;
        api.redis.client.get(key, function(err, content){
          stared--;
          data[key] = content;
          if(stared === 0){
            fs.writeFileSync(file, JSON.stringify(data));
            if(typeof next === 'function'){ next(err, keys.length); }
          }
        });
      });
      if(keys.length === 0){
        fs.writeFileSync(file, JSON.stringify(data));
        if(typeof next === 'function'){ next(err, keys.length); }
      }
    });
  }

  api.cache.dumpRead = function(file, next){
    api.cache.clear(function(err){
      var stared = 0;
      var data = JSON.parse( fs.readFileSync(file) );
      for(var key in data){
        stared++;
        var content = data[key];
        api.cache.saveDumpedElement(key, content, function(err){
          stared--;
          if(stared === 0 && typeof next === 'function'){
            next(err, api.utils.hashLength(data));
          }
        });
      }
      if(api.utils.hashLength(data) === 0){
        if(typeof next === 'function'){ next(err, api.utils.hashLength(data)); }
      }
    });    
  }

  api.cache.saveDumpedElement = function(key, content, callback){
    var parsedContent = JSON.parse(content);
    api.redis.client.set(key, content, function(err){
      if(parsedContent.expireTimestamp){
        var expireTimeSeconds = Math.ceil((parsedContent.expireTimestamp - new Date().getTime()) / 1000);
        api.redis.client.expire(key, expireTimeSeconds, function(){
          callback(err);
        });
      }else{
        callback(err)
      }
    });
  }

  api.cache.load = function(key, options, next){
    // optons: options.expireTimeMS, options.retry
    if(typeof options === 'function'){
      next = options;
      options = {};
    }

    api.redis.client.get(api.cache.redisPrefix + key, function(err, cacheObj){
      if(err){ api.log(err, 'error') }
      try { cacheObj = JSON.parse(cacheObj) } catch(e){}
      if(!cacheObj){
        if(typeof next === 'function'){
          process.nextTick(function(){ next(new Error('Object not found'), null, null, null, null); });
        }
      } else if(cacheObj){
        var expireTimeSeconds = 0;
        var now = new Date().getTime();
        if(options.expireTimeMS){
          expireTimeSeconds = Math.ceil(options.expireTimeMS / 1000);
          api.redis.client.expire(api.cache.redisPrefix + key, expireTimeSeconds);
        }

        if(typeof next === 'function'){
          process.nextTick(function(){ next(err, cacheObj.value, now + (expireTimeSeconds * 1000), cacheObj.createdAt, now); });
        }
      } else {
        if(typeof next === 'function'){
          process.nextTick(function(){ next(new Error('Object expired'), null, null, null, null); });
        }
      }
    });
  };

  api.cache.destroy = function(key, next){
    api.cache.checkLock(key, null, function(err, lockOk){
      if(err || lockOk !== true){
        if(typeof next === 'function'){ next(new Error('Object Locked')); }
      }else{
        api.redis.client.del(api.cache.redisPrefix + key, function(err, count){
          if(err){ api.log(err, 'error') }
          var resp = true;
          if(count !== 1){ resp = false }
          if(typeof next === 'function'){ next(null, resp); }
        });
      }
    });
  };

  api.cache.save = function(key, value, expireTimeMS, next){
    if(typeof expireTimeMS === 'function' && typeof next === 'undefined'){
      next = expireTimeMS;
      expireTimeMS = null;
    }
    var expireTimeSeconds = null
    var expireTimestamp = null
    if(null !== expireTimeMS){
      expireTimeSeconds = Math.ceil(expireTimeMS / 1000);
      expireTimestamp   = new Date().getTime() + expireTimeMS;
    }
    var cacheObj = {
      value:           value,
      expireTimestamp: expireTimestamp,
      createdAt:       new Date().getTime(),
      readAt:          null
    }

    if (api.cache.useLocking) {
      api.cache.checkLock(key, null, function(err, lockOk){
        if(err || lockOk !== true){
          if(typeof next === 'function'){ next(new Error('Object Locked')); }
        }else{
          api.redis.client.set(api.cache.redisPrefix + key, JSON.stringify(cacheObj), function(err){
            if(err === null && expireTimeSeconds){
              api.redis.client.expire(api.cache.redisPrefix + key, expireTimeSeconds);
            }
            if(typeof next === 'function'){ process.nextTick(function(){ next(err, true) }) }
          });
        }
      });
    } else {
      var setParams = [api.cache.redisPrefix + key, JSON.stringify(cacheObj)];
      if (expireTimeSeconds){
        setParams.push('EX');
        setParams.push(expireTimeSeconds);
      }

      if(typeof next === 'function'){
        setParams.push(function(err){
          process.nextTick(function(){ next(err, true) });
        });
      }

      api.redis.client.set.apply(api.redis.client, setParams);
    }
  };

  api.cache.lock = function(key, expireTimeMS, next){
    if(typeof expireTimeMS === 'function' && next === null){
      expireTimeMS = expireTimeMS;
      expireTimeMS = null;
    }
    if(expireTimeMS === null){
      expireTimeMS = api.cache.lockDuration;
    }

    api.cache.checkLock(key, null, function(err, lockOk){
      if(err || lockOk !== true){
        next(err, false);
      }else{
        api.redis.client.setnx(api.cache.lockPrefix + key, api.cache.lockName, function(err){
          if(err){
            next(err)
          }else{
            api.redis.client.expire(api.cache.lockPrefix + key, Math.ceil(expireTimeMS/1000), function(err){
              lockOk = true;
              if(err){ lockOk = false; }
              next(err, lockOk);
            });
          }
        });
      }
    });
  }

  api.cache.unlock = function(key, next){
    api.cache.checkLock(key, null, function(err, lockOk){
      if(err || lockOk !== true){
        next(err, false);
      }else{
        api.redis.client.del(api.cache.lockPrefix + key, function(err){
          lockOk = true;
          if(err){ lockOk = false; }
          next(err, lockOk);
        });
      }
    });
  }

  api.cache.checkLock = function(key, retry, next, startTime){
    if(startTime === null){ startTime = new Date().getTime(); }

    api.redis.client.get(api.cache.lockPrefix + key, function(err, lockedBy){
      if(err){
        next(err, false);
      }else if(lockedBy === api.cache.lockName || lockedBy === null){
        next(null, true);
      }else{
        var delta = new Date().getTime() - startTime;
        if(retry === null || retry === false || delta > retry){
          next(null, false);
        }else{
          setTimeout(function(){
            api.cache.checkLock(key, retry, next, startTime);
          }, api.cache.lockRetry);
        }
      }
    });
  }

  next();
}

/////////////////////////////////////////////////////////////////////
// exports
exports.cache = cache;
