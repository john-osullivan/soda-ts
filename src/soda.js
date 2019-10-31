/*
 * decaffeinate suggestions:
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS203: Remove `|| {}` from converted for-own loops
 * DS205: Consider reworking code to avoid use of IIFEs
 * DS207: Consider shorter variations of null checks
 * DS208: Avoid top-level this
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
// soda.coffee -- chained, evented, buzzworded library for accessing SODA via JS.

// sodaOpts options:
//   username: https basic auth username
//   password: https basic auth password
//   apiToken: socrata api token
//
//   emitterOpts: options to override EventEmitter2 declaration options

//  TODO:
//    * we're inconsistent about validating query correctness. do we continue with catch-what-we-can,
//      or do we just back off and leave all failures to the api to return?

let str;
const eelib = require('eventemitter2');
const EventEmitter = eelib.EventEmitter2 || eelib;
const httpClient = require('superagent');

// internal util funcs
const isString = obj => typeof obj === 'string';
const isArray = obj => Array.isArray(obj);
const isNumber = obj => !isNaN(parseFloat(obj));
const extend = function(target, ...sources) { for (let source of Array.from(sources)) { for (let k in source) { const v = source[k]; target[k] = v; } } return null; };

// it's really, really, really stupid that i have to solve this problem here
const toBase64 =
  (() => {
  if (typeof Buffer !== 'undefined' && Buffer !== null) {
    return str => new Buffer(str).toString('base64');
  } else {
    // adapted/modified from https://github.com/rwz/base64.coffee
    const base64Lookup = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/='.split('');
    const rawToBase64 = typeof btoa !== 'undefined' && btoa !== null ? btoa : function(str) {
      const result = [];
      let i = 0;
      while (i < str.length) {
        const chr1 = str.charCodeAt(i++);
        const chr2 = str.charCodeAt(i++);
        const chr3 = str.charCodeAt(i++);
        if (Math.max(chr1, chr2, chr3) > 0xFF) { throw new Error('Invalid character!'); }

        const enc1 = chr1 >> 2;
        const enc2 = ((chr1 & 3) << 4) | (chr2 >> 4);
        let enc3 = ((chr2 & 15) << 2) | (chr3 >> 6);
        let enc4 = chr3 & 63;

        if (isNaN(chr2)) {
          enc3 = (enc4 = 64);
        } else if (isNaN(chr3)) {
          enc4 = 64;
        }

        result.push(base64Lookup[enc1]);
        result.push(base64Lookup[enc2]);
        result.push(base64Lookup[enc3]);
        result.push(base64Lookup[enc4]);
      }
      return result.join('');
    };
    return str => rawToBase64(unescape(encodeURIComponent(str)));
  }
})();

const handleLiteral = function(literal) {
  if (isString(literal)) {
    return `'${literal}'`;
  } else if (isNumber(literal)) {
    // TODO: possibly ensure number cleanliness for sending to the api? sci not?
    return literal;
  } else {
    return literal;
  }
};

const handleOrder = function(order) {
  if (/( asc$| desc$)/i.test(order)) {
    return order;
  } else {
    return order + ' asc';
  }
};

const addExpr = (target, args) => Array.from(args).map((arg) =>
  isString(arg) ?
    target.push(arg)
  :
    (() => {
      const result = [];
      for (let k in arg) {
        const v = arg[k];
        result.push(target.push(`${k} = ${handleLiteral(v)}`));
      }
      return result;
    })());

// extern util funcs

// convenience functions for building where clauses, if so desired
const expr = {
  and(...clauses) { return (Array.from(clauses).map((clause) => `(${clause})`)).join(' and '); },
  or(...clauses) { return (Array.from(clauses).map((clause) => `(${clause})`)).join(' or '); },

  gt(column, literal) { return `${column} > ${handleLiteral(literal)}`; },
  gte(column, literal) { return `${column} >= ${handleLiteral(literal)}`; },
  lt(column, literal) { return `${column} < ${handleLiteral(literal)}`; },
  lte(column, literal) { return `${column} <= ${handleLiteral(literal)}`; },
  eq(column, literal) { return `${column} = ${handleLiteral(literal)}`; }
};
  
// serialize object to querystring
const toQuerystring = function(obj) {
  str = [];
  for (let key of Object.keys(obj || {})) {
    const val = obj[key];
    str.push(encodeURIComponent(key) + '=' + encodeURIComponent(val));
  }
  return str.join('&');
};

class Connection {
  constructor(dataSite, sodaOpts) {
    this.dataSite = dataSite;
    if (sodaOpts == null) { sodaOpts = {}; }
    this.sodaOpts = sodaOpts;
    if (!/^[a-z0-9-_.]+(:[0-9]+)?$/i.test(this.dataSite)) { throw new Error('dataSite does not appear to be valid! Please supply a domain name, eg data.seattle.gov'); }

    // options passed directly into EventEmitter2 construction
    this.emitterOpts = this.sodaOpts.emitterOpts != null ? this.sodaOpts.emitterOpts : {
      wildcard: true,
      delimiter: '.',
      maxListeners: 15
    };

    this.networker = function(opts, data) {
      const url = `https://${this.dataSite}${opts.path}`;

      const client = httpClient(opts.method, url);

      if (data != null) { client.set('Accept', "application/json"); }
      if (data != null) { client.set('Content-type', "application/json"); }
      if (this.sodaOpts.apiToken != null) { client.set('X-App-Token', this.sodaOpts.apiToken); }
      if ((this.sodaOpts.username != null) && (this.sodaOpts.password != null)) { client.set('Authorization', "Basic " + toBase64(`${this.sodaOpts.username}:${this.sodaOpts.password}`)); }
      if (this.sodaOpts.accessToken != null) { client.set('Authorization', "OAuth " + accessToken); }

      if (opts.query != null) { client.query(opts.query); }
      if (data != null) { client.send(data); }

      return responseHandler => client.end(responseHandler || this.getDefaultHandler());
    };
  }

  getDefaultHandler() {
    // instance variable for easy chaining
    let emitter, handler;
    this.emitter = (emitter = new EventEmitter(this.emitterOpts));

    // return the handler
    return handler = function(error, response) {
      // TODO: possibly more granular handling?
      if (response.ok) {
        if (response.accepted) {
          // handle 202 by remaking request. inform of possible progress.
          emitter.emit('progress', response.body);
          setTimeout((function() { return this.consumer.networker(opts)(handler); }), 5000);
        } else {
          emitter.emit('success', response.body);
        }
      } else {
        emitter.emit('error', response.body != null ? response.body : response.text);
      }

      // just emit the raw superagent obj if they just want complete event
      return emitter.emit('complete', response);
    };
  }
}




// main class
class Consumer {
  constructor(dataSite, sodaOpts) {
    this.dataSite = dataSite;
    if (sodaOpts == null) { sodaOpts = {}; }
    this.sodaOpts = sodaOpts;
    this.connection = new Connection(this.dataSite, this.sodaOpts);
  }

  query() {
    return new Query(this);
  }

  getDataset(id) {
    let emitter;
    return emitter = new EventEmitter(this.emitterOpts);
  }
}
    // TODO: implement me

// Producer class
class Producer {
  constructor(dataSite, sodaOpts) {
    this.dataSite = dataSite;
    if (sodaOpts == null) { sodaOpts = {}; }
    this.sodaOpts = sodaOpts;
    this.connection = new Connection(this.dataSite, this.sodaOpts);
  }

  operation() {
    return new Operation(this);
  }
}

class Operation {
  constructor(producer) {
    this.producer = producer;
  }

  withDataset(datasetId) { this._datasetId = datasetId; return this; }

  // truncate the entire dataset
  truncate() {
    const opts = {method: 'delete'};
    opts.path = `/resource/${this._datasetId}`;
    return this._exec(opts);
  }

  // add a new row - explicitly avoids upserting (updating/deleting existing rows)
  add(data) {
    const opts = {method: 'post'};
    opts.path = `/resource/${this._datasetId}`;

    const _data = JSON.parse(JSON.stringify(data));
    delete _data[':id'];
    delete _data[':delete'];
    for (let obj of Array.from(_data)) {
      delete obj[':id'];
      delete obj[':delete'];
    }

    return this._exec(opts, _data);
  }

  // modify existing rows
  delete(id) {
    const opts = {method: 'delete'};
    opts.path = `/resource/${this._datasetId}/${id}`;
    return this._exec(opts);
  }
  update(id, data) {
    const opts = {method: 'post'};
    opts.path = `/resource/${this._datasetId}/${id}`;
    return this._exec(opts, data);
  }
  replace(id, data) {
    const opts = {method: 'put'};
    opts.path = `/resource/${this._datasetId}/${id}`;
    return this._exec(opts, data);
  }
  
  // add objects, update if existing, delete if :delete=true
  upsert(data) {
    const opts = {method: 'post'};
    opts.path = `/resource/${this._datasetId}`;
    return this._exec(opts, data);
  }

  _exec(opts, data) {
    if (this._datasetId == null) { throw new Error('no dataset given to work against!'); }
    this.producer.connection.networker(opts, data)();
    return this.producer.connection.emitter;
  }
}


// querybuilder class
class Query {
  constructor(consumer) {
    this.consumer = consumer;
    this._select = [];
    this._where = [];
    this._group = [];
    this._having = [];
    this._order = [];
    this._offset = (this._limit = (this._q = null));
  }

  withDataset(datasetId) { this._datasetId = datasetId; return this; }

  // for passing in a fully formed soql query. all other params will be ignored
  soql(query) { this._soql = query; return this; }

  select(...selects) { for (let select of Array.from(selects)) { this._select.push(select); } return this; }

  // args: ('clause', [...])
  //       ({ column: value1, columnb: value2 }, [...]])
  // multiple calls are assumed to be and-chained
  where(...args) { addExpr(this._where, args); return this; }
  having(...args) { addExpr(this._having, args); return this; }

  group(...groups) { for (let group of Array.from(groups)) { this._group.push(group); } return this; }

  // args: ("column direction", ["column direction", [...]])
  order(...orders) { for (let order of Array.from(orders)) { this._order.push(handleOrder(order)); } return this; }

  offset(offset) { this._offset = offset; return this; }

  limit(limit) { this._limit = limit; return this; }
  
  q(q) { this._q = q; return this; }

  getOpts() {
    const opts = {method: 'get'};
    
    if (this._datasetId == null) { throw new Error('no dataset given to work against!'); }
    opts.path = `/resource/${this._datasetId}.json`;

    const queryComponents = this._buildQueryComponents();
    opts.query = {};
    for (let k in queryComponents) { const v = queryComponents[k]; opts.query['$' + k] = v; }
    
    return opts;
  }
    
  getURL() {
    const opts = this.getOpts();
    const query = toQuerystring(opts.query);
    
    return `https://${this.consumer.dataSite}${opts.path}` + (query ? `?${query}` : "");
  }

  getRows() {
    const opts = this.getOpts();

    this.consumer.connection.networker(opts)();
    return this.consumer.connection.emitter;
  }

  _buildQueryComponents() {
    const query = {};

    if (this._soql != null) {
      query.query = this._soql;
    } else {
      if (this._select.length > 0) { query.select = this._select.join(', '); }

      if (this._where.length > 0) { query.where = expr.and.apply(this, this._where); }

      if (this._group.length > 0) { query.group = this._group.join(', '); }

      if (this._having.length > 0) {
        if (!(this._group.length > 0)) { throw new Error('Having provided without group by!'); }
        query.having = expr.and.apply(this, this._having);
      }

      if (this._order.length > 0) { query.order = this._order.join(', '); }

      if (isNumber(this._offset)) { query.offset = this._offset; }
      if (isNumber(this._limit)) { query.limit = this._limit; }
      
      if (this._q) { query.q = this._q; }
    }

    return query;
  }
}

class Dataset {
  constructor(data, client) {
    this.data = data;
    this.client = client;
  }
}
    // TODO: implement me

extend(typeof exports !== 'undefined' && exports !== null ? exports : this.soda, {
  Consumer,
  Producer,
  expr,

  // exported for testing reasons
  _internal: {
    Connection,
    Query,
    Operation,
    util: {
      toBase64,
      handleLiteral,
      handleOrder
    }
  }
}
);

