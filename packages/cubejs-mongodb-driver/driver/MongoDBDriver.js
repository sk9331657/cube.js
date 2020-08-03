const { MongoClient } = require('mongodb');
const { Parser } = require('node-sql-parser');
const genericPool = require('generic-pool');
const { promisify } = require('util');
const BaseDriver = require('@cubejs-backend/query-orchestrator/driver/BaseDriver');

class MongoDBDriver extends BaseDriver {
  constructor(config) {
    super();
    config = config || {};
    let ssl;
    const sslOptions = [
      { name: 'ca', value: 'CUBEJS_DB_SSL_CA' },
      { name: 'cert', value: 'CUBEJS_DB_SSL_CERT' },
      { name: 'ciphers', value: 'CUBEJS_DB_SSL_CIPHERS' },
      { name: 'passphrase', value: 'CUBEJS_DB_SSL_PASSPHRASE' },
    ];

    if (
      process.env.CUBEJS_DB_SSL ||
      process.env.CUBEJS_DB_SSL_REJECT_UNAUTHORIZED ||
      sslOptions.find(o => !!process.env[o.value])
    ) {
      ssl = sslOptions.reduce(
        (agg, { name, value }) => ({
          ...agg,
          ...(process.env[value] ? { [name]: process.env[value] } : {}),
        }),
        {}
      );

      if (process.env.CUBEJS_DB_SSL_REJECT_UNAUTHORIZED) {
        ssl.rejectUnauthorized =
          process.env.CUBEJS_DB_SSL_REJECT_UNAUTHORIZED.toLowerCase() === 'true';
      }
    }

    this.config = {
      host: process.env.CUBEJS_DB_HOST,
      database: process.env.CUBEJS_DB_NAME,
      port: process.env.CUBEJS_DB_PORT || 27017,
      user: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
      collection: process.env.CUBEJS_DB_COLLECTION,
      ssl,
      authSwitchHandler: (data, cb) => {
        const password = config.password || process.env.CUBEJS_DB_PASS || '';
        const buffer = Buffer.from((password).concat('\0'));
        cb(null, buffer);
      },
      values:null,
      ...config
    };
    this.pool = genericPool.createPool({
      create: async () => {
        const uri = "mongodb://" + this.config.user + ":" + this.config.password + "@" + this.config.host + ':' + this.config.port + "/" + this.config.database + "?retryWrites=true&w=majority";
        const conn = new MongoClient(uri, { useUnifiedTopology: true });
        const connect = promisify(conn.connect.bind(conn));
        conn.execute = async (mongoObj) => {
          const db = conn.db(this.config.database);
          if (mongoObj['table']) {
            let output = await db.collection(mongoObj['table']).aggregate(mongoObj['agri']).toArray();
            return output;
          } else {
            return [];
          }
         
        }
        await connect();
        return conn;
      },
      destroy: async (connection) => { await connection.close(); },
      validate: async (connection) => {
        try {
          //await connection.execute('SELECT 1');
        } catch (e) {
          return false;
        }
        return true;
      }
    }, {
      min: 0,
      max: 8,
      evictionRunIntervalMillis: 10000,
      softIdleTimeoutMillis: 30000,
      idleTimeoutMillis: 30000,
      testOnBorrow: true,
      acquireTimeoutMillis: 20000
    });
  }


  async testConnection() {
    const conn = await this.pool._factory.create();
    try {
      return [];
    } finally {
      await this.pool._factory.destroy(conn);
    }
  }

  async query(query, values) {

    return new Promise(
      async (resolve, reject) => {
        let conn = await this.pool.acquire();
        let mongoObj = this.processQuery(query,values);
        let output = await conn.execute(mongoObj);
        for (let key of output) {
          if(key['_id']!=null ) for (let keys in key._id) key[keys] = key._id[keys];
          delete key['_id'];
        }
        this.pool.release(conn);
        resolve(output);
      }
    );
  }

  async release() {
    await this.pool.drain();
    await this.pool.clear();
  }

  async tablesSchema() {
    let conn = await this.pool.acquire();
    const db = conn.db(this.config.database);
    let vcol = {};
    let collections = await db.listCollections().toArray();
    for (let element of collections) {
      let fcol = await this.processCollections(db, element);
      vcol[element.name] = fcol;
    }
    let finalarr = {};
    finalarr[this.config.database] = vcol;
    this.pool.release(conn);
    return finalarr;
  }

  async processCollections(db, collection) {
    let col = []
    let dbobj = await db.collection(collection.name).findOne();
    for (let key in dbobj) {
      let row = {}
      row['name'] = key;
      if (typeof dbobj[key] === "number" && !Number.isInteger(dbobj[key])) row['type'] = 'double'
      else if (typeof dbobj[key] === "number" && Number.isInteger(dbobj[key])) row['type'] = 'bigint'
      else row['type'] = 'varchar'
      row['attributes'] = []
      col.push(row);
    }
    return col;
  }

  processQuery(query,values) {
    if (values && values.length > 0) { query = query.replace("?)", '"' + values[0] + '")'); values.shift(); }
    const ast = new Parser().astify(query);
    if (ast.from == null) return null;
    let groupobj = { _id: null };
    // let ascolmap = {}
    let countmap = []
    if (ast.columns != null) {
      ast.columns.map(key => {
        // ascolmap[key.as] = key.expr.column;
        if (key.expr.args != undefined) {
          let outname = key.as;
          let type = "$" + key.expr.name.toLowerCase();
          let json = {};
          if (key.expr.args.expr.value === '*') {
            json["$sum"] = 1;
            groupobj[outname] = json;
            countmap.push(outname);
          } else {
            json[type] = "$" + key.expr.args.expr.args.value.toLowerCase();
            groupobj[outname] = json;
          }
        }
      })
    }
    // Processing Group BY
    var id = {};
    if (ast.groupby != null) {
      ast.groupby.forEach(key => {
        if (key.type === 'number') {
          let key1 = ast.columns[key.value - 1];
          let outcome = key1.expr.column;
          id[key1.as] = "$" + outcome;
        }
        // else {
        //   id[key.column] = "$" + ascolmap[key.column];
        // }
      });
      groupobj._id = id;
    }

    var sorts = {}
    if (ast.orderby != null) {
      ast.orderby.forEach(key => {
        let key1 = ast.columns[key.expr.value - 1];
        sorts[key1.as] = key.type === "DESC" ? -1 : 1;
      });
    }

    let json = {}
    if (ast.where != null && ast.where.operator === '=') {
      let obj = {};
      obj['$eq'] = ast.where.right.value;
      json[ast.where.left.column] = obj;
    }

    let having = {}

    if (ast.having != null) {
      // let map = { '=': '$eq', '>=': '$gte', '>': '$gt', '<': '$lt', '<=': '$lte', '!=': '$ne' };
      if (ast.having.left.type === "aggr_func" && ast.having.operator === "=") {
          having[countmap[0]] = {"$eq":parseInt(ast.having.right.value)}; 
      } else {
        if (ast.having.operator === '=') {
          having[ast.having.left.column] = ast.having.right.value;
        }
        else {
          let index = Object.keys(map).indexOf(ast.having.operator);
          if (index != -1) {
            let sub = {}
            let key = map[Object.keys(map)[index]];
            sub[key] = parseInt(ast.having.right.value);
            having[countmap[0]] = sub;
          }
        }
      }
    }

    let agri = [];
    if (Object.keys(json).length > 0) agri.push({ "$match": json });
    agri.push({ '$group': groupobj });
    if (Object.keys(having).length > 0) agri.push({ "$match": having })
    if (Object.keys(sorts).length > 0) agri.push({ "$sort": sorts })
    if (ast.limit != null && ast.limit.value && ast.limit.value[0]) {
      agri.push({ "$limit": ast.limit.value[ast.limit.value.length - 1].value })
    }

    let mongoObj = { };
    mongoObj['table'] = ast.from[0]['table'];
    mongoObj['agri'] = agri;
    return  mongoObj;
  }


}

module.exports = MongoDBDriver;
