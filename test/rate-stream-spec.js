'use strict';

const {Readable, Writable} = require('stream');
const {RateStream} = require('../lib/rate-stream');
const {SingleThreadWriter} = require('../lib/single-thread-es-writer');
const {MultiThreadWriter} = require('../lib/multi-thread-es-writer');
const {Client} = require('@elastic/elasticsearch');
const pool = require('generic-pool');
const rec = require('./epv_rec');

class RecordStream extends Readable {
    constructor(record, size) {
        super({objectMode: true });
        this.count = 0;
        this.record = record;
        this.size = size;
    }

    async readRecord() {
        return new Promise(resolve => setTimeout(() => resolve(this.record), 0));
    }

    _read(size) {
        this.readRecord().then(r => {
            for (let i = 0; i < 10; i++) {
                this.push(r);
                this.count++;
            }
            if (this.count >= this.size) this.push(null);
        });
    }
}

class DevNull extends Writable {
    constructor() {
        super({objectMode: true});
    }

    _write(record, enc, done) {
        done();
    }
}

const clientPool = pool.createPool({ create: () => new Client({node: 'http://localhost:9200'}), destroy: () => {}}, { max: 10 });
const client = new Client({ node: 'http://localhost:9200' });

async function run() {
    try {
        await client.indices.delete({index: 'test'});
    } catch (err) {

    }

    const start = Date.now();

    new RecordStream(rec, 100000)
        .pipe(new RateStream(({rate, count}) => console.log(`${count} records processed (${Math.floor(rate)} rps)`)))
        // .pipe(new DevNull())
        .pipe(new MultiThreadWriter({ pool: clientPool, index: 'test', type: 'data', maxBatchSize: 1000 }))
        // .pipe(new SingleThreadWriter({ client, index: 'test', type: 'data' }))
        .on('finish', () => {
            console.log(`Took ${Date.now() - start} ms`);
        });
}

run().catch(err => console.error(err));
