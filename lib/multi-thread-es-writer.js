'use strict';

const {Writable} = require('stream');
const util = require('util');


class MultiThreadWriter extends Writable {
    constructor({pool, index, type, id, maxBatchSize = 100, timeout = 1000}) {
        super({objectMode: true});
        this.pool = pool;
        this.buf = [];
        this.index = index;
        this.type = type;
        this.id = id;
        this.maxBatchSize = maxBatchSize;
        this.timeout = timeout;
        this.promises = new Set();
        this.interval = setInterval(() => this.poolInfo(), 3000);
    }

    poolInfo() {
        const pool = this.pool;
        console.log(`Borrowed: ${pool.borrowed}, Available: ${pool.available}, Pending: ${pool.pending}`);
    }

    async sendBatch() {
        if (!this.buf.length) return;
        const client = await this.pool.acquire();
        const batch = this.buf;
        this.buf = [];
        const body = [];
        for (let i = 0; i < batch.length; i++) {
            body.push({index: {_index: this.index, _type: this.type, _id: batch[i][this.id]}});
            body.push(batch[i]);
        }
        const p = client.bulk({body });
        this.promises.add(p);
        p.finally(() => {
            this.promises.delete(p);
            this.pool.release(client);
        });
    }

    _write(rec, enc, done) {
        this.buf.push(rec);
        if (this.buf.length > this.maxBatchSize) {
            this.sendBatch().then(() => done()).catch(done);
        } else {
            done();
        }
    }

    async _finalize() {
        await this.sendBatch();
        await Promise.all(Array.from(this.promises));
        clearInterval(this.interval);
    }

    _final(callback) {
        this._finalize().then(() => callback()).catch(callback);
    }
}

exports.MultiThreadWriter = MultiThreadWriter;