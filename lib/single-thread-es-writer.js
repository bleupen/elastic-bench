'use strict';

const {Writable} = require('stream');
const util = require('util');

class SingleThreadWriter extends Writable {
    constructor({client, index, type, id, maxBatchSize = 1000, timeout = 1000}) {
        super({objectMode: true});
        this.buf = [];
        this.index = index;
        this.type = type;
        this.id = id;
        this.client = client;
        this.maxBatchSize = maxBatchSize;
        this.timeout = timeout;
    }

    async writeBatch() {
        if (!this.buf.length) return;

        const batch = this.buf;
        this.buf = [];
        const body = [];
        for (let i = 0; i < batch.length; i++) {
            body.push({index: {_index: this.index, _type: this.type, _id: batch[i][this.id]}});
            body.push(batch[i]);
        }
        await this.client.bulk({body});
    }

    _write(rec, enc, done) {
        this.buf.push(rec);
        if (this.buf.length > this.maxBatchSize) {
            this.writeBatch().then(() => done()).catch(done);
        } else {
            done();
        }
    }

    _final(callback) {
        this.writeBatch().then(() => callback()).catch(callback);
    }
}

exports.SingleThreadWriter = SingleThreadWriter;