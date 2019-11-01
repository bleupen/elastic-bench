'use strict';

const {Transform} = require('stream');

class RateStream extends Transform {
    constructor(iter = () => {}, { window = 1000, ...opts } = {}) {
        super({objectMode: true, ...opts});
        this.iter = iter;
        this.window = window;
        this.count = 0;
        this.total = 0;
        this.initialized = false;
    }

    calculateRate() {
        return this.count / (Date.now() - this.start) * 1000;
    }

    emitRate() {
        this.iter({ rate: this.calculateRate(), count: this.total });
    }

    _transform(rec, enc, done) {
        if (!this.initialized) {
            this.initialized = true;
            this.start = Date.now();
            this.timer = setInterval(() => this.emitRate(), this.window);
        }
        this.count++;
        this.total++;
        done(null, rec);
    }

    _flush(done) {
        clearInterval(this.timer);
        done();
    }
}

exports.RateStream = RateStream;