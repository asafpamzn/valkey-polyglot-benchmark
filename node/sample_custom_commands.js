/**
 * Sample Custom Commands with Config Initialization
 * ==================================================
 *
 * LIFECYCLE:
 *   1. constructor(args) - Called first (args available here too, but prefer init)
 *   2. init(config)      - Called with full config including args - DO ALL SETUP HERE
 *   3. execute(client)   - Called for each benchmark request
 *
 * CONFIG PROPERTIES AVAILABLE IN init():
 *   config.customCommandArgs       - The --custom-command-args string
 *   config.dataSize                - Size of data in bytes (--datasize)
 *   config.keyspaceOffset          - Keyspace offset (--keyspace-offset)
 *   config.randomKeyspace          - Random keyspace size (--random)
 *   config.useSequential           - Whether sequential mode is enabled
 *   config.sequentialKeyspacelen   - Sequential keyspace length
 *
 * EXAMPLE:
 *   node valkey-benchmark.js -t custom \
 *       --custom-command-file sample_custom_commands.js \
 *       --custom-command-args "operation=lpush_keyspace" \
 *       --sequential 1000000 --keyspace-offset 5000000
 */

class CustomCommands {
    constructor(args = null) {
        // Minimal constructor - all real setup happens in init()
    }

    /**
     * Initialize everything here - both custom args and benchmark config.
     * @param {Object} config - Full benchmark configuration
     */
    init(config) {
        // Parse custom command args
        this.operation = 'set';
        this.batchSize = 1;
        this.keyPrefix = 'sample';
        this.counter = 0;

        const args = config.customCommandArgs || '';
        for (const pair of args.split(',')) {
            const [key, ...rest] = pair.split('=');
            const value = rest.join('=').trim();
            if (key?.trim() === 'operation') this.operation = value;
            else if (key?.trim() === 'batch_size') this.batchSize = parseInt(value, 10) || 1;
            else if (key?.trim() === 'key_prefix') this.keyPrefix = value;
        }

        // Store benchmark config
        this.keyspaceOffset = config.keyspaceOffset || 0;
        this.randomKeyspace = config.randomKeyspace || 0;
        this.useSequential = config.useSequential || false;
        this.sequentialKeyspacelen = config.sequentialKeyspacelen || 0;
        this.dataSize = config.dataSize || 100;

        const ksType = this.useSequential ? 'sequential' : this.randomKeyspace > 0 ? 'random' : 'none';
        console.log(`CustomCommands init: operation=${this.operation}, keyspace=${ksType}, offset=${this.keyspaceOffset}`);
    }

    async execute(client) {
        switch (this.operation) {
            case 'set': return this._executeSet(client);
            case 'mset': return this._executeMset(client);
            case 'hset': return this._executeHset(client);
            case 'lpush': return this._executeLpush(client);
            case 'lpush_keyspace': return this._executeLpushKeyspace(client);
            default: return false;
        }
    }

    _getNextKey() {
        if (this.useSequential) {
            return `key:${this.keyspaceOffset + (this.counter % this.sequentialKeyspacelen)}`;
        } else if (this.randomKeyspace > 0) {
            return `key:${this.keyspaceOffset + Math.floor(Math.random() * this.randomKeyspace)}`;
        }
        return `key:${this.counter}`;
    }

    _generateData() {
        const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
        let result = '';
        for (let i = 0; i < this.dataSize; i++) {
            result += chars.charAt(Math.floor(Math.random() * chars.length));
        }
        return result;
    }

    async _executeSet(client) {
        await client.set(`${this.keyPrefix}:key:${this.counter}`, `value:${this.counter++}`);
        return true;
    }

    async _executeMset(client) {
        const kvPairs = {};
        for (let i = 0; i < this.batchSize; i++) {
            kvPairs[`${this.keyPrefix}:key:${this.counter++}`] = `value:${this.counter}`;
        }
        await client.mset(kvPairs);
        return true;
    }

    async _executeHset(client) {
        await client.hset(`${this.keyPrefix}:hash`, { [`field:${this.counter++}`]: `value:${this.counter}` });
        return true;
    }

    async _executeLpush(client) {
        await client.lpush(`${this.keyPrefix}:list`, [`value:${this.counter++}`]);
        return true;
    }

    async _executeLpushKeyspace(client) {
        const key = this._getNextKey();
        const data = this._generateData();
        this.counter++;
        await client.lpush(key, [data]);
        return true;
    }
}

module.exports = CustomCommands;
