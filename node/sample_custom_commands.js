/**
 * Sample Custom Commands with Arguments
 * ======================================
 *
 * This file demonstrates how to create custom commands that accept
 * command-line arguments via the --custom-command-args flag.
 *
 * The CustomCommands class constructor receives an optional 'args' parameter
 * (as a single string). You can then parse this string however you like
 * in your implementation.
 *
 * Example usage:
 *     node valkey-benchmark.js -t custom \
 *         --custom-command-file sample_custom_commands.js \
 *         --custom-command-args "operation=mset,batch_size=5,key_prefix=test"
 */

/**
 * Sample custom commands class with argument parsing.
 */
class CustomCommands {
    /**
     * Initialize with optional command-line arguments.
     * @param {string|null} args - Command-line arguments as a single string.
     *                             Format: "key1=value1,key2=value2"
     */
    constructor(args = null) {
        // Default configuration
        this.operation = 'set';  // 'set', 'mset', 'hset'
        this.batchSize = 1;
        this.keyPrefix = 'sample';
        this.counter = 0;

        // Parse arguments if provided
        if (args) {
            this._parseArgs(args);
        }

        console.log('CustomCommands initialized:');
        console.log(`  operation: ${this.operation}`);
        console.log(`  batch_size: ${this.batchSize}`);
        console.log(`  key_prefix: ${this.keyPrefix}`);
    }

    /**
     * Parse command-line arguments.
     *
     * Expected format: "key1=value1,key2=value2"
     *
     * Supported arguments:
     *   - operation: Type of operation (set, mset, hset)
     *   - batch_size: Number of keys to set at once (for mset)
     *   - key_prefix: Prefix for generated keys
     *
     * @param {string} argsString - Arguments string to parse
     */
    _parseArgs(argsString) {
        const errorFormatMsg =
            'Expected format: "key1=value1,key2=value2"\n' +
            'Example: "operation=mset,batch_size=5,key_prefix=test"';

        try {
            const pairs = argsString.split(',');
            for (let pair of pairs) {
                pair = pair.trim();
                if (!pair) {
                    continue;  // Skip empty strings
                }

                if (!pair.includes('=')) {
                    console.log(`Warning: Ignoring malformed argument "${pair}" (missing "=")`);
                    continue;
                }

                const eqIndex = pair.indexOf('=');
                const key = pair.substring(0, eqIndex).trim();
                const value = pair.substring(eqIndex + 1).trim();

                // Validate that key and value are not empty
                if (!key) {
                    console.log('Warning: Ignoring argument with empty key');
                    continue;
                }
                if (!value) {
                    console.log(`Warning: Ignoring argument "${key}" with empty value`);
                    continue;
                }

                if (key === 'operation') {
                    if (!['set', 'mset', 'hset'].includes(value)) {
                        throw new Error(`Invalid operation '${value}'. Must be one of: set, mset, hset`);
                    }
                    this.operation = value;
                } else if (key === 'batch_size') {
                    const batchSize = parseInt(value, 10);
                    if (isNaN(batchSize)) {
                        throw new Error(`batch_size must be a valid integer, got '${value}'`);
                    }
                    if (batchSize < 1) {
                        throw new Error(`batch_size must be positive, got ${batchSize}`);
                    }
                    this.batchSize = batchSize;
                } else if (key === 'key_prefix') {
                    this.keyPrefix = value;
                } else {
                    console.log(`Warning: Unknown argument "${key}" will be ignored`);
                }
            }
        } catch (e) {
            if (e instanceof Error) {
                console.error(`Error: Invalid argument value: ${e.message}`);
            } else {
                console.error(`Error: Failed to parse arguments: ${e}`);
            }
            console.error(errorFormatMsg);
            throw e;
        }
    }

    /**
     * Execute the custom command.
     * @param {Object} client - Valkey client instance
     * @returns {Promise<boolean>} True if successful, False otherwise
     */
    async execute(client) {
        try {
            if (this.operation === 'set') {
                return await this._executeSet(client);
            } else if (this.operation === 'mset') {
                return await this._executeMset(client);
            } else if (this.operation === 'hset') {
                return await this._executeHset(client);
            } else {
                console.log(`Unknown operation: ${this.operation}`);
                return false;
            }
        } catch (e) {
            console.error(`Custom command error: ${e}`);
            return false;
        }
    }

    /**
     * Execute a single SET command.
     * @param {Object} client - Valkey client instance
     * @returns {Promise<boolean>} True if successful
     */
    async _executeSet(client) {
        const key = `${this.keyPrefix}:key:${this.counter}`;
        const value = `value:${this.counter}`;
        this.counter++;
        await client.set(key, value);
        return true;
    }

    /**
     * Execute an MSET command with batchSize keys.
     * @param {Object} client - Valkey client instance
     * @returns {Promise<boolean>} True if successful
     */
    async _executeMset(client) {
        // Glide expects mset as an object: { key1: value1, key2: value2 }
        const kvPairs = {};
        for (let i = 0; i < this.batchSize; i++) {
            const key = `${this.keyPrefix}:key:${this.counter}`;
            const value = `value:${this.counter}`;
            kvPairs[key] = value;
            this.counter++;
        }
        await client.mset(kvPairs);
        return true;
    }

    /**
     * Execute an HSET command.
     * @param {Object} client - Valkey client instance
     * @returns {Promise<boolean>} True if successful
     */
    async _executeHset(client) {
        const hashKey = `${this.keyPrefix}:hash`;
        const field = `field:${this.counter}`;
        const value = `value:${this.counter}`;
        this.counter++;
        await client.hset(hashKey, { [field]: value });
        return true;
    }
}

module.exports = CustomCommands;
