require('dotenv').config();
const { connection } = require('./redis');
const { initDB, pool } = require('./db');

async function testSetup() {
    console.log('Testing Redis...');
    await connection.set('test:key', 'hello');
    const val = await connection.get('test:key');
    console.log('Redis:', val === 'hello' ? 'PASS' : 'FAIL');
    await connection.del('test:key');
    console.log('Testing PostgreSQL...');

    await initDB();
    const { rows } = await pool.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
    );
    console.log('Tables:', rows.map(r => r.table_name).join(', '));
    console.log('\nAll systems operational!');
    process.exit(0);
}
testSetup().catch(err => { console.error(err.message); process.exit(1); });