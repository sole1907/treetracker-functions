import pg from "pg";

const dbPoolConnection = (async function () {
	try {
		const connectionString = process.env.DATABASE_URL;
		if (!connectionString) throw new Error('database coonection url not set');
		const pool = new pg.Pool({
			connectionString
		})

		return await pool.connect();
	} catch (error) {
		console.log('something went wrong while connecting db', error);
		throw error;
	}

}
)()

export default dbPoolConnection;