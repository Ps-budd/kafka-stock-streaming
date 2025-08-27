import { Pool } from 'pg';

const connectionString = process.env.POSTGRES_DSN || 'postgresql://postgres:postgres@localhost:5432/stocks';

export const dbPool = new Pool({ connectionString });


