import { NextResponse } from 'next/server';
import { dbPool } from '@/lib/db';

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const symbol = (searchParams.get('symbol') || process.env.SYMBOL || 'TSLA').toUpperCase();
  const limit = Math.min(parseInt(searchParams.get('limit') || '500', 10), 2000);

  try {
    const { rows } = await dbPool.query(
      `SELECT symbol, price, ts
       FROM stock_prices
       WHERE symbol = $1
       ORDER BY ts DESC
       LIMIT $2`,
      [symbol, limit]
    );
    // Return in ascending time for charting
    rows.reverse();
    return NextResponse.json({ symbol, data: rows });
  } catch (err: any) {
    console.error('[api/prices] error', err);
    return NextResponse.json({ error: 'db_error' }, { status: 500 });
  }
}


