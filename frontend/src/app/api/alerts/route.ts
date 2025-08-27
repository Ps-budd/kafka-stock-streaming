import { NextResponse } from 'next/server';
import { dbPool } from '@/lib/db';

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const symbol = (searchParams.get('symbol') || '').toUpperCase();
  const limit = Math.min(parseInt(searchParams.get('limit') || '100', 10), 1000);

  let sql = `SELECT symbol, price, sma, zscore, rule, ts FROM stock_alerts`;
  const params: any[] = [];
  if (symbol) {
    params.push(symbol);
    sql += ` WHERE symbol = $1`;
  }
  sql += ` ORDER BY ts DESC LIMIT ${limit}`;

  try {
    const { rows } = await dbPool.query(sql, params);
    return NextResponse.json({ data: rows });
  } catch (err: any) {
    console.error('[api/alerts] error', err);
    return NextResponse.json({ error: 'db_error' }, { status: 500 });
  }
}


