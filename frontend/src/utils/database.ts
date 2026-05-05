export interface DatabaseSearch {
  database: string;
}

export function normalizeDatabaseSearch(search: Record<string, unknown>): DatabaseSearch {
  const database = typeof search.database === 'string' ? search.database : '';

  return { database };
}
