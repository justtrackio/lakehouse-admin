import { createContext, useContext } from 'react';

export interface DatabaseContextValue {
  database: string;
  defaultDatabase: string;
  databases: string[];
  isLoading: boolean;
  setDatabase: (database: string) => void;
}

export const DatabaseContext = createContext<DatabaseContextValue | null>(null);

export function useDatabase() {
  const context = useContext(DatabaseContext);

  if (!context) {
    throw new Error('useDatabase must be used within a DatabaseContext provider');
  }

  return context;
}
