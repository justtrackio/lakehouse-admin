import { useMemo, useRef } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useNavigate, useRouterState } from '@tanstack/react-router';
import { Alert, Spin } from 'antd';
import { fetchDatabases } from '../api/schema';
import { DatabaseContext } from '../context/DatabaseContext';

interface DatabaseProviderProps {
  children: React.ReactNode;
}

export function DatabaseProvider({ children }: DatabaseProviderProps) {
  const navigate = useNavigate();
  const search = useRouterState({ select: (state) => state.location.search as Record<string, unknown> });
  const pathnameRef = useRef('');
  const pathname = useRouterState({
    select: (state) => {
      pathnameRef.current = state.location.pathname;
      return state.location.pathname;
    },
  });
  void pathname;

  const { data, isLoading, isError, error } = useQuery({
    queryKey: ['databases'],
    queryFn: fetchDatabases,
    staleTime: 1000 * 60 * 5,
  });

  const database = typeof search.database === 'string' ? search.database : '';
  const defaultDatabase = data?.default_database ?? '';
  const activeDatabase = database || defaultDatabase;

  const value = useMemo(() => ({
    database: activeDatabase,
    defaultDatabase,
    databases: data?.databases.map((item) => item.name) ?? [],
    isLoading,
    setDatabase: (nextDatabase: string) => {
      void navigate({
        to: pathnameRef.current,
        search: (prev) => ({
          ...(prev as Record<string, unknown>),
          database: nextDatabase,
        }),
        replace: true,
      });
    },
  }), [activeDatabase, data, defaultDatabase, isLoading, navigate]);

  if (isLoading) {
    return (
      <div style={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <Spin size="large" />
      </div>
    );
  }

  if (isError) {
    return (
      <div style={{ padding: 24 }}>
        <Alert
          type="error"
          showIcon
          message="Failed to load databases"
          description={error instanceof Error ? error.message : 'Unknown error'}
        />
      </div>
    );
  }

  return (
    <DatabaseContext.Provider value={value}>
      {children}
    </DatabaseContext.Provider>
  );
}
