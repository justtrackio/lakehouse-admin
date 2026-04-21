import { createContext, useContext } from 'react';

export interface AdminModeContextValue {
  isAdminMode: boolean;
  setAdminMode: (value: boolean) => void;
}

export const AdminModeContext = createContext<AdminModeContextValue | null>(null);

export function useAdminMode(): AdminModeContextValue {
  const context = useContext(AdminModeContext);
  if (!context) {
    throw new Error('useAdminMode must be used within an AdminModeProvider');
  }

  return context;
}
