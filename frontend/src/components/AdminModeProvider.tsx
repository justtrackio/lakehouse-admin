import { ReactNode, useState } from 'react';
import { AdminModeContext } from '../context/AdminModeContext';

const adminModeStorageKey = 'lakehouse-admin.admin-mode';

interface AdminModeProviderProps {
  children: ReactNode;
}

export function AdminModeProvider({ children }: AdminModeProviderProps) {
  const [isAdminMode, setIsAdminMode] = useState(() => {
    if (typeof window === 'undefined') {
      return false;
    }

    const storedValue = window.localStorage.getItem(adminModeStorageKey);
    return storedValue === 'true';
  });

  const setAdminMode = (value: boolean) => {
    setIsAdminMode(value);
    window.localStorage.setItem(adminModeStorageKey, String(value));
  };

  return (
    <AdminModeContext.Provider value={{ isAdminMode, setAdminMode }}>
      {children}
    </AdminModeContext.Provider>
  );
}
