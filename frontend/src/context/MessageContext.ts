import { createContext, useContext } from 'react';
import type { MessageInstance } from 'antd/es/message/interface';

export const MessageContext = createContext<MessageInstance | null>(null);

export function useMessageApi(): MessageInstance {
  const context = useContext(MessageContext);
  if (!context) {
    throw new Error('useMessageApi must be used within a MessageProvider');
  }
  return context;
}
