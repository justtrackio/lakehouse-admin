import { createContext, useContext, ReactNode } from 'react';
import { message } from 'antd';
import type { MessageInstance } from 'antd/es/message/interface';

const MessageContext = createContext<MessageInstance | null>(null);

export function useMessageApi(): MessageInstance {
  const context = useContext(MessageContext);
  if (!context) {
    throw new Error('useMessageApi must be used within a MessageProvider');
  }
  return context;
}

interface MessageProviderProps {
  children: ReactNode;
}

export function MessageProvider({ children }: MessageProviderProps) {
  const [messageApi, contextHolder] = message.useMessage();

  return (
    <MessageContext.Provider value={messageApi}>
      {contextHolder}
      {children}
    </MessageContext.Provider>
  );
}
