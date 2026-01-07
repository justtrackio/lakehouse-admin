import { ReactNode } from 'react';
import { message } from 'antd';
import { MessageContext } from '../context/MessageContext';

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
