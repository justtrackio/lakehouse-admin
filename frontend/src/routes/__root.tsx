import { createRootRoute, Link, Outlet } from '@tanstack/react-router';
import { TanStackRouterDevtools } from '@tanstack/router-devtools';
import { Layout, Menu, Space, Switch, Typography } from 'antd';
import { AdminModeProvider } from '../components/AdminModeProvider';
import { MessageProvider } from '../components/MessageProvider';
import { TaskStatusIndicator } from '../components/TaskStatusIndicator';
import { useAdminMode } from '../context/AdminModeContext';

const { Header, Content, Footer } = Layout;
const { Text } = Typography;

export const Route = createRootRoute({
  component: RootComponent,
});

function RootComponent() {
  return (
    <MessageProvider>
      <AdminModeProvider>
        <RootLayout />
      </AdminModeProvider>
    </MessageProvider>
  );
}

function RootLayout() {
  const { isAdminMode, setAdminMode } = useAdminMode();

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Header style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 16 }}>
        <div style={{ color: 'white', fontSize: '20px', fontWeight: 'bold' }}>
          <Link to="/">Lakehouse Admin</Link>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', flex: 1, minWidth: 0, gap: 16 }}>
          <Menu
            theme="dark"
            mode="horizontal"
            style={{ flex: 1, minWidth: 0, justifyContent: 'flex-end' }}
            items={[
              {
                key: 'home',
                label: <Link to="/">Home</Link>,
              },
              {
                key: 'maintenance',
                label: <Link to="/maintenance">Maintenance</Link>,
              },
              {
                key: 'tasks',
                label: <Link to="/tasks">Tasks</Link>,
              },
            ]}
          />
          <TaskStatusIndicator />
        </div>
      </Header>
      <Content style={{ padding: '24px', maxWidth: '90%', margin: '0 10%', flex: '1 0 auto' }}>
        <Outlet />
      </Content>
      <Footer style={{ textAlign: 'center' }}>
        Lakehouse Admin {new Date().getFullYear()}
      </Footer>
      <div
        style={{
          position: 'fixed',
          right: 24,
          bottom: 24,
          zIndex: 1000,
          background: 'rgba(0, 0, 0, 0.8)',
          borderRadius: 8,
          padding: '10px 12px',
          boxShadow: '0 6px 16px rgba(0, 0, 0, 0.25)',
        }}
      >
        <Space size="small" align="center">
          <Text style={{ color: 'rgba(255,255,255,0.85)', whiteSpace: 'nowrap' }}>
            Admin mode
          </Text>
          <Switch
            checked={isAdminMode}
            onChange={setAdminMode}
            checkedChildren="On"
            unCheckedChildren="Off"
          />
        </Space>
      </div>
      <TanStackRouterDevtools />
    </Layout>
  );
}
