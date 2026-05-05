import { createRootRoute, Link, Outlet } from '@tanstack/react-router';
import { TanStackRouterDevtools } from '@tanstack/router-devtools';
import { Layout, Menu, Select, Space, Switch, Typography } from 'antd';
import { AdminModeProvider } from '../components/AdminModeProvider';
import { DatabaseProvider } from '../components/DatabaseProvider';
import { MessageProvider } from '../components/MessageProvider';
import { TaskStatusIndicator } from '../components/TaskStatusIndicator';
import { useAdminMode } from '../context/AdminModeContext';
import { useDatabase } from '../context/DatabaseContext';

const { Header, Content, Footer } = Layout;
const { Text } = Typography;

export const Route = createRootRoute({
  component: RootComponent,
});

function RootComponent() {
  return (
    <MessageProvider>
      <DatabaseProvider>
        <AdminModeProvider>
          <RootLayout />
        </AdminModeProvider>
      </DatabaseProvider>
    </MessageProvider>
  );
}

function RootLayout() {
  const { isAdminMode, setAdminMode } = useAdminMode();
  const { database, databases, setDatabase } = useDatabase();

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
                label: <Link to="/" search={{ database }}>Home</Link>,
              },
              {
                key: 'maintenance',
                label: <Link to="/maintenance" search={{ database }}>Maintenance</Link>,
              },
              {
                key: 'tasks',
                label: <Link to="/tasks" search={{ database }}>Tasks</Link>,
              },
            ]}
          />
          <Select
            value={database}
            onChange={setDatabase}
            options={databases.map((name) => ({ value: name, label: name }))}
            style={{ minWidth: 200 }}
            placeholder="Database"
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
