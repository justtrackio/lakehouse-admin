import { createRootRoute, Link, Outlet } from '@tanstack/react-router';
import { TanStackRouterDevtools } from '@tanstack/router-devtools';
import { Layout, Menu } from 'antd';
import { MessageProvider } from '../components/MessageProvider';
import { TaskStatusIndicator } from '../components/TaskStatusIndicator';

const { Header, Content, Footer } = Layout;

export const Route = createRootRoute({
  component: RootComponent,
});

function RootComponent() {
  return (
    <MessageProvider>
      <Layout style={{ minHeight: '100vh' }}>
        <Header style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <div style={{ color: 'white', fontSize: '20px', fontWeight: 'bold' }}>
            <Link to="/">Lakehouse Admin</Link>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', flex: 1, minWidth: 0 }}>
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
        <TanStackRouterDevtools />
      </Layout>
    </MessageProvider>
  );
}
