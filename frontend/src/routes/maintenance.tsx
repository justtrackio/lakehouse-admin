import { createFileRoute, Link, Outlet, redirect, useRouterState } from '@tanstack/react-router';
import { Card, Space, Tabs, Typography } from 'antd';

const { Paragraph, Title } = Typography;

export const Route = createFileRoute('/maintenance')({
  beforeLoad: ({ location }) => {
    if (location.pathname === '/maintenance' || location.pathname === '/maintenance/') {
      throw redirect({ to: '/maintenance/expire-snapshots' });
    }
  },
  component: MaintenanceLayout,
});

function MaintenanceLayout() {
  const navigate = Route.useNavigate();
  const pathname = useRouterState({ select: (state) => state.location.pathname });

  const activeTab = pathname.includes('/remove-orphan-files')
    ? 'remove-orphan-files'
    : pathname.includes('/optimize')
      ? 'optimize'
    : 'expire-snapshots';

  const handleTabChange = (key: string) => {
    if (key === 'remove-orphan-files') {
      navigate({ to: '/maintenance/remove-orphan-files' });
      return;
    }

    if (key === 'optimize') {
      navigate({ to: '/maintenance/optimize' });
      return;
    }

    navigate({ to: '/maintenance/expire-snapshots' });
  };

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Card>
        <Title level={2} style={{ marginBottom: 8 }}>Maintenance</Title>
        <Paragraph style={{ marginBottom: 0 }}>
          Run maintenance tasks across multiple tables. Each task type now has its own route so you can link directly to snapshot expiration, orphan file cleanup, or optimize planning.
        </Paragraph>
      </Card>
      <Card title="Global Maintenance" extra={<Link to="/tasks">View Task Queue</Link>}>
        <Space direction="vertical" size="large" style={{ width: '100%' }}>
          <Paragraph style={{ marginBottom: 0 }}>
            Queue maintenance tasks across multiple tables. Switching tabs updates the URL and loads the matching task configuration.
          </Paragraph>
          <Tabs
            activeKey={activeTab}
            onChange={handleTabChange}
            items={[
              {
                key: 'expire-snapshots',
                label: 'Expire Snapshots',
              },
              {
                key: 'remove-orphan-files',
                label: 'Remove Orphan Files',
              },
              {
                key: 'optimize',
                label: 'Optimize',
              },
            ]}
          />
          <Outlet />
        </Space>
      </Card>
    </Space>
  );
}
