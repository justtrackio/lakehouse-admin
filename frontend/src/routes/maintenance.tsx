import { createFileRoute } from '@tanstack/react-router';
import { Card, Space, Typography } from 'antd';
import { GlobalMaintenanceTable } from '../components/GlobalMaintenanceTable';

const { Paragraph, Title } = Typography;

export const Route = createFileRoute('/maintenance')({
  component: MaintenancePage,
});

function MaintenancePage() {
  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Card>
        <Title level={2} style={{ marginBottom: 8 }}>Maintenance</Title>
        <Paragraph style={{ marginBottom: 0 }}>
          Run maintenance tasks across multiple tables. Use the dedicated tabs to queue snapshot expiration or orphan file cleanup in batches.
        </Paragraph>
      </Card>
      <GlobalMaintenanceTable />
    </Space>
  );
}
