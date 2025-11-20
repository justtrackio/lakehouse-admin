import { createFileRoute, Link, Outlet, useRouterState } from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import {
  Alert,
  Breadcrumb,
  Card,
  Space,
  Spin,
  Tabs,
  Typography,
} from 'antd';
import { fetchTableDetails, TableDetails } from '../api/schema';
import { formatNumber, formatBytes } from '../utils/format';

const { Title, Text } = Typography;

export const Route = createFileRoute('/tables/$tableName')({
  component: TableLayout,
});

function TableLayout() {
  const { tableName } = Route.useParams();
  const navigate = Route.useNavigate();
  const routerState = useRouterState();
  const pathname = routerState.location.pathname;

  const {
    data: table,
    isLoading,
    isError,
    error,
  } = useQuery<TableDetails, Error>({
    queryKey: ['table', tableName],
    queryFn: () => fetchTableDetails(tableName),
  });

  if (isLoading) {
    return (
      <div style={{ textAlign: 'center', padding: '24px 0' }}>
        <Spin tip="Loading table information..." />
      </div>
    );
  }

  if (isError) {
    return (
      <Alert
        type="error"
        showIcon
        message="Failed to load table"
        description={error.message}
      />
    );
  }

  if (!table) {
    return (
      <Alert
        type="warning"
        showIcon
        message="Table not found"
        description={`No table named "${tableName}" was found.`}
      />
    );
  }

  const breadcrumbItems = [
    {
      title: <Link to="/">Tables</Link>,
    },
    {
      title: table.name,
    },
  ];

  const activeTab = pathname.includes('/snapshots') ? 'snapshots' : 'partitions';

  const tabItems = [
    {
      key: 'partitions',
      label: 'Partitions',
    },
    {
      key: 'snapshots',
      label: 'Snapshots',
    },
  ];

  const handleTabChange = (key: string) => {
    if (key === 'partitions') {
      navigate({
        to: '/tables/$tableName/partitions',
        params: { tableName },
        search: {},
      });
    } else if (key === 'snapshots') {
      navigate({
        to: '/tables/$tableName/snapshots',
        params: { tableName },
      });
    }
  };

  return (
    <div style={{ margin: '0 auto' }}>
      <Space direction="vertical" style={{ width: '100%' }} size="large">
        <Breadcrumb items={breadcrumbItems} />

        <Card>
          <Title level={3} style={{ marginBottom: 8 }}>
            {table.name}
          </Title>
          <Space direction="horizontal" size="large">
            <Text type="secondary">
              Snapshots: {formatNumber(table.snapshot_count)}
            </Text>
            <Text type="secondary">
              Partitions: {formatNumber(table.partition_count)}
            </Text>
            <Text type="secondary">
              Files: {formatNumber(table.file_count)}
            </Text>
            <Text type="secondary">
              Records: {formatNumber(table.record_count)}
            </Text>
            <Text type="secondary">
              Size: {formatBytes(table.total_data_file_size_in_bytes)}
            </Text>
          </Space>
        </Card>

        <Card>
          <Tabs
            activeKey={activeTab}
            items={tabItems}
            onChange={handleTabChange}
          />
          <div style={{ marginTop: 16 }}>
            <Outlet />
          </div>
        </Card>

        <Link to="/">← Back to tables</Link>
      </Space>
    </div>
  );
}
