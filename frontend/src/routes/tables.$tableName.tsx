import { createFileRoute, Link, Outlet, useRouterState } from '@tanstack/react-router';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Alert,
  Breadcrumb,
  Button,
  Card,
  Space,
  Spin,
  Tabs,
  Typography,
} from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import { fetchTableDetails, TableDetails, refreshTable } from '../api/schema';
import { formatNumber, formatBytes, formatDateTime } from '../utils/format';
import { useMessageApi } from '../context/MessageContext';

const { Title, Text } = Typography;

export const Route = createFileRoute('/tables/$tableName')({
  component: TableLayout,
});

function TableLayout() {
  const { tableName } = Route.useParams();
  const navigate = Route.useNavigate();
  const routerState = useRouterState();
  const pathname = routerState.location.pathname;
  const messageApi = useMessageApi();
  const queryClient = useQueryClient();

  const {
    data: table,
    isLoading,
    isError,
    error,
  } = useQuery<TableDetails, Error>({
    queryKey: ['table', tableName],
    queryFn: () => fetchTableDetails(tableName),
  });

  const refreshTableMutation = useMutation({
    mutationFn: () => refreshTable(tableName),
    onSuccess: () => {
      messageApi.success(`Successfully refreshed table ${tableName}`);
      queryClient.invalidateQueries({ queryKey: ['table', tableName] });
      queryClient.invalidateQueries({ queryKey: ['partitions', tableName] });
      queryClient.invalidateQueries({ queryKey: ['snapshots', tableName] });
      queryClient.invalidateQueries({ queryKey: ['tables'] }); // Also update the main list
    },
    onError: (error: Error) => {
      messageApi.error(`Failed to refresh table: ${error.message}`);
    },
  });

  if (isLoading) {
    return (
      <div style={{ textAlign: 'center', padding: '24px 0' }}>
        <Spin size="large" />
        <div style={{ marginTop: 8 }}>Loading table information...</div>
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

  const activeTab = pathname.includes('/snapshots') 
    ? 'snapshots' 
    : pathname.includes('/tasks') 
      ? 'tasks' 
      : 'partitions';

  const tabItems = [
    {
      key: 'partitions',
      label: 'Partitions',
    },
    {
      key: 'snapshots',
      label: 'Snapshots',
    },
    {
      key: 'tasks',
      label: 'Tasks',
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
    } else if (key === 'tasks') {
      navigate({
        to: '/tables/$tableName/tasks',
        params: { tableName },
      });
    }
  };

  return (
    <div style={{ margin: '0 auto' }}>
      <Space direction="vertical" style={{ width: '100%' }} size="large">
        <Breadcrumb items={breadcrumbItems} />

        <Card>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
            <Title level={3} style={{ marginBottom: 8, marginTop: 0 }}>
              {table.name}
            </Title>
            <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
              <Text type="secondary" style={{ fontSize: '14px' }}>
                Last refreshed: {table.updated_at ? formatDateTime(table.updated_at) : '-'}
              </Text>
              <Button
                icon={<ReloadOutlined />}
                onClick={() => refreshTableMutation.mutate()}
                loading={refreshTableMutation.isPending}
              >
                Refresh Table
              </Button>
            </div>
          </div>
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
