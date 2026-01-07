import { createFileRoute, Link } from '@tanstack/react-router';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Card, Typography, Table, Spin, Alert, Button } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { fetchTables, ListTableItem, refreshFull } from '../api/schema';
import { formatNumber, formatBytes } from '../utils/format';
import { useMessageApi } from '../context/MessageContext';

const { Title, Paragraph } = Typography;

export const Route = createFileRoute('/')({
  component: IndexComponent,
});
function IndexComponent() {
  const queryClient = useQueryClient();
  const messageApi = useMessageApi();
  const {
    data: tables,
    isLoading,
    isError,
    error,
  } = useQuery<ListTableItem[], Error>({
    queryKey: ['tables'],
    queryFn: fetchTables,
  });

  const refreshMutation = useMutation({
    mutationFn: refreshFull,
    onSuccess: () => {
      messageApi.success('Full refresh completed successfully');
      queryClient.invalidateQueries({ queryKey: ['tables'] });
    },
    onError: (error: Error) => {
      messageApi.error(`Full refresh failed: ${error.message}`);
    },
  });

  const columns: ColumnsType<ListTableItem> = [
    {
      title: 'Table Name',
      dataIndex: 'name',
      key: 'name',
      render: (_value, record) => {
        const hasPartitions = record.partitions && record.partitions.length > 0;

        if (!hasPartitions) {
          return record.name;
        }

        // Like clicking a bucket name in S3:
        // enter at the first partition level with no filters
        return (
          <Link
            to="/tables/$tableName/partitions"
            params={{ tableName: record.name }}
            search={{}}
          >
            {record.name}
          </Link>
        );
      },
    },
    {
      title: 'Snapshots',
      dataIndex: 'snapshot_count',
      key: 'snapshot_count',
      align: 'right',
      render: (value: number) => formatNumber(value),
    },
    {
      title: 'Partitions',
      dataIndex: 'partition_count',
      key: 'partition_count',
      align: 'right',
      render: (value: number) => formatNumber(value),
    },
    {
      title: 'Files',
      dataIndex: 'file_count',
      key: 'file_count',
      align: 'right',
      render: (value: number) => formatNumber(value),
    },
    {
      title: 'Records',
      dataIndex: 'record_count',
      key: 'record_count',
      align: 'right',
      render: (value: number) => formatNumber(value),
    },
    {
      title: 'Total Size',
      dataIndex: 'total_data_file_size_in_bytes',
      key: 'total_data_file_size_in_bytes',
      align: 'right',
      render: (value: number) => formatBytes(value),
    },
  ];

  return (
    <Card>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 16 }}>
        <div>
          <Title level={2} style={{ marginBottom: 8 }}>Lakehouse Tables</Title>
          <Paragraph style={{ marginBottom: 0 }}>
            Below is the list of tables discovered in your lakehouse.
          </Paragraph>
        </div>
        <Button
          type="primary"
          onClick={() => refreshMutation.mutate()}
          loading={refreshMutation.isPending}
        >
          Full Refresh
        </Button>
      </div>

      {isLoading && (
        <div style={{ textAlign: 'center', padding: '24px 0' }}>
          <Spin size="large" />
          <div style={{ marginTop: 8 }}>Loading tables...</div>
        </div>
      )}

      {isError && (
        <Alert
          type="error"
          showIcon
          message="Failed to load tables"
          description={error.message}
          style={{ marginTop: 16 }}
        />
      )}

      {!isLoading && !isError && (
        <Table<ListTableItem>
          style={{ marginTop: 16 }}
          rowKey={(record) => record.name}
          columns={columns}
          dataSource={tables}
          pagination={false}
        />
      )}
    </Card>
  );
}
