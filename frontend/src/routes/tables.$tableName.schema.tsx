import { createFileRoute } from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import { Alert, Space, Spin, Table, Tag, Tooltip, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { fetchTableSchema, type TableSchemaColumn } from '../api/schema';
import { formatSchemaType } from '../utils/format';

const { Text } = Typography;

export const Route = createFileRoute('/tables/$tableName/schema')({
  component: TableSchemaPage,
});

function TableSchemaPage() {
  const { tableName } = Route.useParams();

  const {
    data: tableSchema,
    isLoading,
    isError,
    error,
  } = useQuery({
    queryKey: ['tableSchema', tableName],
    queryFn: () => fetchTableSchema(tableName),
  });

  if (isLoading) {
    return (
      <div style={{ textAlign: 'center', padding: '24px 0' }}>
        <Spin size="large" />
        <div style={{ marginTop: 8 }}>Loading schema...</div>
      </div>
    );
  }

  if (isError) {
    return (
      <Alert
        type="error"
        showIcon
        message="Failed to load schema"
        description={error.message}
      />
    );
  }

  if (!tableSchema) {
    return (
      <Alert
        type="warning"
        showIcon
        message="Schema not found"
        description={`No schema information was found for table "${tableName}".`}
      />
    );
  }

  const columns: ColumnsType<TableSchemaColumn> = [
    {
      title: 'Column',
      dataIndex: 'name',
      key: 'name',
      render: (value: string) => <Text strong>{value}</Text>,
      width: '35%',
    },
    {
      title: 'Type',
      dataIndex: 'type',
      key: 'type',
      render: (value: string) => (
        <Tooltip title={value}>
          <Typography.Text
            style={{
              whiteSpace: 'pre-wrap',
              fontFamily: 'Monaco, Menlo, Consolas, monospace',
              display: 'block',
            }}
          >
            {formatSchemaType(value)}
          </Typography.Text>
        </Tooltip>
      ),
    },
  ];

  const partitionNames = new Set(tableSchema.partitions.map((partition) => partition.name));

  return (
    <Space direction="vertical" style={{ width: '100%' }} size="large">
      {tableSchema.columns.length === 0 ? (
        <Alert
          type="info"
          showIcon
          message="No columns"
          description="No column metadata is available for this table."
        />
      ) : (
        <Table<TableSchemaColumn>
          rowKey={(row) => row.name}
          columns={[
            ...columns,
            {
              title: 'Role',
              key: 'role',
              width: 160,
              render: (_, record) => (
                partitionNames.has(record.name) ? <Tag color="blue">Partition</Tag> : <Text type="secondary">Data</Text>
              ),
            },
          ]}
          dataSource={tableSchema.columns}
          pagination={false}
          scroll={{ x: true }}
        />
      )}
    </Space>
  );
}
