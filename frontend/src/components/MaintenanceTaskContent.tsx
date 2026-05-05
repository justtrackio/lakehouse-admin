import { useMemo } from 'react';
import type { Key, ReactNode } from 'react';
import { Link } from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import { Alert, Button, Space, Table, Tag, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { TableRowSelection } from 'antd/es/table/interface';
import {
  fetchTables,
  type ListTableItem,
} from '../api/schema';
import { formatBytes, formatNumber } from '../utils/format';

const { Paragraph, Text } = Typography;

interface BatchActionConfig {
  description: string;
  selectAllLabel: string;
  selectionLabel: string;
}

interface MaintenanceTaskContentProps {
  database: string;
  config: BatchActionConfig;
  selectedRowKeys: Key[];
  onSelectedRowKeysChange: (keys: Key[]) => void;
  isSubmitting: boolean;
  formContent: (selectedTableNames: string[]) => ReactNode;
}

export function MaintenanceTaskContent({
  database,
  config,
  selectedRowKeys,
  onSelectedRowKeysChange,
  isSubmitting,
  formContent,
}: MaintenanceTaskContentProps) {
  const {
    data: tables = [],
    isLoading,
    isError,
    error,
  } = useQuery({
    queryKey: ['tables', database],
    queryFn: () => fetchTables(database),
  });

  const columns: ColumnsType<ListTableItem> = [
    {
      title: 'Table Name',
      dataIndex: 'name',
      key: 'name',
      render: (value: string, record: ListTableItem) => (
        <Space direction="vertical" size={0}>
          <Link to="/tables/$tableName/tasks" params={{ tableName: value }} search={{ database }}>
            {value}
          </Link>
          {record.partitions.length === 0 && (
            <Text type="secondary">Unpartitioned table</Text>
          )}
        </Space>
      ),
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
      title: 'Size',
      dataIndex: 'total_data_file_size_in_bytes',
      key: 'total_data_file_size_in_bytes',
      align: 'right',
      render: (value: number) => formatBytes(value),
    },
    {
      title: 'Optimization',
      dataIndex: 'needs_optimize',
      key: 'needs_optimize',
      render: (value: boolean) => value ? <Tag color="warning">Needs Optimization</Tag> : <Tag color="success">Healthy</Tag>,
    },
  ];

  if (isError) {
    return (
      <Alert
        type="error"
        showIcon
        message="Failed to load tables"
        description={error instanceof Error ? error.message : 'Unknown error'}
      />
    );
  }

  return (
      <BatchTaskLayout
        tables={tables}
        columns={columns}
        isLoading={isLoading}
      selectedRowKeys={selectedRowKeys}
      onSelectedRowKeysChange={onSelectedRowKeysChange}
      isSubmitting={isSubmitting}
      config={config}
      formContent={formContent}
    />
  );
}

interface BatchTaskLayoutProps {
  tables: ListTableItem[];
  columns: ColumnsType<ListTableItem>;
  isLoading: boolean;
  selectedRowKeys: Key[];
  onSelectedRowKeysChange: (keys: Key[]) => void;
  isSubmitting: boolean;
  config: BatchActionConfig;
  formContent: (selectedTableNames: string[]) => ReactNode;
}

function BatchTaskLayout({
  tables,
  columns,
  isLoading,
  selectedRowKeys,
  onSelectedRowKeysChange,
  isSubmitting,
  config,
  formContent,
}: BatchTaskLayoutProps) {
  const allTableNames = useMemo(
    () => tables.map((table) => table.name),
    [tables],
  );

  const selectedTableNames = useMemo(
    () => selectedRowKeys.map((key) => String(key)),
    [selectedRowKeys],
  );

  const rowSelection: TableRowSelection<ListTableItem> = {
    selectedRowKeys,
    onChange: onSelectedRowKeysChange,
    getCheckboxProps: () => ({
      disabled: isSubmitting,
    }),
  };

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Space direction="vertical" size={4} style={{ width: '100%' }}>
        <Paragraph style={{ marginBottom: 0 }}>{config.description}</Paragraph>
        <Space wrap>
          <Button onClick={() => onSelectedRowKeysChange(allTableNames)} disabled={allTableNames.length === 0 || isSubmitting}>
            {config.selectAllLabel}
          </Button>
          <Button onClick={() => onSelectedRowKeysChange([])} disabled={selectedRowKeys.length === 0 || isSubmitting}>
            Clear Selection
          </Button>
          <Text type="secondary">
            {selectedTableNames.length} {config.selectionLabel}
          </Text>
        </Space>
      </Space>

      {formContent(selectedTableNames)}

      <Table<ListTableItem>
        rowKey={(record) => record.name}
        columns={columns}
        dataSource={tables}
        rowSelection={rowSelection}
        loading={isLoading}
        pagination={false}
        size="small"
      />
    </Space>
  );
}
