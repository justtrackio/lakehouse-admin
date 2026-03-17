import { useMemo } from 'react';
import { Link } from '@tanstack/react-router';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Card, DatePicker, Form, Popconfirm, Select, Space, Table, Tag, Typography } from 'antd';
import type { Dayjs } from 'dayjs';
import type { ColumnsType } from 'antd/es/table';
import {
  fetchTables,
  optimizeTable,
  type ListTableItem,
  type OptimizeChunkBy,
} from '../api/schema';
import { useMessageApi } from '../context/MessageContext';
import { formatBytes, formatNumber } from '../utils/format';

const { Paragraph, Text } = Typography;
const { RangePicker } = DatePicker;

interface OptimizeBatchFormValues {
  table?: string;
  date_range?: [Dayjs, Dayjs];
  chunk_by: OptimizeChunkBy;
}

const optimizeChunkOptions: Array<{ label: string; value: OptimizeChunkBy }> = [
  { label: 'Daily', value: 'daily' },
  { label: 'Weekly', value: 'weekly' },
  { label: 'Monthly', value: 'monthly' },
];

const optimizeTableColumns: ColumnsType<ListTableItem> = [
  {
    title: 'Table Name',
    dataIndex: 'name',
    key: 'name',
    render: (value: string, record: ListTableItem) => (
      <Space direction="vertical" size={0}>
        <Link to="/tables/$tableName/tasks" params={{ tableName: value }}>
          {value}
        </Link>
        {record.partitions.length === 0 ? <Text type="secondary">Unpartitioned table</Text> : null}
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

export function OptimizeBatchContent() {
  const [form] = Form.useForm<OptimizeBatchFormValues>();
  const messageApi = useMessageApi();
  const queryClient = useQueryClient();
  const selectedTableName = Form.useWatch('table', form);

  const {
    data: tables = [],
    isLoading,
    isError,
    error,
  } = useQuery({
    queryKey: ['tables'],
    queryFn: fetchTables,
  });

  const selectedTable = useMemo(
    () => tables.find((table) => table.name === selectedTableName),
    [selectedTableName, tables],
  );

  const mutation = useMutation({
    mutationFn: (values: OptimizeBatchFormValues) => {
      const [from, to] = values.date_range ?? [];

      return optimizeTable(
        values.table ?? '',
        128,
        from?.format('YYYY-MM-DD'),
        to?.format('YYYY-MM-DD'),
        values.chunk_by,
      );
    },
    onSuccess: (data, values) => {
      const count = data.task_ids.length;
      queryClient.invalidateQueries({ queryKey: ['tasks'] });
      queryClient.invalidateQueries({ queryKey: ['tasks', values.table] });
      queryClient.invalidateQueries({ queryKey: ['taskCounts'] });

      messageApi.success(
        `Enqueued ${count} optimize task${count === 1 ? '' : 's'} for ${values.table} using ${values.chunk_by} chunks`
      );
    },
    onError: (mutationError: Error) => {
      messageApi.error(`Failed to enqueue optimize tasks: ${mutationError.message}`);
    },
  });

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
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Card title="Optimize Table Range" size="small">
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
          <Paragraph style={{ marginBottom: 0 }}>
            Optimize enqueues one or more rewrite tasks for a single table. Pick a date range and choose whether to split the work into daily, weekly, or monthly task windows.
          </Paragraph>

          <Form<OptimizeBatchFormValues>
            form={form}
            layout="vertical"
            initialValues={{
              chunk_by: 'daily',
            }}
            disabled={mutation.isPending}
          >
            <Space direction="horizontal" size="large" style={{ width: '100%' }} align="start" wrap>
              <Form.Item
                label="Table"
                name="table"
                rules={[{ required: true, message: 'Please select a table' }]}
                style={{ minWidth: 280, flex: '1 1 280px', marginBottom: 0 }}
              >
                <Select
                  showSearch
                  placeholder="Select one table"
                  optionFilterProp="label"
                  loading={isLoading}
                  options={tables.map((table) => ({
                    label: table.name,
                    value: table.name,
                  }))}
                />
              </Form.Item>

              <Form.Item
                label="Date Range"
                name="date_range"
                rules={[{ required: true, message: 'Please select a date range' }]}
                extra="Only partitions inside this range will be considered."
                style={{ minWidth: 320, flex: '1 1 320px', marginBottom: 0 }}
              >
                <RangePicker allowClear={false} />
              </Form.Item>

              <Form.Item
                label="Chunking"
                name="chunk_by"
                rules={[{ required: true, message: 'Please select a chunking mode' }]}
                extra="Each chunk becomes a separate optimize task."
                style={{ minWidth: 220, marginBottom: 0 }}
              >
                <Select options={optimizeChunkOptions} />
              </Form.Item>
            </Space>

            <div style={{ marginTop: 16 }}>
              <Popconfirm
                title="Queue optimize tasks"
                description={selectedTableName
                  ? `Enqueue optimize tasks for ${selectedTableName}?`
                  : 'Select a table and date range before queuing optimize tasks.'}
                onConfirm={() => {
                  void form.validateFields().then((values) => {
                    mutation.mutate(values);
                  });
                }}
                okText="Yes, enqueue"
                cancelText="Cancel"
                disabled={mutation.isPending}
              >
                <Button type="primary" loading={mutation.isPending}>
                  Queue Optimize Tasks
                </Button>
              </Popconfirm>
            </div>
          </Form>
        </Space>
      </Card>

      <Card title="Selected Table" size="small">
        {selectedTable ? (
          <Table<ListTableItem>
            rowKey={(record) => record.name}
            columns={optimizeTableColumns}
            dataSource={[selectedTable]}
            pagination={false}
            size="small"
          />
        ) : (
          <Paragraph style={{ marginBottom: 0 }}>
            Choose a table to review its current optimization status before creating tasks.
          </Paragraph>
        )}
      </Card>
    </Space>
  );
}
