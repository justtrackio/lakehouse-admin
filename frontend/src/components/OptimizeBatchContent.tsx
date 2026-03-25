import { useMemo, useState } from 'react';
import type { Key } from 'react';
import { Link } from '@tanstack/react-router';
import { useMutation } from '@tanstack/react-query';
import { Alert, Button, Card, DatePicker, Form, Popconfirm, Select, Slider, Space, Table, Typography } from 'antd';
import type { Dayjs } from 'dayjs';
import type { ColumnsType } from 'antd/es/table';
import {
  batchOptimize,
  type BatchOptimizeTableRequest,
  type OptimizeChunkBy,
} from '../api/schema';
import { useMessageApi } from '../context/MessageContext';
import { MaintenanceTaskContent } from './MaintenanceTaskContent';
import { useMaintenanceBatchSuccess } from './useMaintenanceBatchSuccess';

const { Paragraph } = Typography;
const { RangePicker } = DatePicker;

interface OptimizeBatchFormValues {
  date_range?: [Dayjs, Dayjs];
  target_file_size_mb: number;
}

interface SelectedOptimizeTableConfig {
  table: string;
  chunk_by: OptimizeChunkBy;
}

const config = {
  description: 'Queue optimize tasks for multiple tables at once. All selected tables share one date range, while each table can use its own chunking strategy.',
  selectAllLabel: 'Select All',
  selectionLabel: 'selected',
};

const optimizeChunkOptions: Array<{ label: string; value: OptimizeChunkBy }> = [
  { label: 'Daily', value: 'daily' },
  { label: 'Weekly', value: 'weekly' },
  { label: 'Monthly', value: 'monthly' },
];

export function OptimizeBatchContent() {
  const [selectedRowKeys, setSelectedRowKeys] = useState<Key[]>([]);
  const [tableConfigs, setTableConfigs] = useState<Record<string, OptimizeChunkBy>>({});
  const [form] = Form.useForm<OptimizeBatchFormValues>();
  const messageApi = useMessageApi();
  const handleBatchSuccess = useMaintenanceBatchSuccess();
  const targetFileSize = Form.useWatch('target_file_size_mb', form) ?? 512;

  const selectedTableConfigs = useMemo<SelectedOptimizeTableConfig[]>(
    () => selectedRowKeys.map((key) => {
      const table = String(key);
      return {
        table,
        chunk_by: tableConfigs[table] ?? 'daily',
      };
    }),
    [selectedRowKeys, tableConfigs],
  );

  const bulkChunkByValue = useMemo<OptimizeChunkBy | undefined>(() => {
    if (selectedTableConfigs.length === 0) {
      return undefined;
    }

    const [firstConfig, ...restConfigs] = selectedTableConfigs;

    return restConfigs.every((config) => config.chunk_by === firstConfig.chunk_by)
      ? firstConfig.chunk_by
      : undefined;
  }, [selectedTableConfigs]);

  const mutation = useMutation({
    mutationFn: (values: { tables: BatchOptimizeTableRequest[]; from: string; to: string; target_file_size_mb: number }) =>
      batchOptimize(values.tables, values.target_file_size_mb, values.from, values.to),
    onSuccess: (data, values) => {
      handleBatchSuccess(data, values.tables.length, 'optimize');
      setSelectedRowKeys([]);
      setTableConfigs({});
      form.resetFields();
    },
    onError: (error: Error) => {
      messageApi.error(`Failed to enqueue optimize tasks: ${error.message}`);
    },
  });

  const handleSelectedRowKeysChange = (keys: Key[]) => {
    const selectedTables = keys.map((key) => String(key));

    setSelectedRowKeys(keys);
    setTableConfigs((current) => {
      const next: Record<string, OptimizeChunkBy> = {};

      for (const table of selectedTables) {
        next[table] = current[table] ?? 'daily';
      }

      return next;
    });
  };

  const handleBulkChunkByChange = (chunkBy: OptimizeChunkBy) => {
    setTableConfigs((current) => {
      const next = { ...current };

      for (const key of selectedRowKeys) {
        next[String(key)] = chunkBy;
      }

      return next;
    });
  };

  const selectedTableColumns: ColumnsType<SelectedOptimizeTableConfig> = [
    {
      title: 'Table',
      dataIndex: 'table',
      key: 'table',
      render: (value: string) => (
        <Link to="/tables/$tableName/tasks" params={{ tableName: value }}>
          {value}
        </Link>
      ),
    },
    {
      title: 'Chunking',
      dataIndex: 'chunk_by',
      key: 'chunk_by',
      render: (value: OptimizeChunkBy, record: SelectedOptimizeTableConfig) => (
        <Select
          value={value}
          options={optimizeChunkOptions}
          style={{ minWidth: 180 }}
          disabled={mutation.isPending}
          onChange={(chunkBy) => {
            setTableConfigs((current) => ({
              ...current,
              [record.table]: chunkBy,
            }));
          }}
        />
      ),
    },
    {
      title: 'Action',
      key: 'action',
      render: (_, record: SelectedOptimizeTableConfig) => (
        <Button
          type="link"
          danger
          disabled={mutation.isPending}
          onClick={() => {
            const nextKeys = selectedRowKeys.filter((key) => String(key) !== record.table);
            handleSelectedRowKeysChange(nextKeys);
          }}
        >
          Remove
        </Button>
      ),
    },
  ];

  return (
    <MaintenanceTaskContent
      config={config}
      selectedRowKeys={selectedRowKeys}
      onSelectedRowKeysChange={handleSelectedRowKeysChange}
      isSubmitting={mutation.isPending}
      formContent={(selectedTableNames) => (
        <Card title="Batch Optimize" size="small">
          <Space direction="vertical" size="large" style={{ width: '100%' }}>
            <Paragraph style={{ marginBottom: 0 }}>
              Select the tables you want to optimize, choose one shared date range and target file size, and set chunking in bulk or per table before enqueuing tasks.
            </Paragraph>

            <Form<OptimizeBatchFormValues>
              form={form}
              layout="vertical"
              initialValues={{
                target_file_size_mb: 512,
              }}
              disabled={mutation.isPending}
            >
              <Form.Item
                label={`Target File Size (MB): ${targetFileSize}`}
                name="target_file_size_mb"
                rules={[
                  { required: true, message: 'Please input target file size' },
                  { type: 'number', min: 1, message: 'Minimum target size is 1 MB' },
                ]}
                extra="Rewritten files across all selected tables will target approximately this size."
                style={{ maxWidth: 400, marginBottom: 16 }}
              >
                <Slider min={512} max={5120} marks={{ 512: '512MB', 1024: '1GB', 2048: '2GB', 5120: '5GB' }} />
              </Form.Item>

              <Form.Item
                label="Date Range"
                name="date_range"
                rules={[{ required: true, message: 'Please select a date range' }]}
                extra="Only partitions inside this range will be considered for every selected table."
                style={{ maxWidth: 360, marginBottom: 0 }}
              >
                <RangePicker allowClear={false} />
              </Form.Item>

              <div style={{ marginTop: 16 }}>
                {selectedTableConfigs.length > 0 ? (
                  <Space direction="vertical" size="middle" style={{ width: '100%' }}>
                    <Form.Item
                      label="Chunking for all selected tables"
                      extra="This updates every selected table immediately. You can still override individual tables below."
                      style={{ maxWidth: 360, marginBottom: 0 }}
                    >
                      <Select
                        value={bulkChunkByValue}
                        options={optimizeChunkOptions}
                        placeholder="Mixed"
                        disabled={mutation.isPending}
                        onChange={handleBulkChunkByChange}
                      />
                    </Form.Item>

                    <Table<SelectedOptimizeTableConfig>
                      rowKey={(record) => record.table}
                      columns={selectedTableColumns}
                      dataSource={selectedTableConfigs}
                      pagination={false}
                      size="small"
                    />
                  </Space>
                ) : (
                  <Alert
                    type="info"
                    showIcon
                    message="Select one or more tables"
                    description="Choose tables from the list below to configure chunking in bulk or per table before you enqueue optimize tasks."
                  />
                )}
              </div>

              <div style={{ marginTop: 16 }}>
                <Popconfirm
                  title="Queue optimize tasks"
                  description={selectedTableNames.length > 0
                    ? `Enqueue optimize tasks for ${selectedTableNames.length} selected table${selectedTableNames.length === 1 ? '' : 's'}?`
                    : 'Select at least one table and a date range before queuing optimize tasks.'}
                  onConfirm={() => {
                    void form.validateFields().then((values) => {
                      const [from, to] = values.date_range ?? [];
                      if (!from || !to) {
                        return;
                      }

                      mutation.mutate({
                        tables: selectedTableConfigs,
                        from: from.format('YYYY-MM-DD'),
                        to: to.format('YYYY-MM-DD'),
                        target_file_size_mb: values.target_file_size_mb,
                      });
                    });
                  }}
                  okText="Yes, enqueue"
                  cancelText="Cancel"
                  disabled={selectedTableConfigs.length === 0 || mutation.isPending}
                >
                  <Button type="primary" loading={mutation.isPending} disabled={selectedTableConfigs.length === 0}>
                    Queue Optimize Tasks for Selected Tables
                  </Button>
                </Popconfirm>
              </div>
            </Form>
          </Space>
        </Card>
      )}
    />
  );
}
