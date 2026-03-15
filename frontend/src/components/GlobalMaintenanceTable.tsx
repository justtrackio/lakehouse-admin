import { useMemo, useState } from 'react';
import type { Key, ReactNode } from 'react';
import { Link } from '@tanstack/react-router';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  Alert,
  Button,
  Card,
  Form,
  Popconfirm,
  Slider,
  Space,
  Table,
  Tabs,
  Tag,
  Typography,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { TableRowSelection } from 'antd/es/table/interface';
import {
  batchExpireSnapshots,
  batchRemoveOrphanFiles,
  fetchTables,
  type BatchTaskQueuedResponse,
  type ListTableItem,
} from '../api/schema';
import { useMessageApi } from '../context/MessageContext';
import { formatBytes, formatNumber } from '../utils/format';

const { Paragraph, Text } = Typography;

type MaintenanceTabKey = 'expire-snapshots' | 'remove-orphan-files';

interface BatchActionConfig {
  key: MaintenanceTabKey;
  title: string;
  description: string;
  selectAllLabel: string;
  selectionLabel: string;
}

const actionConfigs: Record<MaintenanceTabKey, BatchActionConfig> = {
  'expire-snapshots': {
    key: 'expire-snapshots',
    title: 'Expire Snapshots',
    description: 'Expire old snapshots in batch across multiple tables.',
    selectAllLabel: 'Select All',
    selectionLabel: 'selected',
  },
  'remove-orphan-files': {
    key: 'remove-orphan-files',
    title: 'Remove Orphan Files',
    description: 'Queue orphan file cleanup for multiple tables at once.',
    selectAllLabel: 'Select All',
    selectionLabel: 'selected',
  },
};

export function GlobalMaintenanceTable() {
  const queryClient = useQueryClient();
  const messageApi = useMessageApi();
  const [activeTab, setActiveTab] = useState<MaintenanceTabKey>('expire-snapshots');
  const [expireSelectedRowKeys, setExpireSelectedRowKeys] = useState<Key[]>([]);
  const [removeSelectedRowKeys, setRemoveSelectedRowKeys] = useState<Key[]>([]);

  const { data: tables = [], isLoading, isError, error } = useQuery({
    queryKey: ['tables'],
    queryFn: fetchTables,
  });

  const expireMutation = useMutation({
    mutationFn: (values: { tables: string[]; retention_days: number; retain_last: number }) =>
      batchExpireSnapshots(values.tables, values.retention_days, values.retain_last),
    onSuccess: (data, values) => {
      handleBatchSuccess(data, values.tables.length, 'expire snapshots');
      setExpireSelectedRowKeys([]);
    },
    onError: (mutationError: Error) => {
      messageApi.error(`Failed to enqueue expire snapshots tasks: ${mutationError.message}`);
    },
  });

  const removeMutation = useMutation({
    mutationFn: (values: { tables: string[]; retention_days: number }) =>
      batchRemoveOrphanFiles(values.tables, values.retention_days),
    onSuccess: (data, values) => {
      handleBatchSuccess(data, values.tables.length, 'remove orphan files');
      setRemoveSelectedRowKeys([]);
    },
    onError: (mutationError: Error) => {
      messageApi.error(`Failed to enqueue remove orphan files tasks: ${mutationError.message}`);
    },
  });

  const columns: ColumnsType<ListTableItem> = [
    {
      title: 'Table Name',
      dataIndex: 'name',
      key: 'name',
      render: (value: string, record) => (
        <Space direction="vertical" size={0}>
          <Link to="/tables/$tableName/tasks" params={{ tableName: value }}>
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

  const handleBatchSuccess = (data: BatchTaskQueuedResponse, requestedCount: number, actionLabel: string) => {
    queryClient.invalidateQueries({ queryKey: ['tasks'] });
    queryClient.invalidateQueries({ queryKey: ['taskCounts'] });

    if (data.failed_tables.length === 0) {
      messageApi.success(`Enqueued ${data.enqueued_count} ${actionLabel} task${data.enqueued_count === 1 ? '' : 's'} for ${requestedCount} table${requestedCount === 1 ? '' : 's'}`);
      return;
    }

    const failedPreview = data.failed_tables.slice(0, 3).map((failure) => failure.table).join(', ');
    messageApi.warning(
      `Enqueued ${data.enqueued_count} ${actionLabel} task${data.enqueued_count === 1 ? '' : 's'} with ${data.failed_tables.length} failure${data.failed_tables.length === 1 ? '' : 's'} (${failedPreview}${data.failed_tables.length > 3 ? ', ...' : ''})`
    );
  };

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
    <Card title="Global Maintenance" extra={<Link to="/tasks">View Task Queue</Link>}>
      <Space direction="vertical" size="large" style={{ width: '100%' }}>
        <Paragraph style={{ marginBottom: 0 }}>
          Queue maintenance tasks across multiple tables. Each tab keeps its own table selection so you can prepare different batches independently.
        </Paragraph>
        <Tabs
          activeKey={activeTab}
          onChange={(key) => setActiveTab(key as MaintenanceTabKey)}
          items={[
            {
              key: 'expire-snapshots',
              label: 'Expire Snapshots',
              children: (
                <ExpireSnapshotsBatchTab
                  tables={tables}
                  columns={columns}
                  isLoading={isLoading}
                  selectedRowKeys={expireSelectedRowKeys}
                  onSelectedRowKeysChange={setExpireSelectedRowKeys}
                  isSubmitting={expireMutation.isPending}
                  onSubmit={(values) => expireMutation.mutate(values)}
                />
              ),
            },
            {
              key: 'remove-orphan-files',
              label: 'Remove Orphan Files',
              children: (
                <RemoveOrphanFilesBatchTab
                  tables={tables}
                  columns={columns}
                  isLoading={isLoading}
                  selectedRowKeys={removeSelectedRowKeys}
                  onSelectedRowKeysChange={setRemoveSelectedRowKeys}
                  isSubmitting={removeMutation.isPending}
                  onSubmit={(values) => removeMutation.mutate(values)}
                />
              ),
            },
          ]}
        />
      </Space>
    </Card>
  );
}

interface BatchTabSharedProps {
  tables: ListTableItem[];
  columns: ColumnsType<ListTableItem>;
  isLoading: boolean;
  selectedRowKeys: Key[];
  onSelectedRowKeysChange: (keys: Key[]) => void;
  isSubmitting: boolean;
}

interface ExpireSnapshotsBatchTabProps extends BatchTabSharedProps {
  onSubmit: (values: { tables: string[]; retention_days: number; retain_last: number }) => void;
}

interface RemoveOrphanFilesBatchTabProps extends BatchTabSharedProps {
  onSubmit: (values: { tables: string[]; retention_days: number }) => void;
}

function ExpireSnapshotsBatchTab(props: ExpireSnapshotsBatchTabProps) {
  const config = actionConfigs['expire-snapshots'];
  return (
    <BatchTabLayout
      {...props}
      config={config}
      formContent={(selectedTableNames) => (
        <ExpireSnapshotsBatchForm
          selectedCount={selectedTableNames.length}
          selectedTableNames={selectedTableNames}
          isSubmitting={props.isSubmitting}
          onSubmit={props.onSubmit}
        />
      )}
    />
  );
}

function RemoveOrphanFilesBatchTab(props: RemoveOrphanFilesBatchTabProps) {
  const config = actionConfigs['remove-orphan-files'];
  return (
    <BatchTabLayout
      {...props}
      config={config}
      formContent={(selectedTableNames) => (
        <RemoveOrphanFilesBatchForm
          selectedCount={selectedTableNames.length}
          selectedTableNames={selectedTableNames}
          isSubmitting={props.isSubmitting}
          onSubmit={props.onSubmit}
        />
      )}
    />
  );
}

interface BatchTabLayoutProps extends BatchTabSharedProps {
  config: BatchActionConfig;
  formContent: (selectedTableNames: string[]) => ReactNode;
}

function BatchTabLayout({
  tables,
  columns,
  isLoading,
  selectedRowKeys,
  onSelectedRowKeysChange,
  isSubmitting,
  config,
  formContent,
}: BatchTabLayoutProps) {
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

interface ExpireSnapshotsBatchFormProps {
  selectedCount: number;
  selectedTableNames: string[];
  isSubmitting: boolean;
  onSubmit: (values: { tables: string[]; retention_days: number; retain_last: number }) => void;
}

function ExpireSnapshotsBatchForm({
  selectedCount,
  selectedTableNames,
  isSubmitting,
  onSubmit,
}: ExpireSnapshotsBatchFormProps) {
  const [form] = Form.useForm();
  const retentionDays = Form.useWatch('retention_days', form);
  const retainLast = Form.useWatch('retain_last', form);
  const submitDisabled = selectedCount === 0 || isSubmitting;

  return (
    <Card size="small" title="Batch Expire Snapshots">
      <Form
        form={form}
        layout="vertical"
        initialValues={{ retention_days: 7, retain_last: 10 }}
        disabled={isSubmitting}
      >
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
          <Space size="large" align="start" wrap>
            <div style={{ width: 320 }}>
              <Form.Item
                label={`Retention Period (Days): ${retentionDays}`}
                name="retention_days"
                rules={[
                  { required: true, message: 'Please input retention days!' },
                  { type: 'number', min: 7, message: 'Minimum retention is 7 days' },
                ]}
              >
                <Slider min={7} max={365} marks={{ 7: '7d', 30: '30d', 90: '90d', 365: '1y' }} />
              </Form.Item>
            </div>
            <div style={{ width: 320 }}>
              <Form.Item
                label={`Retain Last (Count): ${retainLast}`}
                name="retain_last"
                rules={[
                  { required: true, message: 'Please input retain count!' },
                  { type: 'number', min: 10, message: 'Must retain at least 10 snapshots' },
                ]}
              >
                <Slider min={10} max={100} marks={{ 10: '10', 25: '25', 50: '50', 100: '100' }} />
              </Form.Item>
            </div>
          </Space>

          <Popconfirm
            title="Queue expire snapshots tasks"
            description={`Enqueue expire snapshots for ${selectedCount} selected table${selectedCount === 1 ? '' : 's'}?`}
            onConfirm={() => {
              void form.validateFields().then((values) => {
                onSubmit({
                  tables: selectedTableNames,
                  retention_days: values.retention_days,
                  retain_last: values.retain_last,
                });
              });
            }}
            okText="Yes, enqueue"
            cancelText="Cancel"
            disabled={submitDisabled}
          >
            <Button type="primary" danger disabled={submitDisabled} loading={isSubmitting}>
              Expire Snapshots for Selected Tables
            </Button>
          </Popconfirm>
        </Space>
      </Form>
    </Card>
  );
}

interface RemoveOrphanFilesBatchFormProps {
  selectedCount: number;
  selectedTableNames: string[];
  isSubmitting: boolean;
  onSubmit: (values: { tables: string[]; retention_days: number }) => void;
}

function RemoveOrphanFilesBatchForm({
  selectedCount,
  selectedTableNames,
  isSubmitting,
  onSubmit,
}: RemoveOrphanFilesBatchFormProps) {
  const [form] = Form.useForm();
  const retentionDays = Form.useWatch('retention_days', form);
  const submitDisabled = selectedCount === 0 || isSubmitting;

  return (
    <Card size="small" title="Batch Remove Orphan Files">
      <Form
        form={form}
        layout="vertical"
        initialValues={{ retention_days: 7 }}
        disabled={isSubmitting}
      >
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
          <div style={{ width: 320 }}>
            <Form.Item
              label={`Retention Period (Days): ${retentionDays}`}
              name="retention_days"
              rules={[
                { required: true, message: 'Please input retention days!' },
                { type: 'number', min: 7, message: 'Minimum retention is 7 days' },
              ]}
            >
              <Slider min={7} max={365} marks={{ 7: '7d', 30: '30d', 90: '90d', 365: '1y' }} />
            </Form.Item>
          </div>

          <Popconfirm
            title="Queue remove orphan files tasks"
            description={`Enqueue remove orphan files for ${selectedCount} selected table${selectedCount === 1 ? '' : 's'}?`}
            onConfirm={() => {
              void form.validateFields().then((values) => {
                onSubmit({
                  tables: selectedTableNames,
                  retention_days: values.retention_days,
                });
              });
            }}
            okText="Yes, enqueue"
            cancelText="Cancel"
            disabled={submitDisabled}
          >
            <Button type="primary" danger disabled={submitDisabled} loading={isSubmitting}>
              Remove Orphan Files for Selected Tables
            </Button>
          </Popconfirm>
        </Space>
      </Form>
    </Card>
  );
}
