import { useEffect, useRef, useState } from 'react';
import { createFileRoute } from '@tanstack/react-router';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  Alert,
  Button,
  Popconfirm,
  Space,
  Spin,
  Table,
  Tag,
  Typography,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { TablePaginationConfig } from 'antd/es/table';
import {
  fetchSnapshotMissingFiles,
  fetchSnapshots,
  fetchTableDetails,
  fetchTasks,
  PaginatedTasks,
  rollbackToSnapshot,
  SnapshotItem,
  TableDetails,
  Task,
} from '../api/schema';
import type { ApiError } from '../api/client';
import { normalizeDatabaseSearch } from '../utils/database';
import { formatTimestamp, formatBytes, formatNumber } from '../utils/format';
import MetricWithDelta from '../components/MetricWithDelta';
import SnapshotSummaryModal from '../components/SnapshotSummaryModal';
import SnapshotMissingFilesModal from '../components/SnapshotMissingFilesModal';
import { useAdminMode } from '../context/AdminModeContext';
import { useMessageApi } from '../context/MessageContext';

const { Title, Text } = Typography;

export const Route = createFileRoute('/tables/$tableName/snapshots')({
  validateSearch: normalizeDatabaseSearch,
  component: SnapshotsPage,
});

const rollbackPollIntervalMs = 3000;

class PollCancelledError extends Error {}

function isTransient(error: unknown): boolean {
  const status = (error as Partial<ApiError>)?.status;

  return status === undefined || status === 0 || status === 408 || status === 429 || status >= 500;
}

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, ms);
    signal.addEventListener('abort', () => {
      clearTimeout(timer);
      reject(new PollCancelledError());
    }, { once: true });
  });
}

// waitForTask polls the table's rollback tasks until the given one finishes.
// Network and server errors are retried on the next poll; other errors end it.
async function waitForTask(database: string, tableName: string, taskId: number, signal: AbortSignal): Promise<Task> {
  for (;;) {
    await sleep(rollbackPollIntervalMs, signal);

    let tasks: PaginatedTasks;
    try {
      tasks = await fetchTasks(database, tableName, 50, 0, ['rollback_to_snapshot']);
    } catch (error) {
      if (isTransient(error)) {
        continue;
      }

      throw new Error(`could not check task ${taskId}: ${(error as ApiError).message}`);
    }

    const task = tasks.items.find((item) => item.id === taskId);

    if (!task) {
      throw new Error(`task ${taskId} no longer exists`);
    }

    if (task.status === 'success') {
      return task;
    }

    if (task.status === 'error') {
      throw new Error(`task ${taskId}: ${task.error_message ?? 'unknown error'}`);
    }
  }
}

function SnapshotsPage() {
  const { tableName } = Route.useParams();
  const { database } = Route.useSearch();
  const messageApi = useMessageApi();
  const { isAdminMode } = useAdminMode();
  const queryClient = useQueryClient();
  const [selectedSnapshot, setSelectedSnapshot] = useState<SnapshotItem | null>(null);
  const [missingFilesModalOpen, setMissingFilesModalOpen] = useState(false);
  const [missingFilesSnapshotId, setMissingFilesSnapshotId] = useState<string | null>(null);
  const [missingFiles, setMissingFiles] = useState<string[]>([]);
  const [missingFilesError, setMissingFilesError] = useState<string | null>(null);
  const [pagination, setPagination] = useState<TablePaginationConfig>({
    current: 1,
    pageSize: 20,
  });

  const { data: table } = useQuery<TableDetails, Error>({
    queryKey: ['table', database, tableName],
    queryFn: () => fetchTableDetails(database, tableName),
  });

  const {
    data: snapshots,
    isLoading,
    isError,
    error,
  } = useQuery<SnapshotItem[], Error>({
    queryKey: ['snapshots', database, tableName],
    queryFn: () => fetchSnapshots(database, tableName),
  });

  const missingFilesMutation = useMutation({
    mutationFn: (snapshotId: string) => fetchSnapshotMissingFiles(database, tableName, snapshotId),
    onSuccess: (data) => {
      setMissingFiles(data.missing_files);
      setMissingFilesError(null);
      if (data.missing_files.length === 0) {
        messageApi.success(`No missing files found for snapshot ${data.snapshot_id}`);
      }
    },
    onError: (error: Error) => {
      setMissingFiles([]);
      setMissingFilesError(error.message);
    },
  });

  const rollbackPoll = useRef<AbortController | null>(null);
  useEffect(() => () => rollbackPoll.current?.abort(), []);

  const rollbackMutation = useMutation({
    mutationFn: async (snapshotId: string) => {
      const queued = await rollbackToSnapshot(database, tableName, snapshotId);
      messageApi.info(`Queued rollback to snapshot ${queued.snapshot_id} as task ${queued.task_id}`);
      queryClient.invalidateQueries({ queryKey: ['tasks', database, tableName] });

      rollbackPoll.current = new AbortController();
      return waitForTask(database, tableName, queued.task_id, rollbackPoll.current.signal);
    },
    onSuccess: (task) => {
      messageApi.success(`Rollback task ${task.id} finished`);
    },
    onError: (error: Error) => {
      if (error instanceof PollCancelledError) {
        return;
      }

      messageApi.error(`Failed to rollback snapshot: ${error.message}`);
    },
    onSettled: () => {
      queryClient.invalidateQueries({ queryKey: ['table', database, tableName] });
      queryClient.invalidateQueries({ queryKey: ['snapshots', database, tableName] });
      queryClient.invalidateQueries({ queryKey: ['partitions', database, tableName] });
      queryClient.invalidateQueries({ queryKey: ['tables', database] });
    },
  });

  const handleCheckMissingFiles = (snapshotId: string) => {
    setMissingFilesSnapshotId(snapshotId);
    setMissingFiles([]);
    setMissingFilesError(null);
    setMissingFilesModalOpen(true);
    missingFilesMutation.mutate(snapshotId);
  };

  const handleCloseMissingFilesModal = () => {
    setMissingFilesModalOpen(false);
    setMissingFilesSnapshotId(null);
    setMissingFiles([]);
    setMissingFilesError(null);
    missingFilesMutation.reset();
  };

  if (isLoading) {
    return (
      <div style={{ textAlign: 'center', padding: '24px 0' }}>
        <Spin size="large" />
        <div style={{ marginTop: 8 }}>Loading snapshots...</div>
      </div>
    );
  }

  if (isError) {
    return (
      <Alert
        type="error"
        showIcon
        message="Failed to load snapshots"
        description={error.message}
      />
    );
  }

  const columns: ColumnsType<SnapshotItem> = [
    {
      title: 'Committed At',
      dataIndex: 'committed_at',
      key: 'committed_at',
      render: (value: string) => formatTimestamp(value),
      sorter: (a, b) =>
        new Date(a.committed_at).getTime() - new Date(b.committed_at).getTime(),
      defaultSortOrder: 'descend',
      width: 180,
    },
    {
      title: 'Snapshot ID',
      dataIndex: 'snapshot_id',
      key: 'snapshot_id',
      render: (value: string) => (
        <Space size="small">
          <code style={{ fontSize: '11px' }}>{value}</code>
          {value === table?.current_snapshot_id ? <Tag color="gold">Current</Tag> : null}
        </Space>
      ),
      width: 150,
    },
    {
      title: 'Parent ID',
      dataIndex: 'parent_id',
      key: 'parent_id',
      render: (value: string | null) =>
        value !== null ? (
          <code style={{ fontSize: '11px' }}>{value}</code>
        ) : (
          <Text type="secondary">-</Text>
        ),
      width: 150,
    },
    {
      title: 'Operation',
      dataIndex: 'operation',
      key: 'operation',
      render: (value: string) => {
        const colorMap: Record<string, string> = {
          append: 'green',
          overwrite: 'orange',
          delete: 'red',
          replace: 'blue',
        };
        return <Tag color={colorMap[value] || 'default'}>{value}</Tag>;
      },
      width: 100,
    },
    {
      title: 'Total Data Files',
      key: 'total_data_files',
      align: 'right',
      render: (_, record: SnapshotItem) => (
        <MetricWithDelta
          total={record.summary?.['total-data-files'] as number}
          added={record.summary?.['added-data-files'] as number}
          deleted={record.summary?.['deleted-data-files'] as number}
          formatter={formatNumber}
        />
      ),
      width: 160,
    },
    {
      title: 'Total Records',
      key: 'total_records',
      align: 'right',
      render: (_, record: SnapshotItem) => (
        <MetricWithDelta
          total={record.summary?.['total-records'] as number}
          added={record.summary?.['added-records'] as number}
          deleted={record.summary?.['deleted-records'] as number}
          formatter={formatNumber}
        />
      ),
      width: 160,
    },
    {
      title: 'Total Data Size',
      key: 'total_data_size',
      align: 'right',
      render: (_, record: SnapshotItem) => (
        <MetricWithDelta
          total={record.summary?.['total-files-size'] as number}
          added={record.summary?.['added-files-size'] as number}
          deleted={record.summary?.['removed-files-size'] as number}
          formatter={formatBytes}
        />
      ),
      width: 180,
    },
    {
      title: 'Actions',
      key: 'actions',
      fixed: 'right',
      width: isAdminMode ? 320 : 220,
      render: (_, record: SnapshotItem) => {
        const hasSummary = record.summary && Object.keys(record.summary).length > 0;
        const isCurrentSnapshot = record.snapshot_id === table?.current_snapshot_id;
        const isRollbackPending = rollbackMutation.isPending && rollbackMutation.variables === record.snapshot_id;

        return (
          <Space size="small">
            <Button
              size="small"
              disabled={!hasSummary}
              onClick={() => setSelectedSnapshot(record)}
            >
              Summary
            </Button>
            <Button
              size="small"
              loading={missingFilesMutation.isPending && missingFilesSnapshotId === record.snapshot_id}
              onClick={() => handleCheckMissingFiles(record.snapshot_id)}
            >
              Check Files
            </Button>
            {isAdminMode ? (
              <Popconfirm
                title="Rollback to snapshot"
                description={`Rollback table ${tableName} to snapshot ${record.snapshot_id}? This will change the current table state.`}
                onConfirm={() => rollbackMutation.mutate(record.snapshot_id)}
                okText="Yes, rollback"
                cancelText="Cancel"
                disabled={isCurrentSnapshot || rollbackMutation.isPending}
              >
                <Button
                  size="small"
                  danger
                  disabled={isCurrentSnapshot}
                  loading={isRollbackPending}
                >
                  Rollback
                </Button>
              </Popconfirm>
            ) : null}
          </Space>
        );
      },
    },
  ];

  return (
    <Space direction="vertical" style={{ width: '100%' }} size="large">
      <div>
        <Title level={5} style={{ marginBottom: 8 }}>
          Snapshot History - current snapshot ID: {table?.current_snapshot_id ?? '-'}
        </Title>
      </div>

      {snapshots && snapshots.length === 0 ? (
        <Alert
          type="info"
          showIcon
          message="No snapshots"
          description="No snapshot data found for this table."
        />
      )       : (
        <Table<SnapshotItem>
          rowKey={(row) => row.snapshot_id}
          columns={columns}
          dataSource={snapshots}
          onRow={(record) => ({
            style: record.snapshot_id === table?.current_snapshot_id
              ? { backgroundColor: '#fffbe6' }
              : undefined,
          })}
          pagination={{
            ...pagination,
            showSizeChanger: true,
          }}
          onChange={(newPagination) => setPagination(newPagination)}
          scroll={{ x: 'max-content' }}
        />
      )}

      <SnapshotSummaryModal
        snapshot={selectedSnapshot}
        onClose={() => setSelectedSnapshot(null)}
      />

      <SnapshotMissingFilesModal
        open={missingFilesModalOpen}
        snapshotId={missingFilesSnapshotId}
        missingFiles={missingFiles}
        isLoading={missingFilesMutation.isPending}
        errorMessage={missingFilesError}
        onClose={handleCloseMissingFilesModal}
      />
    </Space>
  );
}
