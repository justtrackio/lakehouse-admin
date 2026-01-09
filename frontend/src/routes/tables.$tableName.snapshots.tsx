import { useState } from 'react';
import { createFileRoute } from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import {
  Alert,
  Button,
  Space,
  Spin,
  Table,
  Tag,
  Typography,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { fetchSnapshots, SnapshotItem } from '../api/schema';
import { formatTimestamp, formatBytes, formatNumber } from '../utils/format';
import MetricWithDelta from "../components/MetricWithDelta";
import SnapshotSummaryModal from "../components/SnapshotSummaryModal";

const { Title, Text } = Typography;

export const Route = createFileRoute('/tables/$tableName/snapshots')({
  component: SnapshotsPage,
});

function SnapshotsPage() {
  const { tableName } = Route.useParams();
  const [selectedSnapshot, setSelectedSnapshot] = useState<SnapshotItem | null>(null);

  const {
    data: snapshots,
    isLoading,
    isError,
    error,
  } = useQuery<SnapshotItem[], Error>({
    queryKey: ['snapshots', tableName],
    queryFn: () => fetchSnapshots(tableName),
  });

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
        <code style={{ fontSize: '11px' }}>{value}</code>
      ),
      width: 150,
    },
    {
      title: 'Parent ID',
      dataIndex: 'parent_id',
      key: 'parent_id',
      render: (value: string) =>
        value ? (
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
      width: 100,
      render: (_, record: SnapshotItem) => {
        const hasSummary = record.summary && Object.keys(record.summary).length > 0;
        return (
          <Button
            size="small"
            disabled={!hasSummary}
            onClick={() => setSelectedSnapshot(record)}
          >
            Summary
          </Button>
        );
      },
    },
  ];

  return (
    <Space direction="vertical" style={{ width: '100%' }} size="large">
      <div>
        <Title level={5} style={{ marginBottom: 8 }}>
          Snapshot History
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
          pagination={{ pageSize: 20 }}
          scroll={{ x: 'max-content' }}
        />
      )}

      <SnapshotSummaryModal
        snapshot={selectedSnapshot}
        onClose={() => setSelectedSnapshot(null)}
      />
    </Space>
  );
}
