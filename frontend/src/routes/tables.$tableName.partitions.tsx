import { createFileRoute, Link, useNavigate } from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import {
  Alert,
  Space,
  Spin,
  Table,
  Typography,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import {
  fetchTableDetails,
  fetchPartitionValues,
  TableDetails,
  ListPartitionItem,
} from '../api/schema';
import { formatNumber, formatBytes } from '../utils/format';

const { Title, Text } = Typography;

interface SearchParams {
  partitions?: Record<string, string>;
}

export const Route = createFileRoute('/tables/$tableName/partitions')({
  component: PartitionsPage,
  validateSearch: (search: Record<string, unknown>): SearchParams => {
    return {
      partitions: (search.partitions as Record<string, string>) || {},
    };
  },
});

function PartitionsPage() {
  const { tableName } = Route.useParams();
  const { partitions: partitionFilters } = Route.useSearch();
  const navigate = useNavigate();

  const {
    data: table,
    isLoading: isLoadingTable,
    isError: isErrorTable,
    error: errorTable,
  } = useQuery<TableDetails, Error>({
    queryKey: ['table', tableName],
    queryFn: () => fetchTableDetails(tableName),
  });

  const {
    data: partitionValues,
    isLoading: isLoadingPartitions,
    isError: isErrorPartitions,
    error: errorPartitions,
  } = useQuery<ListPartitionItem[], Error>({
    queryKey: ['partitions', tableName, partitionFilters],
    queryFn: () => fetchPartitionValues(tableName, partitionFilters || {}),
    enabled: !!table,
  });

  if (isLoadingTable) {
    return (
      <div style={{ textAlign: 'center', padding: '24px 0' }}>
        <Spin size="large" />
        <div style={{ marginTop: 8 }}>Loading table information...</div>
      </div>
    );
  }

  if (isErrorTable) {
    return (
      <Alert
        type="error"
        showIcon
        message="Failed to load table"
        description={errorTable.message}
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

  const partitions = table.partitions ?? [];

  if (partitions.length === 0) {
    return (
      <Alert
        type="info"
        showIcon
        message="No partitions"
        description="This table does not define any partitions."
      />
    );
  }

  const currentLevelIndex = Object.keys(partitionFilters || {}).length;
  const currentPartition = partitions[currentLevelIndex];

  if (!currentPartition) {
    return (
      <Alert
        type="warning"
        showIcon
        message="Partition level out of range"
        description={`Partition level ${currentLevelIndex} is not available for table "${table.name}".`}
      />
    );
  }

  const handlePartitionClick = (partitionValue: string) => {
    const newFilters = {
      ...(partitionFilters || {}),
      [currentPartition.name]: partitionValue,
    };

    navigate({
      to: '/tables/$tableName/partitions',
      params: { tableName },
      search: { partitions: newFilters },
    });
  };

  // Build partition breadcrumb
  const partitionBreadcrumb: React.ReactNode[] = [];
  const filterKeys = Object.keys(partitionFilters || {});
  for (let i = 0; i < filterKeys.length; i++) {
    const key = filterKeys[i];
    const value = partitionFilters![key];
    const partialFilters: Record<string, string> = {};
    for (let j = 0; j <= i; j++) {
      partialFilters[filterKeys[j]] = partitionFilters![filterKeys[j]];
    }

    partitionBreadcrumb.push(
      <Link
        key={i}
        to="/tables/$tableName/partitions"
        params={{ tableName }}
        search={{ partitions: partialFilters }}
      >
        {key}={value}
      </Link>
    );
    if (i < filterKeys.length - 1) {
      partitionBreadcrumb.push(' / ');
    }
  }

  const columns: ColumnsType<ListPartitionItem> = [
    {
      title: 'Name',
      dataIndex: 'name',
      key: 'name',
      render: (value: string) => {
        const isLastLevel = currentLevelIndex === partitions.length - 1;

        if (isLastLevel) {
          return value;
        }

        return (
          <a onClick={() => handlePartitionClick(value)} style={{ cursor: 'pointer' }}>
            {value}
          </a>
        );
      },
    },
    {
      title: 'File Count',
      dataIndex: 'file_count',
      key: 'file_count',
      align: 'right',
      render: (value: number) => formatNumber(value),
    },
    {
      title: 'Record Count',
      dataIndex: 'record_count',
      key: 'record_count',
      align: 'right',
      render: (value: number) => formatNumber(value),
    },
    {
      title: 'Avg File Size',
      key: 'avg_file_size',
      align: 'right',
      render: (_, record) => {
        if (record.file_count === 0) return '-';
        return formatBytes(record.total_data_file_size_in_bytes / record.file_count);
      },
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
    <Space direction="vertical" style={{ width: '100%' }} size="large">
      {partitionBreadcrumb.length > 0 && (
        <div>
          <Text type="secondary">{partitionBreadcrumb}</Text>
        </div>
      )}

      <div>
        <Title level={5} style={{ marginBottom: 8 }}>
          {currentPartition.name} values
        </Title>
        <Text type="secondary">
          Partition level {currentLevelIndex + 1} of {partitions.length}
        </Text>
      </div>

      {isLoadingPartitions && (
        <div style={{ textAlign: 'center', padding: '24px 0' }}>
          <Spin size="large" />
          <div style={{ marginTop: 8 }}>Loading partition values...</div>
        </div>
      )}

      {isErrorPartitions && (
        <Alert
          type="error"
          showIcon
          message="Failed to load partition values"
          description={errorPartitions.message}
        />
      )}

      {!isLoadingPartitions && !isErrorPartitions && (
        <>
          {partitionValues && partitionValues.length === 0 ? (
            <Alert
              type="info"
              showIcon
              message="No partition values"
              description="No data found for this partition level."
            />
          ) : (
            <Table<ListPartitionItem>
              rowKey={(row) => row.name}
              columns={columns}
              dataSource={partitionValues}
              pagination={{ pageSize: 50 }}
            />
          )}
        </>
      )}
    </Space>
  );
}
