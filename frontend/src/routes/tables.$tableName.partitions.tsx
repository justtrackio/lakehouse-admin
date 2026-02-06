import { createFileRoute, Link, useNavigate } from '@tanstack/react-router';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  Alert,
  Badge,
  Button,
  Popconfirm,
  Space,
  Spin,
  Table,
  Typography,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import {
  fetchTableDetails,
  fetchPartitionValues,
  optimizeTable,
  TableDetails,
  ListPartitionItem,
} from '../api/schema';
import { formatNumber, formatBytes } from '../utils/format';
import { useMessageApi } from '../context/MessageContext';

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
  const queryClient = useQueryClient();
  const messageApi = useMessageApi();

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

  // Derive date range from partition context for optimize task
  const deriveDateRange = (partitionName: string): { from: string; to: string } => {
    const filters = partitionFilters || {};
    
    // Build the full date context by combining filters with the current level's value
    const fullContext = {
      ...filters,
      [currentPartition.name]: partitionName,
    };

    // Extract year, month, day from the full context
    const year = fullContext.year;
    const month = fullContext.month;
    const day = fullContext.day;

    if (day) {
      // Day level: optimize just this day
      const date = `${year}-${month}-${day}`;
      return { from: date, to: date };
    } else if (month) {
      // Month level: optimize the entire month
      const fromDate = `${year}-${month}-01`;
      // Calculate last day of month
      const lastDay = new Date(parseInt(year), parseInt(month), 0).getDate();
      const toDate = `${year}-${month}-${lastDay.toString().padStart(2, '0')}`;
      return { from: fromDate, to: toDate };
    } else if (year) {
      // Year level: optimize the entire year
      const fromDate = `${year}-01-01`;
      const toDate = `${year}-12-31`;
      return { from: fromDate, to: toDate };
    }

    // Fallback (should not happen for day-partitioned tables)
    throw new Error('Cannot derive date range from partition context');
  };

  const optimizeMutation = useMutation({
    mutationFn: (partitionName: string) => {
      const { from, to } = deriveDateRange(partitionName);
      const fileSizeThresholdMb = 128;
      return optimizeTable(tableName, fileSizeThresholdMb, from, to);
    },
    onSuccess: (data, partitionName) => {
      const count = data.task_ids.length;
      messageApi.success(
        `Enqueued ${count} optimize task${count === 1 ? '' : 's'} for partition ${partitionName} (IDs: ${data.task_ids.slice(0, 3).join(', ')}${count > 3 ? '...' : ''})`
      );
      queryClient.invalidateQueries({ queryKey: ['tasks', tableName] });
      queryClient.invalidateQueries({ queryKey: ['partitions', tableName, partitionFilters] });
    },
    onError: (error: Error, partitionName) => {
      messageApi.error(`Failed to enqueue optimize task for partition ${partitionName}: ${error.message}`);
    },
  });

  // Build partition breadcrumb
  const partitionBreadcrumb: React.ReactNode[] = [];
  const filterKeys = Object.keys(partitionFilters || {});
  
  // Add root link to navigate back to first partition level (no filters)
  if (filterKeys.length > 0) {
    partitionBreadcrumb.push(
      <Link
        key="root"
        to="/tables/$tableName/partitions"
        params={{ tableName }}
        search={{}}
      >
        {tableName}
      </Link>
    );
    partitionBreadcrumb.push(' / ');
  }
  
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
    {
      title: '',
      key: 'actions',
      align: 'center',
      width: 120,
      render: (_, record) => {
        if (!record.needs_optimize) {
          return null;
        }

        return (
          <Popconfirm
            title="Optimize partition"
            description={`Are you sure you want to optimize partition ${record.name}?`}
            onConfirm={() => optimizeMutation.mutate(record.name)}
            okText="Yes, optimize"
            cancelText="Cancel"
            disabled={optimizeMutation.isPending}
          >
            <Badge count={record.needs_optimize_count} showZero={false} overflowCount={10000} >
              <Button 
                type="primary" 
                size="small" 
                loading={optimizeMutation.isPending}
              >
                Optimize
              </Button>
            </Badge>
          </Popconfirm>
        );
      },
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
