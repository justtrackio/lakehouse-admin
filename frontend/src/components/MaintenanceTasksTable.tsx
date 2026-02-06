import { Link } from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import { Alert, Table, Tag, Typography, Modal, Button } from 'antd';
import type { ColumnsType, TablePaginationConfig } from 'antd/es/table';
import { FilterValue } from 'antd/es/table/interface';
import { useState } from 'react';
import { Task, fetchTasks } from '../api/schema';

const { Title } = Typography;

interface MaintenanceTasksTableProps {
  tableName?: string;
}

export function MaintenanceTasksTable({ tableName }: MaintenanceTasksTableProps) {
  const [viewModalOpen, setViewModalOpen] = useState(false);
  const [viewModalTitle, setViewModalTitle] = useState('');
  const [viewModalContent, setViewModalContent] = useState<string | Record<string, unknown>>('');

  const [pagination, setPagination] = useState<TablePaginationConfig>({
    current: 1,
    pageSize: 10,
  });

  const [selectedKinds, setSelectedKinds] = useState<string[]>([]);
  const [selectedStatuses, setSelectedStatuses] = useState<string[]>([]);

  const { data, isLoading, isError, error } = useQuery({
    queryKey: ['tasks', tableName, pagination.current, pagination.pageSize, selectedKinds, selectedStatuses],
    queryFn: () =>
      fetchTasks(
        tableName,
        pagination.pageSize || 10,
        ((pagination.current || 1) - 1) * (pagination.pageSize || 10),
        selectedKinds,
        selectedStatuses,
      ),
    refetchInterval: 5000,
  });

  const handleTableChange = (
    newPagination: TablePaginationConfig,
    filters: Record<string, FilterValue | null>,
  ) => {
    
    // Handle filters
    const newKinds = (filters.kind as string[]) || [];
    const newStatuses = (filters.status as string[]) || [];
    
    const kindsChanged = JSON.stringify(newKinds.sort()) !== JSON.stringify(selectedKinds.sort());
    const statusesChanged = JSON.stringify(newStatuses.sort()) !== JSON.stringify(selectedStatuses.sort());

    if (kindsChanged || statusesChanged) {
        setSelectedKinds(newKinds);
        setSelectedStatuses(newStatuses);
        setPagination({ ...newPagination, current: 1 });
    } else {
        setPagination(newPagination);
    }
  };

  const showDetails = (title: string, content: string | Record<string, unknown>) => {
    setViewModalTitle(title);
    setViewModalContent(content);
    setViewModalOpen(true);
  };

  const columns: ColumnsType<Task> = [
    {
      title: 'Table',
      dataIndex: 'table',
      key: 'table',
      hidden: !!tableName, // Hide if showing history for a specific table
      render: (text: string) => (
        <Link to="/tables/$tableName/tasks" params={{ tableName: text }}>
          {text}
        </Link>
      ),
    },
    {
      title: 'Kind',
      dataIndex: 'kind',
      key: 'kind',
      render: (kind: string) => <Tag color="blue">{kind}</Tag>,
      filters: [
        { text: 'Optimize', value: 'optimize' },
        { text: 'Expire Snapshots', value: 'expire_snapshots' },
        { text: 'Remove Orphan Files', value: 'remove_orphan_files' },
      ],
      filteredValue: selectedKinds,
    },
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
      render: (status: string, record) => {
        let color = 'default';
        if (status === 'success') color = 'success';
        if (status === 'error') color = 'error';
        if (status === 'running') color = 'processing';
        if (status === 'queued') color = 'default';
        
        return (
           <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
             <Tag color={color}>{status.toUpperCase()}</Tag>
             {status === 'error' && record.error_message && (
               <Button 
                 type="link" 
                 danger 
                 size="small" 
                 onClick={() => showDetails('Error Details', record.error_message!)}
               >
                 View Error
               </Button>
             )}
           </div>
        );
      },
      filters: [
        { text: 'Queued', value: 'queued' },
        { text: 'Running', value: 'running' },
        { text: 'Success', value: 'success' },
        { text: 'Error', value: 'error' },
      ],
      filteredValue: selectedStatuses,
    },
    {
      title: 'Queued At',
      dataIndex: 'started_at',
      key: 'started_at',
      render: (text: string) => new Date(text).toLocaleString(),
    },
    {
      title: 'Finished At',
      dataIndex: 'finished_at',
      key: 'finished_at',
      render: (text: string | null) => (text ? new Date(text).toLocaleString() : '-'),
    },
    {
      title: 'Execution Time',
      key: 'execution_time',
      render: (_, record) => {
        if (!record.picked_up_at || !record.finished_at) {
            if (record.status === 'running') return <Tag color="processing">Running</Tag>;
            return '-';
        }
        const pickedUp = new Date(record.picked_up_at).getTime();
        const end = new Date(record.finished_at).getTime();
        const diff = end - pickedUp;
        return `${(diff / 1000).toFixed(2)}s`;
      },
    },
    {
      title: 'Input',
      key: 'input',
      render: (_, record) => (
        <Button size="small" onClick={() => showDetails('Input Parameters', record.input)}>
          View
        </Button>
      ),
    },
    {
      title: 'Result',
      key: 'result',
      render: (_, record) => (
        <Button size="small" onClick={() => showDetails('Operation Result', record.result)}>
          View
        </Button>
      ),
    },
  ];

  if (isError) {
    return (
      <Alert
        type="error"
        message="Failed to load maintenance tasks"
        description={error instanceof Error ? error.message : 'Unknown error'}
        showIcon
      />
    );
  }

  return (
    <div style={{ marginTop: 24 }}>
      <Title level={4}>{tableName ? 'Tasks' : 'Global Tasks'}</Title>
      <Table
        columns={columns}
        dataSource={data?.items || []}
        rowKey="id"
        loading={isLoading}
        pagination={{
          ...pagination,
          total: data?.total || 0,
          showSizeChanger: true,
        }}
        onChange={handleTableChange}
        size="small"
      />
      <Modal
        title={viewModalTitle}
        open={viewModalOpen}
        onCancel={() => setViewModalOpen(false)}
        footer={[
          <Button key="close" onClick={() => setViewModalOpen(false)}>
            Close
          </Button>,
        ]}
        width={600}
      >
        <pre style={{ maxHeight: '400px', overflow: 'auto' }}>
          {typeof viewModalContent === 'string' ? viewModalContent : JSON.stringify(viewModalContent, null, 2)}
        </pre>
      </Modal>
    </div>
  );
}
