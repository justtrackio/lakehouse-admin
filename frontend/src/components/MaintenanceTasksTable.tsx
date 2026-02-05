import { Link } from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import { Alert, Table, Tag, Typography, Modal, Button, Tooltip } from 'antd';
import type { ColumnsType, TablePaginationConfig } from 'antd/es/table';
import { useState } from 'react';
import { MaintenanceTask, fetchMaintenanceTasks } from '../api/schema';

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

  const { data, isLoading, isError, error } = useQuery({
    queryKey: ['maintenanceTasks', tableName, pagination.current, pagination.pageSize],
    queryFn: () =>
      fetchMaintenanceTasks(
        tableName,
        pagination.pageSize || 10,
        ((pagination.current || 1) - 1) * (pagination.pageSize || 10),
      ),
    refetchInterval: 5000,
  });

  const handleTableChange = (newPagination: TablePaginationConfig) => {
    setPagination(newPagination);
  };

  const showDetails = (title: string, content: string | Record<string, unknown>) => {
    setViewModalTitle(title);
    setViewModalContent(content);
    setViewModalOpen(true);
  };

  const columns: ColumnsType<MaintenanceTask> = [
    {
      title: 'Table',
      dataIndex: 'table',
      key: 'table',
      hidden: !!tableName, // Hide if showing history for a specific table
      render: (text: string) => (
        <Link to="/tables/$tableName/maintenance" params={{ tableName: text }}>
          {text}
        </Link>
      ),
    },
    {
      title: 'Kind',
      dataIndex: 'kind',
      key: 'kind',
      render: (kind: string) => <Tag color="blue">{kind}</Tag>,
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
    },
    {
      title: 'Queued At',
      dataIndex: 'started_at',
      key: 'started_at',
      render: (text: string) => new Date(text).toLocaleString(),
    },
    {
      title: 'Wait Time',
      key: 'wait_time',
      render: (_, record) => {
        if (!record.picked_up_at) return <Tag>Queued</Tag>;
        const start = new Date(record.started_at).getTime();
        const pickedUp = new Date(record.picked_up_at).getTime();
        const diff = pickedUp - start;
        return <Tooltip title={`Picked up at ${new Date(record.picked_up_at).toLocaleString()}`}>{`${(diff / 1000).toFixed(2)}s`}</Tooltip>;
      },
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
      <Title level={4}>{tableName ? 'Maintenance Tasks' : 'Global Maintenance Tasks'}</Title>
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
