import { createFileRoute } from '@tanstack/react-router';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Button, Space, Typography, message, Popconfirm } from 'antd';
import { MinusOutlined, PlusOutlined, DeleteOutlined } from '@ant-design/icons';
import { MaintenanceTasksTable } from '../components/MaintenanceTasksTable';
import { fetchTaskConcurrency, setTaskConcurrency, flushTasks } from '../api/schema';

const { Text } = Typography;

export const Route = createFileRoute('/tasks')({
  component: TasksPage,
});

function TasksPage() {
  const queryClient = useQueryClient();

  const { data: concurrencyData, isLoading } = useQuery({
    queryKey: ['taskConcurrency'],
    queryFn: fetchTaskConcurrency,
    staleTime: 30000, // Consider data fresh for 30 seconds
  });

  const mutation = useMutation({
    mutationFn: setTaskConcurrency,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['taskConcurrency'] });
      message.success('Concurrency updated');
    },
    onError: (error: Error) => {
      message.error(`Failed to update concurrency: ${error.message}`);
    },
  });

  const flushMutation = useMutation({
    mutationFn: flushTasks,
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['tasks'] });
      queryClient.invalidateQueries({ queryKey: ['taskCounts'] });
      message.success(`Flushed ${data.deleted} task${data.deleted !== 1 ? 's' : ''}`);
    },
    onError: (error: Error) => {
      message.error(`Failed to flush tasks: ${error.message}`);
    },
  });

  const currentValue = concurrencyData?.value ?? 1;

  const handleDecrement = () => {
    if (currentValue > 1) {
      mutation.mutate(currentValue - 1);
    }
  };

  const handleIncrement = () => {
    mutation.mutate(currentValue + 1);
  };

  const handleFlush = () => {
    flushMutation.mutate();
  };

  return (
    <div style={{ padding: 24 }}>
      <Space direction="vertical" size="middle" style={{ width: '100%' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <Text>Concurrent tasks:</Text>
            <Button
              icon={<MinusOutlined />}
              size="small"
              disabled={currentValue <= 1 || mutation.isPending || isLoading}
              onClick={handleDecrement}
            />
            <Text strong>{isLoading ? '...' : currentValue}</Text>
            <Button
              icon={<PlusOutlined />}
              size="small"
              disabled={mutation.isPending || isLoading}
              onClick={handleIncrement}
            />
          </div>
          <Popconfirm
            title="Are you sure you want to clear all tasks?"
            description="This will permanently delete all tasks from the database."
            onConfirm={handleFlush}
            okText="Yes"
            cancelText="No"
            okButtonProps={{ danger: true }}
          >
            <Button
              danger
              icon={<DeleteOutlined />}
              loading={flushMutation.isPending}
            >
              Clear All Tasks
            </Button>
          </Popconfirm>
        </div>
        <MaintenanceTasksTable />
      </Space>
    </div>
  );
}
