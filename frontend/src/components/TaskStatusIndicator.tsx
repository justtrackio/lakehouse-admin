import { useQuery } from '@tanstack/react-query';
import { Link } from '@tanstack/react-router';
import { Badge, Space } from 'antd';
import { LoadingOutlined, ClockCircleOutlined } from '@ant-design/icons';
import { fetchTaskCounts } from '../api/schema';

export function TaskStatusIndicator() {
  const { data } = useQuery({
    queryKey: ['taskCounts'],
    queryFn: fetchTaskCounts,
    refetchInterval: 5000, // Poll every 5 seconds (same as MaintenanceTasksTable)
  });

  // Only show if there are active tasks (running or queued)
  if (!data || (data.running === 0 && data.queued === 0)) {
    return null;
  }

  return (
    <Link to="/tasks" style={{ textDecoration: 'none' }}>
      <Space size="small" style={{ marginLeft: '16px', cursor: 'pointer' }}>
        {data.running > 0 && (
          <Badge count={data.running} style={{ backgroundColor: '#1890ff' }}>
            <LoadingOutlined spin style={{ fontSize: '20px', color: '#1890ff' }} />
          </Badge>
        )}
        {data.queued > 0 && (
          <Badge count={data.queued} overflowCount={1000} style={{ backgroundColor: '#8c8c8c' }}>
            <ClockCircleOutlined style={{ fontSize: '20px', color: '#8c8c8c' }} />
          </Badge>
        )}
      </Space>
    </Link>
  );
}
