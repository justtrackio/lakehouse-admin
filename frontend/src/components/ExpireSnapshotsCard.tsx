import { useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Alert,
  Button,
  Card,
  Form,
  Slider,
  Space,
  Typography,
  message,
  Popconfirm,
} from 'antd';
import { expireSnapshots } from '../api/schema';

const { Paragraph } = Typography;

interface ExpireSnapshotsCardProps {
  tableName: string;
}

export function ExpireSnapshotsCard({ tableName }: ExpireSnapshotsCardProps) {
  const queryClient = useQueryClient();
  const [form] = Form.useForm();

  const mutation = useMutation({
    mutationFn: (values: { retention_days: number; retain_last: number }) =>
      expireSnapshots(tableName, values.retention_days, values.retain_last),
    onSuccess: (data) => {
      message.success(`Successfully expired snapshots for table ${data.table}`);
      queryClient.invalidateQueries({ queryKey: ['table', tableName] });
      queryClient.invalidateQueries({ queryKey: ['snapshots', tableName] });
    },
    onError: (error: Error) => {
      message.error(`Failed to expire snapshots: ${error.message}`);
    },
  });

  const onFinish = (values: { retention_days: number; retain_last: number }) => {
    mutation.mutate(values);
  };

  return (
    <Card title="Expire Snapshots">
      <Space direction="vertical" size="middle" style={{ width: '100%' }}>
        <Paragraph>
          Removes snapshots older than the specified retention period. This action frees up storage space
          and cleans up metadata, but it is irreversible.
        </Paragraph>

        <Form
          form={form}
          layout="vertical"
          onFinish={onFinish}
          initialValues={{
            retention_days: 7,
            retain_last: 10,
          }}
          disabled={mutation.isPending}
        >
          <Space direction="horizontal" size="large" style={{ width: '100%' }} align="start">
            <div style={{ width: 500 }}>
              <Form.Item
                label="Retention Period (Days)"
                name="retention_days"
                rules={[
                  { required: true, message: 'Please input retention days!' },
                  { type: 'number', min: 7, message: 'Minimum retention is 7 days' },
                ]}
                extra="Snapshots older than this will be removed."
              >
                <Slider min={7} max={365} marks={{ 7: '7d', 30: '30d', 90: '90d', 365: '1y' }} />
              </Form.Item>
            </div>

            <div style={{ width: 500 }}>
              <Form.Item
                label="Retain Last (Count)"
                name="retain_last"
                rules={[
                  { required: true, message: 'Please input retain count!' },
                  { type: 'number', min: 1, message: 'Must retain at least 1 snapshot' },
                ]}
                extra="Minimum number of recent snapshots to keep."
              >
                <Slider min={1} max={100} marks={{ 1: '1', 10: '10', 50: '50', 100: '100' }} />
              </Form.Item>
            </div>
          </Space>

          <div style={{ marginTop: 16 }}>
            <Popconfirm
              title="Expire snapshots"
              description="Are you sure you want to expire old snapshots? This cannot be undone."
              onConfirm={form.submit}
              okText="Yes, expire"
              cancelText="Cancel"
              disabled={mutation.isPending}
            >
              <Button type="primary" danger loading={mutation.isPending}>
                Expire Snapshots
              </Button>
            </Popconfirm>
          </div>
        </Form>

        {mutation.isError && (
          <Alert
            type="error"
            showIcon
            message="Operation Failed"
            description={mutation.error.message}
          />
        )}
      </Space>
    </Card>
  );
}
