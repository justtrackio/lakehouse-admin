import { useEffect } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Alert,
  Button,
  Card,
  Form,
  Slider,
  Space,
  Typography,
  Popconfirm,
  Spin,
} from 'antd';
import { expireSnapshots } from '../api/schema';
import { useMessageApi } from './MessageProvider';

const { Paragraph } = Typography;

interface ExpireSnapshotsCardProps {
  tableName: string;
  snapshotCount?: number;
  snapshotCountLoading?: boolean;
}

export function ExpireSnapshotsCard({
  tableName,
  snapshotCount,
  snapshotCountLoading,
}: ExpireSnapshotsCardProps) {
  const queryClient = useQueryClient();
  const [form] = Form.useForm();
  const messageApi = useMessageApi();
  const retentionDays = Form.useWatch('retention_days', form);
  const retainLast = Form.useWatch('retain_last', form);

  const mutation = useMutation({
    mutationFn: (values: { retention_days: number; retain_last: number }) =>
      expireSnapshots(tableName, values.retention_days, values.retain_last),
    onSuccess: (data) => {
      messageApi.success(`Successfully expired snapshots for table ${data.table}`);
      queryClient.invalidateQueries({ queryKey: ['table', tableName] });
      queryClient.invalidateQueries({ queryKey: ['snapshots', tableName] });
    },
    onError: (error: Error) => {
      messageApi.error(`Failed to expire snapshots: ${error.message}`);
    },
  });

  const onFinish = (values: { retention_days: number; retain_last: number }) => {
    mutation.mutate(values);
  };

  const isReady = !snapshotCountLoading;
  const hasSnapshots = isReady && (snapshotCount ?? 0) > 0;
  // Use snapshotCount as max if available, otherwise fallback to 100 for safety (though it will be disabled)
  const retainLastMax = Math.max(snapshotCount ?? 1, 1);

  // Dynamic marks: always show 1 and max. Show 10 if it falls nicely in between.
  const marks: Record<number, string> = {
    10: '10',
    [retainLastMax]: String(retainLastMax),
  };
  if (retainLastMax >= 100) {
    marks[100] = '100';
  }

  // Clamp retain_last if the loaded snapshotCount is smaller than current form value
  useEffect(() => {
    if (isReady && hasSnapshots && snapshotCount !== undefined) {
      const currentValue = form.getFieldValue('retain_last');
      if (currentValue > snapshotCount) {
        form.setFieldsValue({ retain_last: snapshotCount });
      }
    }
  }, [isReady, hasSnapshots, snapshotCount, form]);

  const isDisabled = !isReady || !hasSnapshots || mutation.isPending;

  return (
    <Card title="Expire Snapshots">
      <Space direction="vertical" size="middle" style={{ width: '100%' }}>
        <Paragraph>
          Removes snapshots older than the specified retention period. This action frees up storage space
          and cleans up metadata, but it is irreversible.
        </Paragraph>

        {!isReady && (
          <div style={{ textAlign: 'center', padding: '16px 0' }}>
            <Spin tip="Loading snapshot count..." />
          </div>
        )}

        {isReady && !hasSnapshots && (
          <Alert
            type="warning"
            showIcon
            message="No snapshots available"
            description="This table currently has no snapshots, so there is nothing to expire."
          />
        )}

        <Form
          form={form}
          layout="vertical"
          onFinish={onFinish}
          initialValues={{
            retention_days: 7,
            retain_last: Math.min(10, retainLastMax),
          }}
          disabled={isDisabled}
        >
          <Space direction="horizontal" size="large" style={{ width: '100%' }} align="start">
            <div style={{ width: 500 }}>
              <Form.Item
                label={`Retention Period (Days): ${retentionDays}`}
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
                label={`Retain Last (Count): ${retainLast} ${isReady && hasSnapshots ? `(Max: ${retainLastMax})` : ''}`}
                name="retain_last"
                rules={[
                  { required: true, message: 'Please input retain count!' },
                  { type: 'number', min: 1, message: 'Must retain at least 1 snapshot' },
                ]}
                extra="Minimum number of recent snapshots to keep."
              >
                <Slider min={10} max={retainLastMax} marks={marks} />
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
              disabled={isDisabled}
            >
              <Button type="primary" danger loading={mutation.isPending} disabled={isDisabled}>
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
