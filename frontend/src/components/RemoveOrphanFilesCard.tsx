import { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Alert,
  Button,
  Card,
  Descriptions,
  Form,
  Slider,
  Space,
  Typography,
  Popconfirm,
} from 'antd';
import { removeOrphanFiles, type RemoveOrphanFilesResponse } from '../api/schema';
import { formatNumber } from '../utils/format';
import { useMessageApi } from '../context/MessageContext';

const { Paragraph } = Typography;

interface RemoveOrphanFilesCardProps {
  tableName: string;
}

export function RemoveOrphanFilesCard({ tableName }: RemoveOrphanFilesCardProps) {
  const queryClient = useQueryClient();
  const [form] = Form.useForm();
  const [result, setResult] = useState<RemoveOrphanFilesResponse | null>(null);
  const messageApi = useMessageApi();
  const retentionDays = Form.useWatch('retention_days', form);

  const mutation = useMutation({
    mutationFn: (values: { retention_days: number }) => removeOrphanFiles(tableName, values.retention_days),
    onMutate: () => {
      setResult(null);
    },
    onSuccess: (data) => {
      setResult(data);
      messageApi.success(`Successfully removed orphan files for table ${data.table}`);
      queryClient.invalidateQueries({ queryKey: ['maintenanceHistory', tableName] });
    },
    onError: (error: Error) => {
      messageApi.error(`Failed to remove orphan files: ${error.message}`);
    },
  });

  const onFinish = (values: { retention_days: number }) => {
    mutation.mutate(values);
  };

  return (
    <Card title="Remove Orphan Files">
      <Space direction="vertical" size="middle" style={{ width: '100%' }}>
        <Paragraph>
          Removes files that are no longer referenced by any snapshot. This helps reclaim storage space.
          This operation can be time-consuming for large tables.
        </Paragraph>

        <Form
          form={form}
          layout="vertical"
          onFinish={onFinish}
          initialValues={{
            retention_days: 7,
          }}
          disabled={mutation.isPending}
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
                extra="Files older than this that are not referenced by any snapshot will be removed."
              >
                <Slider min={7} max={365} marks={{ 7: '7d', 30: '30d', 90: '90d', 365: '1y' }} />
              </Form.Item>
            </div>
          </Space>

          <div style={{ marginTop: 16 }}>
            <Popconfirm
              title="Remove orphan files"
              description="Are you sure you want to remove orphan files?"
              onConfirm={form.submit}
              okText="Yes, remove"
              cancelText="Cancel"
              disabled={mutation.isPending}
            >
              <Button type="primary" danger loading={mutation.isPending}>
                Remove Orphan Files
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

        {result && (
          <div style={{ marginTop: 16 }}>
            <Descriptions
              title="Last Result"
              bordered
              size="small"
              column={1}
              style={{ marginBottom: 16 }}
            >
              <Descriptions.Item label="Table">{result.table}</Descriptions.Item>
              <Descriptions.Item label="Retention Days">
                {result.retention_days}
              </Descriptions.Item>
              <Descriptions.Item label="Status">{result.status}</Descriptions.Item>
            </Descriptions>

            <Descriptions title="Metrics" bordered size="small" column={1}>
              {Object.keys(result.metrics || {}).length === 0 ? (
                <Descriptions.Item label="Info">No metrics returned</Descriptions.Item>
              ) : (
                Object.entries(result.metrics || {})
                  .sort(([a], [b]) => a.localeCompare(b))
                  .map(([key, value]) => (
                    <Descriptions.Item key={key} label={key}>
                      {typeof value === 'number' ? formatNumber(value) : String(value)}
                    </Descriptions.Item>
                  ))
              )}
            </Descriptions>
          </div>
        )}
      </Space>
    </Card>
  );
}
