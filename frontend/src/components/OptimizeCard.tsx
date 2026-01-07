import { useState } from 'react';
import { useMutation } from '@tanstack/react-query';
import {
  Alert,
  Button,
  Card,
  DatePicker,
  Descriptions,
  Form,
  Slider,
  Space,
  Typography,
  Popconfirm,
} from 'antd';
import type { Dayjs } from 'dayjs';
import { optimizeTable, type OptimizeResponse } from '../api/schema';
import { formatNumber } from '../utils/format';
import { useMessageApi } from '../context/MessageContext';

const { Paragraph } = Typography;
const { RangePicker } = DatePicker;

interface OptimizeCardProps {
  tableName: string;
}

export function OptimizeCard({ tableName }: OptimizeCardProps) {
  const [form] = Form.useForm();
  const [result, setResult] = useState<OptimizeResponse | null>(null);
  const messageApi = useMessageApi();
  const fileSizeThreshold = Form.useWatch('file_size_threshold_mb', form);

  const mutation = useMutation({
    mutationFn: (values: {
      file_size_threshold_mb: number;
      date_range?: [Dayjs, Dayjs] | null;
    }) => {
      let from: string | undefined;
      let to: string | undefined;

      if (values.date_range && values.date_range[0] && values.date_range[1]) {
        from = values.date_range[0].format('YYYY-MM-DD');
        to = values.date_range[1].format('YYYY-MM-DD');
      }

      return optimizeTable(tableName, values.file_size_threshold_mb, from, to);
    },
    onMutate: () => {
      setResult(null);
    },
    onSuccess: (data) => {
      setResult(data);
      messageApi.success(`Successfully optimized table ${data.table}`);
    },
    onError: (error: Error) => {
      messageApi.error(`Failed to optimize table: ${error.message}`);
    },
  });

  const onFinish = (values: {
    file_size_threshold_mb: number;
    date_range?: [Dayjs, Dayjs] | null;
  }) => {
    mutation.mutate(values);
  };

  return (
    <Card title="Optimize Table">
      <Space direction="vertical" size="middle" style={{ width: '100%' }}>
        <Paragraph>
          Compacts small files and improves read performance by organizing data for efficiency.
          This operation rewrites data files and can be resource-intensive.
        </Paragraph>

        <Form
          form={form}
          layout="vertical"
          onFinish={onFinish}
          initialValues={{
            file_size_threshold_mb: 128,
            date_range: null,
          }}
          disabled={mutation.isPending}
        >
          <Space direction="horizontal" size="large" style={{ width: '100%' }} align="start">
            <div style={{ width: 500 }}>
              <Form.Item
                label={`File Size Threshold (MB): ${fileSizeThreshold}`}
                name="file_size_threshold_mb"
                rules={[
                  { required: true, message: 'Please input file size threshold!' },
                  { type: 'number', min: 1, message: 'Minimum threshold is 1 MB' },
                ]}
                extra="Files smaller than this threshold will be compacted."
              >
                <Slider min={1} max={1024} marks={{ 1: '1MB', 128: '128MB', 512: '512MB', 1024: '1GB' }} />
              </Form.Item>
            </div>
            
            <Form.Item
              label="Date Range"
              name="date_range"
              rules={[
                { required: true, message: 'Please select a date range!' },
              ]}
              extra="Only optimize data within this date range (based on partition column)."
            >
              <RangePicker allowClear />
            </Form.Item>
          </Space>

          <div style={{ marginTop: 16 }}>
            <Popconfirm
              title="Optimize table"
              description="Are you sure you want to optimize this table? This may take a while."
              onConfirm={form.submit}
              okText="Yes, optimize"
              cancelText="Cancel"
              disabled={mutation.isPending}
            >
              <Button type="primary" loading={mutation.isPending}>
                Optimize Table
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
              <Descriptions.Item label="Threshold">{result.file_size_threshold_mb} MB</Descriptions.Item>
              {result.where && <Descriptions.Item label="Filter">{result.where}</Descriptions.Item>}
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
