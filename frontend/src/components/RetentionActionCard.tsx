import type { ReactNode } from 'react';
import { Button, Card, Form, Popconfirm, Slider, Space, Typography } from 'antd';

const { Paragraph } = Typography;

const retentionDaysSliderMarks = {
  7: '7d',
  30: '30d',
  90: '90d',
  365: '1y',
};

export interface RetentionMaintenanceFormValues {
  retention_days: number;
}

interface RetentionActionCardProps {
  title: string;
  description?: ReactNode;
  beforeForm?: ReactNode;
  afterForm?: ReactNode;
  cardSize?: 'default' | 'small';
  disabled: boolean;
  isSubmitting: boolean;
  retentionDaysExtra?: string;
  initialRetentionDays?: number;
  sliderWidth?: number;
  confirmTitle: ReactNode;
  confirmDescription: ReactNode;
  confirmOkText: string;
  submitLabel: string;
  onSubmit: (values: RetentionMaintenanceFormValues) => void;
}

export function RetentionActionCard({
  title,
  description,
  beforeForm,
  afterForm,
  cardSize = 'default',
  disabled,
  isSubmitting,
  retentionDaysExtra,
  initialRetentionDays = 7,
  sliderWidth = 500,
  confirmTitle,
  confirmDescription,
  confirmOkText,
  submitLabel,
  onSubmit,
}: RetentionActionCardProps) {
  const [form] = Form.useForm<RetentionMaintenanceFormValues>();
  const watchedRetentionDays = Form.useWatch('retention_days', form);
  const retentionDays = watchedRetentionDays ?? initialRetentionDays;

  return (
    <Card title={title} size={cardSize}>
      <Space direction="vertical" size="middle" style={{ width: '100%' }}>
        {description ? <Paragraph>{description}</Paragraph> : null}
        {beforeForm}

        <Form
          form={form}
          layout="vertical"
          initialValues={{
            retention_days: initialRetentionDays,
          }}
          disabled={disabled}
        >
          <Space direction="horizontal" size="large" style={{ width: '100%' }} align="start">
            <div style={{ width: sliderWidth }}>
              <Form.Item
                label={`Retention Period (Days): ${retentionDays}`}
                name="retention_days"
                rules={[
                  { required: true, message: 'Please input retention days!' },
                  { type: 'number', min: 7, message: 'Minimum retention is 7 days' },
                ]}
                extra={retentionDaysExtra}
              >
                <Slider min={7} max={365} marks={retentionDaysSliderMarks} />
              </Form.Item>
            </div>
          </Space>

          <div style={{ marginTop: 16 }}>
            <Popconfirm
              title={confirmTitle}
              description={confirmDescription}
              onConfirm={() => {
                void form.validateFields().then((values) => {
                  onSubmit(values);
                });
              }}
              okText={confirmOkText}
              cancelText="Cancel"
              disabled={disabled}
            >
              <Button type="primary" danger loading={isSubmitting} disabled={disabled}>
                {submitLabel}
              </Button>
            </Popconfirm>
          </div>
        </Form>

        {afterForm}
      </Space>
    </Card>
  );
}
