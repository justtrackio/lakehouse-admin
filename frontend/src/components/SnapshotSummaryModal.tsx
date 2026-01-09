import { Alert, Descriptions, Modal, Typography } from 'antd';
import type { SnapshotItem } from '../api/schema';
import { formatTimestamp, formatBytes, formatNumber } from '../utils/format';

const { Text } = Typography;

interface SnapshotSummaryModalProps {
  snapshot: SnapshotItem | null;
  onClose: () => void;
}

/**
 * Formats a summary value based on its key name.
 * - Keys containing 'size' or 'bytes' are formatted as bytes.
 * - Other numeric values are formatted as locale numbers.
 * - Non-numeric values are returned as strings.
 */
function formatSummaryValue(key: string, value: unknown): string {
  if (typeof value === 'number') {
    if (/size|bytes/i.test(key)) {
      return formatBytes(value);
    }
    return formatNumber(value);
  }
  if (typeof value === 'string') {
    // Check if it's a numeric string and format accordingly
    const num = Number(value);
    if (!isNaN(num) && value.trim() !== '') {
      if (/size|bytes/i.test(key)) {
        return formatBytes(num);
      }
      return formatNumber(num);
    }
    return value;
  }
  if (typeof value === 'object' && value !== null) {
    return JSON.stringify(value, null, 2);
  }
  return String(value);
}

/**
 * Modal component that displays all summary fields for a snapshot.
 */
export default function SnapshotSummaryModal({ snapshot, onClose }: SnapshotSummaryModalProps) {
  const hasSummary = snapshot?.summary && Object.keys(snapshot.summary).length > 0;

  return (
    <Modal
      title="Snapshot Summary"
      open={snapshot !== null}
      onCancel={onClose}
      footer={null}
      width={600}
    >
      {snapshot && (
        <>
          <div style={{ marginBottom: 16 }}>
            <Text type="secondary">
              Snapshot ID: <code>{snapshot.snapshot_id}</code>
            </Text>
            <br />
            <Text type="secondary">
              Committed: {formatTimestamp(snapshot.committed_at)}
            </Text>
          </div>
          {hasSummary ? (
            <Descriptions column={1} size="small" bordered>
              {Object.entries(snapshot.summary).map(([key, value]) => (
                <Descriptions.Item
                  key={key}
                  label={<code>{key}</code>}
                  labelStyle={{ width: 200 }}
                >
                  {typeof value === 'object' && value !== null ? (
                    <pre style={{ margin: 0, whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
                      {formatSummaryValue(key, value)}
                    </pre>
                  ) : (
                    formatSummaryValue(key, value)
                  )}
                </Descriptions.Item>
              ))}
            </Descriptions>
          ) : (
            <Alert
              type="info"
              showIcon
              message="No summary available"
              description="This snapshot does not contain summary information."
            />
          )}
        </>
      )}
    </Modal>
  );
}
