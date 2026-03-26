import { Alert, List, Modal, Spin, Typography } from 'antd';

const { Paragraph, Text } = Typography;

interface SnapshotMissingFilesModalProps {
  open: boolean;
  snapshotId: string | null;
  missingFiles: string[];
  isLoading: boolean;
  errorMessage: string | null;
  onClose: () => void;
}

export default function SnapshotMissingFilesModal({
  open,
  snapshotId,
  missingFiles,
  isLoading,
  errorMessage,
  onClose,
}: SnapshotMissingFilesModalProps) {
  return (
    <Modal
      title="Missing Files Check"
      open={open}
      onCancel={onClose}
      footer={null}
      width={"75%"}
    >
      {snapshotId !== null && (
        <Paragraph type="secondary" style={{ marginBottom: 16 }}>
          Snapshot ID: <Text code>{snapshotId}</Text>
        </Paragraph>
      )}

      {isLoading ? (
        <div style={{ textAlign: 'center', padding: '24px 0' }}>
          <Spin size="large" />
          <div style={{ marginTop: 8 }}>Checking snapshot files...</div>
        </div>
      ) : errorMessage ? (
        <Alert
          type="error"
          showIcon
          message="Failed to check missing files"
          description={errorMessage}
        />
      ) : missingFiles.length === 0 ? (
        <Alert
          type="success"
          showIcon
          message="No missing files"
          description="All data files referenced by this snapshot were found in object storage."
        />
      ) : (
        <List
          bordered
          size="small"
          dataSource={missingFiles}
          renderItem={(filePath) => (
            <List.Item>
              <Text style={{ wordBreak: 'break-all' }}>
                {filePath}
              </Text>
            </List.Item>
          )}
        />
      )}
    </Modal>
  );
}
