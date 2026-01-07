interface MetricWithDeltaProps {
  total: string | number | undefined | null;
  added: string | number | undefined | null;
  deleted?: string | number | undefined | null;
  formatter: (value: number) => string;
}

/**
 * Renders a metric value with an optional delta indicator showing the change.
 * Used for displaying snapshot metrics like file count, record count, and data size.
 */
export default function MetricWithDelta({ total, added, deleted, formatter }: MetricWithDeltaProps) {
  if (total === undefined || total === null) return <>-</>;

  const totalNum = typeof total === 'string' ? parseInt(total, 10) : total;
  const addedNum =
    added !== undefined && added !== null
      ? typeof added === 'string'
        ? parseInt(added, 10)
        : added
      : 0;

  const deletedNum =
    deleted !== undefined && deleted !== null
      ? typeof deleted === 'string'
        ? parseInt(deleted, 10)
        : deleted
      : 0;

  if (isNaN(totalNum)) return <>-</>;

  const netDelta = addedNum - deletedNum;
  const hasDelta = netDelta !== 0 && !isNaN(netDelta);

  return (
    <div>
      <span>{formatter(totalNum)}</span>
      {hasDelta && (
        <span
          style={{
            marginLeft: 8,
            fontSize: '0.85em',
            color: netDelta > 0 ? '#52c41a' : '#ff4d4f',
          }}
        >
          ({netDelta > 0 ? '+' : '-'}{formatter(Math.abs(netDelta))})
        </span>
      )}
    </div>
  );
}
