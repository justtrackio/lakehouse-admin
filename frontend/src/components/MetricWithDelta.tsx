interface MetricWithDeltaProps {
  total: string | number | undefined | null;
  added: string | number | undefined | null;
  formatter: (value: number) => string;
}

/**
 * Renders a metric value with an optional delta indicator showing the change.
 * Used for displaying snapshot metrics like file count, record count, and data size.
 */
export default function MetricWithDelta({ total, added, formatter }: MetricWithDeltaProps) {
  if (total === undefined || total === null) return <>-</>;

  const totalNum = typeof total === 'string' ? parseInt(total, 10) : total;
  const addedNum =
    added !== undefined && added !== null
      ? typeof added === 'string'
        ? parseInt(added, 10)
        : added
      : 0;

  if (isNaN(totalNum)) return <>-</>;

  const hasChange = addedNum !== 0 && !isNaN(addedNum);

  return (
    <div>
      <span>{formatter(totalNum)}</span>
      {hasChange && (
        <span
          style={{
            marginLeft: 8,
            fontSize: '0.85em',
            color: addedNum > 0 ? '#52c41a' : '#ff4d4f',
          }}
        >
          ({addedNum > 0 ? '+' : ''}
          {formatter(Math.abs(addedNum))})
        </span>
      )}
    </div>
  );
}