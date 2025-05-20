package internal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/destrex271/postgresexporter/internal/db"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const (
	expHistogramMetricTableInsertSQL = `
	INSERT INTO "%s"."%s" (
		resource_url, 
		resource_attributes,
		scope_name, 
		scope_version, 
		scope_attributes, 
		scope_dropped_attr_count, 
		scope_url, 
		service_name,
		name, 
		type, 
		description, 
		unit,
		start_timestamp, 
		timestamp, 
		attributes
		metadata,
		count, 
		sum, 
		scale, 
		zero_count,
		positive_offset,
		positive_bucket_counts, 
		negative_offset, 
		negative_bucket_counts,
		exemplars, 
		flags, 
		min,
		max, 
		zero_threshold, 
		aggregation_temporality
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30)
	`
)

var (
	expHistogramMetricTableColumns = []string{
		"count BIGINT",
		"sum   DOUBLE PRECISION",
		"scale INTEGER",
		"zero_count BIGINT",

		"positive_offset        INTEGER",
		"positive_bucket_counts JSONB",
		"negative_offset        INTEGER",
		"negative_bucket_counts JSONB",

		"exemplars  JSONB",
		"flags      INTEGER",

		"min            DOUBLE PRECISION",
		"max            DOUBLE PRECISION",
		"zero_threshold DOUBLE PRECISION",

		"aggregation_temporality INTEGER",
	}
)

type expHistogramMetric struct {
	resMetadata *ResourceMetadata

	expHistogram pmetric.ExponentialHistogram
	name         string
	description  string
	unit         string
	metadata     pcommon.Map
}

type expHistogramMetricsGroup struct {
	MetricsType pmetric.MetricType

	DBType     DBType
	SchemaName string

	metrics []*expHistogramMetric
	count   int
}

func (g *expHistogramMetricsGroup) Add(resMetadata *ResourceMetadata, metric any, name, description, unit string, metadata pcommon.Map) error {
	expHistogram, ok := metric.(pmetric.ExponentialHistogram)
	if !ok {
		return fmt.Errorf("metric param is not ExponentialHistogram type")
	}

	// CAB - remove "." from metric name and replace with "_"
	newName := strings.Replace(name, ".", "_", -1)
	g.count += expHistogram.DataPoints().Len()
	g.metrics = append(g.metrics, &expHistogramMetric{
		resMetadata:  resMetadata,
		expHistogram: expHistogram,
		// name:         name,
		name:        newName,
		description: description,
		unit:        unit,
		metadata:    metadata,
	})

	return nil
}

func (g *expHistogramMetricsGroup) insert(ctx context.Context, client *sql.DB) error {
	logger.Debug("Inserting exp histogram metrics")

	if g.count == 0 {
		return nil
	}

	var errs error
	for _, m := range g.metrics {
		err := db.DoWithTx(ctx, client, func(tx *sql.Tx) error {
			exists, err := CheckIfTableExists(ctx, client, g.SchemaName, m.name)
			if err != nil {
				return err
			}

			if !exists {
				g.createTable(ctx, client, m.name)
			}

			statement, err := tx.PrepareContext(ctx, fmt.Sprintf(expHistogramMetricTableInsertSQL, g.SchemaName, m.name))
			if err != nil {
				return err
			}

			defer func() {
				_ = statement.Close()
			}()

			resAttrs, err := json.Marshal(m.resMetadata.ResAttrs.AsRaw())
			if err != nil {
				return err
			}

			scopeAttrs, err := json.Marshal(m.resMetadata.InstrScope.Attributes().AsRaw())
			if err != nil {
				return err
			}

			metadata, err := json.Marshal(m.metadata.AsRaw())
			if err != nil {
				return err
			}

			for i := range m.expHistogram.DataPoints().Len() {
				dp := m.expHistogram.DataPoints().At(i)

				if dp.Timestamp().AsTime().IsZero() {
					errs = errors.Join(errs, fmt.Errorf("data points with the 0 value for TimeUnixNano SHOULD be rejected by consumers"))
					continue
				}

				//var a pcommon.Map = dp.Attributes()
				a := dp.Attributes()
				attrs, err := json.Marshal(a.AsRaw())
				if err != nil {
					return err
				}

				positiveBucketCounts, err := json.Marshal(dp.Positive().BucketCounts().AsRaw())
				if err != nil {
					errs = errors.Join(errs, err)
					continue
				}

				negativeBucketCounts, err := json.Marshal(dp.Negative().BucketCounts().AsRaw())
				if err != nil {
					errs = errors.Join(errs, err)
					continue
				}

				tx.Stmt(statement).ExecContext(ctx,
					m.resMetadata.ResURL, resAttrs,
					m.resMetadata.InstrScope.Name(),
					m.resMetadata.InstrScope.Version(),
					scopeAttrs,
					m.resMetadata.InstrScope.DroppedAttributesCount(),
					m.resMetadata.ScopeUrl,
					getServiceName(m.resMetadata.ResAttrs),
					m.name, int32(g.MetricsType), m.description, m.unit,
					dp.StartTimestamp().AsTime(), dp.Timestamp().AsTime(),
					attrs, metadata,
					dp.Count(),
					dp.Sum(),
					dp.Scale(),
					dp.ZeroCount(),
					dp.Positive().Offset(),
					positiveBucketCounts,
					dp.Negative().Offset(),
					negativeBucketCounts,
					dp.Exemplars(),
					uint32(dp.Flags()),
					dp.Min(),
					dp.Max(),
					dp.ZeroThreshold(),
					int32(m.expHistogram.AggregationTemporality()),
				)
			}

			return nil
		})
		errs = errors.Join(errs, err)
	}
	if errs != nil {
		return fmt.Errorf("insert exp histogram metrics failed: %w", errs)
	}

	return nil
}

func (g *expHistogramMetricsGroup) createTable(ctx context.Context, client *sql.DB, metricName string) error {
	metricTableColumns := slices.Concat(getBaseMetricTableColumns(g.DBType), expHistogramMetricTableColumns)

	return createMetricTable(ctx, client, g.SchemaName, metricName, metricTableColumns, g.DBType)
}

func (g *expHistogramMetricsGroup) getMetricsNames() []string {
	result := []string{}

	for _, m := range g.metrics {
		result = append(result, m.name)
	}

	return result
}
