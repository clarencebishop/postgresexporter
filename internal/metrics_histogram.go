package internal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/clarencebishop/postgresexporter/internal/db"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const (
	histogramMetricTableInsertSQL = `
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
		attributes,
		metadata,
		count,
		sum, 
		bucket_counts, 
		explicit_bounds, 
		exemplars, 
		flags,
		min,
		max, 
		aggregation_temporality
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)
	`
)

var (
	histogramMetricTableColumns = []string{
		"count BIGINT",
		"sum   DOUBLE PRECISION",

		"bucket_counts   JSONB",
		"explicit_bounds JSONB",

		"exemplars JSONB",
		"flags     INTEGER",

		"min DOUBLE PRECISION",
		"max DOUBLE PRECISION",

		"aggregation_temporality INTEGER",
	}
)

type histogramMetric struct {
	resMetadata *ResourceMetadata

	histogram   pmetric.Histogram
	name        string
	description string
	unit        string
	metadata    pcommon.Map
}

type histogramMetricsGroup struct {
	MetricsType pmetric.MetricType

	DBType     DBType
	SchemaName string

	metrics []*histogramMetric
	count   int
}

func (g *histogramMetricsGroup) Add(resMetadata *ResourceMetadata, metric any, name, description, unit string, metadata pcommon.Map) error {
	histogram, ok := metric.(pmetric.Histogram)
	if !ok {
		return fmt.Errorf("metric param is not Histogram type")
	}

	// CAB - remove "." from metric name and replace with "_"
	newName := strings.Replace(name, ".", "_", -1)

	g.count += histogram.DataPoints().Len()
	g.metrics = append(g.metrics, &histogramMetric{
		resMetadata: resMetadata,
		histogram:   histogram,
		// name:        name,
		name:        newName,
		description: description,
		unit:        unit,
		metadata:    metadata,
	})

	return nil
}

func (g *histogramMetricsGroup) insert(ctx context.Context, client *sql.DB) error {
	logger.Debug("Inserting histogram metrics")

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

			statement, err := tx.PrepareContext(ctx, fmt.Sprintf(histogramMetricTableInsertSQL, g.SchemaName, m.name))
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

			for i := range m.histogram.DataPoints().Len() {
				dp := m.histogram.DataPoints().At(i)

				if dp.Timestamp().AsTime().IsZero() {
					errs = errors.Join(errs, fmt.Errorf("data points with the 0 value for TimeUnixNano SHOULD be rejected by consumers"))
					continue
				}

				//var a pcommon.Map
				a := dp.Attributes()
				attrs, err := json.Marshal(a.AsRaw())
				if err != nil {
					return err
				}

				bucketCounts, err := json.Marshal(dp.BucketCounts().AsRaw())
				if err != nil {
					errs = errors.Join(errs, err)
					continue
				}

				explicitBounds, err := json.Marshal(dp.ExplicitBounds().AsRaw())
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
					bucketCounts,
					explicitBounds,
					dp.Exemplars(),
					uint32(dp.Flags()),
					dp.Min(),
					dp.Max(),
					int32(m.histogram.AggregationTemporality()),
				)
			}

			return nil
		})
		errs = errors.Join(errs, err)
	}
	if errs != nil {
		return fmt.Errorf("insert histogram metrics failed: %w", errs)
	}

	return nil
}

func (g *histogramMetricsGroup) createTable(ctx context.Context, client *sql.DB, metricName string) error {
	metricTableColumns := slices.Concat(getBaseMetricTableColumns(g.DBType), histogramMetricTableColumns)

	return createMetricTable(ctx, client, g.SchemaName, metricName, metricTableColumns, g.DBType)
}

func (g *histogramMetricsGroup) getMetricsNames() []string {
	result := []string{}

	for _, m := range g.metrics {
		result = append(result, m.name)
	}

	return result
}
