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
	summaryMetricTableInsertSQL = `
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
		quantile_values, 
		flags
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
	`
)

var (
	summaryMetricTableColumns = []string{
		"count BIGINT",
		"sum   DOUBLE PRECISION",

		"quantile_values JSONB",
		"flags           INTEGER",
	}
)

type summaryMetric struct {
	resMetadata *ResourceMetadata

	summary     pmetric.Summary
	name        string
	description string
	unit        string
	metadata    pcommon.Map
}

type summaryMetricsGroup struct {
	MetricsType pmetric.MetricType

	DBType     DBType
	SchemaName string

	metrics []*summaryMetric
	count   int
}

func (g *summaryMetricsGroup) Add(resMetadata *ResourceMetadata, metric any, name, description, unit string, metadata pcommon.Map) error {
	summary, ok := metric.(pmetric.Summary)
	if !ok {
		return fmt.Errorf("metric param is not Summary type")
	}

	// CAB - remove "." from metric name and replace with "_"
	newName := strings.Replace(name, ".", "_", -1)

	g.count += summary.DataPoints().Len()
	g.metrics = append(g.metrics, &summaryMetric{
		resMetadata: resMetadata,
		summary:     summary,
		// name:        name,
		name:        newName,
		description: description,
		unit:        unit,
		metadata:    metadata,
	})

	return nil
}

func (g *summaryMetricsGroup) insert(ctx context.Context, client *sql.DB) error {
	logger.Debug("Inserting summary metrics")

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

			statement, err := tx.PrepareContext(ctx, fmt.Sprintf(summaryMetricTableInsertSQL, g.SchemaName, m.name))
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

			for i := range m.summary.DataPoints().Len() {
				dp := m.summary.DataPoints().At(i)

				if dp.Timestamp().AsTime().IsZero() {
					errs = errors.Join(errs, fmt.Errorf("data points with the 0 value for TimeUnixNano SHOULD be rejected by consumers"))
					continue
				}

				var a pcommon.Map = dp.Attributes()
				attrs, err := json.Marshal(a.AsRaw())
				if err != nil {
					return err
				}

				quantileValues, err := json.Marshal(convertValueAtQuantileSliceToMap(dp.QuantileValues()))
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
					quantileValues,
					uint32(dp.Flags()),
				)
			}

			return nil
		})
		errs = errors.Join(errs, err)
	}
	if errs != nil {
		return fmt.Errorf("insert summary metrics failed: %w", errs)
	}

	return nil
}

func (g *summaryMetricsGroup) createTable(ctx context.Context, client *sql.DB, metricName string) error {
	metricTableColumns := slices.Concat(getBaseMetricTableColumns(g.DBType), summaryMetricTableColumns)

	return createMetricTable(ctx, client, g.SchemaName, metricName, metricTableColumns, g.DBType)
}

func (g *summaryMetricsGroup) getMetricsNames() []string {
	result := []string{}

	for _, m := range g.metrics {
		result = append(result, m.name)
	}

	return result
}

func convertValueAtQuantileSliceToMap(slice pmetric.SummaryDataPointValueAtQuantileSlice) map[string][]float64 {
	var (
		quantiles []float64
		values    []float64
	)

	for i := range slice.Len() {
		value := slice.At(i)
		quantiles = append(quantiles, value.Quantile())
		values = append(values, value.Value())
	}

	return map[string][]float64{
		"quantiles": quantiles,
		"values":    values,
	}
}
