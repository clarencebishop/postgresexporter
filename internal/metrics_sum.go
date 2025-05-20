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
	sumMetricTableInsertSQL = `
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
		value, 
		exemplars, 
		flags, 
		aggregation_temporality, 
		is_monotonic
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
	`
)

var (
	sumMetricTableColumns = []string{
		"value DOUBLE PRECISION",

		"exemplars JSONB",
		"flags     INTEGER",

		"aggregation_temporality INTEGER",
		"is_monotonic            BOOLEAN",
	}
)

type sumMetric struct {
	resMetadata *ResourceMetadata

	sum         pmetric.Sum
	name        string
	description string
	unit        string
	metadata    pcommon.Map
}

type sumMetricsGroup struct {
	MetricsType pmetric.MetricType

	DBType     DBType
	SchemaName string

	metrics []*sumMetric
	count   int
}

func (g *sumMetricsGroup) Add(resMetadata *ResourceMetadata, metric any, name, description, unit string, metadata pcommon.Map) error {
	sum, ok := metric.(pmetric.Sum)
	if !ok {
		return fmt.Errorf("metric param is not Sum type")
	}

	// CAB - remove "." from metric name and replace with "_"
	newName := strings.Replace(name, ".", "_", -1)

	g.count += sum.DataPoints().Len()
	g.metrics = append(g.metrics, &sumMetric{
		resMetadata: resMetadata,
		sum:         sum,
		// name:        name,
		name:        newName,
		description: description,
		unit:        unit,
		metadata:    metadata,
	})

	return nil
}

func (g *sumMetricsGroup) insert(ctx context.Context, client *sql.DB) error {
	logger.Debug("Inserting sum metrics")

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

			statement, err := tx.PrepareContext(ctx, fmt.Sprintf(sumMetricTableInsertSQL, g.SchemaName, m.name))
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

			for i := range m.sum.DataPoints().Len() {
				dp := m.sum.DataPoints().At(i)

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

				tx.Stmt(statement).ExecContext(ctx,
					m.resMetadata.ResURL,
					resAttrs,
					m.resMetadata.InstrScope.Name(),
					m.resMetadata.InstrScope.Version(),
					scopeAttrs,
					m.resMetadata.InstrScope.DroppedAttributesCount(),
					m.resMetadata.ScopeUrl,
					getServiceName(m.resMetadata.ResAttrs),
					m.name,
					int32(g.MetricsType),
					m.description,
					m.unit,
					dp.StartTimestamp().AsTime(),
					dp.Timestamp().AsTime(),
					attrs,
					metadata,
					getValue(dp.IntValue(), dp.DoubleValue(), dp.ValueType()),
					dp.Exemplars(),
					uint32(dp.Flags()),
					int32(m.sum.AggregationTemporality()),
					m.sum.IsMonotonic(),
				)
			}

			return nil
		})
		errs = errors.Join(errs, err)
	}
	if errs != nil {
		return fmt.Errorf("insert sum metrics failed: %w", errs)
	}

	return nil
}

func (g *sumMetricsGroup) createTable(ctx context.Context, client *sql.DB, metricName string) error {
	metricTableColumns := slices.Concat(getBaseMetricTableColumns(g.DBType), sumMetricTableColumns)

	return createMetricTable(ctx, client, g.SchemaName, metricName, metricTableColumns, g.DBType)
}

func (g *sumMetricsGroup) getMetricsNames() []string {
	result := []string{}

	for _, m := range g.metrics {
		result = append(result, m.name)
	}

	return result
}
