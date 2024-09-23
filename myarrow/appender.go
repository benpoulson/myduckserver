package myarrow

import (
	"fmt"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/shopspring/decimal"
)

type ArrowAppender struct {
	schema  sql.Schema
	builder *array.RecordBuilder
}

func NewArrowAppender(schema sql.Schema) (ArrowAppender, error) {
	pool := memory.NewGoAllocator()
	arrowSchema, err := ToArrowSchema(schema)
	if err != nil {
		return ArrowAppender{}, err
	}
	return ArrowAppender{schema, array.NewRecordBuilder(pool, arrowSchema)}, nil
}

func (a ArrowAppender) Release() {
	a.builder.Release()
}

func (a ArrowAppender) Build() arrow.Record {
	return a.builder.NewRecord()
}

func (a ArrowAppender) MySQLSchema() sql.Schema {
	return a.schema
}

func (a ArrowAppender) Field(i int) array.Builder {
	return a.builder.Field(i)
}

func (a ArrowAppender) Append(row sql.Row) error {
	for i, b := range a.builder.Fields() {
		v := row[i]
		if v == nil {
			b.AppendNull()
			continue
		}
		switch b.Type().ID() {
		case arrow.UINT8:
			b.(*array.Uint8Builder).Append(v.(uint8))
		case arrow.INT8:
			b.(*array.Int8Builder).Append(v.(int8))
		case arrow.UINT16:
			b.(*array.Uint16Builder).Append(v.(uint16))
		case arrow.INT16:
			b.(*array.Int16Builder).Append(v.(int16))
		case arrow.UINT32:
			b.(*array.Uint32Builder).Append(v.(uint32))
		case arrow.INT32:
			b.(*array.Int32Builder).Append(v.(int32))
		case arrow.UINT64:
			b.(*array.Uint64Builder).Append(v.(uint64))
		case arrow.INT64:
			b.(*array.Int64Builder).Append(v.(int64))
		case arrow.FLOAT32:
			b.(*array.Float32Builder).Append(v.(float32))
		case arrow.FLOAT64:
			b.(*array.Float64Builder).Append(v.(float64))
		case arrow.STRING:
			b.(*array.StringBuilder).Append(v.(string))
		case arrow.BINARY:
			b.(*array.BinaryBuilder).Append(v.([]byte))
		case arrow.DECIMAL:
			dv := v.(decimal.Decimal)
			sv := dv.String()
			b.AppendValueFromString(sv)
		case arrow.TIMESTAMP:
			tv := v.(time.Time)
			at, err := arrow.TimestampFromTime(tv, b.Type().(*arrow.TimestampType).Unit)
			if err != nil {
				return err
			}
			b.(*array.TimestampBuilder).Append(at)
		case arrow.DATE32:
			tv := v.(time.Time)
			b.(*array.Date32Builder).Append(arrow.Date32FromTime(tv))
		case arrow.DURATION:
			sv := v.(string)
			duration, err := types.Time.ConvertToTimeDuration(sv)
			if err != nil {
				return err
			}
			b.(*array.DurationBuilder).Append(arrow.Duration(duration.Microseconds()))
		default:
			b.AppendValueFromString(fmt.Sprint(v))
		}
	}
	return nil
}
