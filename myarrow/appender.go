package myarrow

import (
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/shopspring/decimal"
)

type ArrowAppender struct {
	mysqlSchema sql.Schema
	builder     *array.RecordBuilder
}

func NewArrowAppender(mysqlSchema sql.Schema) *ArrowAppender {
	pool := memory.NewGoAllocator()
	arrowSchema := ToArrowSchema(mysqlSchema)
	return &ArrowAppender{
		mysqlSchema: mysqlSchema,
		builder:     array.NewRecordBuilder(pool, arrowSchema),
	}
}

func (a *ArrowAppender) Release() {
	a.builder.Release()
}

func (a *ArrowAppender) Append(row sql.Row) {
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
		case arrow.DECIMAL:
			dv := v.(decimal.Decimal)
			switch db := b.(type) {
			case *array.Decimal128Builder:
				db.Append(dv)
			case *array.Decimal256Builder:
				db.Append(dv)
			default:
				panic("Unknown decimal builder")
			}
		case arrow.TIMESTAMP:
			tv := v.(time.Time)
			b.(*array.TimestampBuilder).Append(tv)
		case arrow.DATE:
			dv := v.(time.Time)
			b.(*array.DateBuilder).Append(dv)
		}
	}
}
