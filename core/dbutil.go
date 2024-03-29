package core

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"strconv"

	"gitee.com/runner.mei/dm" // 达梦
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

var (
	_ driver.Valuer = &Payload{}
	_ sql.Scanner   = &Payload{}
	_ driver.Valuer = &NullTime{}
	_ sql.Scanner   = &NullTime{}
)

type DBRunner interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

type TxDBRunner interface {
	DBRunner

	Commit() error
	Rollback() error
}

type txKeyType struct{}

func (*txKeyType) String() string {
	return "kinglink-tx-key"
}

var txKey = &txKeyType{}

func WithDbConnection(ctx context.Context, tx DBRunner) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, txKey, tx)
}

func WithTx(ctx context.Context, tx TxDBRunner) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, txKey, tx)
}

func DbConnectionFromContext(ctx context.Context) DBRunner {
	if ctx == nil {
		return nil
	}
	v := ctx.Value(txKey)
	if v == nil {
		return nil
	}
	dbRnner, ok := v.(DBRunner)
	if ok {
		return dbRnner
	}
	return nil
}

func TxFromContext(ctx context.Context) TxDBRunner {
	if ctx == nil {
		return nil
	}
	v := ctx.Value(txKey)
	if v == nil {
		return nil
	}
	dbRnner, ok := v.(TxDBRunner)
	if ok {
		return dbRnner
	}
	return nil
}

// NullTime represents an time that may be null.
// NullTime implements the Scanner interface so
// it can be used as a scan destination, similar to NullTime.
type NullTime struct {
	Time  time.Time
	Valid bool // Valid is true if Int64 is not NULL
}

// Scan implements the Scanner interface.
func (n *NullTime) Scan(value interface{}) error {
	if value == nil {
		n.Time, n.Valid = time.Time{}, false
		return nil
	}
	// fmt.Println("wwwwwwwwwwwww", value)
	n.Time, n.Valid = value.(time.Time)
	if !n.Valid {
		if s, ok := value.(string); ok {
			var e error
			for _, layout := range []string{"2006-01-02 15:04:05.000000000", "2006-01-02 15:04:05.000000", "2006-01-02 15:04:05.000", "2006-01-02 15:04:05", "2006-01-02"} {
				if n.Time, e = time.ParseInLocation(layout, s, time.UTC); nil == e {
					n.Valid = true
					break
				}
			}
		}
	}
	return nil
}

// Value implements the driver Valuer interface.
func (n NullTime) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Time, nil
}

// JSON 代表一个数据库中一个 json
type Payload struct {
	bs     []byte
	values map[string]interface{}
}

func (payload *Payload) IsEmpty() bool {
	if len(payload.values) > 0 {
		return false
	}
	if len(payload.bs) == 0 {
		return true
	}

	return bytes.Equal(payload.bs, []byte("{}"))
}

func (payload *Payload) SetFields(values map[string]interface{}) {
	payload.bs = nil
	payload.values = values
}

func (payload *Payload) SetBytes(data []byte) {
	payload.bs = data
	payload.values = nil
}

// Fields 将字节数组转成一个 Fields 对象
func (payload *Payload) Fields() (map[string]interface{}, error) {
	if payload.values != nil {
		return payload.values, nil
	}
	if len(payload.bs) == 0 {
		return nil, nil
	}
	err := json.Unmarshal(payload.bs, &payload.values)
	return payload.values, err
}

// MustFields 将字节数组转成一个 Fields 对象，出错时会 Panic
func (payload *Payload) MustFields() map[string]interface{} {
	values, err := payload.Fields()
	if err != nil {
		panic(err)
	}
	return values
}

// String 将字节数组转成一个 JSON 对象
func (payload *Payload) String() string {
	bs, err := payload.MarshalText()
	if err != nil {
		return "*** marshal fail: " + err.Error() + " ***"
	}
	return string(bs)
}

func (payload *Payload) MarshalText() ([]byte, error) {
	if len(payload.bs) > 0 {
		return payload.bs, nil
	}
	if len(payload.values) == 0 {
		payload.bs = []byte("{}")
		return payload.bs, nil
	}

	bs, err := json.Marshal(payload.values)
	if err != nil {
		return nil, err
	}
	payload.bs = bs
	return payload.bs, nil
}

// MarshalJSON returns *m as the JSON encoding of m.
func (payload *Payload) MarshalJSON() ([]byte, error) {
	return payload.MarshalText()
}

// UnmarshalJSON sets *m to a copy of data.
func (payload *Payload) UnmarshalJSON(data []byte) error {
	if payload == nil {
		return errors.New("models.JSON: UnmarshalJSON on nil pointer")
	}
	if len(payload.bs) == 0 {
		payload.bs = make([]byte, len(data))
		copy(payload.bs, data)
	} else {
		payload.bs = append(payload.bs[0:0], data...)
	}
	payload.values = nil
	return nil
}

// Scan implements the Scanner interface.
func (payload *Payload) Scan(value interface{}) error {
	if value == nil {
		payload.bs = nil
		payload.values = nil
		return nil
	}

	switch s := value.(type) {
	case string:
		payload.bs = []byte(s)
		payload.values = nil
		return nil
	case []byte:
		payload.bs = make([]byte, len(s))
		copy(payload.bs, s)
		payload.values = nil
		return nil
	case *dm.DmClob:
		l, err := s.GetLength()
		if err != nil {
			return err
		}
		if l == 0 {
			payload.bs = nil
			payload.values = nil
			return nil
		}
		n, err := s.ReadString(1, int(l))
		if err != nil {
			return err
		}
		payload.bs = []byte(n)
		return nil
	}
	return fmt.Errorf("unsupported Scan, storing driver.Value type %T into type Payload", value)
}

// Value implements the driver Valuer interface.
func (payload *Payload) Value() (driver.Value, error) {
	bs, err := payload.MarshalJSON()
	return string(bs), err
}

func MakePayload(bs []byte,
	values map[string]interface{}) Payload {
	return Payload{bs: bs, values: values}
}

func I18nError(drv string, err error) error {
	if err == nil {
		return err
	}

	if "oci8" == drv {
		decoder := simplifiedchinese.GB18030.NewDecoder()
		msg, _, e := transform.String(decoder, err.Error())
		if e != nil {
			return errors.New(msg)
		}
	}
	return err
}

func I18nString(drv string, txt string) string {
	if "oci8" == drv {
		decoder := simplifiedchinese.GB18030.NewDecoder()
		msg, _, err := transform.String(decoder, txt)
		if nil == err {
			return msg
		}
	}
	return txt
}

func IsNumericParams(drv string) bool {
	switch drv {
	case "postgres":
		return true
	case "oracle", "odbc_with_oracle", "oci8", "dm":
		return false
	default:
		return false
	}
}

func  hasLastInsertId(drv string) bool {
	return drv == "dm" || drv == "oracle"
}

func  Placeholder(drv string,  index int) string {
	if !IsNumericParams( drv ) {
		return "?"
	}

	switch index {
	case 1:
		return "$1"
	case 2:
		return "$2"
	case 3:
		return "$3"
	case 4:
		return "$4"
	}

	return "$" + strconv.Itoa(index)
}


type NullString struct {
	String string
	Valid  bool // Valid is true if Int64 is not NULL
}

// Scan implements the Scanner interface.
func (n *NullString) Scan(value interface{}) error {
	if value == nil {
		n.String, n.Valid = "", false
		return nil
	}
	switch s := value.(type) {
	case []byte:
		if s != nil {
			n.Valid = true
			n.String = string(s)
		} else {
			n.String, n.Valid = "", false
		}
		return nil
	case string:
		n.Valid = true
		n.String = s
		return nil
	case *[]byte:
		if s != nil && *s != nil {
			n.Valid = true
			n.String = string(*s)
		} else {
			n.String, n.Valid = "", false
		}
		return nil
	case *string:
		if s == nil {
			n.String, n.Valid = "", false
		} else {
			n.Valid = true
			n.String = *s
		}
		return nil
	case *dm.DmClob:
		l, err := s.GetLength()
		if err != nil {
			return err
		}
		if l == 0 {
			n.Valid = true
			return nil
		}
		n.String, err = s.ReadString(1, int(l))
		if err != nil {
			return err
		}
		n.Valid = true
		return nil
	}
	return fmt.Errorf("unsupported Scan, storing driver.Value type %T into type NullString", value)
}

// Value implements the driver Valuer interface.
func (n NullString) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.String, nil
}