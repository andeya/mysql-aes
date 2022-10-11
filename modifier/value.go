package modifier

import (
	"database/sql/driver"
	"strconv"
	"time"
	"unsafe"
)

func convertValue(val any) (string, bool, error) {
	// Use the default parameter converter
	arg, err := driver.DefaultParameterConverter.ConvertValue(val)
	if err != nil {
		return "", false, err
	}
	switch v := arg.(type) {
	case int64:
		return strconv.FormatInt(v, 10), true, nil
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64), true, nil
	case bool:
		// no need
		return "", false, err
	case time.Time:
		// Not supported, neither necessary nor complex to implement
		return "", false, nil
	case []byte:
		return *(*string)(unsafe.Pointer(&v)), true, err
	case string:
		return v, true, nil
	default:
		return "", false, driver.ErrSkip
	}
}
