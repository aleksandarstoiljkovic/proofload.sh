package kafkadriver

// params.go holds small, nil-tolerant helpers for reading typed values out of a
// driver.Config.Params map (decoded from YAML, so numbers may arrive as int,
// int64, or float64). They are pure and shared by config resolution.

// paramString reads a string-valued param, tolerating a nil map.
func paramString(params map[string]any, key string) string {
	if params == nil {
		return ""
	}
	if s, ok := params[key].(string); ok {
		return s
	}
	return ""
}

// paramInt reads an integer-valued param, falling back to def when absent or not
// a recognised numeric type. YAML parsers may decode numbers as any of the
// numeric kinds below.
func paramInt(params map[string]any, key string, def int) int {
	if params == nil {
		return def
	}
	switch n := params[key].(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		return def
	}
}

// paramBool reads a bool-valued param, falling back to def when absent or not a
// bool.
func paramBool(params map[string]any, key string, def bool) bool {
	if params == nil {
		return def
	}
	if b, ok := params[key].(bool); ok {
		return b
	}
	return def
}

// firstNonEmpty returns the first non-empty argument, or "".
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
