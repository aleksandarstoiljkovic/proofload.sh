package pgdriver

import (
	"fmt"
	"os"
	"strings"
)

// dsnOptions carries the connection parameters that are not part of an endpoint
// host:port pair. They are resolved from driver.Config.Params and the
// environment, then combined with each endpoint to form a pgx DSN.
type dsnOptions struct {
	User     string
	Password string
	DBName   string
	SSLMode  string
}

// defaults applied when neither params nor environment supply a value.
const (
	defaultUser    = "postgres"
	defaultDBName  = "proofload"
	defaultSSLMode = "disable"
	defaultPGPort  = "5432"
)

// buildDSN combines one endpoint with the resolved options into a pgx
// keyword/value DSN. An endpoint that is already a full DSN (URL form with
// "://" or keyword form containing "=") is passed through unchanged so callers
// can supply a complete connection string. A bare "host" or "host:port" is
// expanded with the resolved user/password/dbname/sslmode.
//
// buildDSN is pure (no environment access) to keep it unit testable; callers
// resolve dsnOptions separately via resolveOptions.
func buildDSN(endpoint string, o dsnOptions) string {
	endpoint = strings.TrimSpace(endpoint)
	if strings.Contains(endpoint, "://") || strings.Contains(endpoint, "=") {
		return endpoint
	}

	host, port := splitHostPort(endpoint)
	parts := []string{
		kv("host", host),
		kv("port", port),
		kv("user", o.User),
		kv("dbname", o.DBName),
		kv("sslmode", o.SSLMode),
	}
	if o.Password != "" {
		parts = append(parts, kv("password", o.Password))
	}
	return strings.Join(parts, " ")
}

// splitHostPort splits a "host:port" endpoint, defaulting the port to 5432 when
// absent. It splits on the last colon so IPv6 literals in brackets survive.
func splitHostPort(endpoint string) (host, port string) {
	if endpoint == "" {
		return "localhost", defaultPGPort
	}
	if i := strings.LastIndex(endpoint, ":"); i >= 0 && !strings.HasSuffix(endpoint, "]") {
		return endpoint[:i], endpoint[i+1:]
	}
	return endpoint, defaultPGPort
}

// kv renders one DSN keyword/value pair, quoting empty or space-bearing values.
func kv(key, val string) string {
	if val == "" || strings.ContainsAny(val, " '\\") {
		return fmt.Sprintf("%s='%s'", key, escapeDSN(val))
	}
	return key + "=" + val
}

// escapeDSN escapes backslashes and single quotes for a quoted DSN value.
func escapeDSN(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	return strings.ReplaceAll(v, `'`, `\'`)
}

// resolveOptions derives dsnOptions from config params and the environment.
// Precedence for each field: Params, then environment, then a built-in default.
// The password is only ever taken from the environment (PROOFLOAD_PG_PASSWORD,
// then PGPASSWORD) so it never has to appear in checked-in config.
func resolveOptions(params map[string]any) dsnOptions {
	return dsnOptions{
		User:     firstNonEmpty(paramString(params, "user"), os.Getenv("PGUSER"), defaultUser),
		Password: firstNonEmpty(os.Getenv("PROOFLOAD_PG_PASSWORD"), os.Getenv("PGPASSWORD")),
		DBName:   firstNonEmpty(paramString(params, "dbname"), paramString(params, "database"), os.Getenv("PGDATABASE"), defaultDBName),
		SSLMode:  firstNonEmpty(paramString(params, "sslmode"), os.Getenv("PGSSLMODE"), defaultSSLMode),
	}
}

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

// firstNonEmpty returns the first non-empty argument, or "".
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
