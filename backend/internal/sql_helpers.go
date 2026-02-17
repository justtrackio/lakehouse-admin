package internal

import "strings"

func quoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func quoteLiteral(literal string) string {
	return `'` + strings.ReplaceAll(literal, `'`, `''`) + `'`
}

func qualifiedTableName(catalog, schema, table string) string {
	return quoteIdent(catalog) + "." + quoteIdent(schema) + "." + quoteIdent(table)
}
